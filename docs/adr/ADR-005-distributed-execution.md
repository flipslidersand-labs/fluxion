# ADR-005: 分散実行 — リモートワーカーへのジョブディスパッチ

**Status**: Accepted  
**Date**: 2026-08-08  
**Issue**: #34

---

## Context

`FluxionHost` は単一プロセス内でのみ Wasm ジョブを実行できる。
CPU/メモリ制約が大きいワークフローを複数マシンで並列処理するために、
オーケストレーター/ワーカー分離モデルを導入する。

## Decision

### アーキテクチャ

```
fluxion run (orchestrator)
  │
  ├── HTTP POST /run → worker-1:7777 (Wasm実行)
  ├── HTTP POST /run → worker-2:7777 (Wasm実行)
  └── ローカル実行 (workerなし)
```

### ワーカー HTTP API

**POST /run** — Wasm コンポーネントをリモートで実行

```jsonc
// Request
{
  "component": "<base64: .wasm bytes>",
  "input":     "<base64: input bytes>",
  "permissions": { "filesystem": {...}, "network": {...}, "limits": {...} },
  "env": { "KEY": "VALUE" }
}

// Response 200
{
  "output":       "<base64: output bytes>",
  "compile_ms":   10,
  "instantiate_ms": 5,
  "execute_ms":   100
}

// Response 500
{ "error": "reason string" }
```

**GET /health** — 死活確認

```json
{ "status": "ok" }
```

### YAML 拡張

```yaml
name: distributed-pipeline
workers:
  - http://worker1:7777
  - http://worker2:7777
jobs:
  fetch:
    component: fetch.wasm
    worker: worker1 # 省略時: workers リストからラウンドロビン
```

### ディスパッチ戦略

1. ジョブに `worker:` フィールドあり → 指定ワーカーに送信
2. `workers:` リストあり → ラウンドロビンで自動割り当て
3. どちらもなし → ローカル実行（既存動作）

### 失敗時の挙動

- HTTP タイムアウト / 接続エラー → `JobStatus::Failed` (フェイルオーバーは Phase 2)
- ワーカー側の Wasm 実行エラー → レスポンス 500 の `error` を reason に格納

## Consequences

### Pros

- 既存ローカル実行パスに影響なし（下位互換）
- ワーカーはステートレス（.wasm バイト列を毎回受信）— スケールアウト容易
- HTTP/JSON で実装が単純

### Cons

- .wasm バイトを毎回転送するためネットワーク帯域を消費（将来: コンテンツアドレス可能キャッシュで解決）
- ワーカー間でコンパイルキャッシュが共有されない（各ワーカーが L1/L2 キャッシュを持つ）

## Implementation

- `crates/fluxion-worker/` — axum ベースのワーカーサーバー
- `fluxion worker serve --port <PORT>` — CLI コマンド
- `fluxion_host::remote` — reqwest ベースのリモートディスパッチクライアント
- `scheduler::launch()` — worker URL の有無で local/remote を切り替え

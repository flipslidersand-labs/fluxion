# Implementation Guide — Fluxion

## Phase 1: ランタイム基礎（目安: 1〜2週）

### ゴール

`fluxion component run hello.wasm` が動く。

### タスク

1. Cargo workspace 構築（`fluxion-cli` / `fluxion-core` / `fluxion-host`）
2. `wit/task.wit` の WIT インターフェース定義
3. `wasmtime` + `wasmtime-wasi` を `fluxion-host` に組み込む
4. `cargo-component` で `components/hello` を Wasm コンポーネントとしてビルド
5. ホストから `process()` を呼び出し、出力を表示する CLI コマンド実装

### 完成確認コマンド

```bash
cargo component build --manifest-path components/hello/Cargo.toml
cargo run -- component run components/hello/target/wasm32-wasip2/debug/hello.wasm
# → Hello from Fluxion!
```

### 難所と対策

- WIT バインディング生成の理解 → `cargo component new` のサンプルを読む
- `wasmtime` の Component Model API は Wasm Module API と異なる → 公式 examples を参照

---

## Phase 2: ワークフロー処理系（目安: 1〜2週）

### ゴール

3ジョブの YAML ワークフローが DAG 順に実行される。

### タスク

1. YAML パーサー（`serde_yaml`）で `Workflow` / `JobDefinition` を読み込む
2. 内部 DAG の構築（隣接リスト）
3. 循環依存検出（Kahn's algorithm）
4. `tokio` による非同期スケジューラー実装
5. 状態遷移（Pending → Ready → Running → Succeeded / Failed）
6. 独立ジョブの並列実行（依存がないジョブは同時起動）

### 完成確認コマンド

```bash
cargo run -- run examples/three-stage.yaml
# [00:00] stage-a  RUNNING
# [00:01] stage-a  SUCCESS  1.0s
# [00:01] stage-b  RUNNING
# [00:02] stage-b  SUCCESS  1.0s
# [00:02] stage-c  RUNNING
# [00:03] stage-c  SUCCESS  1.0s
```

### 依存制約

Phase 1 完了後に着手。

---

## Phase 3: Sandbox（目安: 1週）

### ゴール

ファイル権限外へのアクセスが `Denied` になる。

### タスク

1. `wasmtime-wasi` の `WasiCtxBuilder` でディレクトリを限定マウント
2. `PermissionSet` から `WasiCtx` を構築するロジック実装
3. ネットワーク接続先フィルタリング（`WasiCtxBuilder::socket_addr_check`）
4. メモリ上限の設定（`Store::limiter`）
5. タイムアウト（`tokio::time::timeout`）

### 完成確認コマンド

```bash
# 権限なしコンポーネントがネットワークにアクセスしようとしたとき
Component attempted network access:
  destination: example.org:443
Denied: component 'fetch' has no network permission
```

### 依存制約

Phase 2 完了後に着手。

---

## Phase 4: 永続化と再実行（目安: 1週）

### ゴール

失敗したジョブから `--from` で再実行できる。

### タスク

1. SQLite スキーマ作成（`rusqlite` + マイグレーション）
2. `WorkflowRun` / `JobRun` / `Artifact` の永続化
3. `fluxion status <run-id>` コマンド実装
4. `fluxion logs <run-id>` コマンド実装
5. `fluxion retry <run-id> --from <job-id>` の再実行ロジック
6. 成功済みジョブのアーティファクトキャッシュ（コンテンツハッシュで判定）

### 完成確認コマンド

```bash
fluxion retry <run-id> --from validate
# [00:00] validate  RUNNING  (fetch・normalize はキャッシュヒット)
# [00:01] validate  SUCCESS
```

### 依存制約

Phase 3 完了後に着手。

---

## Phase 5: 観測（目安: 1週）

### ゴール

実行トレースが OpenTelemetry 形式で出力される。

### タスク

1. `opentelemetry` + `tracing-opentelemetry` の統合
2. ジョブごとの Span 作成（開始・終了・エラー）
3. メトリクス収集（実行時間・リトライ回数・メモリ使用量）
4. Flame Graph 出力（`pprof` または `inferno`）
5. ボトルネック表示 CLI

### 依存制約

Phase 4 完了後に着手。

---

## Phase 6: AI / MCP 連携（目安: 2週）

### ゴール

MCP サーバーとして Fluxion を公開し、AI エージェントからワークフローを実行できる。

### タスク

1. MCP サーバーの HTTP エンドポイント実装
2. `run_workflow` / `get_status` / `retry_job` ツール定義
3. 長時間ジョブの非同期レスポンス（polling or webhook）
4. AI 生成コードを Wasm コンポーネントとしてコンパイル・実行するパイプライン

### 依存制約

Phase 5 完了後に着手。

---

## 実装順序の根拠

1. **Phase 1 を最初に**: WIT + Wasmtime の学習コストが最大。ここを乗り越えれば残りは Rust の一般的な実装
2. **Phase 3（Sandbox）を Phase 2 の直後に**: セキュリティは後付けにしない。設計段階で組み込む
3. **Phase 4（永続化）の前に Phase 3**: 権限制御が動いてから状態を保存する方が整合性が取れる
4. **Phase 6（MCP）を最後に**: 処理系として完成してから利用者（AI）を追加する

---

## Python コンポーネント実装ガイド

### 概要

`componentize-py` を使うと Python スクリプトを Wasm Component Model コンポーネントとしてビルドできます。  
Fluxion の WIT インターフェース（`wit/task.wit`）を Python から実装することで、Rust 以外の言語で Fluxion ジョブを記述できます。

### 環境要件

| ツール | バージョン |
|--------|-----------|
| Python | 3.10+ |
| componentize-py | 0.13+ |

```bash
pip install componentize-py
```

### WIT インターフェース仕様

`wit/task.wit` で定義されているインターフェース:

```wit
interface processor {
    record task-input {
        content:  list<u8>,
        metadata: list<tuple<string, string>>,
    }
    record task-output {
        content:  list<u8>,
        metadata: list<tuple<string, string>>,
    }
    process: func(input: task-input) -> result<task-output, string>;
}
```

Python 側では `task_component.imports` から `TaskInput` / `TaskOutput` をインポートして `process()` 関数を実装します。

### ビルドフロー

```
hello.py + wit/task.wit
        ↓ componentize-py componentize
     hello.wasm (Wasm Component)
        ↓ fluxion component run / fluxion run
       実行結果
```

`fluxion build python` コマンドがこのフローをラップしています:

```bash
fluxion build python hello.py -o hello.wasm
```

### エラー処理

`process()` が例外を送出した場合、Fluxion はジョブを `failed` として記録します。  
`result<task-output, string>` の `err` バリアントを返したい場合は Python 側で例外を raise してください。

### 制限事項

- `componentize-py` が対応している Python 標準ライブラリのみ利用可能（`socket`・`subprocess` 等は不可）
- pandas / numpy 等の C 拡張ライブラリは現時点では非対応
- `task-output.content` はバイト列（`list<u8>`）のため、文字列は `encode("utf-8")` が必要

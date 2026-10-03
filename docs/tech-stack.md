# Tech Stack — Fluxion

## 言語・バージョン

| 役割                                  | 言語                     | バージョン                 |
| ------------------------------------- | ------------------------ | -------------------------- |
| ホストランタイム・CLI・スケジューラー | Rust                     | 1.85+ (edition 2024 クレートに必要: host/cli/core。fluxion-worker は edition 2021 / wasm32-wasip2 対応) |
| Wasm コンポーネント (初期)            | Rust → wasm32-wasip2     | 同上                       |
| Wasm コンポーネント (将来)            | JavaScript / Python / Go | TBD                        |

## 主要クレート

### ランタイムホスト

| クレート               | バージョン | 用途                          | 選定理由                                      |
| ---------------------- | ---------- | ----------------------------- | --------------------------------------------- |
| `wasmtime`             | 28.x       | Wasm Component Model の実行   | Component Model の参照実装。WASI 0.2 対応済み |
| `wasmtime-wasi`        | 同上       | WASI 実装                     | wasmtime とセット                             |
| `tokio`                | 1.x        | 非同期ランタイム              | Rust の非同期エコシステムのデファクト         |
| `clap`                 | 4.x        | CLI パーサー                  | derive マクロで型安全に書ける                 |
| `serde` + `serde_yaml` | 1.x        | YAML ワークフロー定義のパース | シリアライズ/デシリアライズの標準             |
| `rusqlite`             | 0.31+      | SQLite 永続化                 | 組み込み DB として最もシンプル                |
| `anyhow`               | 1.x        | エラー処理                    | プロトタイピング段階でのエラー伝搬を簡潔に    |
| `tracing`              | 0.1        | 構造化ログ                    | OpenTelemetry との統合が容易                  |
| `opentelemetry`        | 0.26+      | Trace / Metrics               | Phase 5 で導入予定                            |

### Wasm コンポーネント側

| ツール            | 用途                                          |
| ----------------- | --------------------------------------------- |
| `cargo-component` | Rust → Wasm Component のビルドツール          |
| `wasm-tools`      | WIT パース・コンポーネント検査・変換          |
| `wit-bindgen`     | WIT からホスト/ゲスト双方のバインディング生成 |

## ビルドツール・実行環境

```
rustup target add wasm32-wasip2   # コンポーネントビルドターゲット
cargo install wasm-tools          # v1.252.0
cargo install cargo-component     # v0.21.1
```

## 開発ツール

| ツール          | 用途                         |
| --------------- | ---------------------------- |
| `rustfmt`       | コードフォーマット           |
| `clippy`        | Lint                         |
| `cargo test`    | ユニットテスト               |
| `cargo nextest` | 並列テスト実行（オプション） |

## 依存関係の構成図

```
fluxion (workspace)
├── crates/
│   ├── fluxion-cli       → clap, anyhow
│   ├── fluxion-core      → serde, serde_yaml, rusqlite, tokio, tracing
│   └── fluxion-host      → wasmtime, wasmtime-wasi
└── components/
    └── hello/            → cargo-component (wasm32-wasip2)
```

## WASI 戦略

- **Phase 1〜4**: WASI 0.2（安定版、ツールチェーン対応済み）
- **Phase 5 以降**: WASI 0.3 の非同期 API を段階的に追加
  - 2026-06-11 に WASI 0.3 が批准されたが wasmtime 側の実装は進行中
  - ランタイム抽象化を挟んでおくことで移行コストを下げる

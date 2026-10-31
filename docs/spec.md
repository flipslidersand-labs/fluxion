# Spec — Fluxion

## プロジェクトの目的

WebAssembly Component Model を実行単位とする、安全なジョブ実行エンジン。
各処理を Wasm コンポーネントとして隔離し、ファイル・ネットワーク・メモリの権限を細粒度で制御しながら、DAG で定義されたワークフローを実行する。

## 解決する問題

| 問題                                               | Fluxion での解決策                                    |
| -------------------------------------------------- | ----------------------------------------------------- |
| Python 環境・ライブラリ依存の衝突                  | Wasm コンポーネントとして隔離、ホスト環境に依存しない |
| 処理がファイル・ネットワークに自由にアクセスできる | Capability ベースの権限制御（許可した能力のみ使用可） |
| ジョブの再実行・失敗復旧が難しい                   | 失敗地点からの `--from <job>` 再実行                  |
| AI 生成コードを直接実行するのが危険                | Wasm サンドボックス内で実行、権限違反は即座に拒否     |

## MVP の境界線

### やること (Phase 1〜4)

- 単一 Wasm コンポーネントの実行 (`fluxion component run`)
- YAML ワークフロー定義のパース・バリデーション
- DAG 生成（循環依存検出を含む）
- 非同期スケジューラーによる並列実行
- ジョブ状態管理（Pending → Running → Succeeded / Failed）
- タイムアウト・リトライ・キャンセル
- SQLite による実行履歴の永続化
- 失敗地点からの再実行 (`fluxion retry`)
- 基本的な Capability 制御（ファイル読み書き・ネットワーク接続先の制限）

### やらないこと (MVP 外)

- 分散実行・リモートワーカー
- Web UI / ダッシュボード
- Python / JavaScript コンポーネントのビルドサポート（初期は Rust のみ）
- スケジュール実行（cron）
- MCP 連携（Phase 6 で追加）

## ユーザーが使うコマンド

```bash
# 単一コンポーネントを実行
fluxion component run uppercase.wasm --input input.txt

# ワークフローを実行
fluxion run workflow.yaml

# 実行状態を確認
fluxion status <run-id>

# ログを表示
fluxion logs <run-id>

# 失敗地点から再実行
fluxion retry <run-id> --from validate

# コンポーネントの情報を表示（インターフェース・権限要件）
fluxion inspect component.wasm
```

## 実行時の出力イメージ

```
$ fluxion run examples/vehicle-pipeline.yaml

[12:01:02] fetch      RUNNING
[12:01:04] fetch      SUCCESS  2.1s
[12:01:04] normalize  RUNNING
[12:01:05] normalize  SUCCESS  0.8s
[12:01:05] validate   RUNNING
[12:01:06] validate   FAILED

Reason:
  invalid registration_year at row 184

Retry:
  fluxion retry <run-id> --from validate
```

権限違反の出力例：

```
Component attempted network access:
  destination: example.org:443

Denied:
  component 'csv-normalizer' has no network permission
```

## 成功条件（Phase 別）

| Phase   | 完成条件                                                  |
| ------- | --------------------------------------------------------- |
| Phase 1 | `fluxion component run hello.wasm` が実行され、出力が返る |
| Phase 2 | 3ジョブの YAML ワークフローが DAG 順に実行される          |
| Phase 3 | ファイル権限外へのアクセスが Denied になる                |
| Phase 4 | 失敗したジョブから `--from` で再実行できる                |
| Phase 5 | 実行トレースが OpenTelemetry 形式で出力される             |

## ワーカーの信頼モデル（`fluxion worker serve`）

- 既定は `--bind 127.0.0.1`（ループバックのみ）。外部公開は `--bind <addr>` で明示する。
- TLS 無し（`--tls-cert/--tls-key/--ca-cert` 省略）では `/run`・`/jobs`・`/components` に認証が無い。非ループバックで TLS 無しの場合は起動時に警告する。外部公開時は mTLS を使うこと。
- リクエストの `permissions` は信頼しない前提で、ワーカー側ポリシーで上限を設けられる。超過したリクエストは 403。
  - `--allow-fs-root <dir>`（複数可）: `filesystem.read/write` を配下に限定（`..`・相対パス・シンボリックリンク脱出は拒否）
  - `--max-memory-mb <N>`: `limits.memory_mb` の上限
  - `--deny-network`: `network.allow` を指定したリクエストを拒否
- ポリシー未指定の場合はリクエストの権限がそのまま適用される（起動時に警告）。

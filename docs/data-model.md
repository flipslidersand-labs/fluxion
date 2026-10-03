# Data Model — Fluxion

> 正本は実装です。ワークフロー定義は `crates/fluxion-core/src/workflow.rs`、
> ジョブ状態は `crates/fluxion-core/src/state.rs`、永続化スキーマは
> `crates/fluxion-core/src/store.rs`、WIT は `wit/task.wit` を参照してください。
> このドキュメントはそれらの現行実装を要約したものです。

## ワークフロー定義（YAML から読み込む静的定義）

```rust
struct Workflow {
    name: String,
    jobs: IndexMap<String, JobDefinition>, // キー = ジョブ ID（YAML のキー名）
    workers: Vec<WorkerConfig>,            // 省略時は空
    max_parallel: Option<usize>,           // None = 無制限
    workers_srv: Option<String>,           // DNS SRV 名によるワーカー自動検出
}

struct JobDefinition {
    component: String,                // .wasm ファイルのパス
    depends_on: Vec<String>,
    input: Option<String>,
    permissions: PermissionSet,
    worker: Option<String>,           // 特定ワーカー URL にピン留め
    env: HashMap<String, String>,     // WASI 経由で注入する環境変数
    when: Option<String>,             // false のときジョブは SKIPPED
    foreach: Option<String>,          // JSONPath。<job_id>.0, <job_id>.1, … に展開
    input_from: Option<String>,       // foreach ジョブの出力を配列で受け取る（fan-in）
    max_parallel: Option<usize>,      // ジョブ単位の並列上限
    output_size_limit_mb: Option<u64>,// 省略時 64MB
    fail_fast: bool,                  // 既定 false
    component_sha256: Option<String>, // 実行前に .wasm を検証
    reduce: Option<ReduceMode>,       // input_from と併用
    executor: ExecutorKind,           // local（既定）| remote
    async_dispatch: bool,             // executor: remote のとき POST /jobs + ポーリング
    oci_ref: Option<String>,          // 指定時は OCI レジストリから取得（component は無視）
}

enum ExecutorKind { Local, Remote }              // YAML では snake_case
enum ReduceMode { Concat, JsonArray, JsonMerge, Custom(String) }
// YAML: `reduce: json_array` / `reduce: { custom: /path/to/reducer.wasm }`
```

再試行ポリシー（`RetryPolicy`）やジョブ単位の `timeout` フィールドは存在しません。
タイムアウトは `permissions.limits.timeout_secs` で指定します。

### ワーカー設定

```rust
// untagged enum。URL 文字列だけの形式と拡張形式の両方を受け付ける
enum WorkerConfig {
    Simple(String),                 // weight = 1、TLS なし
    Full {
        url: String,
        tls: Option<TlsConfig>,
        weight: u32,                // 既定 1
    },
}

struct TlsConfig {
    cert: PathBuf,                  // クライアント証明書（PEM）
    key: PathBuf,                   // クライアント秘密鍵（PEM）
    ca: PathBuf,                    // サーバー検証用 CA 証明書（PEM）
}
```

### パーミッション

```rust
struct PermissionSet {
    filesystem: FilesystemPermission,
    network: NetworkPermission,
    limits: ResourceLimits,         // memory_mb / timeout_secs は limits の配下
}

struct FilesystemPermission {
    read: Vec<PathBuf>,
    write: Vec<PathBuf>,
}

struct NetworkPermission {
    allow: Vec<String>,             // host:port の許可リスト。空 = 全拒否
}

struct ResourceLimits {
    memory_mb: u64,                 // 既定 256
    timeout_secs: u64,              // 既定 60
}
```

## 実行状態

実行時のジョブ状態（メモリ上）は `state.rs` の `JobStatus` です。

```rust
enum JobStatus {
    Pending,
    Ready,
    Running,
    Succeeded { elapsed: Duration },
    Failed { elapsed: Duration, reason: String },
    Cancelled,
    Skipped,  // when: が false、または依存ジョブが skipped
}
```

`Succeeded` / `Failed` / `Cancelled` / `Skipped` が終端状態です。
SQLite には `succeeded` / `failed` / `cancelled` / `running` / `skipped` / `pending` の
小文字ラベルで保存されます（`Ready` は `pending` として保存されます）。

実行（run）単位の `status` は `runs` テーブルの文字列で、`running`（開始時）、
`succeeded`、`failed`（`complete_run` で更新）が使われます。

## 永続化スキーマ（SQLite）

DB ファイルは `$HOME/.fluxion/runs.db` です（`RunStore::open`）。
テーブルは次の 4 つで、アーティファクトを保存するテーブルはありません。

```sql
CREATE TABLE IF NOT EXISTS schedules (
    id            TEXT PRIMARY KEY,
    workflow_path TEXT NOT NULL,
    cron_expr     TEXT NOT NULL,
    created_at    INTEGER NOT NULL,
    last_run_at   INTEGER,
    next_run_at   INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS runs (
    id            TEXT PRIMARY KEY,
    workflow_name TEXT NOT NULL,
    workflow_path TEXT NOT NULL,
    started_at    INTEGER NOT NULL,
    completed_at  INTEGER,
    status        TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS job_states (
    run_id    TEXT NOT NULL,
    job_id    TEXT NOT NULL,
    status    TEXT NOT NULL,
    elapsed_ms INTEGER,
    reason    TEXT,
    PRIMARY KEY (run_id, job_id)
);
CREATE TABLE IF NOT EXISTS workers (
    url            TEXT PRIMARY KEY,
    registered_at  INTEGER NOT NULL,
    last_health    TEXT
);
```

時刻カラムは Unix 秒の INTEGER です。`workers.last_health` は `healthy` / `unreachable`
（未チェックは NULL）です。`schedules` は `claim_schedule` が
`next_run_at` を条件にした UPDATE で楽観ロックを行い、重複実行を避けます。

## WIT インターフェース定義

```wit
package fluxion:task@0.1.0;

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

world task-component {
    export processor;
}
```

全コンポーネントはこの `task-component` world を実装する。
ホストは `process()` を呼び出し、`result<task-output, string>` で成否を受け取る。

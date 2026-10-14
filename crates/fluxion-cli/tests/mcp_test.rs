//! End-to-end tests for `fluxion mcp-serve` (Content-Length framed JSON-RPC over
//! stdio). The child runs with `HOME` pointing at a tempdir whose RunStore DB is
//! seeded directly, so no Wasm build is needed (#276).

use assert_cmd::Command;
use serde_json::{Value, json};

fn frame(v: &Value) -> Vec<u8> {
    let body = v.to_string();
    format!("Content-Length: {}\r\n\r\n{}", body.len(), body).into_bytes()
}

/// Split a stream of `Content-Length` frames into JSON values.
fn parse_frames(out: &[u8]) -> Vec<Value> {
    let mut msgs = Vec::new();
    let mut rest = out;
    while !rest.is_empty() {
        let sep = rest
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .expect("header terminator");
        let head = std::str::from_utf8(&rest[..sep]).unwrap();
        let len: usize = head
            .strip_prefix("Content-Length: ")
            .expect("Content-Length header")
            .trim()
            .parse()
            .unwrap();
        let body = &rest[sep + 4..sep + 4 + len];
        msgs.push(serde_json::from_slice(body).unwrap());
        rest = &rest[sep + 4 + len..];
    }
    msgs
}

fn run_mcp(home: &std::path::Path, requests: &[Value]) -> Vec<Value> {
    let input: Vec<u8> = requests.iter().flat_map(frame).collect();
    let out = Command::cargo_bin("fluxion")
        .unwrap()
        .env("HOME", home)
        .arg("mcp-serve")
        .write_stdin(input)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    parse_frames(&out)
}

fn call(id: u64, name: &str, args: Value) -> Value {
    json!({"jsonrpc":"2.0","id":id,"method":"tools/call",
           "params":{"name":name,"arguments":args}})
}

fn tool_text(resp: &Value) -> Value {
    let text = resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_else(|| panic!("no text content: {resp}"));
    serde_json::from_str(text).unwrap()
}

/// Create `$HOME/.fluxion/runs.db` with the RunStore schema and seed one run.
fn seed(home: &std::path::Path) {
    let dir = home.join(".fluxion");
    std::fs::create_dir_all(&dir).unwrap();
    let conn = rusqlite::Connection::open(dir.join("runs.db")).unwrap();
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS runs (
            id TEXT PRIMARY KEY, workflow_name TEXT NOT NULL, workflow_path TEXT NOT NULL,
            started_at INTEGER NOT NULL, completed_at INTEGER, status TEXT NOT NULL);
         CREATE TABLE IF NOT EXISTS job_states (
            run_id TEXT NOT NULL, job_id TEXT NOT NULL, status TEXT NOT NULL,
            elapsed_ms INTEGER, reason TEXT, PRIMARY KEY (run_id, job_id));
         INSERT INTO runs VALUES ('run-1','demo','/tmp/demo.yaml',1000,1010,'failed');
         INSERT INTO job_states VALUES ('run-1','a','succeeded',999,NULL);
         INSERT INTO job_states VALUES ('run-1','b','failed',2500,'boom');
         INSERT INTO job_states VALUES ('run-1','c','skipped',NULL,NULL);",
    )
    .unwrap();
}

#[test]
fn smoke_initialize_then_tools_list_and_notification_is_silent() {
    let home = tempfile::tempdir().unwrap();
    let resp = run_mcp(
        home.path(),
        &[
            json!({"jsonrpc":"2.0","id":1,"method":"initialize"}),
            json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
            json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}),
        ],
    );
    // The notification in the middle produces no response frame.
    assert_eq!(resp.len(), 2, "{resp:?}");
    assert_eq!(resp[0]["id"], 1);
    assert_eq!(resp[0]["result"]["serverInfo"]["name"], "fluxion-mcp");
    assert_eq!(resp[1]["id"], 2);
    assert_eq!(resp[1]["result"]["tools"].as_array().unwrap().len(), 5);
}

#[test]
fn runs_list_status_and_logs_over_seeded_store() {
    let home = tempfile::tempdir().unwrap();
    seed(home.path());
    let resp = run_mcp(
        home.path(),
        &[
            call(1, "runs_list", json!({})),
            call(2, "workflow_status", json!({"run_id":"run-1"})),
            call(3, "workflow_logs", json!({"run_id":"run-1"})),
            call(4, "workflow_status", json!({"run_id":"nope"})),
        ],
    );
    assert_eq!(resp.len(), 4);

    let runs = tool_text(&resp[0]);
    assert_eq!(runs.as_array().unwrap().len(), 1);
    assert_eq!(runs[0]["id"], "run-1");
    assert_eq!(runs[0]["workflow_name"], "demo");
    assert_eq!(runs[0]["status"], "failed");

    let status = tool_text(&resp[1]);
    assert_eq!(status["run"]["id"], "run-1");
    assert_eq!(status["run"]["elapsed_s"], 10.0);
    assert_eq!(status["run"]["workflow_path"], "/tmp/demo.yaml");
    let jobs = status["jobs"].as_array().unwrap();
    assert_eq!(jobs.len(), 3);
    let b = jobs.iter().find(|j| j["job_id"] == "b").unwrap();
    assert_eq!(b["status"], "failed");
    assert_eq!(b["elapsed_ms"], 2500);
    assert_eq!(b["reason"], "boom");

    // Timeline: each job emits RUNNING then its terminal event. `ts` advances
    // by elapsed_ms / 1000 (integer division, so 999ms advances 0s and a NULL
    // elapsed advances 0s). Job order follows get_run_jobs (ORDER BY in store).
    let logs = tool_text(&resp[2]);
    let events = logs.as_array().unwrap();
    assert_eq!(events.len(), 6);
    let ts_of = |job: &str, ev: &str| -> u64 {
        events
            .iter()
            .find(|e| e["job_id"] == job && e["event"] == ev)
            .unwrap_or_else(|| panic!("{job} {ev} in {logs}"))["ts"]
            .as_u64()
            .unwrap()
    };
    assert_eq!(ts_of("a", "RUNNING"), 1000);
    assert_eq!(ts_of("a", "SUCCEEDED"), 1000, "999ms rounds down to 0s");
    assert_eq!(ts_of("b", "RUNNING"), 1000);
    assert_eq!(ts_of("b", "FAILED"), 1002, "2500ms rounds down to 2s");
    assert_eq!(ts_of("c", "RUNNING"), 1002);
    assert_eq!(ts_of("c", "SKIPPED"), 1002, "NULL elapsed advances 0s");
    let failed = events.iter().find(|e| e["event"] == "FAILED").unwrap();
    assert_eq!(failed["reason"], "boom");
    assert_eq!(failed["elapsed_s"], 2.5);

    // Unknown run id → JSON-RPC error response, server keeps going.
    assert!(resp[3]["error"]["message"].is_string(), "{}", resp[3]);
}

#[test]
fn runs_list_respects_limit() {
    let home = tempfile::tempdir().unwrap();
    seed(home.path());
    let conn = rusqlite::Connection::open(home.path().join(".fluxion/runs.db")).unwrap();
    conn.execute(
        "INSERT INTO runs VALUES ('run-2','demo2','/tmp/d2.yaml',2000,NULL,'running')",
        [],
    )
    .unwrap();
    drop(conn);
    let resp = run_mcp(home.path(), &[call(1, "runs_list", json!({"limit": 1}))]);
    assert_eq!(tool_text(&resp[0]).as_array().unwrap().len(), 1);
}

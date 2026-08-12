//! Integration tests for the REST API + Web UI server (#102).
//!
//! Each test spins up the Axum app on a random OS-assigned port, makes HTTP
//! requests via reqwest, and asserts on status codes and body contents.
//! The DB is backed by a NamedTempFile so tests are fully isolated.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use fluxion_core::store::RunStore;
use fluxion_host::api::{ApiState, router};
use rusqlite::Connection;
use tempfile::NamedTempFile;

// ── test helpers ──────────────────────────────────────────────────────────────

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS schedules (
    id TEXT PRIMARY KEY, workflow_path TEXT NOT NULL, cron_expr TEXT NOT NULL,
    created_at INTEGER NOT NULL, last_run_at INTEGER, next_run_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY, workflow_name TEXT NOT NULL, workflow_path TEXT NOT NULL,
    started_at INTEGER NOT NULL, completed_at INTEGER, status TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS job_states (
    run_id TEXT NOT NULL, job_id TEXT NOT NULL, status TEXT NOT NULL,
    elapsed_ms INTEGER, reason TEXT, PRIMARY KEY (run_id, job_id)
);
CREATE TABLE IF NOT EXISTS workers (
    url TEXT PRIMARY KEY, registered_at INTEGER NOT NULL, last_health TEXT
);
";

fn open_tmp_store() -> (RunStore, NamedTempFile) {
    let f = NamedTempFile::new().expect("tempfile");
    let conn = Connection::open(f.path()).expect("open db");
    conn.execute_batch(SCHEMA).expect("schema");
    (RunStore::from_conn(conn), f)
}

/// Bind on 127.0.0.1:0 and return the assigned port + a shutdown handle.
async fn spawn_server(store: RunStore) -> (u16, tokio::task::JoinHandle<()>) {
    let state = ApiState {
        store: Arc::new(Mutex::new(store)),
    };
    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr: SocketAddr = listener.local_addr().expect("local addr");
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    (addr.port(), handle)
}

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .expect("client")
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn get_api_runs_returns_empty_json() {
    let (store, _f) = open_tmp_store();
    let (port, _srv) = spawn_server(store).await;
    let url = format!("http://127.0.0.1:{port}/api/runs");

    let resp = client().get(&url).send().await.expect("request");
    assert_eq!(resp.status(), 200);
    assert!(resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("application/json"));
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(body, serde_json::json!([]));
}

#[tokio::test]
async fn get_api_schedules_returns_200() {
    let (store, _f) = open_tmp_store();
    let (port, _srv) = spawn_server(store).await;
    let url = format!("http://127.0.0.1:{port}/api/schedules");

    let resp = client().get(&url).send().await.expect("request");
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn get_api_workers_returns_200() {
    let (store, _f) = open_tmp_store();
    let (port, _srv) = spawn_server(store).await;

    let resp = client()
        .get(format!("http://127.0.0.1:{port}/api/workers"))
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn get_root_returns_html() {
    let (store, _f) = open_tmp_store();
    let (port, _srv) = spawn_server(store).await;

    let resp = client()
        .get(format!("http://127.0.0.1:{port}/"))
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), 200);
    let ct = resp.headers()["content-type"].to_str().unwrap().to_string();
    assert!(ct.contains("text/html"), "expected text/html, got {ct}");
    let body = resp.text().await.expect("body");
    assert!(body.contains("Fluxion"), "expected 'Fluxion' in HTML body");
}

#[tokio::test]
async fn get_metrics_contains_fluxion_prefix() {
    let (store, _f) = open_tmp_store();
    let (port, _srv) = spawn_server(store).await;

    let resp = client()
        .get(format!("http://127.0.0.1:{port}/metrics"))
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    assert!(
        body.contains("fluxion_"),
        "metrics body should contain 'fluxion_' prefix, got:\n{body}"
    );
}

#[tokio::test]
async fn get_api_runs_id_jobs_returns_empty_for_known_run() {
    let (store, _f) = open_tmp_store();
    // Insert a run so the run_id is known; no job_states → empty list.
    store
        .create_run("run-test-1", "test-wf", std::path::Path::new("wf.yaml"))
        .expect("create run");

    let (port, _srv) = spawn_server(store).await;
    let resp = client()
        .get(format!("http://127.0.0.1:{port}/api/run/run-test-1/jobs"))
        .send()
        .await
        .expect("request");
    let status = resp.status();
    let body = resp.text().await.expect("body");
    assert_eq!(status.as_u16(), 200, "body: {body}");
    let parsed: serde_json::Value = serde_json::from_str(&body).expect("json");
    assert_eq!(parsed, serde_json::json!([]));
}


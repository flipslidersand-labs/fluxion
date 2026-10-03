//! Tests for `remote::run_remote_async` (POST /jobs + GET /jobs/:id polling) and
//! the async failover path in the scheduler (#277).
//!
//! A tiny axum mock worker is spun up per test; no Wasm build is required
//! (the "component" is a few dummy bytes that only get hashed and uploaded).

use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{get, head, post},
};
use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
use fluxion_core::workflow::{PermissionSet, ResourceLimits};
use fluxion_host::remote::{self, RemoteError};
use serde_json::{Value, json};

/// Scripted behaviour of the mock worker.
#[derive(Clone)]
struct Mock {
    /// Body returned by `POST /jobs` (status 200).
    submit_body: Value,
    /// Successive `GET /jobs/:id` responses. The last one repeats forever.
    polls: Vec<(StatusCode, Value)>,
    poll_count: Arc<AtomicUsize>,
    submit_count: Arc<AtomicUsize>,
}

impl Mock {
    fn new(polls: Vec<(StatusCode, Value)>) -> Self {
        Self {
            submit_body: json!({ "job_id": "job-1" }),
            polls,
            poll_count: Arc::new(AtomicUsize::new(0)),
            submit_count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

async fn spawn(mock: Mock) -> String {
    async fn submit(State(m): State<Mock>) -> Json<Value> {
        m.submit_count.fetch_add(1, Ordering::SeqCst);
        Json(m.submit_body.clone())
    }
    async fn poll(State(m): State<Mock>) -> (StatusCode, Json<Value>) {
        let n = m.poll_count.fetch_add(1, Ordering::SeqCst);
        let (code, body) = m.polls[n.min(m.polls.len() - 1)].clone();
        (code, Json(body))
    }
    let app = Router::new()
        .route("/health", get(|| async { StatusCode::OK }))
        .route("/components/:sha", head(|| async { StatusCode::OK }))
        .route("/jobs", post(submit))
        .route("/jobs/:id", get(poll))
        .with_state(mock);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

fn tmp_wasm() -> tempfile::NamedTempFile {
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(b"\0asm").unwrap();
    f
}

fn perms(timeout_secs: u64) -> PermissionSet {
    PermissionSet {
        limits: ResourceLimits {
            timeout_secs,
            ..ResourceLimits::default()
        },
        ..PermissionSet::default()
    }
}

async fn call(url: &str, timeout_secs: u64) -> Result<Vec<u8>, RemoteError> {
    let wasm = tmp_wasm();
    // Outer guard so a regression hangs for at most 90s instead of forever.
    tokio::time::timeout(
        Duration::from_secs(90),
        remote::run_remote_async(
            url,
            wasm.path(),
            b"in".to_vec(),
            &perms(timeout_secs),
            &HashMap::new(),
            None,
        ),
    )
    .await
    .expect("run_remote_async hung")
    .map(|(out, _)| out)
}

fn running() -> (StatusCode, Value) {
    (StatusCode::OK, json!({ "status": "running" }))
}

fn succeeded(output: &[u8]) -> (StatusCode, Value) {
    (
        StatusCode::OK,
        json!({ "status": "succeeded", "output": B64.encode(output) }),
    )
}

#[tokio::test]
async fn running_twice_then_succeeded_returns_output() {
    let mock = Mock::new(vec![running(), running(), succeeded(b"done")]);
    let polls = mock.poll_count.clone();
    let url = spawn(mock).await;
    let out = call(&url, 5).await.expect("should succeed");
    assert_eq!(out, b"done");
    assert_eq!(polls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn failed_status_is_execution_error_with_worker_message() {
    let mock = Mock::new(vec![(
        StatusCode::OK,
        json!({ "status": "failed", "error": "guest trapped: boom" }),
    )]);
    let url = spawn(mock).await;
    let err = call(&url, 5).await.unwrap_err();
    assert!(matches!(err, RemoteError::Execution(_)), "{err}");
    assert!(!err.is_failover());
    let msg = err.to_string();
    assert!(msg.contains("guest trapped: boom"), "{msg}");
    assert!(msg.contains("job-1"), "{msg}");
}

#[tokio::test]
async fn failed_without_error_field_reports_unknown_error() {
    let mock = Mock::new(vec![(StatusCode::OK, json!({ "status": "failed" }))]);
    let url = spawn(mock).await;
    let err = call(&url, 5).await.unwrap_err();
    assert!(err.to_string().contains("unknown error"), "{err}");
}

#[tokio::test]
async fn submit_response_without_job_id_is_execution_error() {
    let mut mock = Mock::new(vec![succeeded(b"unused")]);
    mock.submit_body = json!({});
    let polls = mock.poll_count.clone();
    let url = spawn(mock).await;
    let err = call(&url, 5).await.unwrap_err();
    assert!(matches!(err, RemoteError::Execution(_)), "{err}");
    assert!(err.to_string().contains("missing job_id"), "{err}");
    assert_eq!(polls.load(Ordering::SeqCst), 0, "must not poll without id");
}

#[tokio::test]
async fn poll_404_is_execution_error() {
    // e.g. the worker restarted and lost its job table.
    let mock = Mock::new(vec![(
        StatusCode::NOT_FOUND,
        json!({ "error": "no such job" }),
    )]);
    let url = spawn(mock).await;
    let err = call(&url, 5).await.unwrap_err();
    assert!(matches!(err, RemoteError::Execution(_)), "{err}");
    let msg = err.to_string();
    assert!(msg.contains("404"), "{msg}");
}

#[tokio::test]
async fn succeeded_without_output_yields_empty_output() {
    // Pins current behaviour: a missing `output` is treated as empty success.
    let mock = Mock::new(vec![(StatusCode::OK, json!({ "status": "succeeded" }))]);
    let url = spawn(mock).await;
    let out = call(&url, 5).await.expect("empty output is success today");
    assert!(out.is_empty());
}

#[tokio::test]
async fn succeeded_with_invalid_base64_is_execution_error() {
    let mock = Mock::new(vec![(
        StatusCode::OK,
        json!({ "status": "succeeded", "output": "!!not base64!!" }),
    )]);
    let url = spawn(mock).await;
    let err = call(&url, 5).await.unwrap_err();
    assert!(matches!(err, RemoteError::Execution(_)), "{err}");
}

#[tokio::test]
async fn unknown_or_missing_status_keeps_polling_until_terminal() {
    // Pins current behaviour: a missing/unknown `status` is treated as "running".
    let mock = Mock::new(vec![
        (StatusCode::OK, json!({})),
        (StatusCode::OK, json!({ "status": "weird" })),
        succeeded(b"late"),
    ]);
    let polls = mock.poll_count.clone();
    let url = spawn(mock).await;
    let out = call(&url, 5).await.expect("should eventually succeed");
    assert_eq!(out, b"late");
    assert_eq!(polls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn unreachable_worker_is_failover_error() {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", l.local_addr().unwrap());
    drop(l);
    let err = call(&url, 2).await.unwrap_err();
    assert!(
        err.is_failover(),
        "connection refused must allow failover: {err}"
    );
}

/// The polling deadline is `timeout_secs + 30` (wall clock), so this test
/// takes ~30s. Tokio's paused clock can't be used: auto-advance would also
/// fire reqwest's own timeout while real socket I/O is pending.
#[tokio::test]
async fn never_finishing_job_times_out() {
    let mock = Mock::new(vec![running()]);
    let url = spawn(mock).await;
    let err = call(&url, 0).await.unwrap_err();
    assert!(matches!(err, RemoteError::Execution(_)), "{err}");
    assert!(err.to_string().contains("timed out"), "{err}");
}

/// Scheduler-level: `async_dispatch: true` fails over from a worker whose
/// connection drops to a healthy one (covers `run_with_failover_async`).
#[tokio::test]
async fn scheduler_async_dispatch_fails_over_to_second_worker() {
    let home = tempfile::tempdir().unwrap();
    // SAFETY: no other test in this file reads or writes HOME.
    unsafe { std::env::set_var("HOME", home.path()) };

    // Passes the scheduler's GET /health pre-check, then drops every other
    // connection without replying, so POST /jobs fails with a transport error
    // (RemoteError::Unreachable) and the scheduler must fail over.
    let flaky_posts = Arc::new(AtomicUsize::new(0));
    let flaky = {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let u = format!("http://{}", l.local_addr().unwrap());
        let posts = flaky_posts.clone();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = l.accept().await {
                let posts = posts.clone();
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    let n = sock.read(&mut buf).await.unwrap_or(0);
                    let head = String::from_utf8_lossy(&buf[..n]);
                    if head.starts_with("GET /health") {
                        let _ = sock
                            .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                            .await;
                    } else if head.starts_with("POST /jobs") {
                        posts.fetch_add(1, Ordering::SeqCst);
                    }
                    // drop => connection closed without a response
                });
            }
        });
        u
    };
    let mock = Mock::new(vec![running(), succeeded(b"from-live")]);
    let submits = mock.submit_count.clone();
    let live = spawn(mock).await;

    let wasm = tmp_wasm();
    let wf: fluxion_core::workflow::Workflow = serde_json::from_value(json!({
        "name": "async-failover",
        "workers": [{ "url": flaky }, { "url": live }],
        "jobs": { "j": {
            "component": wasm.path().to_str().unwrap(),
            "executor": "remote",
            "async_dispatch": true,
            "permissions": { "limits": { "timeout_secs": 5 } }
        }}
    }))
    .unwrap();

    let host = Arc::new(fluxion_host::FluxionHost::new().unwrap());
    let wf_path = home.path().join("wf.yaml");
    let result = tokio::time::timeout(
        Duration::from_secs(60),
        fluxion_host::scheduler::run(&wf, &wf_path, host),
    )
    .await
    .expect("scheduler run hung")
    .expect("run should complete");
    assert!(result.success, "{:?}", result.jobs);
    assert_eq!(
        flaky_posts.load(Ordering::SeqCst),
        1,
        "flaky worker tried first"
    );
    assert_eq!(submits.load(Ordering::SeqCst), 1, "live worker got the job");
}

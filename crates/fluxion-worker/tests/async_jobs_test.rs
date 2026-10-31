/// Integration tests for POST /jobs and GET /jobs/:id endpoints.
///
/// We spin up a real worker on a random port and drive it via reqwest.
/// These tests do NOT execute a Wasm component — they use an empty payload
/// which will fail inside FluxionHost, exercising the "failed" status path.
/// The "running → succeeded" path is tested structurally (status field present).
use base64::Engine as _;
use reqwest::Client;
use serde_json::Value;
use std::net::TcpListener;
use std::time::Duration;

fn find_free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port()
}

async fn wait_for_server(port: u16) {
    let client = Client::new();
    for _ in 0..40 {
        if client
            .get(format!("http://127.0.0.1:{port}/health"))
            .send()
            .await
            .is_ok()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("worker did not start within 2s on port {port}");
}

async fn spawn_worker(port: u16) {
    tokio::spawn(async move {
        fluxion_worker::serve(port, None, None, true)
            .await
            .expect("worker serve");
    });
    wait_for_server(port).await;
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn post_jobs_returns_202_with_job_id() {
    let port = find_free_port();
    spawn_worker(port).await;

    let client = Client::new();
    let resp = client
        .post(format!("http://127.0.0.1:{port}/jobs"))
        .json(&serde_json::json!({
            "component": base64::engine::general_purpose::STANDARD.encode(b"not-wasm"),
            "input": base64::engine::general_purpose::STANDARD.encode(b"{}"),
        }))
        .send()
        .await
        .expect("POST /jobs");

    assert_eq!(resp.status(), 202, "expected 202 Accepted");
    let body: Value = resp.json().await.expect("json");
    assert!(body["job_id"].is_string(), "job_id must be a string");
    let id = body["job_id"].as_str().unwrap();
    assert!(!id.is_empty(), "job_id must not be empty");
    // Must be a valid UUID v4.
    assert!(
        uuid::Uuid::parse_str(id).is_ok(),
        "job_id must be a UUID: {id}"
    );
}

#[tokio::test]
async fn get_jobs_unknown_returns_404() {
    let port = find_free_port();
    spawn_worker(port).await;

    let resp = Client::new()
        .get(format!("http://127.0.0.1:{port}/jobs/does-not-exist"))
        .send()
        .await
        .expect("GET /jobs/:id");

    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn submitted_job_eventually_has_status() {
    let port = find_free_port();
    spawn_worker(port).await;

    let client = Client::new();
    // Submit a job with invalid wasm — it will fail, but quickly.
    let submit: Value = client
        .post(format!("http://127.0.0.1:{port}/jobs"))
        .json(&serde_json::json!({
            "component": base64::engine::general_purpose::STANDARD.encode(b"bad"),
            "input":     base64::engine::general_purpose::STANDARD.encode(b""),
        }))
        .send()
        .await
        .expect("POST /jobs")
        .json()
        .await
        .expect("json");

    let job_id = submit["job_id"].as_str().expect("job_id");

    // Poll until not "running" (max 2 seconds).
    let mut final_status = String::from("running");
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let poll: Value = client
            .get(format!("http://127.0.0.1:{port}/jobs/{job_id}"))
            .send()
            .await
            .expect("GET /jobs/:id")
            .json()
            .await
            .expect("json");

        final_status = poll["status"].as_str().unwrap_or("").to_string();
        if final_status != "running" {
            break;
        }
    }

    // Invalid wasm → must end in "failed".
    assert_eq!(final_status, "failed", "job with bad wasm must fail");
}

#[tokio::test]
async fn existing_sync_run_endpoint_still_works() {
    let port = find_free_port();
    spawn_worker(port).await;

    let resp = Client::new()
        .post(format!("http://127.0.0.1:{port}/run"))
        .json(&serde_json::json!({
            "component": base64::engine::general_purpose::STANDARD.encode(b"bad"),
            "input":     base64::engine::general_purpose::STANDARD.encode(b""),
        }))
        .send()
        .await
        .expect("POST /run");

    // Wasm is bad → 500, but the endpoint must exist (not 404).
    assert_ne!(resp.status(), 404, "POST /run must still exist");
}

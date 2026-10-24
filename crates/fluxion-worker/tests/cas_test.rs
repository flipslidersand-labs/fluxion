//! Integration tests for the CAS endpoints (HEAD/PUT /components/:sha256)
//! and CAS-only POST /run.
//!
//! HOME is pointed at a tempdir so the worker's `~/.fluxion/cas` is isolated.
//! Everything lives in one test fn so the process-wide HOME change cannot race.
use reqwest::Client;
use sha2::{Digest, Sha256};
use std::net::TcpListener;
use std::time::Duration;

fn find_free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port()
}

#[tokio::test]
async fn cas_put_head_and_run_lookup() {
    let home = tempfile::tempdir().expect("tempdir");
    std::env::set_var("HOME", home.path());

    let port = find_free_port();
    tokio::spawn(async move {
        fluxion_worker::serve(port, None, None, false)
            .await
            .expect("worker serve");
    });
    let client = Client::new();
    let base = format!("http://127.0.0.1:{port}");
    let mut up = false;
    for _ in 0..40 {
        if client.get(format!("{base}/health")).send().await.is_ok() {
            up = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(up, "worker did not start");

    let body: &[u8] = b"not-really-wasm";
    let sha: String = Sha256::digest(body)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();

    // (2) unknown sha256 -> HEAD 404
    let r = client
        .head(format!("{base}/components/{sha}"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);

    // (3) body does not match sha256 -> PUT 400
    let wrong = "0".repeat(64);
    let r = client
        .put(format!("{base}/components/{wrong}"))
        .body(body.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);

    // (4) sha256-only /run for a component not in CAS -> 404
    let r = client
        .post(format!("{base}/run"))
        .json(&serde_json::json!({ "component_sha256": sha, "input": "" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);

    // (1) correct sha256 -> PUT 201, then HEAD 200
    let r = client
        .put(format!("{base}/components/{sha}"))
        .body(body.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 201);
    let r = client
        .head(format!("{base}/components/{sha}"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
}

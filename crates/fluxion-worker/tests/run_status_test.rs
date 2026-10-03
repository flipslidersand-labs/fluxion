//! POST /run status codes and response shape must stay stable.
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use reqwest::Client;
use std::net::TcpListener;
use std::time::Duration;

async fn spawn_worker() -> u16 {
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    tokio::spawn(async move {
        fluxion_worker::serve(port, None, None, false)
            .await
            .expect("worker serve");
    });
    let client = Client::new();
    for _ in 0..40 {
        if client
            .get(format!("http://127.0.0.1:{port}/health"))
            .send()
            .await
            .is_ok()
        {
            return port;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("worker did not start");
}

async fn post_run(port: u16, body: serde_json::Value) -> (u16, serde_json::Value) {
    let r = Client::new()
        .post(format!("http://127.0.0.1:{port}/run"))
        .json(&body)
        .send()
        .await
        .unwrap();
    (r.status().as_u16(), r.json().await.unwrap())
}

#[tokio::test]
async fn run_status_codes_are_preserved() {
    let port = spawn_worker().await;
    let ok_input = B64.encode(b"{}");

    // sha256 not in CAS and no inline bytes -> 404
    let (st, body) = post_run(
        port,
        serde_json::json!({ "component_sha256": "0".repeat(64), "input": ok_input }),
    )
    .await;
    assert_eq!(st, 404);
    assert!(body["error"].as_str().unwrap().contains("not in CAS"));

    // invalid component base64 -> 400
    let (st, body) = post_run(
        port,
        serde_json::json!({ "component": "!!!", "input": ok_input }),
    )
    .await;
    assert_eq!(st, 400);
    assert!(body["error"]
        .as_str()
        .unwrap()
        .contains("invalid component base64"));

    // invalid input base64 -> 400
    let (st, body) = post_run(
        port,
        serde_json::json!({ "component": B64.encode(b"x"), "input": "!!!" }),
    )
    .await;
    assert_eq!(st, 400);
    assert!(body["error"]
        .as_str()
        .unwrap()
        .contains("invalid input base64"));

    // not a wasm component -> 500 with an error body
    let (st, body) = post_run(
        port,
        serde_json::json!({ "component": B64.encode(b"not-wasm"), "input": ok_input }),
    )
    .await;
    assert_eq!(st, 500);
    assert!(body["error"].is_string());

    // failed runs must not leak the active-jobs counter
    let h: serde_json::Value = Client::new()
        .get(format!("http://127.0.0.1:{port}/health"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(h["active_jobs"], 0);
}

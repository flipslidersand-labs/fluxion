//! Malformed `component_sha256` / `/components/{sha256}` values must be
//! rejected with 400 before touching the filesystem.
use reqwest::Client;
use std::net::TcpListener;
use std::time::Duration;

fn find_free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port()
}

async fn spawn_worker() -> u16 {
    let port = find_free_port();
    tokio::spawn(async move {
        fluxion_worker::serve(port, None, None, true)
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

#[tokio::test]
async fn run_and_jobs_reject_invalid_component_sha256() {
    let port = spawn_worker().await;
    let client = Client::new();
    let bad = [
        "/tmp/x".to_string(),
        "../../etc/passwd".to_string(),
        "a".repeat(63),
        "a".repeat(65),
        format!("{}z", "a".repeat(63)),
    ];
    for sha in &bad {
        let resp = client
            .post(format!("http://127.0.0.1:{port}/run"))
            .json(&serde_json::json!({ "component_sha256": sha, "input": "" }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 400, "/run accepted {sha:?}");

        // /jobs accepts the job then fails it; the failure must be the validation error.
        let resp = client
            .post(format!("http://127.0.0.1:{port}/jobs"))
            .json(&serde_json::json!({ "component_sha256": sha, "input": "" }))
            .send()
            .await
            .unwrap();
        let id = resp.json::<serde_json::Value>().await.unwrap()["job_id"]
            .as_str()
            .unwrap()
            .to_string();
        let mut last = serde_json::Value::Null;
        for _ in 0..40 {
            last = client
                .get(format!("http://127.0.0.1:{port}/jobs/{id}"))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            if last["status"] != "running" && last["status"] != "pending" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(last["status"], "failed", "job for {sha:?}: {last}");
        assert!(last["error"]
            .as_str()
            .unwrap()
            .contains("invalid component_sha256"));
    }
}

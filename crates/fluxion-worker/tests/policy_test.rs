/// Worker permission policy over HTTP (#275): out-of-policy requests get 403
/// on both POST /run and POST /jobs; in-policy requests are not rejected.
use base64::Engine as _;
use fluxion_worker::{WorkerConfig, WorkerPolicy};
use reqwest::Client;
use std::net::TcpListener;
use std::time::Duration;

fn find_free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn spawn(port: u16, policy: WorkerPolicy) {
    tokio::spawn(async move {
        let config = WorkerConfig {
            policy,
            ..Default::default()
        };
        fluxion_worker::serve_with(port, None, None, true, config)
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
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("worker did not start on {port}");
}

fn body(perms: serde_json::Value) -> serde_json::Value {
    let b64 = base64::engine::general_purpose::STANDARD;
    serde_json::json!({
        "component": b64.encode(b"not-wasm"),
        "input": b64.encode(b"{}"),
        "permissions": perms,
    })
}

#[tokio::test]
async fn out_of_policy_requests_get_403() {
    let root = tempfile::tempdir().unwrap();
    let port = find_free_port();
    spawn(
        port,
        WorkerPolicy {
            allow_fs_roots: vec![root.path().to_path_buf()],
            max_memory_mb: Some(128),
            deny_network: true,
        },
    )
    .await;
    let client = Client::new();
    let denied = [
        serde_json::json!({"filesystem": {"write": ["/"]}}),
        serde_json::json!({"filesystem": {"read": [root.path().join("../x")]}}),
        serde_json::json!({"limits": {"memory_mb": 4096}}),
        serde_json::json!({"network": {"allow": ["127.0.0.1:1"]}}),
    ];
    for perms in denied {
        for path in ["run", "jobs"] {
            let resp = client
                .post(format!("http://127.0.0.1:{port}/{path}"))
                .json(&body(perms.clone()))
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 403, "/{path} perms={perms}");
        }
    }

    // In-policy request is not rejected by the policy (the bogus wasm fails later, not with 403).
    let ok = serde_json::json!({
        "filesystem": {"write": [root.path().join("work")]},
        "limits": {"memory_mb": 64}
    });
    let resp = client
        .post(format!("http://127.0.0.1:{port}/jobs"))
        .json(&body(ok))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);
}

#[test]
fn default_config_binds_loopback() {
    assert!(WorkerConfig::default().bind.is_loopback());
}

use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
use fluxion_core::workflow::{PermissionSet, TlsConfig};
use std::collections::HashMap;
use std::fmt;
use std::path::Path;
use std::time::Duration;

use crate::JobMetrics;

/// Failure modes when dispatching to a remote worker.
///
/// `Unreachable` means the worker could not be contacted (connection refused,
/// DNS failure, request timeout) — the job should be retried on another worker.
/// `Execution` means the worker was reached but the job itself failed (a 5xx
/// carrying the guest's error, or a malformed response) — retrying elsewhere
/// would just reproduce the same component bug, so we do NOT fail over.
#[derive(Debug)]
pub enum RemoteError {
    Unreachable(anyhow::Error),
    Execution(anyhow::Error),
}

impl RemoteError {
    /// Whether this failure should trigger failover to another worker.
    pub fn is_failover(&self) -> bool {
        matches!(self, RemoteError::Unreachable(_))
    }
}

impl fmt::Display for RemoteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RemoteError::Unreachable(e) => write!(f, "unreachable: {e}"),
            RemoteError::Execution(e) => write!(f, "execution failed: {e}"),
        }
    }
}

impl std::error::Error for RemoteError {}

/// Dispatch a Wasm job to a remote worker via HTTP POST /run.
///
/// When `tls` is `Some`, the client presents a mutual-TLS identity and
/// verifies the server against the supplied CA.
pub async fn run_remote(
    worker_url: &str,
    wasm_path: impl AsRef<Path>,
    input: Vec<u8>,
    perms: &PermissionSet,
    env: &HashMap<String, String>,
    tls: Option<&TlsConfig>,
) -> Result<(Vec<u8>, JobMetrics), RemoteError> {
    // Reading the local .wasm file is an orchestrator-side error, not a worker
    // fault — surface it as Execution so we don't pointlessly try every worker.
    let wasm_bytes =
        std::fs::read(wasm_path.as_ref()).map_err(|e| RemoteError::Execution(e.into()))?;

    let body = serde_json::json!({
        "component": B64.encode(&wasm_bytes),
        "input": B64.encode(&input),
        "permissions": perms,
        "env": env,
    });

    let mut builder = reqwest::Client::builder()
        .timeout(Duration::from_secs(perms.limits.timeout_secs + 10));

    if let Some(tls) = tls {
        let cert_pem =
            std::fs::read(&tls.cert).map_err(|e| RemoteError::Execution(e.into()))?;
        let key_pem =
            std::fs::read(&tls.key).map_err(|e| RemoteError::Execution(e.into()))?;
        // reqwest::Identity::from_pem expects the cert followed by the key in one PEM blob.
        let identity_pem = [cert_pem, key_pem].concat();
        let identity = reqwest::Identity::from_pem(&identity_pem)
            .map_err(|e| RemoteError::Execution(e.into()))?;
        let ca_pem =
            std::fs::read(&tls.ca).map_err(|e| RemoteError::Execution(e.into()))?;
        let ca_cert = reqwest::Certificate::from_pem(&ca_pem)
            .map_err(|e| RemoteError::Execution(e.into()))?;
        builder = builder
            .identity(identity)
            .add_root_certificate(ca_cert)
            .use_rustls_tls();
    }

    let client = builder
        .build()
        .map_err(|e| RemoteError::Execution(e.into()))?;

    let url = format!("{}/run", worker_url.trim_end_matches('/'));

    // A send() error is a transport failure (refused/timeout/DNS) → failover.
    let resp =
        client.post(&url).json(&body).send().await.map_err(|e| {
            RemoteError::Unreachable(anyhow::anyhow!("worker {}: {}", worker_url, e))
        })?;

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        // The worker answered — this is the guest's failure, not the worker's.
        return Err(RemoteError::Execution(anyhow::anyhow!(
            "worker {} returned {}: {}",
            worker_url,
            status,
            text
        )));
    }

    let json: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| RemoteError::Execution(e.into()))?;

    let output = B64
        .decode(json["output"].as_str().ok_or_else(|| {
            RemoteError::Execution(anyhow::anyhow!("missing output field in worker response"))
        })?)
        .map_err(|e| RemoteError::Execution(e.into()))?;

    let metrics = JobMetrics {
        compile: Duration::from_millis(json["compile_ms"].as_u64().unwrap_or(0)),
        instantiate: Duration::from_millis(json["instantiate_ms"].as_u64().unwrap_or(0)),
        execute: Duration::from_millis(json["execute_ms"].as_u64().unwrap_or(0)),
    };

    Ok((output, metrics))
}

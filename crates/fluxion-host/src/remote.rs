use anyhow::Result;
use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
use fluxion_core::workflow::PermissionSet;
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use crate::JobMetrics;

/// Dispatch a Wasm job to a remote worker via HTTP POST /run.
pub async fn run_remote(
    worker_url: &str,
    wasm_path: impl AsRef<Path>,
    input: Vec<u8>,
    perms: &PermissionSet,
    env: &HashMap<String, String>,
) -> Result<(Vec<u8>, JobMetrics)> {
    let wasm_bytes = std::fs::read(wasm_path.as_ref())?;

    let body = serde_json::json!({
        "component": B64.encode(&wasm_bytes),
        "input": B64.encode(&input),
        "permissions": perms,
        "env": env,
    });

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(perms.limits.timeout_secs + 10))
        .build()?;

    let url = format!("{}/run", worker_url.trim_end_matches('/'));
    let resp = client.post(&url).json(&body).send().await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        anyhow::bail!("worker {} returned {}: {}", worker_url, status, text);
    }

    let json: serde_json::Value = resp.json().await?;

    let output = B64.decode(
        json["output"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("missing output field in worker response"))?,
    )?;

    let metrics = JobMetrics {
        compile: Duration::from_millis(json["compile_ms"].as_u64().unwrap_or(0)),
        instantiate: Duration::from_millis(json["instantiate_ms"].as_u64().unwrap_or(0)),
        execute: Duration::from_millis(json["execute_ms"].as_u64().unwrap_or(0)),
    };

    Ok((output, metrics))
}

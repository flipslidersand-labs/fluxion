use anyhow::Result;
use axum::{extract::State, http::StatusCode, routing::get, routing::post, Json, Router};
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use fluxion_core::workflow::PermissionSet;
use fluxion_host::FluxionHost;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use tempfile::NamedTempFile;

// ── Request / Response types ──────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct RunRequest {
    /// Base64-encoded .wasm component bytes.
    pub component: String,
    /// Base64-encoded input bytes.
    pub input: String,
    #[serde(default)]
    pub permissions: PermissionSet,
    #[serde(default)]
    pub env: HashMap<String, String>,
}

#[derive(Serialize)]
pub struct RunResponse {
    /// Base64-encoded output bytes.
    pub output: String,
    pub compile_ms: u128,
    pub instantiate_ms: u128,
    pub execute_ms: u128,
}

#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

// ── Handler ───────────────────────────────────────────────────────────────────

async fn handle_run(
    State(host): State<Arc<FluxionHost>>,
    Json(req): Json<RunRequest>,
) -> Result<Json<RunResponse>, (StatusCode, Json<ErrorResponse>)> {
    let component_bytes = B64.decode(&req.component).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!("invalid component base64: {e}"),
            }),
        )
    })?;

    let input = B64.decode(&req.input).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!("invalid input base64: {e}"),
            }),
        )
    })?;

    // Write the component bytes to a temp file so FluxionHost can read them.
    let mut tmp = NamedTempFile::new().map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )
    })?;
    tmp.write_all(&component_bytes).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )
    })?;
    let tmp_path = tmp.path().to_path_buf();

    let perms = req.permissions;
    let env = req.env;

    let result = tokio::task::spawn_blocking(move || {
        host.run_component_measured(&tmp_path, input, &perms, &env)
    })
    .await
    .map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )
    })?
    .map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )
    })?;

    let (output, metrics) = result;
    Ok(Json(RunResponse {
        output: B64.encode(&output),
        compile_ms: metrics.compile.as_millis(),
        instantiate_ms: metrics.instantiate.as_millis(),
        execute_ms: metrics.execute.as_millis(),
    }))
}

async fn handle_health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}

// ── Server ────────────────────────────────────────────────────────────────────

pub async fn serve(port: u16, metrics_port: Option<u16>) -> Result<()> {
    let host = Arc::new(FluxionHost::new()?);

    if let Some(mp) = metrics_port {
        tokio::spawn(fluxion_host::metrics::serve(mp));
    }

    let app = Router::new()
        .route("/run", post(handle_run))
        .route("/health", get(handle_health))
        .with_state(host);

    let addr = format!("0.0.0.0:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("fluxion worker listening on {addr}");
    axum::serve(listener, app).await?;
    Ok(())
}

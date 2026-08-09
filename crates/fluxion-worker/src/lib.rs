use anyhow::Result;
use axum::{extract::State, http::StatusCode, routing::get, routing::post, Json, Router};
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use fluxion_core::workflow::PermissionSet;
use fluxion_host::FluxionHost;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tempfile::NamedTempFile;

/// Global counter of jobs currently executing on this worker.
/// Reported in the `/health` response so the host-side scheduler can
/// implement least-connections load balancing.
static ACTIVE_JOBS: AtomicUsize = AtomicUsize::new(0);

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

/// mTLS configuration for the worker server.
pub struct WorkerTls {
    /// Path to the PEM-encoded server certificate.
    pub cert: PathBuf,
    /// Path to the PEM-encoded server private key.
    pub key: PathBuf,
    /// Path to the PEM-encoded CA certificate used to verify clients.
    pub ca: PathBuf,
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

    ACTIVE_JOBS.fetch_add(1, Ordering::Relaxed);
    let result = tokio::task::spawn_blocking(move || {
        host.run_component_measured(&tmp_path, input, &perms, &env)
    })
    .await
    .map_err(|e| {
        ACTIVE_JOBS.fetch_sub(1, Ordering::Relaxed);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )
    })?
    .map_err(|e| {
        ACTIVE_JOBS.fetch_sub(1, Ordering::Relaxed);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )
    })?;
    ACTIVE_JOBS.fetch_sub(1, Ordering::Relaxed);

    let (output, metrics) = result;
    Ok(Json(RunResponse {
        output: B64.encode(&output),
        compile_ms: metrics.compile.as_millis(),
        instantiate_ms: metrics.instantiate.as_millis(),
        execute_ms: metrics.execute.as_millis(),
    }))
}

async fn handle_health() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "ok",
        "active_jobs": ACTIVE_JOBS.load(Ordering::Relaxed),
    }))
}

// ── Server ────────────────────────────────────────────────────────────────────

pub async fn serve(port: u16, metrics_port: Option<u16>, tls: Option<WorkerTls>) -> Result<()> {
    let host = Arc::new(FluxionHost::new()?);

    if let Some(mp) = metrics_port {
        tokio::spawn(fluxion_host::metrics::serve(mp));
    }

    let app = Router::new()
        .route("/run", post(handle_run))
        .route("/health", get(handle_health))
        .with_state(host);

    let addr = format!("0.0.0.0:{port}");

    if let Some(tls) = tls {
        serve_tls(app, &addr, tls).await
    } else {
        let listener = tokio::net::TcpListener::bind(&addr).await?;
        tracing::info!("fluxion worker listening on {addr}");
        axum::serve(listener, app).await?;
        Ok(())
    }
}

async fn serve_tls(
    app: Router,
    addr: &str,
    tls: WorkerTls,
) -> Result<()> {
    use rustls::ServerConfig;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use rustls_pemfile::{certs, private_key};
    use std::io::BufReader;
    use std::sync::Arc as StdArc;
    use tokio_rustls::TlsAcceptor;
    use tower::Service;

    // Load server certificate chain.
    let cert_file = std::fs::File::open(&tls.cert)?;
    let server_certs: Vec<CertificateDer<'static>> =
        certs(&mut BufReader::new(cert_file))
            .collect::<std::result::Result<_, _>>()?;

    // Load server private key.
    let key_file = std::fs::File::open(&tls.key)?;
    let server_key: PrivateKeyDer<'static> = private_key(&mut BufReader::new(key_file))?
        .ok_or_else(|| anyhow::anyhow!("no private key found in {:?}", tls.key))?;

    // Build client certificate verifier from CA (mTLS).
    let ca_file = std::fs::File::open(&tls.ca)?;
    let ca_certs: Vec<CertificateDer<'static>> =
        certs(&mut BufReader::new(ca_file))
            .collect::<std::result::Result<_, _>>()?;

    let mut root_store = rustls::RootCertStore::empty();
    for cert in ca_certs {
        root_store.add(cert)?;
    }
    let client_verifier =
        rustls::server::WebPkiClientVerifier::builder(StdArc::new(root_store))
            .build()?;

    let server_config = ServerConfig::builder()
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(server_certs, server_key)?;

    let acceptor = TlsAcceptor::from(StdArc::new(server_config));
    let tcp_listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("fluxion worker listening (mTLS) on {addr}");

    loop {
        let (stream, _peer) = tcp_listener.accept().await?;
        let acceptor = acceptor.clone();
        let app = app.clone();
        tokio::spawn(async move {
            match acceptor.accept(stream).await {
                Ok(tls_stream) => {
                    let io = hyper_util::rt::TokioIo::new(tls_stream);
                    let service = hyper::service::service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                        let req = req.map(axum::body::Body::new);
                        let mut app = app.clone();
                        async move { app.call(req).await }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(io, service)
                        .await;
                }
                Err(e) => {
                    tracing::warn!("TLS handshake failed: {e}");
                }
            }
        });
    }
}

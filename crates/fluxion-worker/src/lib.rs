use anyhow::Result;
use axum::{
    body::Bytes,
    extract::{Path as AxumPath, State},
    http::StatusCode,
    routing::{get, head, post, put},
    Json, Router,
};
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use dashmap::DashMap;
use fluxion_core::workflow::PermissionSet;
use fluxion_host::FluxionHost;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tempfile::NamedTempFile;
use uuid::Uuid;

/// Global counter of jobs currently executing on this worker.
/// Reported in the `/health` response so the host-side scheduler can
/// implement least-connections load balancing.
static ACTIVE_JOBS: AtomicUsize = AtomicUsize::new(0);

/// CAS metrics.
static CAS_HITS: AtomicU64 = AtomicU64::new(0);
static CAS_MISSES: AtomicU64 = AtomicU64::new(0);

/// Return the CAS directory for storing cached .wasm components.
fn cas_dir() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home).join(".fluxion").join("cas")
}

fn cas_path(sha256: &str) -> PathBuf {
    cas_dir().join(sha256).with_extension("wasm")
}

// ── Async job store ───────────────────────────────────────────────────────────

#[derive(Clone, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum JobStatus {
    Running,
    Succeeded,
    Failed,
}

#[derive(Clone, Serialize)]
pub struct JobEntry {
    pub status: JobStatus,
    pub output: Option<String>,
    pub error: Option<String>,
}

type JobStore = Arc<DashMap<String, JobEntry>>;

// ── Request / Response types ──────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct RunRequest {
    /// Base64-encoded .wasm component bytes (mutually exclusive with `component_sha256`).
    #[serde(default)]
    pub component: Option<String>,
    /// SHA-256 hex digest of a component already uploaded via PUT /components/{sha256}.
    /// When present and cached, `component` bytes are not required.
    #[serde(default)]
    pub component_sha256: Option<String>,
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

// ── Async job store ───────────────────────────────────────────────────────────

/// In-memory record for an async job.
#[derive(Clone, Serialize)]
pub struct JobEntry {
    pub status: String,    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

type JobStore = Arc<DashMap<String, JobEntry>>;

/// Shared state threaded through all axum handlers.
#[derive(Clone)]
pub struct WorkerState {
    host: Arc<FluxionHost>,
    jobs: JobStore,
}

// ── POST /jobs ────────────────────────────────────────────────────────────────

#[derive(Serialize)]
struct SubmitResponse {
    job_id: String,
}

async fn handle_submit_job(
    State(state): State<WorkerState>,
    Json(req): Json<RunRequest>,
) -> Result<(StatusCode, Json<SubmitResponse>), (StatusCode, Json<ErrorResponse>)> {
    let job_id = Uuid::new_v4().to_string();

    state.jobs.insert(
        job_id.clone(),
        JobEntry {
            status: "running".into(),
            output: None,
            error: None,
        },
    );

    let state2 = state.clone();
    let jid = job_id.clone();
    tokio::spawn(async move {
        let result = run_request_inner(&state2.host, req).await;
        let entry = match result {
            Ok(output_b64) => JobEntry {
                status: "succeeded".into(),
                output: Some(output_b64),
                error: None,
            },
            Err(msg) => JobEntry {
                status: "failed".into(),
                output: None,
                error: Some(msg),
            },
        };
        state2.jobs.insert(jid, entry);
    });

    Ok((StatusCode::ACCEPTED, Json(SubmitResponse { job_id })))
}

// ── GET /jobs/:id ─────────────────────────────────────────────────────────────

async fn handle_get_job(
    State(state): State<WorkerState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<JobEntry>, (StatusCode, Json<ErrorResponse>)> {
    match state.jobs.get(&id) {
        Some(entry) => Ok(Json(entry.clone())),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("job {id} not found"),
            }),
        )),
    }
}

// ── Shared run logic (used by both sync POST /run and async POST /jobs) ───────

async fn run_request_inner(host: &Arc<FluxionHost>, req: RunRequest) -> Result<String, String> {
    let component_bytes: Vec<u8> = if let Some(sha256) = &req.component_sha256 {
        let p = cas_path(sha256);
        if p.exists() {
            CAS_HITS.fetch_add(1, Ordering::Relaxed);
            std::fs::read(&p).map_err(|e| e.to_string())?
        } else {
            CAS_MISSES.fetch_add(1, Ordering::Relaxed);
            match &req.component {
                Some(b64) => B64.decode(b64).map_err(|e| e.to_string())?,
                None => return Err(format!("component {sha256} not in CAS")),
            }
        }
    } else {
        CAS_MISSES.fetch_add(1, Ordering::Relaxed);
        let b64 = req.component.as_deref().unwrap_or("");
        B64.decode(b64).map_err(|e| e.to_string())?
    };

    let input = B64.decode(&req.input).map_err(|e| e.to_string())?;

    let mut tmp = tempfile::NamedTempFile::new().map_err(|e| e.to_string())?;
    tmp.write_all(&component_bytes).map_err(|e| e.to_string())?;
    let tmp_path = tmp.path().to_path_buf();

    let perms = req.permissions;
    let env = req.env;
    let host = Arc::clone(host);

    ACTIVE_JOBS.fetch_add(1, Ordering::Relaxed);
    let res = tokio::task::spawn_blocking(move || {
        host.run_component_measured(&tmp_path, input, &perms, &env)
    })
    .await
    .map_err(|e| e.to_string())?
    .map_err(|e| e.to_string());
    ACTIVE_JOBS.fetch_sub(1, Ordering::Relaxed);

    let (output, _metrics) = res?;
    Ok(B64.encode(&output))}

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
    State(state): State<WorkerState>,
    Json(req): Json<RunRequest>,
) -> Result<Json<RunResponse>, (StatusCode, Json<ErrorResponse>)> {
    let host = &state.host;    // Resolve component bytes: try CAS first, fall back to inline bytes.
    let component_bytes: Vec<u8> = if let Some(sha256) = &req.component_sha256 {
        let p = cas_path(sha256);
        if p.exists() {
            CAS_HITS.fetch_add(1, Ordering::Relaxed);
            std::fs::read(&p).map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: e.to_string(),
                    }),
                )
            })?
        } else {
            CAS_MISSES.fetch_add(1, Ordering::Relaxed);
            // Component not in CAS — require inline bytes from the caller.
            match &req.component {
                Some(b64) => B64.decode(b64).map_err(|e| {
                    (
                        StatusCode::BAD_REQUEST,
                        Json(ErrorResponse {
                            error: format!("invalid component base64: {e}"),
                        }),
                    )
                })?,
                None => {
                    return Err((
                        StatusCode::NOT_FOUND,
                        Json(ErrorResponse {
                            error: format!(
                            "component {sha256} not in CAS — upload via PUT /components/{sha256}"
                        ),
                        }),
                    ))
                }
            }
        }
    } else {
        // Classic inline mode.
        CAS_MISSES.fetch_add(1, Ordering::Relaxed);
        let b64 = req.component.as_deref().unwrap_or("");
        B64.decode(b64).map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("invalid component base64: {e}"),
                }),
            )
        })?
    };

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
    let host = Arc::clone(host);

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

async fn handle_submit(
    State(state): State<AppState>,
    Json(req): Json<RunRequest>,
) -> Result<Json<SubmitResponse>, (StatusCode, Json<ErrorResponse>)> {
    let job_id = Uuid::new_v4().to_string();

    // Resolve component bytes synchronously before spawning.
    let component_bytes: Vec<u8> = if let Some(sha256) = &req.component_sha256 {
        let p = cas_path(sha256);
        if p.exists() {
            CAS_HITS.fetch_add(1, Ordering::Relaxed);
            std::fs::read(&p).map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse { error: e.to_string() }),
                )
            })?
        } else {
            CAS_MISSES.fetch_add(1, Ordering::Relaxed);
            match &req.component {
                Some(b64) => B64.decode(b64).map_err(|e| {
                    (
                        StatusCode::BAD_REQUEST,
                        Json(ErrorResponse { error: format!("invalid component base64: {e}") }),
                    )
                })?,
                None => {
                    return Err((
                        StatusCode::NOT_FOUND,
                        Json(ErrorResponse {
                            error: format!(
                                "component {sha256} not in CAS — upload via PUT /components/{sha256}"
                            ),
                        }),
                    ))
                }
            }
        }
    } else {
        CAS_MISSES.fetch_add(1, Ordering::Relaxed);
        let b64 = req.component.as_deref().unwrap_or("");
        B64.decode(b64).map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse { error: format!("invalid component base64: {e}") }),
            )
        })?
    };

    let input = B64.decode(&req.input).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse { error: format!("invalid input base64: {e}") }),
        )
    })?;

    state.jobs.insert(
        job_id.clone(),
        JobEntry { status: JobStatus::Running, output: None, error: None },
    );
    ACTIVE_JOBS.fetch_add(1, Ordering::Relaxed);

    let jobs = Arc::clone(&state.jobs);
    let host = Arc::clone(&state.host);
    let jid = job_id.clone();
    tokio::spawn(async move {
        let mut tmp = match NamedTempFile::new() {
            Ok(f) => f,
            Err(e) => {
                jobs.insert(
                    jid,
                    JobEntry { status: JobStatus::Failed, output: None, error: Some(e.to_string()) },
                );
                ACTIVE_JOBS.fetch_sub(1, Ordering::Relaxed);
                return;
            }
        };
        if let Err(e) = tmp.write_all(&component_bytes) {
            jobs.insert(
                jid,
                JobEntry { status: JobStatus::Failed, output: None, error: Some(e.to_string()) },
            );
            ACTIVE_JOBS.fetch_sub(1, Ordering::Relaxed);
            return;
        }
        let tmp_path = tmp.path().to_path_buf();
        let perms = req.permissions;
        let env = req.env;
        let result = tokio::task::spawn_blocking(move || {
            host.run_component_measured(&tmp_path, input, &perms, &env)
        })
        .await;
        ACTIVE_JOBS.fetch_sub(1, Ordering::Relaxed);
        match result {
            Ok(Ok((output, _))) => {
                jobs.insert(
                    jid,
                    JobEntry {
                        status: JobStatus::Succeeded,
                        output: Some(B64.encode(&output)),
                        error: None,
                    },
                );
            }
            Ok(Err(e)) => {
                jobs.insert(
                    jid,
                    JobEntry { status: JobStatus::Failed, output: None, error: Some(e.to_string()) },
                );
            }
            Err(e) => {
                jobs.insert(
                    jid,
                    JobEntry { status: JobStatus::Failed, output: None, error: Some(e.to_string()) },
                );
            }
        }
    });

    Ok(Json(SubmitResponse { job_id }))
}

async fn handle_job_status(
    State(state): State<AppState>,
    AxumPath(job_id): AxumPath<String>,
) -> Result<Json<JobStatusResponse>, (StatusCode, Json<ErrorResponse>)> {
    match state.jobs.get(&job_id) {
        Some(entry) => Ok(Json(JobStatusResponse {
            status: entry.status.clone(),
            output: entry.output.clone(),
            error: entry.error.clone(),
        })),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse { error: format!("job {job_id} not found") }),
        )),
    }
}

async fn handle_health() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "ok",
        "active_jobs": ACTIVE_JOBS.load(Ordering::Relaxed),
        "cas_hits":  CAS_HITS.load(Ordering::Relaxed),
        "cas_misses": CAS_MISSES.load(Ordering::Relaxed),
    }))
}

// ── CAS endpoints ─────────────────────────────────────────────────────────────

async fn handle_cas_head(
    _state: State<AppState>,
    AxumPath(sha256): AxumPath<String>,
) -> StatusCode {
    if cas_path(&sha256).exists() {
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

async fn handle_cas_put(
    _state: State<AppState>,
    AxumPath(sha256): AxumPath<String>,
    body: Bytes,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    // Verify digest matches the uploaded bytes.
    use sha2::{Digest, Sha256};
    let actual: String = Sha256::digest(&body)
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect();
    if !actual.eq_ignore_ascii_case(&sha256) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!("SHA-256 mismatch: expected {sha256}, got {actual}"),
            }),
        ));
    }
    let dir = cas_dir();
    std::fs::create_dir_all(&dir).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )
    })?;
    let path = cas_path(&sha256);
    std::fs::write(&path, &body).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )
    })?;
    Ok(StatusCode::CREATED)
}

// ── Server ────────────────────────────────────────────────────────────────────

pub async fn serve(port: u16, metrics_port: Option<u16>, tls: Option<WorkerTls>) -> Result<()> {
    let state = WorkerState {        host: Arc::new(FluxionHost::new()?),
        jobs: Arc::new(DashMap::new()),
    };

    if let Some(mp) = metrics_port {
        tokio::spawn(fluxion_host::metrics::serve(mp));
    }

    let app = Router::new()
        .route("/run", post(handle_run))
        .route("/jobs", post(handle_submit_job))
        .route("/jobs/:id", get(handle_get_job))        .route("/health", get(handle_health))
        .route("/components/{sha256}", head(handle_cas_head))
        .route("/components/{sha256}", put(handle_cas_put))
        .with_state(state);

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

async fn serve_tls(app: Router, addr: &str, tls: WorkerTls) -> Result<()> {
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use rustls::ServerConfig;
    use rustls_pemfile::{certs, private_key};
    use std::io::BufReader;
    use std::sync::Arc as StdArc;
    use tokio_rustls::TlsAcceptor;
    use tower::Service;

    // Load server certificate chain.
    let cert_file = std::fs::File::open(&tls.cert)?;
    let server_certs: Vec<CertificateDer<'static>> =
        certs(&mut BufReader::new(cert_file)).collect::<std::result::Result<_, _>>()?;

    // Load server private key.
    let key_file = std::fs::File::open(&tls.key)?;
    let server_key: PrivateKeyDer<'static> = private_key(&mut BufReader::new(key_file))?
        .ok_or_else(|| anyhow::anyhow!("no private key found in {:?}", tls.key))?;

    // Build client certificate verifier from CA (mTLS).
    let ca_file = std::fs::File::open(&tls.ca)?;
    let ca_certs: Vec<CertificateDer<'static>> =
        certs(&mut BufReader::new(ca_file)).collect::<std::result::Result<_, _>>()?;

    let mut root_store = rustls::RootCertStore::empty();
    for cert in ca_certs {
        root_store.add(cert)?;
    }
    let client_verifier =
        rustls::server::WebPkiClientVerifier::builder(StdArc::new(root_store)).build()?;

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
                    let service = hyper::service::service_fn(
                        move |req: hyper::Request<hyper::body::Incoming>| {
                            let req = req.map(axum::body::Body::new);
                            let mut app = app.clone();
                            async move { app.call(req).await }
                        },
                    );
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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_state() -> AppState {
        AppState {
            host: Arc::new(FluxionHost::new().unwrap()),
            jobs: Arc::new(DashMap::new()),
        }
    }

    #[tokio::test]
    async fn job_store_running_then_succeeded() {
        let state = make_state();
        let job_id = Uuid::new_v4().to_string();

        state.jobs.insert(
            job_id.clone(),
            JobEntry { status: JobStatus::Running, output: None, error: None },
        );
        assert!(matches!(
            state.jobs.get(&job_id).unwrap().status,
            JobStatus::Running
        ));

        state.jobs.insert(
            job_id.clone(),
            JobEntry {
                status: JobStatus::Succeeded,
                output: Some("dGVzdA==".to_string()),
                error: None,
            },
        );
        let entry = state.jobs.get(&job_id).unwrap();
        assert!(matches!(entry.status, JobStatus::Succeeded));
        assert_eq!(entry.output.as_deref(), Some("dGVzdA=="));
    }

    #[tokio::test]
    async fn job_store_missing_returns_none() {
        let state = make_state();
        assert!(state.jobs.get("nonexistent").is_none());
    }

    #[tokio::test]
    async fn job_store_failed_records_error() {
        let state = make_state();
        let job_id = Uuid::new_v4().to_string();

        state.jobs.insert(
            job_id.clone(),
            JobEntry { status: JobStatus::Failed, output: None, error: Some("boom".to_string()) },
        );
        let entry = state.jobs.get(&job_id).unwrap();
        assert!(matches!(entry.status, JobStatus::Failed));
        assert_eq!(entry.error.as_deref(), Some("boom"));
    }
}

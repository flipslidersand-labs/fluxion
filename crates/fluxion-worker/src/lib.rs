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
use fluxion_host::{FluxionHost, JobMetrics};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
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
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// When this entry reached a terminal state ("succeeded"/"failed").
    /// `None` while still "running". Used by `sweep_expired_jobs` to purge
    /// completed entries after `JOB_RETENTION` — otherwise a long-lived
    /// worker accumulates one entry per job forever (#239).
    #[serde(skip)]
    completed_at: Option<Instant>,
}

type JobStore = Arc<DashMap<String, JobEntry>>;

/// How long a completed job's entry stays in memory before being purged.
const JOB_RETENTION: Duration = Duration::from_secs(300);
/// How often the background sweep checks for expired entries.
const SWEEP_INTERVAL: Duration = Duration::from_secs(60);

/// Remove completed job entries older than `retention`. Entries still
/// "running" (`completed_at: None`) are never swept.
fn sweep_expired_jobs(jobs: &JobStore, retention: Duration) {
    jobs.retain(|_, entry| {
        entry
            .completed_at
            .map(|t| t.elapsed() < retention)
            .unwrap_or(true)
    });
}

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
            completed_at: None,
        },
    );

    let state2 = state.clone();
    let jid = job_id.clone();
    tokio::spawn(async move {
        let result = run_request_inner(&state2.host, req).await;
        let entry = match result {
            Ok((output, _metrics)) => JobEntry {
                status: "succeeded".into(),
                output: Some(B64.encode(&output)),
                error: None,
                completed_at: Some(Instant::now()),
            },
            Err((_status, msg)) => JobEntry {
                status: "failed".into(),
                output: None,
                error: Some(msg),
                completed_at: Some(Instant::now()),
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

/// Error type of the shared run logic: HTTP status + message.
type RunError = (StatusCode, String);

fn run_err(status: StatusCode, e: impl ToString) -> RunError {
    (status, e.to_string())
}

/// Decrements `ACTIVE_JOBS` on drop so every exit path is covered.
struct ActiveJobGuard;

impl ActiveJobGuard {
    fn new() -> Self {
        ACTIVE_JOBS.fetch_add(1, Ordering::Relaxed);
        ActiveJobGuard
    }
}

impl Drop for ActiveJobGuard {
    fn drop(&mut self) {
        ACTIVE_JOBS.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Resolve the component (CAS first, inline bytes as fallback), run it, and
/// return the raw output plus metrics. Errors carry the HTTP status that
/// `POST /run` reports.
async fn run_request_inner(
    host: &Arc<FluxionHost>,
    req: RunRequest,
) -> Result<(Vec<u8>, JobMetrics), RunError> {
    let decode_component = |b64: &str| {
        B64.decode(b64).map_err(|e| {
            run_err(
                StatusCode::BAD_REQUEST,
                format!("invalid component base64: {e}"),
            )
        })
    };

    let component_bytes: Vec<u8> = if let Some(sha256) = &req.component_sha256 {
        let p = cas_path(sha256);
        if p.exists() {
            CAS_HITS.fetch_add(1, Ordering::Relaxed);
            std::fs::read(&p).map_err(|e| run_err(StatusCode::INTERNAL_SERVER_ERROR, e))?
        } else {
            CAS_MISSES.fetch_add(1, Ordering::Relaxed);
            // Component not in CAS — require inline bytes from the caller.
            match &req.component {
                Some(b64) => decode_component(b64)?,
                None => {
                    return Err(run_err(
                        StatusCode::NOT_FOUND,
                        format!(
                            "component {sha256} not in CAS — upload via PUT /components/{sha256}"
                        ),
                    ));
                }
            }
        }
    } else {
        // Classic inline mode.
        CAS_MISSES.fetch_add(1, Ordering::Relaxed);
        decode_component(req.component.as_deref().unwrap_or(""))?
    };

    let input = B64.decode(&req.input).map_err(|e| {
        run_err(
            StatusCode::BAD_REQUEST,
            format!("invalid input base64: {e}"),
        )
    })?;

    // Write the component bytes to a temp file so FluxionHost can read them.
    let mut tmp =
        NamedTempFile::new().map_err(|e| run_err(StatusCode::INTERNAL_SERVER_ERROR, e))?;
    tmp.write_all(&component_bytes)
        .map_err(|e| run_err(StatusCode::INTERNAL_SERVER_ERROR, e))?;
    let tmp_path = tmp.path().to_path_buf();

    let perms = req.permissions;
    let env = req.env;
    let host = Arc::clone(host);

    let _active = ActiveJobGuard::new();
    tokio::task::spawn_blocking(move || host.run_component_measured(&tmp_path, input, &perms, &env))
        .await
        .map_err(|e| run_err(StatusCode::INTERNAL_SERVER_ERROR, e))?
        .map_err(|e| run_err(StatusCode::INTERNAL_SERVER_ERROR, e))
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
    State(state): State<WorkerState>,
    Json(req): Json<RunRequest>,
) -> Result<Json<RunResponse>, (StatusCode, Json<ErrorResponse>)> {
    let (output, metrics) = run_request_inner(&state.host, req)
        .await
        .map_err(|(status, error)| (status, Json(ErrorResponse { error })))?;
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
        "cas_hits":  CAS_HITS.load(Ordering::Relaxed),
        "cas_misses": CAS_MISSES.load(Ordering::Relaxed),
    }))
}

// ── CAS endpoints ─────────────────────────────────────────────────────────────

async fn handle_cas_head(AxumPath(sha256): AxumPath<String>) -> StatusCode {
    if cas_path(&sha256).exists() {
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

async fn handle_cas_put(
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

pub async fn serve(
    port: u16,
    metrics_port: Option<u16>,
    tls: Option<WorkerTls>,
    async_jobs: bool,
) -> Result<()> {
    let state = WorkerState {
        host: Arc::new(FluxionHost::new()?),
        jobs: Arc::new(DashMap::new()),
    };

    if let Some(mp) = metrics_port {
        tokio::spawn(fluxion_host::metrics::serve(mp));
    }

    let mut app = Router::new()
        .route("/run", post(handle_run))
        .route("/health", get(handle_health))
        .route("/components/{sha256}", head(handle_cas_head))
        .route("/components/{sha256}", put(handle_cas_put));

    if async_jobs {
        app = app
            .route("/jobs", post(handle_submit_job))
            .route("/jobs/:id", get(handle_get_job));

        let sweep_jobs = Arc::clone(&state.jobs);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(SWEEP_INTERVAL);
            loop {
                interval.tick().await;
                sweep_expired_jobs(&sweep_jobs, JOB_RETENTION);
            }
        });
    }

    let app = app.with_state(state);

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
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use rustls::ServerConfig;
    use std::sync::Arc as StdArc;
    use tokio_rustls::TlsAcceptor;
    use tower::Service;

    // Load server certificate chain.
    let server_certs: Vec<CertificateDer<'static>> =
        CertificateDer::pem_file_iter(&tls.cert)?.collect::<std::result::Result<_, _>>()?;

    // Load server private key.
    let server_key: PrivateKeyDer<'static> = PrivateKeyDer::from_pem_file(&tls.key)?;

    // Build client certificate verifier from CA (mTLS).
    let ca_certs: Vec<CertificateDer<'static>> =
        CertificateDer::pem_file_iter(&tls.ca)?.collect::<std::result::Result<_, _>>()?;

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

    const TEST_CERT_PEM: &str = "-----BEGIN CERTIFICATE-----\nMIIBfzCCASWgAwIBAgIUBAUNdTRtIudbWKEEbfd3xl2bh00wCgYIKoZIzj0EAwIw\nFDESMBAGA1UEAwwJbG9jYWxob3N0MCAXDTI2MTAwMzAzMjIzMFoYDzIxMjYwOTA5\nMDMyMjMwWjAUMRIwEAYDVQQDDAlsb2NhbGhvc3QwWTATBgcqhkjOPQIBBggqhkjO\nPQMBBwNCAAQEwGgOr30cIGOMVDodWniz7cpYg+Fx//KQZtJtCqzjeCxQrfLSqcXt\nygGr0DOc4Y655xUOk0MxvUzquSmHliodo1MwUTAdBgNVHQ4EFgQUlOy9dNL3R8IJ\nmTrEyKtw1PWpz08wHwYDVR0jBBgwFoAUlOy9dNL3R8IJmTrEyKtw1PWpz08wDwYD\nVR0TAQH/BAUwAwEB/zAKBggqhkjOPQQDAgNIADBFAiEA0jahTYL3c4zTXw0dEmrU\n6f0Xb4mIXkbmPcOiOWkJupACIESaAvzQKcgNdXzwXLaJVdjvYn6bvN0RQOxI69Es\nwrfZ\n-----END CERTIFICATE-----\n";
    const TEST_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----\nMIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQg4aqcgIiKGn/mk2m0\n3qmdxVWqzC9+08p8b6R5qJX5cP6hRANCAAQEwGgOr30cIGOMVDodWniz7cpYg+Fx\n//KQZtJtCqzjeCxQrfLSqcXtygGr0DOc4Y655xUOk0MxvUzquSmHliod\n-----END PRIVATE KEY-----\n";

    fn write_pem(content: &str) -> NamedTempFile {
        use std::io::Write;
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f
    }

    fn tls_files(cert: &str, key: &str, ca: &str) -> (WorkerTls, [NamedTempFile; 3]) {
        let files = [write_pem(cert), write_pem(key), write_pem(ca)];
        let tls = WorkerTls {
            cert: files[0].path().to_path_buf(),
            key: files[1].path().to_path_buf(),
            ca: files[2].path().to_path_buf(),
        };
        (tls, files)
    }

    #[tokio::test]
    async fn serve_tls_loads_valid_pem_and_serves() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let (tls, _files) = tls_files(TEST_CERT_PEM, TEST_KEY_PEM, TEST_CERT_PEM);
        // Valid PEM: loading succeeds and the server keeps running (timeout elapses).
        let res = tokio::time::timeout(
            Duration::from_millis(300),
            serve_tls(Router::new(), "127.0.0.1:0", tls),
        )
        .await;
        assert!(res.is_err(), "serve_tls returned early: {res:?}");
    }

    #[tokio::test]
    async fn serve_tls_errors_on_invalid_key_pem() {
        let (tls, _files) = tls_files(TEST_CERT_PEM, "not a pem", TEST_CERT_PEM);
        let res = serve_tls(Router::new(), "127.0.0.1:0", tls).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn serve_tls_errors_on_invalid_cert_pem() {
        let (tls, _files) = tls_files(
            "-----BEGIN CERTIFICATE-----
!!!
-----END CERTIFICATE-----
",
            TEST_KEY_PEM,
            TEST_CERT_PEM,
        );
        let res = serve_tls(Router::new(), "127.0.0.1:0", tls).await;
        assert!(res.is_err());
    }

    fn entry(status: &str, completed_at: Option<Instant>) -> JobEntry {
        JobEntry {
            status: status.into(),
            output: None,
            error: None,
            completed_at,
        }
    }

    fn past(secs_ago: u64) -> Instant {
        Instant::now()
            .checked_sub(Duration::from_secs(secs_ago))
            .expect("time went backwards")
    }

    #[test]
    fn sweep_removes_completed_entries_past_retention() {
        let jobs: JobStore = Arc::new(DashMap::new());
        jobs.insert("old".into(), entry("succeeded", Some(past(600))));
        jobs.insert("fresh".into(), entry("succeeded", Some(past(10))));

        sweep_expired_jobs(&jobs, Duration::from_secs(300));

        assert!(!jobs.contains_key("old"), "expired entry must be swept");
        assert!(jobs.contains_key("fresh"), "recent entry must survive");
    }

    #[test]
    fn sweep_never_removes_running_entries() {
        let jobs: JobStore = Arc::new(DashMap::new());
        jobs.insert("running".into(), entry("running", None));

        sweep_expired_jobs(&jobs, Duration::from_secs(0));

        assert!(
            jobs.contains_key("running"),
            "in-flight jobs must never be swept regardless of age"
        );
    }

    #[test]
    fn sweep_on_empty_store_is_a_noop() {
        let jobs: JobStore = Arc::new(DashMap::new());
        sweep_expired_jobs(&jobs, Duration::from_secs(300));
        assert!(jobs.is_empty());
    }
}

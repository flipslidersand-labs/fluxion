use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::{
    Router,
    extract::{Path, State},
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use fluxion_core::store::RunStore;

#[derive(Clone)]
pub struct ApiState {
    pub store: Arc<Mutex<RunStore>>,
}

pub fn router(state: ApiState) -> Router {
    Router::new()
        .route("/api/runs", get(list_runs))
        // Use singular /api/run/:id/jobs to avoid matchit prefix collision with /api/runs.
        .route("/api/run/:id/jobs", get(list_run_jobs))
        .route("/api/schedules", get(list_schedules))
        .route("/api/workers", get(list_workers))
        .route("/metrics", get(metrics))
        .merge(crate::ui::router())
        .with_state(state)
}

async fn list_runs(State(s): State<ApiState>) -> Result<impl IntoResponse, ApiError> {
    let rows = s.store.lock().unwrap().list_runs(100)?;
    Ok(json_response(&rows))
}

async fn list_run_jobs(
    State(s): State<ApiState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let rows = s.store.lock().unwrap().get_run_jobs(&id)?;
    Ok(json_response(&rows))
}

async fn list_schedules(State(s): State<ApiState>) -> Result<impl IntoResponse, ApiError> {
    let rows = s.store.lock().unwrap().list_schedules()?;
    Ok(json_response(&rows))
}

async fn list_workers(State(s): State<ApiState>) -> Result<impl IntoResponse, ApiError> {
    let rows = s.store.lock().unwrap().list_workers()?;
    Ok(json_response(&rows))
}

async fn metrics() -> impl IntoResponse {
    let body = crate::metrics::gather();
    Response::builder()
        .status(StatusCode::OK)
        .header(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
        )
        .body(body)
        .unwrap()
}

fn json_response<T: serde::Serialize>(value: &T) -> Response {
    match serde_json::to_vec(value) {
        Ok(body) => Response::builder()
            .status(StatusCode::OK)
            .header(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            )
            .body(axum::body::Body::from(body))
            .unwrap(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Bind the API + metrics router on `port` and serve until the process exits.
pub async fn start(port: u16) -> anyhow::Result<()> {
    let store = Arc::new(Mutex::new(fluxion_core::store::RunStore::open()?));
    let app = router(ApiState { store });
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!("fluxion API server listening on http://{addr}");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

struct ApiError(anyhow::Error);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (StatusCode::INTERNAL_SERVER_ERROR, self.0.to_string()).into_response()
    }
}

impl<E: Into<anyhow::Error>> From<E> for ApiError {
    fn from(e: E) -> Self {
        Self(e.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::http::Request;
    use rusqlite::Connection;
    use std::path::Path as StdPath;
    use tower::ServiceExt;

    fn in_memory_state() -> ApiState {
        let conn = Connection::open_in_memory().expect("in-memory db");
        let store = RunStore::from_conn(conn);
        ApiState {
            store: Arc::new(Mutex::new(store)),
        }
    }

    async fn body_bytes(resp: axum::response::Response) -> Vec<u8> {
        to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec()
    }

    // ── GET /api/runs ─────────────────────────────────────────────────────────

    #[tokio::test]
    async fn list_runs_empty() {
        let state = in_memory_state();
        let app = router(state);
        let req = Request::builder()
            .uri("/api/runs")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers()
                .get(header::CONTENT_TYPE)
                .unwrap()
                .to_str()
                .unwrap(),
            "application/json"
        );
        let body: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        assert_eq!(body, serde_json::json!([]));
    }

    #[tokio::test]
    async fn list_runs_returns_created_runs() {
        let state = in_memory_state();
        {
            let store = state.store.lock().unwrap();
            store
                .create_run("run-1", "wf-a", StdPath::new("wf-a.yaml"))
                .unwrap();
            store
                .create_run("run-2", "wf-b", StdPath::new("wf-b.yaml"))
                .unwrap();
        }
        let app = router(state);
        let req = Request::builder()
            .uri("/api/runs")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        let arr = body.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        let ids: Vec<&str> = arr.iter().map(|v| v["id"].as_str().unwrap()).collect();
        assert!(ids.contains(&"run-1"));
        assert!(ids.contains(&"run-2"));
    }

    // ── GET /api/run/:id/jobs ─────────────────────────────────────────────────

    #[tokio::test]
    async fn list_run_jobs_empty() {
        let state = in_memory_state();
        {
            let store = state.store.lock().unwrap();
            store
                .create_run("run-x", "wf", StdPath::new("wf.yaml"))
                .unwrap();
        }
        let app = router(state);
        let req = Request::builder()
            .uri("/api/run/run-x/jobs")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        assert_eq!(body, serde_json::json!([]));
    }

    #[tokio::test]
    async fn list_run_jobs_unknown_id_returns_empty() {
        let state = in_memory_state();
        let app = router(state);
        let req = Request::builder()
            .uri("/api/run/does-not-exist/jobs")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        assert_eq!(body, serde_json::json!([]));
    }

    #[tokio::test]
    async fn list_run_jobs_returns_jobs() {
        use fluxion_core::state::JobStatus;

        let state = in_memory_state();
        {
            let store = state.store.lock().unwrap();
            store
                .create_run("run-j", "wf", StdPath::new("wf.yaml"))
                .unwrap();
            store
                .upsert_job(
                    "run-j",
                    "job-a",
                    &JobStatus::Succeeded {
                        elapsed: std::time::Duration::from_millis(42),
                    },
                )
                .unwrap();
            store
                .upsert_job(
                    "run-j",
                    "job-b",
                    &JobStatus::Failed {
                        elapsed: std::time::Duration::from_millis(1),
                        reason: "oops".into(),
                    },
                )
                .unwrap();
        }
        let app = router(state);
        let req = Request::builder()
            .uri("/api/run/run-j/jobs")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        let arr = body.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        let statuses: Vec<&str> = arr.iter().map(|v| v["status"].as_str().unwrap()).collect();
        assert!(statuses.contains(&"succeeded"));
        assert!(statuses.contains(&"failed"));
    }

    // ── GET /api/schedules ────────────────────────────────────────────────────

    #[tokio::test]
    async fn list_schedules_empty() {
        let state = in_memory_state();
        let app = router(state);
        let req = Request::builder()
            .uri("/api/schedules")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        assert_eq!(body, serde_json::json!([]));
    }

    #[tokio::test]
    async fn list_schedules_returns_added_entry() {
        let state = in_memory_state();
        {
            let store = state.store.lock().unwrap();
            store
                .add_schedule("sched-1", "wf.yaml", "0 * * * * *", 9_999_999)
                .unwrap();
        }
        let app = router(state);
        let req = Request::builder()
            .uri("/api/schedules")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        let arr = body.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["cron_expr"].as_str().unwrap(), "0 * * * * *");
    }

    // ── GET /api/workers ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn list_workers_empty() {
        let state = in_memory_state();
        let app = router(state);
        let req = Request::builder()
            .uri("/api/workers")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        assert_eq!(body, serde_json::json!([]));
    }

    #[tokio::test]
    async fn list_workers_returns_registered_worker() {
        let state = in_memory_state();
        {
            let store = state.store.lock().unwrap();
            store.register_worker("http://worker1:7777").unwrap();
        }
        let app = router(state);
        let req = Request::builder()
            .uri("/api/workers")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        let arr = body.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["url"].as_str().unwrap(), "http://worker1:7777");
    }

    // ── GET /metrics ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn metrics_returns_text_plain() {
        let state = in_memory_state();
        let app = router(state);
        let req = Request::builder()
            .uri("/metrics")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let ct = resp
            .headers()
            .get(header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(ct.starts_with("text/plain"), "got: {ct}");
    }
}

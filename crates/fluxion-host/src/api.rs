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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use fluxion_core::store::RunStore;
    use http_body_util::BodyExt;
    use rusqlite::Connection;
    use tower::ServiceExt;

    use super::{ApiState, router};

    fn test_state() -> ApiState {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS runs \
             (id TEXT PRIMARY KEY, workflow_name TEXT NOT NULL, workflow_path TEXT NOT NULL, \
              started_at INTEGER NOT NULL, completed_at INTEGER, status TEXT NOT NULL); \
             CREATE TABLE IF NOT EXISTS job_states \
             (run_id TEXT NOT NULL, job_id TEXT NOT NULL, status TEXT NOT NULL, \
              elapsed_ms INTEGER, reason TEXT, PRIMARY KEY (run_id, job_id)); \
             CREATE TABLE IF NOT EXISTS schedules \
             (id TEXT PRIMARY KEY, workflow_path TEXT NOT NULL, cron_expr TEXT NOT NULL, \
              created_at INTEGER NOT NULL, last_run_at INTEGER, next_run_at INTEGER NOT NULL); \
             CREATE TABLE IF NOT EXISTS workers \
             (url TEXT PRIMARY KEY, registered_at INTEGER NOT NULL, last_health TEXT);",
        )
        .unwrap();
        ApiState {
            store: Arc::new(Mutex::new(RunStore::from_conn(conn))),
        }
    }

    async fn body_bytes(body: Body) -> Vec<u8> {
        body.collect().await.unwrap().to_bytes().to_vec()
    }

    #[tokio::test]
    async fn get_runs_returns_200_json_array() {
        let app = router(test_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/runs")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let ct = resp.headers().get("content-type").unwrap().to_str().unwrap();
        assert!(ct.contains("application/json"));
        let body = body_bytes(resp.into_body()).await;
        assert_eq!(String::from_utf8(body).unwrap(), "[]");
    }

    #[tokio::test]
    async fn get_run_jobs_returns_200_for_unknown_id() {
        let app = router(test_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/run/nonexistent/jobs")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_bytes(resp.into_body()).await;
        assert_eq!(String::from_utf8(body).unwrap(), "[]");
    }

    #[tokio::test]
    async fn get_schedules_returns_200_json_array() {
        let app = router(test_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/schedules")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_bytes(resp.into_body()).await;
        assert_eq!(String::from_utf8(body).unwrap(), "[]");
    }

    #[tokio::test]
    async fn get_workers_returns_200_json_array() {
        let app = router(test_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/workers")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_bytes(resp.into_body()).await;
        assert_eq!(String::from_utf8(body).unwrap(), "[]");
    }

    #[tokio::test]
    async fn get_metrics_returns_prometheus_text() {
        let app = router(test_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let ct = resp.headers().get("content-type").unwrap().to_str().unwrap();
        assert!(ct.contains("text/plain"));
    }
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

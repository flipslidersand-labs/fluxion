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
        .route("/api/runs/{id}/jobs", get(list_run_jobs))
        .route("/api/schedules", get(list_schedules))
        .route("/api/workers", get(list_workers))
        .route("/metrics", get(metrics))
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
            .header(header::CONTENT_TYPE, HeaderValue::from_static("application/json"))
            .body(axum::body::Body::from(body))
            .unwrap(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
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

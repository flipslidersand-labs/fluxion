use crate::api::ApiState;
use axum::{
    Router,
    extract::{Path, State},
    http::{HeaderValue, StatusCode, header},
    response::{Html, IntoResponse, Response},
    routing::get,
};

static INDEX_HTML: &str = include_str!("../assets/index.html");

pub fn router() -> Router<ApiState> {
    Router::new()
        .route("/", get(index))
        .route("/ui/runs", get(ui_runs))
        .route("/ui/runs/{id}", get(ui_run_jobs))
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn ui_runs(State(s): State<ApiState>) -> Result<impl IntoResponse, UiError> {
    let rows = s.store.lock().unwrap().list_runs(50)?;
    let mut html = String::new();
    if rows.is_empty() {
        html.push_str("<tr><td colspan=\"4\">No runs yet.</td></tr>");
    } else {
        for r in &rows {
            let ts = format_ts(r.started_at);
            let badge = status_badge(&r.status);
            html.push_str(&format!(
                "<tr>\
                   <td><a href=\"/ui/runs/{id}\">{id}</a></td>\
                   <td>{name}</td>\
                   <td>{badge}</td>\
                   <td>{ts}</td>\
                 </tr>",
                id = escape(&r.id),
                name = escape(&r.workflow_name),
            ));
        }
    }
    Ok(html_fragment(html))
}

async fn ui_run_jobs(
    State(s): State<ApiState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, UiError> {
    let jobs = s.store.lock().unwrap().get_run_jobs(&id)?;
    let mut html = String::from(
        "<table style=\"width:100%;border-collapse:collapse\">\
         <thead><tr>\
           <th style=\"text-align:left;padding:.4rem .8rem;background:#343a40;color:#fff\">Job</th>\
           <th style=\"text-align:left;padding:.4rem .8rem;background:#343a40;color:#fff\">Status</th>\
           <th style=\"text-align:left;padding:.4rem .8rem;background:#343a40;color:#fff\">Elapsed (ms)</th>\
         </tr></thead><tbody>",
    );
    if jobs.is_empty() {
        html.push_str("<tr><td colspan=\"3\">No jobs recorded.</td></tr>");
    } else {
        for j in &jobs {
            let badge = status_badge(&j.status);
            let elapsed = j.elapsed_ms.map(|ms| ms.to_string()).unwrap_or_default();
            html.push_str(&format!(
                "<tr>\
                   <td style=\"padding:.4rem .8rem;border-bottom:1px solid #dee2e6\">{job}</td>\
                   <td style=\"padding:.4rem .8rem;border-bottom:1px solid #dee2e6\">{badge}</td>\
                   <td style=\"padding:.4rem .8rem;border-bottom:1px solid #dee2e6\">{elapsed}</td>\
                 </tr>",
                job = escape(&j.job_id),
            ));
        }
    }
    html.push_str("</tbody></table>");
    Ok(html_fragment(html))
}

fn status_badge(status: &str) -> String {
    let cls = match status {
        "running" => "badge-running",
        "succeeded" => "badge-succeeded",
        "failed" => "badge-failed",
        _ => "badge-pending",
    };
    format!("<span class=\"badge {cls}\">{}</span>", escape(status))
}

fn format_ts(secs: u64) -> String {
    use std::time::{Duration, UNIX_EPOCH};
    let dt = UNIX_EPOCH + Duration::from_secs(secs);
    // Simple ISO-8601 approximation without external deps.
    let total = secs;
    let s = total % 60;
    let m = (total / 60) % 60;
    let h = (total / 3600) % 24;
    let days = total / 86400;
    // days since epoch → year/month/day is complex; just show epoch offset for now.
    let _ = dt;
    format!("{days}d {h:02}:{m:02}:{s:02} UTC")
}

fn escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn html_fragment(body: String) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/html; charset=utf-8"),
        )
        .body(axum::body::Body::from(body))
        .unwrap()
}

struct UiError(anyhow::Error);

impl IntoResponse for UiError {
    fn into_response(self) -> Response {
        (StatusCode::INTERNAL_SERVER_ERROR, self.0.to_string()).into_response()
    }
}

impl<E: Into<anyhow::Error>> From<E> for UiError {
    fn from(e: E) -> Self {
        Self(e.into())
    }
}

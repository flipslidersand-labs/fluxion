<<<<<<< HEAD
use crate::api::ApiState;
=======
>>>>>>> e7c4a73 (feat(fluxion-host): htmx ダッシュボード HTML テンプレート（runs 一覧） (#101))
use axum::{
    Router,
    extract::{Path, State},
    http::{HeaderValue, StatusCode, header},
    response::{Html, IntoResponse, Response},
    routing::get,
};
<<<<<<< HEAD
=======
use crate::api::ApiState;
>>>>>>> e7c4a73 (feat(fluxion-host): htmx ダッシュボード HTML テンプレート（runs 一覧） (#101))

static INDEX_HTML: &str = include_str!("../assets/index.html");

pub fn router() -> Router<ApiState> {
    Router::new()
        .route("/", get(index))
        .route("/ui/runs", get(ui_runs))
        .route("/ui/runs/{id}", get(ui_run_jobs))
<<<<<<< HEAD
        .route("/ui/schedules", get(ui_schedules))
        .route("/ui/workers", get(ui_workers))
=======
>>>>>>> e7c4a73 (feat(fluxion-host): htmx ダッシュボード HTML テンプレート（runs 一覧） (#101))
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

<<<<<<< HEAD
// ── Runs ──────────────────────────────────────────────────────────────────────

async fn ui_runs(State(s): State<ApiState>) -> Result<impl IntoResponse, UiError> {
    let rows = s.store.lock().unwrap().list_runs(50)?;
    let mut body = String::from(
        "<table>\
           <thead><tr>\
             <th>Run ID</th><th>Workflow</th><th>Status</th><th>Started At</th>\
           </tr></thead>\
           <tbody>",
    );
    if rows.is_empty() {
        body.push_str("<tr><td colspan=\"4\">No runs yet.</td></tr>");
=======
async fn ui_runs(State(s): State<ApiState>) -> Result<impl IntoResponse, UiError> {
    let rows = s.store.lock().unwrap().list_runs(50)?;
    let mut html = String::new();
    if rows.is_empty() {
        html.push_str("<tr><td colspan=\"4\">No runs yet.</td></tr>");
>>>>>>> e7c4a73 (feat(fluxion-host): htmx ダッシュボード HTML テンプレート（runs 一覧） (#101))
    } else {
        for r in &rows {
            let ts = format_ts(r.started_at);
            let badge = status_badge(&r.status);
<<<<<<< HEAD
            body.push_str(&format!(
                "<tr>\
                   <td><a href=\"/ui/runs/{id}\" hx-get=\"/ui/runs/{id}\" hx-target=\"#content\">{id}</a></td>\
=======
            html.push_str(&format!(
                "<tr>\
                   <td><a href=\"/ui/runs/{id}\">{id}</a></td>\
>>>>>>> e7c4a73 (feat(fluxion-host): htmx ダッシュボード HTML テンプレート（runs 一覧） (#101))
                   <td>{name}</td>\
                   <td>{badge}</td>\
                   <td>{ts}</td>\
                 </tr>",
                id = escape(&r.id),
                name = escape(&r.workflow_name),
            ));
        }
    }
<<<<<<< HEAD
    body.push_str("</tbody></table>");
    // Wrap in a self-refreshing div (every 5s).
    Ok(html_fragment(polling_wrap("/ui/runs", "5s", &body)))
=======
    Ok(html_fragment(html))
>>>>>>> e7c4a73 (feat(fluxion-host): htmx ダッシュボード HTML テンプレート（runs 一覧） (#101))
}

async fn ui_run_jobs(
    State(s): State<ApiState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, UiError> {
    let jobs = s.store.lock().unwrap().get_run_jobs(&id)?;
<<<<<<< HEAD
    let mut body = String::from(
        "<table>\
           <thead><tr>\
             <th>Job</th><th>Status</th><th>Elapsed (ms)</th>\
           </tr></thead>\
           <tbody>",
    );
    if jobs.is_empty() {
        body.push_str("<tr><td colspan=\"3\">No jobs recorded.</td></tr>");
    } else {
        for j in &jobs {
            let badge = status_badge(&j.status);
            let elapsed = j.elapsed_ms.map(|ms| ms.to_string()).unwrap_or_default();
            body.push_str(&format!(
                "<tr>\
                   <td>{job}</td>\
                   <td>{badge}</td>\
                   <td>{elapsed}</td>\
=======
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
            let elapsed = j
                .elapsed_ms
                .map(|ms| ms.to_string())
                .unwrap_or_default();
            html.push_str(&format!(
                "<tr>\
                   <td style=\"padding:.4rem .8rem;border-bottom:1px solid #dee2e6\">{job}</td>\
                   <td style=\"padding:.4rem .8rem;border-bottom:1px solid #dee2e6\">{badge}</td>\
                   <td style=\"padding:.4rem .8rem;border-bottom:1px solid #dee2e6\">{elapsed}</td>\
>>>>>>> e7c4a73 (feat(fluxion-host): htmx ダッシュボード HTML テンプレート（runs 一覧） (#101))
                 </tr>",
                job = escape(&j.job_id),
            ));
        }
    }
<<<<<<< HEAD
    body.push_str("</tbody></table>");
    let url = format!("/ui/runs/{}", escape(&id));
    Ok(html_fragment(polling_wrap(&url, "5s", &body)))
}

// ── Schedules ─────────────────────────────────────────────────────────────────

async fn ui_schedules(State(s): State<ApiState>) -> Result<impl IntoResponse, UiError> {
    let rows = s.store.lock().unwrap().list_schedules()?;
    let mut body = String::from(
        "<table>\
           <thead><tr>\
             <th>ID</th><th>Workflow</th><th>Cron</th><th>Last Run</th><th>Next Run</th>\
           </tr></thead>\
           <tbody>",
    );
    if rows.is_empty() {
        body.push_str("<tr><td colspan=\"5\">No schedules registered.</td></tr>");
    } else {
        for s in &rows {
            let last = s.last_run_at.map(format_ts).unwrap_or_else(|| "—".to_string());
            body.push_str(&format!(
                "<tr>\
                   <td>{id}</td>\
                   <td>{path}</td>\
                   <td><code>{cron}</code></td>\
                   <td>{last}</td>\
                   <td>{next}</td>\
                 </tr>",
                id = escape(&s.id),
                path = escape(&s.workflow_path),
                cron = escape(&s.cron_expr),
                next = format_ts(s.next_run_at),
            ));
        }
    }
    body.push_str("</tbody></table>");
    Ok(html_fragment(polling_wrap("/ui/schedules", "10s", &body)))
}

// ── Workers ───────────────────────────────────────────────────────────────────

async fn ui_workers(State(s): State<ApiState>) -> Result<impl IntoResponse, UiError> {
    let rows = s.store.lock().unwrap().list_workers()?;
    let mut body = String::from(
        "<table>\
           <thead><tr>\
             <th>URL</th><th>Registered At</th><th>Health</th>\
           </tr></thead>\
           <tbody>",
    );
    if rows.is_empty() {
        body.push_str("<tr><td colspan=\"3\">No workers registered.</td></tr>");
    } else {
        for w in &rows {
            let health_badge = match w.last_health.as_deref() {
                Some("healthy") => "<span class=\"badge badge-healthy\">healthy</span>",
                Some("unreachable") => "<span class=\"badge badge-unreachable\">unreachable</span>",
                _ => "<span class=\"badge badge-unknown\">unknown</span>",
            };
            body.push_str(&format!(
                "<tr>\
                   <td>{url}</td>\
                   <td>{ts}</td>\
                   <td>{health_badge}</td>\
                 </tr>",
                url = escape(&w.url),
                ts = format_ts(w.registered_at),
            ));
        }
    }
    body.push_str("</tbody></table>");
    Ok(html_fragment(polling_wrap("/ui/workers", "10s", &body)))
}

// ── helpers ───────────────────────────────────────────────────────────────────

/// Wrap content in a div that re-fetches itself on the given interval.
fn polling_wrap(url: &str, interval: &str, inner: &str) -> String {
    format!(
        "<div hx-get=\"{url}\" hx-trigger=\"every {interval}\" hx-swap=\"outerHTML\">{inner}</div>",
    )
=======
    html.push_str("</tbody></table>");
    Ok(html_fragment(html))
>>>>>>> e7c4a73 (feat(fluxion-host): htmx ダッシュボード HTML テンプレート（runs 一覧） (#101))
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
<<<<<<< HEAD
    let s = secs % 60;
    let m = (secs / 60) % 60;
    let h = (secs / 3600) % 24;
    let days = secs / 86400;
=======
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
>>>>>>> e7c4a73 (feat(fluxion-host): htmx ダッシュボード HTML テンプレート（runs 一覧） (#101))
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
<<<<<<< HEAD
        .header(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/html; charset=utf-8"),
        )
=======
        .header(header::CONTENT_TYPE, HeaderValue::from_static("text/html; charset=utf-8"))
>>>>>>> e7c4a73 (feat(fluxion-host): htmx ダッシュボード HTML テンプレート（runs 一覧） (#101))
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
<<<<<<< HEAD
=======

>>>>>>> e7c4a73 (feat(fluxion-host): htmx ダッシュボード HTML テンプレート（runs 一覧） (#101))

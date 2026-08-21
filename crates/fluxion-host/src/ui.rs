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
        .route("/ui/runs/:id", get(ui_run_jobs))
        .route("/ui/schedules", get(ui_schedules))
        .route("/ui/workers", get(ui_workers))
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

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
    } else {
        for r in &rows {
            let ts = format_ts(r.started_at);
            let badge = status_badge(&r.status);
            body.push_str(&format!(
                "<tr>\
                   <td><a href=\"/ui/runs/{id}\" hx-get=\"/ui/runs/{id}\" hx-target=\"#content\">{id}</a></td>\
                   <td>{name}</td>\
                   <td>{badge}</td>\
                   <td>{ts}</td>\
                 </tr>",
                id = escape(&r.id),
                name = escape(&r.workflow_name),
            ));
        }
    }
    body.push_str("</tbody></table>");
    // Wrap in a self-refreshing div (every 5s).
    Ok(html_fragment(polling_wrap("/ui/runs", "5s", &body)))
}

async fn ui_run_jobs(
    State(s): State<ApiState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, UiError> {
    let jobs = s.store.lock().unwrap().get_run_jobs(&id)?;
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
                 </tr>",
                job = escape(&j.job_id),
            ));
        }
    }
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
            let last = s
                .last_run_at
                .map(format_ts)
                .unwrap_or_else(|| "—".to_string());
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
    let s = secs % 60;
    let m = (secs / 60) % 60;
    let h = (secs / 3600) % 24;
    let days = secs / 86400;
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use fluxion_core::store::RunStore;
    use http_body_util::BodyExt;
    use rusqlite::Connection;
    use tower::ServiceExt;

    use super::{escape, format_ts, polling_wrap, status_badge};
    use crate::api::{ApiState, router};

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

    async fn response_body(body: Body) -> String {
        let bytes = body.collect().await.unwrap().to_bytes().to_vec();
        String::from_utf8(bytes).unwrap()
    }

    // ── helper unit tests ────────────────────────────────────────────────────

    #[test]
    fn escape_replaces_html_special_chars() {
        assert_eq!(escape("<script>"), "&lt;script&gt;");
        assert_eq!(escape("a&b"), "a&amp;b");
        assert_eq!(escape("\"quoted\""), "&quot;quoted&quot;");
    }

    #[test]
    fn format_ts_formats_seconds_correctly() {
        // 1 day + 2h + 3m + 4s = 93784s
        assert_eq!(format_ts(93784), "1d 02:03:04 UTC");
        assert_eq!(format_ts(0), "0d 00:00:00 UTC");
    }

    #[test]
    fn status_badge_uses_correct_css_class() {
        assert!(status_badge("running").contains("badge-running"));
        assert!(status_badge("succeeded").contains("badge-succeeded"));
        assert!(status_badge("failed").contains("badge-failed"));
        assert!(status_badge("pending").contains("badge-pending"));
    }

    #[test]
    fn polling_wrap_contains_url_and_interval() {
        let html = polling_wrap("/ui/runs", "5s", "<p>inner</p>");
        assert!(html.contains("hx-get=\"/ui/runs\""));
        assert!(html.contains("every 5s"));
        assert!(html.contains("<p>inner</p>"));
    }

    // ── route tests ─────────────────────────────────────────────────────────

    #[tokio::test]
    async fn index_returns_html() {
        let app = router(test_state());
        let resp = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let ct = resp
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(ct.contains("text/html"));
    }

    #[tokio::test]
    async fn ui_runs_returns_html_table() {
        let app = router(test_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/ui/runs")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = response_body(resp.into_body()).await;
        assert!(body.contains("<table>"));
        assert!(body.contains("No runs yet."));
    }

    #[tokio::test]
    async fn ui_schedules_returns_html_table() {
        let app = router(test_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/ui/schedules")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = response_body(resp.into_body()).await;
        assert!(body.contains("<table>"));
        assert!(body.contains("No schedules registered."));
    }

    #[tokio::test]
    async fn ui_workers_returns_html_table() {
        let app = router(test_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/ui/workers")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = response_body(resp.into_body()).await;
        assert!(body.contains("<table>"));
        assert!(body.contains("No workers registered."));
    }

    #[tokio::test]
    async fn ui_run_jobs_returns_html_for_unknown_run() {
        let app = router(test_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/ui/runs/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = response_body(resp.into_body()).await;
        assert!(body.contains("No jobs recorded."));
    }
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

use prometheus::{CounterVec, Encoder, Gauge, GaugeVec, HistogramVec, TextEncoder};
use std::sync::LazyLock;

pub static JOBS_TOTAL: LazyLock<CounterVec> = LazyLock::new(|| {
    prometheus::register_counter_vec!(
        "fluxion_jobs_total",
        "Total number of completed jobs",
        &["status", "job_name"]
    )
    .unwrap()
});

pub static JOB_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    prometheus::register_histogram_vec!(
        prometheus::histogram_opts!(
            "fluxion_job_duration_seconds",
            "Job execution duration in seconds",
            vec![
                0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0
            ]
        ),
        &["job_name"]
    )
    .unwrap()
});

pub static ACTIVE_JOBS: LazyLock<Gauge> = LazyLock::new(|| {
    prometheus::register_gauge!("fluxion_active_jobs", "Number of currently running jobs").unwrap()
});

pub static WORKER_HEALTH: LazyLock<GaugeVec> = LazyLock::new(|| {
    prometheus::register_gauge_vec!(
        "fluxion_worker_health",
        "Worker node health (1 = reachable, 0 = unreachable)",
        &["worker_url"]
    )
    .unwrap()
});

/// Render all registered metrics in Prometheus text format.
pub fn gather() -> String {
    // Touch each static to ensure registration before the first scrape.
    let _ = &*JOBS_TOTAL;
    let _ = &*JOB_DURATION;
    let _ = &*ACTIVE_JOBS;
    let _ = &*WORKER_HEALTH;

    let encoder = TextEncoder::new();
    let families = prometheus::gather();
    let mut buf = Vec::new();
    encoder.encode(&families, &mut buf).unwrap_or_default();
    String::from_utf8(buf).unwrap_or_default()
}

/// Serve `/metrics` on the given port (blocking; run in a spawned task).
pub async fn serve(port: u16) -> anyhow::Result<()> {
    use axum::{Router, routing::get};
    let app = Router::new().route("/metrics", get(|| async { gather() }));
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    tracing::info!("Prometheus metrics on :{port}/metrics");
    axum::serve(listener, app).await?;
    Ok(())
}

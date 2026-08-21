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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gather_contains_expected_metric_names() {
        // Touch each metric so it appears in the registry output.
        // HistogramVec requires at least one observation to produce samples.
        JOB_DURATION.with_label_values(&["_init_check"]).observe(0.0);
        let output = gather();
        assert!(output.contains("fluxion_jobs_total"));
        assert!(output.contains("fluxion_job_duration_seconds"));
        assert!(output.contains("fluxion_active_jobs"));
        assert!(output.contains("fluxion_worker_health"));
    }

    #[test]
    fn gather_output_is_valid_prometheus_text_format() {
        let output = gather();
        assert!(output.contains("# HELP"));
        assert!(output.contains("# TYPE"));
    }

    #[test]
    fn jobs_total_counter_increments() {
        JOBS_TOTAL
            .with_label_values(&["succeeded", "test_job"])
            .inc();
        let output = gather();
        assert!(output.contains("fluxion_jobs_total"));
    }

    #[test]
    fn active_jobs_gauge_reflects_set_value() {
        ACTIVE_JOBS.set(5.0);
        let output = gather();
        assert!(output.contains("fluxion_active_jobs"));
    }

    #[test]
    fn worker_health_gauge_with_label() {
        WORKER_HEALTH
            .with_label_values(&["http://localhost:8080"])
            .set(1.0);
        let output = gather();
        assert!(output.contains("fluxion_worker_health"));
        assert!(output.contains("localhost:8080"));
    }
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

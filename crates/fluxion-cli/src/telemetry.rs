use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::TracerProvider;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

/// Initialize tracing. If `otel` is true, attach an OTEL stdout exporter.
/// Returns the provider so the caller can shut it down cleanly.
pub fn init(otel: bool) -> Option<TracerProvider> {
    // Default: show fluxion INFO+, silence noisy upstream crates.
    // Override with RUST_LOG env var.
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("fluxion=info,warn"));

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(false)
        .compact();

    if otel {
        let exporter = opentelemetry_stdout::SpanExporter::default();
        let provider = TracerProvider::builder()
            .with_simple_exporter(exporter)
            .build();

        opentelemetry::global::set_tracer_provider(provider.clone());

        // A named tracer is required for the bridge to forward spans to the exporter.
        let tracer = provider.tracer("fluxion");
        let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

        tracing_subscriber::registry()
            .with(filter)
            .with(fmt_layer)
            .with(otel_layer)
            .init();

        Some(provider)
    } else {
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt_layer)
            .init();

        None
    }
}

pub fn shutdown(provider: Option<TracerProvider>) {
    if let Some(p) = provider
        && let Err(e) = p.shutdown()
    {
        eprintln!("Tracer shutdown error: {e}");
    }
    opentelemetry::global::shutdown_tracer_provider();
}

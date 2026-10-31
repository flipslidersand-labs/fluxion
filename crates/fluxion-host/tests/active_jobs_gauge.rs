//! #296: `fluxion_active_jobs` must return to its starting value after a job
//! that fails before execution (digest mismatch), not go negative.
//!
//! Lives in its own integration-test binary so no other test touches the
//! process-global gauge concurrently.

use std::io::Write;
use std::sync::Arc;

use fluxion_core::store::RunStore;
use fluxion_core::workflow::Workflow;
use fluxion_host::{FluxionHost, metrics::ACTIVE_JOBS, scheduler};

#[tokio::test]
async fn early_launch_failure_leaves_active_jobs_at_zero() {
    let home = tempfile::tempdir().unwrap();
    let store = RunStore::open_at(&home.path().join("runs.db")).unwrap();

    let mut wasm = tempfile::NamedTempFile::new().unwrap();
    wasm.write_all(b"\0asm").unwrap();
    let wf_json = serde_json::json!({
        "name": "t",
        "jobs": {
            "j": {
                "component": wasm.path().to_string_lossy(),
                // Valid-looking hex that cannot match the file contents.
                "component_sha256": "0".repeat(64),
            }
        }
    });
    let wf: Workflow = serde_json::from_value(wf_json).unwrap();

    let before = ACTIVE_JOBS.get();
    let host = Arc::new(FluxionHost::with_cache_dir(home.path().join("cache")).unwrap());
    let res = scheduler::run_with_store(
        &wf,
        home.path().join("wf.yaml").as_path(),
        host,
        &scheduler::RunOptions {
            progress: false,
            ..Default::default()
        },
        &store,
    )
    .await
    .unwrap();
    assert!(!res.success, "digest mismatch must fail the run");
    assert_eq!(
        ACTIVE_JOBS.get(),
        before,
        "ACTIVE_JOBS must be balanced after an early launch failure"
    );
}

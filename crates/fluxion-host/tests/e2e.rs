use std::path::PathBuf;
use std::sync::Arc;

use fluxion_core::store::RunStore;
use fluxion_core::workflow::Workflow;
use fluxion_host::{FluxionHost, scheduler};

fn silent() -> scheduler::RunOptions {
    scheduler::RunOptions {
        progress: false,
        ..Default::default()
    }
}

/// Host with a tempdir compiled-component cache (kept alive by the guard), so e2e
/// runs never touch the user's real cache location (#403).
fn test_host() -> (tempfile::TempDir, Arc<FluxionHost>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let host = Arc::new(FluxionHost::with_cache_dir(dir.path()).expect("host"));
    (dir, host)
}

/// Fresh RunStore in a tempdir, so e2e runs never touch the real `~/.fluxion` (#292).
fn temp_store() -> (tempfile::TempDir, RunStore) {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = RunStore::open_at(&dir.path().join("runs.db")).expect("open store");
    (dir, store)
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root")
}

fn wasm(component: &str) -> String {
    let bin = component.replace('-', "_");
    workspace_root()
        .join("components")
        .join(component)
        .join(format!("target/wasm32-wasip1/debug/{bin}.wasm"))
        .to_string_lossy()
        .into_owned()
}

fn wasm2(component: &str) -> String {
    let bin = component.replace('-', "_");
    workspace_root()
        .join("components")
        .join(component)
        .join(format!("target/wasm32-wasip2/debug/{bin}.wasm"))
        .to_string_lossy()
        .into_owned()
}

// Load a YAML workflow and patch all component paths to absolute.
fn load_wf(yaml: &str, component: &str) -> (Workflow, PathBuf) {
    let wf_path = workspace_root().join("examples").join(yaml);
    let mut wf = Workflow::from_file(&wf_path).expect("load yaml");
    let abs = wasm(component);
    for job in wf.jobs.values_mut() {
        job.component = abs.clone();
    }
    (wf, wf_path)
}

// Patch filesystem paths in vehicle-pipeline to use a custom tmp dir.
fn patch_pipeline_paths(wf: &mut Workflow, data_dir: &str, out_dir: &str) {
    for job in wf.jobs.values_mut() {
        if let Some(input) = &job.input {
            job.input = Some(
                input
                    .replace("/tmp/fluxion-pipeline", data_dir)
                    .replace("/tmp/fluxion-output", out_dir),
            );
        }
        for p in job.permissions.filesystem.read.iter_mut() {
            let s = p.to_string_lossy().into_owned();
            *p = PathBuf::from(s.replace("/tmp/fluxion-pipeline", data_dir));
        }
        for p in job.permissions.filesystem.write.iter_mut() {
            let s = p.to_string_lossy().into_owned();
            *p = PathBuf::from(
                s.replace("/tmp/fluxion-pipeline", data_dir)
                    .replace("/tmp/fluxion-output", out_dir),
            );
        }
    }
}

/// vehicle-pipeline: first run fails at validate (row 184 has year=1999),
/// then we fix normalized.csv and retry from validate — expect full success.
#[tokio::test]
#[cfg_attr(not(feature = "ci"), ignore = "requires pre-built Wasm components")]
async fn vehicle_pipeline_validate_retry() {
    let data_dir = format!("/tmp/fluxion-e2e-{}-pipeline", std::process::id());
    let out_dir = format!("/tmp/fluxion-e2e-{}-output", std::process::id());
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&out_dir).unwrap();

    let (_cache, host) = test_host();
    let (mut wf, wf_path) = load_wf("vehicle-pipeline.yaml", "pipeline-stage");
    patch_pipeline_paths(&mut wf, &data_dir, &out_dir);

    // First run: fetch and normalize succeed, validate fails (year=1999 in row 184).
    let (_dir, store) = temp_store();
    let r1 = scheduler::run_with_store(&wf, &wf_path, host.clone(), &silent(), &store)
        .await
        .unwrap();
    assert!(!r1.success, "first run should fail at validate");
    let failed = r1
        .jobs
        .iter()
        .find(|j| j.status == "failed")
        .expect("failed job");
    assert_eq!(failed.job_id, "validate");
    assert!(
        failed.reason.as_deref().unwrap_or("").contains("1999"),
        "reason should mention the bad year"
    );

    // Fix: replace the invalid year in normalized.csv.
    let norm = format!("{}/normalized.csv", data_dir);
    let content = std::fs::read_to_string(&norm).unwrap();
    std::fs::write(&norm, content.replacen(",1999,", ",2019,", 1)).unwrap();

    // Retry from validate: only validate + export re-run.
    let r2 = scheduler::retry_with_store(
        &wf,
        &wf_path,
        host,
        &r1.run_id,
        "validate",
        &silent(),
        &store,
    )
    .await
    .unwrap();
    assert!(
        r2.success,
        "retry should succeed after fixing the year: {:?}",
        r2
    );

    let _ = std::fs::remove_dir_all(&data_dir);
    let _ = std::fs::remove_dir_all(&out_dir);
}

/// resource-limits-demo: spin-forever is killed by epoch timeout after 2s;
/// fast-sum (short run) is expected to have been in-flight or succeeded.
#[tokio::test]
#[cfg_attr(not(feature = "ci"), ignore = "requires pre-built Wasm components")]
async fn resource_limits_spin_timeout() {
    let (_cache, host) = test_host();
    let (wf, wf_path) = load_wf("resource-limits-demo.yaml", "spin");

    let (_dir, store) = temp_store();
    let result = scheduler::run_with_store(&wf, &wf_path, host, &silent(), &store)
        .await
        .unwrap();

    assert!(
        !result.success,
        "workflow should fail due to spin-forever timeout"
    );
    let spin = result
        .jobs
        .iter()
        .find(|j| j.job_id == "spin-forever")
        .expect("spin-forever in results");
    assert_eq!(
        spin.status, "failed",
        "spin-forever should be killed by epoch timeout"
    );
    // Assert the kill mechanism directly: the epoch interrupt must be what
    // ended the job, not some unrelated failure.
    let reason = spin.reason.as_deref().unwrap_or_default();
    assert!(
        reason.contains("interrupt"),
        "spin-forever should be killed by the epoch interrupt, got reason: {reason:?}"
    );
    // The epoch deadline cannot interrupt native (Cranelift) compilation, so
    // elapsed = cold compile + ~timeout. On a loaded shared runner compile
    // alone can take several seconds (#267), so this bound only guards
    // against falling back to the 60s default or running unbounded.
    assert!(
        spin.elapsed_ms < 30_000,
        "epoch interruption should kill the job well under 30s, got {}ms",
        spin.elapsed_ms
    );
}

/// three-stage: simple sequential pipeline — all 3 hello jobs succeed in order.
#[tokio::test]
#[cfg_attr(not(feature = "ci"), ignore = "requires pre-built Wasm components")]
async fn three_stage_sequential() {
    let (_cache, host) = test_host();
    let wf_path = workspace_root().join("examples").join("three-stage.yaml");
    let mut wf = Workflow::from_file(&wf_path).expect("load yaml");
    let hello = wasm("hello");
    for job in wf.jobs.values_mut() {
        job.component = hello.clone();
    }

    let (_dir, store) = temp_store();
    let result = scheduler::run_with_store(&wf, &wf_path, host, &silent(), &store)
        .await
        .unwrap();

    assert!(
        result.success,
        "all three stages should succeed: {:?}",
        result
    );
    assert_eq!(result.jobs.len(), 3, "expected 3 job results");
    for job in &result.jobs {
        assert_eq!(job.status, "succeeded", "job {} should succeed", job.job_id);
    }
}

/// sandbox-demo: read-allowed succeeds (FS cap grants /tmp),
/// read-denied fails (no filesystem permission granted).
#[tokio::test]
#[cfg_attr(not(feature = "ci"), ignore = "requires pre-built Wasm components")]
async fn sandbox_fs_cap() {
    let test_file = format!("/tmp/fluxion-e2e-{}.txt", std::process::id());
    std::fs::write(&test_file, "hello from e2e test").unwrap();

    let (_cache, host) = test_host();
    let wf_path = workspace_root().join("examples").join("sandbox-demo.yaml");
    let mut wf = Workflow::from_file(&wf_path).expect("load yaml");
    let file_reader = wasm("file-reader");
    for job in wf.jobs.values_mut() {
        job.component = file_reader.clone();
        if let Some(input) = &job.input {
            job.input = Some(input.replace("/tmp/fluxion-test.txt", &test_file));
        }
    }

    let (_dir, store) = temp_store();
    let result = scheduler::run_with_store(&wf, &wf_path, host, &silent(), &store)
        .await
        .unwrap();

    assert!(!result.success, "workflow should fail at read-denied");
    let allowed = result
        .jobs
        .iter()
        .find(|j| j.job_id == "read-allowed")
        .expect("read-allowed");
    assert_eq!(
        allowed.status, "succeeded",
        "read-allowed should succeed with /tmp permission"
    );
    let denied = result
        .jobs
        .iter()
        .find(|j| j.job_id == "read-denied")
        .expect("read-denied");
    assert_eq!(
        denied.status, "failed",
        "read-denied should fail without filesystem permission"
    );

    let _ = std::fs::remove_file(&test_file);
}

/// network-sandbox: empty allowlist blocks connection before reaching the OS
/// (the security-critical property). The ALLOW side is not tested here because
/// Tokio's spawn_blocking + h.block_on interaction with sync WASI sockets causes
/// timeouts in headless CI; it is covered by local integration runs.
#[tokio::test]
#[cfg_attr(not(feature = "ci"), ignore = "requires pre-built Wasm components")]
async fn sandbox_network_cap() {
    // Run only the deny job — it should be blocked by the empty allowlist before
    // any real I/O occurs, so it completes instantly without network access.
    let (_cache, host) = test_host();
    let wf_path = workspace_root()
        .join("examples")
        .join("network-sandbox.yaml");
    let mut wf = Workflow::from_file(&wf_path).expect("load yaml");
    let probe = wasm2("network-probe");

    // Remove connect-allowed and its dependency from the workflow.
    wf.jobs.shift_remove("connect-allowed");
    let denied_job = wf
        .jobs
        .get_mut("connect-denied")
        .expect("connect-denied job");
    denied_job.component = probe;
    denied_job.depends_on.clear();
    // Use any address — the sandbox check fires before the connect reaches the OS.
    denied_job.input = Some("127.0.0.1:19999".to_string());

    let (_dir, store) = temp_store();
    let result = scheduler::run_with_store(&wf, &wf_path, host, &silent(), &store)
        .await
        .unwrap();

    assert!(
        !result.success,
        "workflow should fail because connect-denied is blocked"
    );
    let denied = result
        .jobs
        .iter()
        .find(|j| j.job_id == "connect-denied")
        .expect("connect-denied");
    assert_eq!(
        denied.status, "failed",
        "connect-denied should be blocked by empty allowlist: reason={:?}",
        denied.reason
    );
}

/// memory-limits-demo: ok-job (alloc 1MB within 16MB limit) succeeds,
/// then oom-job (alloc 10MB within 1MB limit) is rejected by StoreLimits.
#[tokio::test]
#[cfg_attr(not(feature = "ci"), ignore = "requires pre-built Wasm components")]
async fn memory_limits_oom_enforcement() {
    let (_cache, host) = test_host();
    let (wf, wf_path) = load_wf("memory-limits-demo.yaml", "alloc-bomb");

    let (_dir, store) = temp_store();
    let result = scheduler::run_with_store(&wf, &wf_path, host, &silent(), &store)
        .await
        .unwrap();

    assert!(!result.success, "workflow should fail due to oom-job OOM");

    let ok = result
        .jobs
        .iter()
        .find(|j| j.job_id == "ok-job")
        .expect("ok-job result");
    assert_eq!(
        ok.status, "succeeded",
        "ok-job should succeed (1MB within 16MB limit)"
    );

    let oom = result
        .jobs
        .iter()
        .find(|j| j.job_id == "oom-job")
        .expect("oom-job result");
    assert_eq!(
        oom.status, "failed",
        "oom-job should fail (10MB exceeds 1MB limit)"
    );
    assert!(
        oom.reason
            .as_deref()
            .unwrap_or("")
            .to_lowercase()
            .contains("oom")
            || oom
                .reason
                .as_deref()
                .unwrap_or("")
                .to_lowercase()
                .contains("memory"),
        "failure reason should mention OOM/memory: {:?}",
        oom.reason
    );
}

/// map-reduce-demo: static foreach transform (3 items) → json_array aggregate.
/// Verifies the full Map/Reduce pipeline executes and each stage produces output.
#[tokio::test]
#[cfg_attr(not(feature = "ci"), ignore = "requires pre-built Wasm components")]
async fn map_reduce_static_foreach() {
    let (_cache, host) = test_host();
    let wf_path = workspace_root().join("examples/map-reduce/workflow.yaml");
    let mut wf = Workflow::from_file(&wf_path).expect("load map-reduce yaml");

    // Patch all components to the pre-built hello.wasm.
    let hello = wasm("hello");
    for job in wf.jobs.values_mut() {
        job.component = hello.clone();
    }

    let (_dir, store) = temp_store();
    let result = scheduler::run_with_store(&wf, &wf_path, host, &silent(), &store)
        .await
        .expect("run_silent");

    assert!(
        result.success,
        "map-reduce workflow should succeed; jobs={:?}",
        result.jobs
    );

    // 3 static transform children + aggregate = at least 4 jobs
    assert!(
        result.jobs.len() >= 4,
        "expected ≥4 job results (3×transform + aggregate), got {}",
        result.jobs.len()
    );

    // All jobs must have succeeded (not failed/skipped).
    for j in &result.jobs {
        assert_eq!(
            j.status, "succeeded",
            "job {} should have succeeded, got {:?}",
            j.job_id, j.status
        );
    }

    // The aggregate job must be present and succeeded.
    let agg = result
        .jobs
        .iter()
        .find(|j| j.job_id == "aggregate")
        .expect("aggregate job in results");
    assert_eq!(agg.status, "succeeded", "aggregate job must succeed");
}

/// python-hello: verify `fluxion build python` succeeds via componentize-py.
/// This test confirms the CLI build pipeline works end-to-end in CI.
/// Running the resulting component via FluxionHost is a separate concern:
/// componentize-py components require an internal `init` call before WIT exports
/// are callable, which the host does not currently perform.
/// Requires componentize-py to be installed; skipped in non-ci builds.
#[tokio::test]
#[cfg_attr(not(feature = "ci"), ignore = "requires componentize-py")]
async fn python_hello_build() {
    let root = workspace_root();
    let script = root.join("components/python-hello/task.py");
    let out = root.join("components/python-hello/hello.wasm");
    let wit = root.join("wit");

    // Build: fluxion build python task.py -o hello.wasm --wit-path wit
    let build_out = std::process::Command::new(root.join("target/debug/fluxion"))
        .args(["build", "python"])
        .arg(&script)
        .arg("-o")
        .arg(&out)
        .arg("--wit-path")
        .arg(&wit)
        .output()
        .expect("fluxion build python failed to launch");

    assert!(
        build_out.status.success(),
        "fluxion build python exited with {}\nstdout: {}\nstderr: {}",
        build_out.status,
        String::from_utf8_lossy(&build_out.stdout),
        String::from_utf8_lossy(&build_out.stderr)
    );

    assert!(out.exists(), "hello.wasm was not created");

    let wasm_bytes = std::fs::read(&out).expect("read hello.wasm");
    assert!(wasm_bytes.len() > 1024, "hello.wasm is suspiciously small");
}

/// Minimal worker that answers every job with `output` (and accepts CAS
/// HEAD/PUT), so one job of an otherwise real-Wasm workflow can emit JSON.
async fn spawn_json_worker(output: &'static str) -> String {
    use axum::{
        Json, Router,
        http::StatusCode,
        routing::{get, head, post, put},
    };
    use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
    let app = Router::new()
        .route("/health", get(|| async { StatusCode::OK }))
        .route("/components/:sha", head(|| async { StatusCode::NOT_FOUND }))
        .route("/components/:sha", put(|| async { StatusCode::OK }))
        .route(
            "/run",
            post(move || async move {
                Json(serde_json::json!({
                    "output": B64.encode(output),
                    "compile_ms": 0, "instantiate_ms": 0, "execute_ms": 0
                }))
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

/// #316: examples/dynamic-fanout.yaml with real hello.wasm for process/aggregate.
/// `fetch` (hello.wasm emits text, not JSON) is served by a mock worker that
/// returns a 3-element array, so `process` must expand to process.0/1/2.
#[tokio::test]
#[cfg_attr(not(feature = "ci"), ignore = "requires pre-built Wasm components")]
async fn dynamic_fanout_expands_three_children_and_aggregates() {
    let (_cache, host) = test_host();
    let (mut wf, wf_path) = load_wf("dynamic-fanout.yaml", "hello");
    let url = spawn_json_worker(r#"{"items": ["a", "b", "c"]}"#).await;
    let fetch = wf.jobs.get_mut("fetch").expect("fetch job");
    fetch.executor = fluxion_core::workflow::ExecutorKind::Remote;
    fetch.worker = Some(url);

    let (_dir, store) = temp_store();
    let result = scheduler::run_with_store(&wf, &wf_path, host, &silent(), &store)
        .await
        .expect("run");

    assert!(result.success, "jobs={:?}", result.jobs);
    let mut ids: Vec<&str> = result.jobs.iter().map(|j| j.job_id.as_str()).collect();
    ids.sort();
    assert_eq!(
        ids,
        ["aggregate", "fetch", "process.0", "process.1", "process.2"]
    );
    assert!(result.jobs.iter().all(|j| j.status == "succeeded"));
}

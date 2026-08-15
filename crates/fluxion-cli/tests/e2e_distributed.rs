//! Distributed E2E tests — fluxion worker serve --async-jobs + remote executor (#111).
//!
//! Tests are gated on `feature = "ci"` because they require pre-built Wasm
//! components (built by the e2e CI job) and spawn a live worker process.

use assert_cmd::Command;
use std::io::Write;
use std::time::Duration;
use tempfile::NamedTempFile;

fn fluxion() -> Command {
    Command::cargo_bin("fluxion").expect("fluxion binary not found")
}

fn write_yaml(content: &str) -> NamedTempFile {
    let mut f = NamedTempFile::new().expect("tempfile");
    f.write_all(content.as_bytes()).expect("write");
    f
}

fn workspace_root() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root")
}

fn hello_wasm() -> String {
    workspace_root()
        .join("components/hello/target/wasm32-wasip1/debug/hello.wasm")
        .to_string_lossy()
        .into_owned()
}

/// Wait until the worker's /health endpoint responds OK (up to `timeout`).
fn wait_for_worker(port: u16, timeout: Duration) {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if let Ok(resp) = reqwest::blocking::get(format!("http://127.0.0.1:{port}/health")) {
            if resp.status().is_success() {
                return;
            }
        }
        assert!(std::time::Instant::now() < deadline, "worker did not start in time");
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Two-job remote workflow: both jobs use executor: remote against a local worker.
#[test]
#[cfg_attr(not(feature = "ci"), ignore = "requires pre-built Wasm components")]
fn two_job_remote_workflow_both_succeed() {
    let port: u16 = 17777;
    let hello = hello_wasm();

    // Start worker with --async-jobs on a background thread.
    let mut worker = std::process::Command::new(
        assert_cmd::cargo::cargo_bin("fluxion"),
    )
    .args(["worker", "serve", "--port", &port.to_string(), "--async-jobs"])
    .spawn()
    .expect("spawn worker");

    // Wait for the worker to be ready.
    wait_for_worker(port, Duration::from_secs(10));

    // Register the worker so the scheduler can find it.
    fluxion()
        .args(["worker", "register", &format!("http://127.0.0.1:{port}")])
        .assert()
        .success();

    let yaml = format!(
        r#"
name: two-stage-remote
jobs:
  stage1:
    component: "{hello}"
    executor: remote
    input: "hello from stage1"
  stage2:
    component: "{hello}"
    executor: remote
    depends_on: [stage1]
    input: "hello from stage2"
"#
    );
    let wf = write_yaml(&yaml);

    let output = fluxion()
        .args(["run", wf.path().to_str().unwrap()])
        .output()
        .expect("run workflow");

    // Cleanup: deregister worker and kill process.
    let _ = fluxion()
        .args(["worker", "remove", &format!("http://127.0.0.1:{port}")])
        .output();
    worker.kill().ok();
    worker.wait().ok();

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "workflow should succeed\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stdout.contains("succeeded") || stderr.contains("succeeded"),
        "expected 'succeeded' in output\nstdout: {stdout}\nstderr: {stderr}"
    );
}

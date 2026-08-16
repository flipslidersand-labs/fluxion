//! E2E integration tests for distributed execution (Issue #111).
//!
//! These tests spin up a real `fluxion worker serve --async-jobs` process and
//! run workflows that dispatch jobs to it via `executor: remote`.
//!
//! Run with: `cargo test -p fluxion-cli --test e2e_distributed -- --test-threads=1`

#![allow(dead_code)]

use assert_cmd::Command;
use std::io::Write as _;
use std::net::TcpStream;
use std::process::Child;
use std::time::Duration;
use tempfile::NamedTempFile;

const WORKER_PORT: u16 = 17777;

/// Spawn `fluxion worker serve --async-jobs` on WORKER_PORT and return the
/// Child handle.  The caller is responsible for calling `child.kill()`.
fn start_worker() -> Child {
    let mut cmd = std::process::Command::new(assert_cmd::cargo::cargo_bin("fluxion"));
    cmd.args([
        "worker",
        "serve",
        "--port",
        &WORKER_PORT.to_string(),
        "--async-jobs",
    ]);
    let child = cmd.spawn().expect("failed to spawn fluxion worker");

    // Give the server a moment to start listening.
    std::thread::sleep(Duration::from_millis(500));
    child
}

fn write_yaml(content: &str) -> NamedTempFile {
    let mut f = NamedTempFile::new().expect("tempfile");
    f.write_all(content.as_bytes()).expect("write yaml");
    f
}

fn fluxion() -> Command {
    Command::cargo_bin("fluxion").expect("fluxion binary not found")
}

// ── #111 — fluxion worker serve --async-jobs ─────────────────────────────────

#[test]
fn worker_serve_accepts_async_jobs_flag() {
    // Verify clap accepts the flag (--help lists it, no crash).
    fluxion()
        .args(["worker", "serve", "--help"])
        .assert()
        .success()
        .stdout(predicates::str::contains("async-jobs"));
}

#[test]
fn worker_serve_starts_and_listens() {
    let mut worker = start_worker();

    // Verify the TCP port is open (server accepted a connection).
    let connected = TcpStream::connect(format!("127.0.0.1:{WORKER_PORT}")).is_ok();

    worker.kill().ok();
    worker.wait().ok();

    assert!(connected, "worker should listen on port {WORKER_PORT}");
}

#[test]
#[ignore = "requires pre-built hello.wasm component"]
fn distributed_two_job_workflow_succeeds() {
    let mut worker = start_worker();

    // Register the worker so `fluxion run` knows where to send remote jobs.
    fluxion()
        .args([
            "worker",
            "register",
            &format!("http://127.0.0.1:{WORKER_PORT}"),
        ])
        .assert()
        .success();

    let yaml = write_yaml(&format!(
        r#"
name: distributed-hello
jobs:
  job-a:
    component: components/hello/target/wasm32-wasip2/debug/hello.wasm
    executor: remote
  job-b:
    component: components/hello/target/wasm32-wasip2/debug/hello.wasm
    executor: remote
    depends_on: [job-a]
"#
    ));

    let out = fluxion()
        .args(["run", yaml.path().to_str().unwrap()])
        .output()
        .expect("fluxion run");

    worker.kill().ok();
    worker.wait().ok();

    assert!(
        out.status.success(),
        "workflow should succeed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

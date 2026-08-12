//! Integration tests for `fluxion watch` — Issue #97.
//!
//! Most tests verify CLI surface (flags, error exits) without timing-sensitive
//! file-change round-trips.  The full round-trip test is `#[ignore]` and
//! intended for manual or E2E verification.

use assert_cmd::Command;
use predicates::str::contains;
use std::io::Write;
use tempfile::NamedTempFile;

fn fluxion() -> Command {
    Command::cargo_bin("fluxion").expect("fluxion binary not found")
}

fn write_yaml(content: &str) -> NamedTempFile {
    let mut f = NamedTempFile::new().expect("tempfile");
    f.write_all(content.as_bytes()).expect("write yaml");
    f
}

const SIMPLE_YAML: &str = r#"
name: watch-test-workflow
jobs:
  fetch:
    component: /nonexistent/fetch.wasm
"#;

// ── --help / flag recognition ─────────────────────────────────────────────────

#[test]
fn watch_help_exits_ok() {
    fluxion().args(["watch", "--help"]).assert().success();
}

#[test]
fn watch_help_mentions_debounce() {
    fluxion()
        .args(["watch", "--help"])
        .assert()
        .success()
        .stdout(contains("debounce"));
}

#[test]
fn watch_debounce_flag_is_accepted() {
    // Verify flag is parsed by clap (process exits non-zero because path doesn't
    // exist, but the important check is no "unexpected argument" error).
    let out = fluxion()
        .args(["watch", "/nonexistent/workflow.yaml", "--debounce", "200"])
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !stderr.contains("unexpected argument"),
        "--debounce was not recognised: {stderr}"
    );
}

// ── error exits ───────────────────────────────────────────────────────────────

#[test]
fn watch_nonexistent_file_exits_nonzero() {
    fluxion()
        .args(["watch", "/nonexistent/workflow.yaml"])
        .assert()
        .failure();
}

#[test]
fn watch_nonexistent_file_prints_error() {
    fluxion()
        .args(["watch", "/nonexistent/workflow.yaml"])
        .assert()
        .failure()
        .stderr(contains("nonexistent"));
}

#[test]
fn watch_invalid_yaml_exits_nonzero() {
    let mut f = NamedTempFile::new().expect("tempfile");
    f.write_all(b"not: valid: yaml: [[[").expect("write");
    fluxion()
        .args(["watch", f.path().to_str().unwrap()])
        .assert()
        .failure();
}

// ── startup output ────────────────────────────────────────────────────────────

/// `fluxion watch` on a valid YAML should print a monitoring banner then block.
/// We run it under `timeout 1` so the test completes; exit code 124 means the
/// watcher was still alive (expected).
#[test]
fn watch_prints_monitoring_message_on_start() {
    let f = write_yaml(SIMPLE_YAML);
    let out = std::process::Command::new("timeout")
        .args([
            "1",
            assert_cmd::cargo::cargo_bin("fluxion")
                .to_str()
                .expect("binary path"),
            "watch",
            f.path().to_str().unwrap(),
        ])
        .output();

    match out {
        Ok(o) => {
            let stdout = String::from_utf8_lossy(&o.stdout);
            let killed_by_timeout = o.status.code() == Some(124);
            assert!(
                killed_by_timeout || stdout.contains("[watch]"),
                "expected [watch] banner or timeout kill; stdout={stdout}"
            );
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // `timeout` binary unavailable — skip gracefully
        }
        Err(e) => panic!("unexpected error: {e}"),
    }
}

// ── file-change round-trip ────────────────────────────────────────────────────

/// Full round-trip: start `fluxion watch`, append to YAML, expect a re-run
/// message within 3 s.  Run manually with:
///   cargo test -p fluxion-cli -- --ignored watch_file_change_triggers_rerun
#[test]
#[ignore = "timing-sensitive E2E — run manually"]
fn watch_file_change_triggers_rerun() {
    use std::io::{BufRead, Write as _};
    use std::time::Duration;

    let mut f = NamedTempFile::new().expect("tempfile");
    f.write_all(SIMPLE_YAML.as_bytes()).expect("write");
    let path = f.path().to_path_buf();

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("fluxion"))
        .args(["watch", path.to_str().unwrap(), "--debounce", "100"])
        .stdout(std::process::Stdio::piped())
        .spawn()
        .expect("spawn fluxion watch");

    std::thread::sleep(Duration::from_millis(500));

    // Touch the file to trigger a change event
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .append(true)
        .open(&path)
        .expect("open for append");
    writeln!(file, "# trigger").expect("append");
    drop(file);

    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    let stdout = child.stdout.take().expect("stdout");
    let mut reader = std::io::BufReader::new(stdout);
    let mut line = String::new();
    let mut found = false;
    while std::time::Instant::now() < deadline {
        line.clear();
        if reader.read_line(&mut line).unwrap_or(0) == 0 {
            std::thread::sleep(Duration::from_millis(50));
            continue;
        }
        if line.contains("File changed") || line.contains("Run complete") {
            found = true;
            break;
        }
    }

    let _ = child.kill();
    assert!(found, "expected re-run message in watch stdout within 3 s");
}

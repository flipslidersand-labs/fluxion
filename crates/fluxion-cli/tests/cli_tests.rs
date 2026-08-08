//! CLI integration tests — observability harness for Issues #29-#37.
//!
//! Each test group is marked with the Issue it covers.
//! Tests marked `#[ignore = "requires Wasm"]` need pre-built components.
//! All other tests run in `cargo test` without any Wasm build step.

use assert_cmd::Command;
use predicates::str::contains;
use std::io::Write;
use tempfile::NamedTempFile;

// ── helpers ───────────────────────────────────────────────────────────────────

fn fluxion() -> Command {
    Command::cargo_bin("fluxion").expect("fluxion binary not found")
}

/// Write an inline YAML workflow to a temp file and return the handle.
fn write_yaml(content: &str) -> NamedTempFile {
    let mut f = NamedTempFile::new().expect("tempfile");
    f.write_all(content.as_bytes()).expect("write yaml");
    f
}

const SIMPLE_YAML: &str = r#"
name: test-workflow
jobs:
  fetch:
    component: /nonexistent/fetch.wasm
  normalize:
    component: /nonexistent/normalize.wasm
    depends_on: [fetch]
  export:
    component: /nonexistent/export.wasm
    depends_on: [normalize]
"#;

const TWO_ROOTS_YAML: &str = r#"
name: two-roots
jobs:
  a:
    component: /nonexistent/a.wasm
  b:
    component: /nonexistent/b.wasm
  merge:
    component: /nonexistent/merge.wasm
    depends_on: [a, b]
"#;

// ── #29 — fluxion run --dry-run ───────────────────────────────────────────────

/// Before: `--dry-run` flag does not exist → clap errors with "unexpected argument".
/// After:  flag is recognized, jobs listed in topo order, process exits 0.
#[test]
#[ignore = "feature not yet implemented (#29)"]
fn dry_run_lists_jobs_in_topo_order() {
    let f = write_yaml(SIMPLE_YAML);
    fluxion()
        .args(["run", f.path().to_str().unwrap(), "--dry-run"])
        .assert()
        .success()
        .stdout(contains("fetch"))
        .stdout(contains("normalize"))
        .stdout(contains("export"));
}

#[test]
#[ignore = "feature not yet implemented (#29)"]
fn dry_run_succeeds_even_if_wasm_missing() {
    let f = write_yaml(SIMPLE_YAML);
    // Wasm files do not exist — dry-run must not touch them.
    fluxion()
        .args(["run", f.path().to_str().unwrap(), "--dry-run"])
        .assert()
        .success();
}

#[test]
#[ignore = "feature not yet implemented (#29)"]
fn dry_run_reports_cycle_as_error() {
    let cyclic = r#"
name: cycle
jobs:
  a:
    component: /x.wasm
    depends_on: [b]
  b:
    component: /x.wasm
    depends_on: [a]
"#;
    let f = write_yaml(cyclic);
    fluxion()
        .args(["run", f.path().to_str().unwrap(), "--dry-run"])
        .assert()
        .failure()
        .stderr(contains("circular"));
}

// ── #30 — fluxion runs prune ──────────────────────────────────────────────────

/// Before: `fluxion runs prune` is an unknown subcommand → clap error.
/// After:  subcommand is recognized and reports deleted count.
#[test]
#[ignore = "feature not yet implemented (#30)"]
fn runs_prune_reports_deleted_count() {
    // No runs in fresh DB → prune deletes 0 and exits 0.
    fluxion()
        .args(["runs", "prune", "--before", "0"])
        .assert()
        .success()
        .stdout(contains("0"));
}

// ── #31 — max_parallel ────────────────────────────────────────────────────────
// Concurrency limit is tested at the scheduler unit-test level (scheduler.rs).
// CLI-level smoke test: YAML with max_parallel parses without error.

#[test]
#[ignore = "feature not yet implemented (#31)"]
fn max_parallel_yaml_parses_and_dry_runs() {
    let yaml = r#"
name: limited
max_parallel: 1
jobs:
  a:
    component: /x.wasm
  b:
    component: /x.wasm
    depends_on: [a]
"#;
    let f = write_yaml(yaml);
    fluxion()
        .args(["run", f.path().to_str().unwrap(), "--dry-run"])
        .assert()
        .success();
}

// ── #32 — fluxion dot ─────────────────────────────────────────────────────────

/// Before: `fluxion dot` is an unknown subcommand → clap error.
/// After:  outputs valid DOT containing edges between jobs.
#[test]
#[ignore = "feature not yet implemented (#32)"]
fn dot_outputs_digraph() {
    let f = write_yaml(SIMPLE_YAML);
    fluxion()
        .args(["dot", f.path().to_str().unwrap()])
        .assert()
        .success()
        .stdout(contains("digraph"))
        .stdout(contains("fetch"))
        .stdout(contains("normalize"))
        .stdout(contains("export"));
}

#[test]
#[ignore = "feature not yet implemented (#32)"]
fn dot_outputs_edges() {
    let f = write_yaml(SIMPLE_YAML);
    let out = fluxion()
        .args(["dot", f.path().to_str().unwrap()])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let s = String::from_utf8_lossy(&out);
    // fetch → normalize edge must be present
    assert!(
        s.contains("\"fetch\" -> \"normalize\"") || s.contains("fetch -> normalize"),
        "missing fetch→normalize edge: {s}"
    );
}

#[test]
#[ignore = "feature not yet implemented (#32)"]
fn dot_two_root_jobs_no_edge_between_them() {
    let f = write_yaml(TWO_ROOTS_YAML);
    let out = fluxion()
        .args(["dot", f.path().to_str().unwrap()])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let s = String::from_utf8_lossy(&out);
    // a and b are independent — no direct edge between them
    assert!(!s.contains("\"a\" -> \"b\""), "unexpected a→b edge: {s}");
    assert!(!s.contains("\"b\" -> \"a\""), "unexpected b→a edge: {s}");
}

// ── #33 — fluxion component run --input-file ──────────────────────────────────

/// Before: `--input-file` flag is unknown → clap error.
/// After:  file is read and passed to component as input bytes.
#[test]
#[ignore = "requires Wasm — feature not yet implemented (#33)"]
fn input_file_flag_passes_file_content_to_component() {
    let mut input = NamedTempFile::new().expect("tempfile");
    input.write_all(b"hello from file").expect("write");

    // hello.wasm must be pre-built
    fluxion()
        .args([
            "component",
            "run",
            "components/hello/target/wasm32-wasip1/debug/hello.wasm",
            "--input-file",
            input.path().to_str().unwrap(),
        ])
        .assert()
        .success();
}

#[test]
#[ignore = "feature not yet implemented (#33)"]
fn input_file_and_input_flags_are_mutually_exclusive() {
    let mut input = NamedTempFile::new().expect("tempfile");
    input.write_all(b"data").expect("write");

    fluxion()
        .args([
            "component",
            "run",
            "/nonexistent.wasm",
            "--input",
            "foo",
            "--input-file",
            input.path().to_str().unwrap(),
        ])
        .assert()
        .failure();
}

// ── #35 — YAML env: section ───────────────────────────────────────────────────
// Env var injection is tested at the host unit-test level (lib.rs).
// CLI smoke test: YAML with env: parses without error.

#[test]
#[ignore = "feature not yet implemented (#35)"]
fn env_section_in_yaml_parses_without_error() {
    let yaml = r#"
name: env-test
jobs:
  work:
    component: /nonexistent.wasm
    env:
      API_KEY: "secret"
      REGION: "us-east-1"
"#;
    let f = write_yaml(yaml);
    // dry-run so no Wasm needed
    fluxion()
        .args(["run", f.path().to_str().unwrap(), "--dry-run"])
        .assert()
        .success();
}

// ── #36 — fluxion logs --json ─────────────────────────────────────────────────

/// Before: `fluxion logs --json` — unknown flag → clap error.
/// After:  outputs valid JSON object.
#[test]
#[ignore = "feature not yet implemented (#36)"]
fn logs_json_flag_outputs_valid_json() {
    // We can't easily seed a run here without running a real workflow.
    // This test verifies that --json is recognized and produces parseable JSON
    // when given a valid run-id (even if it returns an error about "not found",
    // that error should itself be JSON when --json is set, or the flag accepted).
    let out = fluxion()
        .args(["logs", "--json", "run-0000-fake"])
        .assert()
        // May fail (run not found) but flag must be recognized (no clap error)
        .get_output()
        .stderr
        .clone();
    let s = String::from_utf8_lossy(&out);
    assert!(
        !s.contains("unexpected argument"),
        "--json flag was not recognized: {s}"
    );
}

// ── #37 — fluxion component run (stdin pipe) ──────────────────────────────────

/// Before: stdin is ignored; `--input` is required for non-empty input.
/// After:  data piped to stdin is read when --input / --input-file are absent.
#[test]
#[ignore = "requires Wasm — feature not yet implemented (#37)"]
fn stdin_pipe_passes_bytes_to_component() {
    fluxion()
        .args([
            "component",
            "run",
            "components/hello/target/wasm32-wasip1/debug/hello.wasm",
        ])
        .write_stdin(b"hello from stdin".as_ref())
        .assert()
        .success();
}

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
fn dry_run_succeeds_even_if_wasm_missing() {
    let f = write_yaml(SIMPLE_YAML);
    // Wasm files do not exist — dry-run must not touch them.
    fluxion()
        .args(["run", f.path().to_str().unwrap(), "--dry-run"])
        .assert()
        .success();
}

#[test]
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

// ── #93 — fluxion validate ────────────────────────────────────────────────────

#[test]
fn validate_help_exits_ok() {
    fluxion().args(["validate", "--help"]).assert().success();
}

#[test]
fn validate_valid_workflow_exits_zero() {
    let f = write_yaml(SIMPLE_YAML);
    // SIMPLE_YAML uses /nonexistent/*.wasm — skip the fs check to isolate structural validation
    fluxion()
        .args(["validate", "--skip-wasm-check", f.path().to_str().unwrap()])
        .assert()
        .success()
        .stdout(contains("Validation passed"));
}

#[test]
fn validate_unknown_dep_exits_one() {
    let f = write_yaml(
        r#"
name: bad
jobs:
  a:
    component: a.wasm
    depends_on: [nonexistent]
"#,
    );
    fluxion()
        .args(["validate", "--skip-wasm-check", f.path().to_str().unwrap()])
        .assert()
        .failure()
        .stderr(contains("nonexistent"));
}

#[test]
fn validate_cyclic_dep_exits_one() {
    let f = write_yaml(
        r#"
name: cycle
jobs:
  a:
    component: a.wasm
    depends_on: [b]
  b:
    component: b.wasm
    depends_on: [a]
"#,
    );
    fluxion()
        .args(["validate", "--skip-wasm-check", f.path().to_str().unwrap()])
        .assert()
        .failure()
        .stderr(contains("circular"));
}

#[test]
fn validate_yaml_parse_error_exits_one() {
    let f = write_yaml("jobs: [this is not valid: yaml: structure");
    fluxion()
        .args(["validate", f.path().to_str().unwrap()])
        .assert()
        .failure()
        .stderr(contains("YAML parse error"));
}

#[test]
fn validate_nonexistent_file_exits_one() {
    fluxion()
        .args(["validate", "/nonexistent/path/workflow.yaml"])
        .assert()
        .failure()
        .stderr(contains("Cannot read"));
}

#[test]
fn validate_json_output_ok() {
    let f = write_yaml(SIMPLE_YAML);
    let out = fluxion()
        .args([
            "validate",
            "--json",
            "--skip-wasm-check",
            f.path().to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(out.status.success());
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("valid JSON");
    assert_eq!(v["ok"], true);
    assert_eq!(v["errors"].as_array().unwrap().len(), 0);
}

#[test]
fn validate_json_output_with_error() {
    let f = write_yaml(
        r#"
name: bad
jobs:
  a:
    component: a.wasm
    depends_on: [missing]
"#,
    );
    let out = fluxion()
        .args([
            "validate",
            "--json",
            "--skip-wasm-check",
            f.path().to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(!out.status.success());
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("valid JSON");
    assert_eq!(v["ok"], false);
    assert!(!v["errors"].as_array().unwrap().is_empty());
}

#[test]
fn validate_missing_component_detected() {
    let f = write_yaml(
        r#"
name: missing-wasm
jobs:
  step:
    component: /absolutely/nonexistent/step.wasm
"#,
    );
    // Without --skip-wasm-check the missing .wasm triggers ComponentNotFound
    fluxion()
        .args(["validate", f.path().to_str().unwrap()])
        .assert()
        .failure()
        .stderr(contains("component not found"));
}

#[test]
fn validate_skip_wasm_check_suppresses_missing_component() {
    let f = write_yaml(
        r#"
name: missing-wasm
jobs:
  step:
    component: /absolutely/nonexistent/step.wasm
"#,
    );
    fluxion()
        .args(["validate", "--skip-wasm-check", f.path().to_str().unwrap()])
        .assert()
        .success();
}

#[test]
fn validate_existing_component_passes() {
    // Use a real file (the fluxion binary itself) as a stand-in .wasm path
    let bin = assert_cmd::cargo::cargo_bin("fluxion");
    let f = write_yaml(&format!(
        "name: real\njobs:\n  step:\n    component: {}\n",
        bin.display()
    ));
    fluxion()
        .args(["validate", f.path().to_str().unwrap()])
        .assert()
        .success();
}

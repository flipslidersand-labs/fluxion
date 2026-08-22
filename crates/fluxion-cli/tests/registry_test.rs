//! Integration tests for `fluxion registry` subcommand (#112).

use assert_cmd::Command;
use tempfile::TempDir;

fn fluxion() -> Command {
    Command::cargo_bin("fluxion").expect("fluxion binary not found")
}

/// `fluxion registry list` with an empty DB prints a header and "(no entries)".
#[test]
fn registry_list_empty() {
    let tmp = TempDir::new().expect("tempdir");
    // Redirect HOME so the registry DB is isolated to this test.
    fluxion()
        .env("HOME", tmp.path())
        .args(["registry", "list"])
        .assert()
        .success()
        .stdout(predicates::str::contains("(no entries)"));
}

/// `fluxion registry rm` with an unknown ID exits non-zero.
#[test]
fn registry_rm_unknown_id_fails() {
    let tmp = TempDir::new().expect("tempdir");
    fluxion()
        .env("HOME", tmp.path())
        .args(["registry", "rm", "nonexistent-id"])
        .assert()
        .failure();
}

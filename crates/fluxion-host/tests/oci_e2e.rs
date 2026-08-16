//! OCI E2E tests: push hello.wasm to a local registry, then run a workflow
//! that specifies `oci_ref` — verifying the full pull → execute path.
//!
//! Requires a local OCI registry on `localhost:5000`.
//! Enable in CI with `services: registry:2` or locally with:
//!   docker run -d -p 5000:5000 registry:2

use std::path::PathBuf;

fn hello_wasm_path() -> PathBuf {
    // Expect the component to have been built via `cargo component build` in CI.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../components/hello/target/wasm32-wasip2/debug/hello.wasm")
}

fn registry_available() -> bool {
    // Quick TCP check: don't run the test if no registry is listening.
    std::net::TcpStream::connect("localhost:5000").is_ok()
}

/// Push `hello.wasm` to the local registry, then pull it back and verify bytes match.
#[tokio::test]
#[ignore = "requires local OCI registry on localhost:5000 and pre-built hello.wasm"]
async fn push_and_pull_roundtrip() {
    if !registry_available() {
        eprintln!("SKIP: no OCI registry on localhost:5000");
        return;
    }

    let path = hello_wasm_path();
    assert!(path.exists(), "hello.wasm not found at {}", path.display());

    let wasm_bytes = std::fs::read(&path).unwrap();
    let oci_ref = "localhost:5000/fluxion/hello:roundtrip-test";

    fluxion_host::oci::push(oci_ref, &wasm_bytes)
        .await
        .expect("push should succeed");

    let pulled = fluxion_host::oci::pull(oci_ref)
        .await
        .expect("pull should succeed");

    assert_eq!(
        pulled, wasm_bytes,
        "pulled bytes must match pushed bytes (roundtrip)"
    );
}

/// Push `hello.wasm` to the local registry, then run a workflow using `oci_ref`.
#[tokio::test]
#[ignore = "requires local OCI registry on localhost:5000 and pre-built hello.wasm"]
async fn oci_ref_workflow_runs_component() {
    if !registry_available() {
        eprintln!("SKIP: no OCI registry on localhost:5000");
        return;
    }

    let path = hello_wasm_path();
    assert!(path.exists(), "hello.wasm not found at {}", path.display());

    let wasm_bytes = std::fs::read(&path).unwrap();
    let oci_ref = "localhost:5000/fluxion/hello:e2e-workflow";

    fluxion_host::oci::push(oci_ref, &wasm_bytes)
        .await
        .expect("push should succeed");

    // Build a minimal workflow with oci_ref.
    let yaml = format!(
        r#"
name: oci-e2e
jobs:
  greet:
    component: ignored.wasm
    oci_ref: "{oci_ref}"
    input: '"hello from oci"'
"#
    );

    let wf: fluxion_core::workflow::Workflow = serde_yaml::from_str(&yaml).unwrap();
    let tmp_dir = tempfile::tempdir().unwrap();
    let wf_path = tmp_dir.path().join("oci-e2e.yaml");
    std::fs::write(&wf_path, &yaml).unwrap();

    let host = std::sync::Arc::new(fluxion_host::FluxionHost::new().unwrap());
    let result = fluxion_host::scheduler::run(&wf, &wf_path, host)
        .await
        .expect("workflow run should succeed");

    assert!(result.success, "workflow must succeed: {result:?}");
    assert_eq!(result.succeeded, 1);
}

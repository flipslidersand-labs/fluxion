//! E2E test for OCI registry integration.
//!
//! Requires a Docker registry listening on localhost:5000.
//! Run: docker run -d -p 5000:5000 registry:2
//! Clean up: docker stop <container-id>

use fluxion_core::registry::{OciRef, RegistryStore};
use fluxion_core::workflow::Workflow;
use fluxion_host::oci::OciClient;
use std::time::Duration;

fn hello_wasm() -> Vec<u8> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("components/hello/target/wasm32-wasip1/debug/hello.wasm");
    std::fs::read(&root).expect("hello.wasm not built — run `cargo component build` in components/hello first")
}

#[tokio::test]
#[ignore = "requires docker registry:2 on localhost:5000"]
async fn push_pull_roundtrip() {
    let client = OciClient::new("http://localhost:5000", None)
        .expect("OciClient::new");

    let wasm = hello_wasm();
    let oci_ref = OciRef::parse("localhost:5000/fluxion/hello:test-e2e").expect("parse OCI ref");

    // Push
    let digest = client
        .push("fluxion/hello", "test-e2e", &wasm)
        .await
        .expect("push");
    assert!(digest.starts_with("sha256:"), "digest must be sha256");

    // Wait for registry to settle
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Pull
    let pulled = client
        .pull("fluxion/hello", "test-e2e")
        .await
        .expect("pull");
    assert_eq!(pulled, wasm, "pulled bytes must match pushed");
}

#[tokio::test]
#[ignore = "requires docker registry:2 on localhost:5000"]
async fn registry_store_resolve_after_push() {
    let client = OciClient::new("http://localhost:5000", None)
        .expect("OciClient::new");
    let store = RegistryStore::open().expect("RegistryStore::open");

    let wasm = hello_wasm();
    let oci_ref = OciRef::parse("localhost:5000/fluxion/hello:test-resolve").expect("parse OCI ref");

    // Push to registry
    client
        .push("fluxion/hello", "test-resolve", &wasm)
        .await
        .expect("push");

    // Register in local store
    let id = store.register(&oci_ref, "/tmp/hello.wasm").expect("register");

    // Verify resolve works
    let entry = store.resolve(&oci_ref).expect("resolve").expect("should find entry");
    assert_eq!(entry.id, id);
    assert_eq!(entry.oci_ref, oci_ref);
}

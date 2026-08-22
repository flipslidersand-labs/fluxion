//! `fluxion registry` subcommand — OCI registry operations.

use anyhow::Result;
use fluxion_core::registry::{OciRef, RegistryStore};
use fluxion_host::oci::OciClient;
use std::path::PathBuf;

pub async fn pull(oci_ref_str: &str, output: Option<PathBuf>) -> Result<()> {
    let r = OciRef::parse(oci_ref_str)?;
    let base_url = format!("http://{}", r.registry);
    let client = OciClient::new(base_url, None)?;
    let reference = r.tag.as_deref().or(r.digest.as_deref()).unwrap_or("latest");

    eprintln!("Pulling {oci_ref_str} …");
    let bytes = client.pull(&r.repository, reference).await?;

    let out_path = output.unwrap_or_else(|| {
        let name = r.repository.rsplit('/').next().unwrap_or("component");
        PathBuf::from(format!("{name}.wasm"))
    });
    std::fs::write(&out_path, &bytes)?;
    println!("Saved {} bytes → {}", bytes.len(), out_path.display());

    let store = RegistryStore::open()?;
    store.register(&r, out_path.to_string_lossy().as_ref())?;
    Ok(())
}

pub async fn push(wasm_path: &PathBuf, oci_ref_str: &str) -> Result<()> {
    let r = OciRef::parse(oci_ref_str)?;
    let base_url = format!("http://{}", r.registry);
    let client = OciClient::new(base_url, None)?;
    let reference = r.tag.as_deref().or(r.digest.as_deref()).unwrap_or("latest");

    let bytes = std::fs::read(wasm_path)
        .map_err(|e| anyhow::anyhow!("Cannot read '{}': {e}", wasm_path.display()))?;
    eprintln!(
        "Pushing {} ({} bytes) → {oci_ref_str} …",
        wasm_path.display(),
        bytes.len()
    );
    let digest = client.push(&r.repository, reference, &bytes).await?;
    println!("Pushed. Layer digest: {digest}");
    Ok(())
}

pub fn local_list() -> Result<()> {
    let store = RegistryStore::open()?;
    let entries = store.list()?;
    if entries.is_empty() {
        println!("{:<20}  {:<45}  PATH", "ID", "OCI REF");
        println!("{}", "-".repeat(80));
        println!("(no entries)");
    } else {
        println!("{:<20}  {:<45}  PATH", "ID", "OCI REF");
        println!("{}", "-".repeat(80));
        for e in &entries {
            println!(
                "{:<20}  {:<45}  {}",
                &e.id[..e.id.len().min(20)],
                e.oci_ref.to_string_repr(),
                e.wasm_path
            );
        }
    }
    Ok(())
}

pub fn rm(id: &str) -> Result<()> {
    let store = RegistryStore::open()?;
    if store.delete(id)? {
        println!("Removed: {id}");
    } else {
        anyhow::bail!("No entry found with id: {id}");
    }
    Ok(())
}

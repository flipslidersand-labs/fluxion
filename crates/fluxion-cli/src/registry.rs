//! `fluxion registry` subcommand — OCI registry operations.

use anyhow::Result;
use fluxion_host::oci;
use std::path::PathBuf;

pub async fn pull(oci_ref: &str, output: Option<PathBuf>) -> Result<()> {
    eprintln!("Pulling {oci_ref} …");
    let bytes = oci::pull(oci_ref).await?;
    let out_path = output.unwrap_or_else(|| {
        // Derive a filename from the ref: last path component before `:` or `@`.
        let name = oci_ref
            .rsplit('/')
            .next()
            .unwrap_or("component")
            .split(':')
            .next()
            .unwrap_or("component");
        PathBuf::from(format!("{name}.wasm"))
    });
    std::fs::write(&out_path, &bytes)?;
    println!("Saved {} bytes → {}", bytes.len(), out_path.display());
    Ok(())
}

pub async fn push(wasm_path: &PathBuf, oci_ref: &str) -> Result<()> {
    let bytes = std::fs::read(wasm_path)
        .map_err(|e| anyhow::anyhow!("Cannot read '{}': {e}", wasm_path.display()))?;
    eprintln!(
        "Pushing {} ({} bytes) → {oci_ref} …",
        wasm_path.display(),
        bytes.len()
    );
    let digest = oci::push(oci_ref, &bytes).await?;
    println!("Pushed. Layer digest: {digest}");
    Ok(())
}

pub async fn list(registry: &str, repo: &str) -> Result<()> {
    let tags = oci::list_tags(registry, repo).await?;
    if tags.is_empty() {
        println!("No tags found for {registry}/{repo}");
    } else {
        println!("{registry}/{repo}:");
        for tag in &tags {
            println!("  {tag}");
        }
    }
    Ok(())
}

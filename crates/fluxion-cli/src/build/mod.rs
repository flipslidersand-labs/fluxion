pub mod wit_stub;

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::process::Command;

/// Build a Python script into a Wasm component using `componentize-py`.
///
/// componentize-py 0.25+ uses top-level flags before the subcommand:
///   componentize-py -d <wit_path> -w <world> componentize <app_name> -o <out>
///
/// Returns an error with an actionable message if `componentize-py` is not on PATH.
pub fn build_python(script: &Path, out: &Path, wit_path: &Path) -> Result<()> {
    let app_name = script
        .file_stem()
        .ok_or_else(|| anyhow::anyhow!("script path has no filename"))?
        .to_string_lossy()
        .into_owned();

    let script_dir = script
        .parent()
        .ok_or_else(|| anyhow::anyhow!("script path has no parent directory"))?;

    // Resolve wit_path to absolute so it stays valid after current_dir change.
    let abs_wit = if wit_path.is_absolute() {
        wit_path.to_path_buf()
    } else {
        std::env::current_dir().unwrap_or_default().join(wit_path)
    };

    // -d / --wit-path and -w / --world are global flags that must come before the subcommand.
    let status = Command::new("componentize-py")
        .args([
            "-d",
            &abs_wit.to_string_lossy(),
            "-w",
            "task-component",
            "componentize",
            &app_name,
            "-o",
            &out.to_string_lossy(),
        ])
        .current_dir(script_dir)
        .status()
        .map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                anyhow::anyhow!(
                    "`componentize-py` not found on PATH.\n\
                     Install it with: pip install componentize-py"
                )
            } else {
                anyhow::anyhow!("failed to run componentize-py: {e}")
            }
        })?;

    if !status.success() {
        anyhow::bail!(
            "componentize-py exited with status {}",
            status.code().unwrap_or(-1)
        );
    }

    println!("Built {} → {}", script.display(), out.display());
    Ok(())
}

/// Resolve the wit_path: use provided value, else fall back to `./wit`.
pub fn resolve_wit_path(wit_path: Option<PathBuf>) -> PathBuf {
    wit_path.unwrap_or_else(|| PathBuf::from("./wit"))
}

/// Generate a Python stub for the given WIT path and write it to `output`.
pub fn generate_stub(wit_path: &Path, output: &Path) -> Result<()> {
    let stub = wit_stub::generate(wit_path)
        .with_context(|| format!("failed to generate stub from {}", wit_path.display()))?;
    std::fs::write(output, stub)
        .with_context(|| format!("failed to write stub to {}", output.display()))?;
    println!("Generated stub → {}", output.display());
    Ok(())
}

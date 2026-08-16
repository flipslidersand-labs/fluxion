pub mod wit_stub;

use anyhow::{Context, Result};
use std::path::Path;
use std::process::Command;

/// Compile a Python script into a Wasm component using `componentize-py`.
///
/// Requires `componentize-py` to be available in PATH. Install with:
///   `pip install componentize-py`
///
/// Also writes `task.py` (the WIT stub) alongside the script if it does not
/// already exist, so users can implement `Processor.process()` without
/// hand-writing the boilerplate.
///
/// Equivalent shell command:
///   `componentize-py componentize --wit-path <wit_path> --world task-component <script> -o <out>`
pub fn build_python(script: &Path, out: &Path, wit_path: &Path) -> Result<()> {
    // Emit task.py (WIT stub) next to the script if not already present.
    maybe_write_stub(script, wit_path)?;

    let status = Command::new("componentize-py")
        .args([
            "componentize",
            "--wit-path",
            &wit_path.to_string_lossy(),
            "--world",
            "task-component",
            &script.to_string_lossy(),
            "-o",
            &out.to_string_lossy(),
        ])
        .status()
        .with_context(
            || "componentize-py not found in PATH — install it with: pip install componentize-py",
        )?;

    if !status.success() {
        anyhow::bail!(
            "componentize-py exited with status {}",
            status.code().unwrap_or(-1)
        );
    }

    println!("Built: {}", out.display());
    Ok(())
}

/// Write `task.py` (WIT stub) into the same directory as `script` if
/// the stub does not already exist. The WIT is read from `<wit_path>/task.wit`.
fn maybe_write_stub(script: &Path, wit_path: &Path) -> Result<()> {
    let stub_path = script.parent().unwrap_or(Path::new(".")).join("task.py");

    if stub_path.exists() {
        return Ok(());
    }

    let wit_file = wit_path.join("task.wit");
    let wit_source = std::fs::read_to_string(&wit_file).with_context(|| {
        format!(
            "Cannot read WIT file '{}' — run from the project root or set --wit-path",
            wit_file.display()
        )
    })?;

    let stub = wit_stub::generate(&wit_source);
    std::fs::write(&stub_path, &stub)
        .with_context(|| format!("Cannot write stub '{}'", stub_path.display()))?;

    println!("Generated stub: {}", stub_path.display());
    Ok(())
}

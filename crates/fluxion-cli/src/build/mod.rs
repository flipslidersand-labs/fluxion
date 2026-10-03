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
///
/// Refuses to clobber an existing non-empty file unless `force` is set — with
/// `--stub`, `output` is often the same path as the script being built, so an
/// unconditional write would silently destroy hand-written implementation code.
pub fn generate_stub(wit_path: &Path, output: &Path, force: bool) -> Result<()> {
    if !force
        && let Ok(meta) = std::fs::metadata(output)
        && meta.len() > 0
    {
        anyhow::bail!(
            "{} already exists and is not empty; refusing to overwrite it.\n\
             Re-run with --force to overwrite anyway.",
            output.display()
        );
    }

    let stub = wit_stub::generate(wit_path)
        .with_context(|| format!("failed to generate stub from {}", wit_path.display()))?;
    std::fs::write(output, stub)
        .with_context(|| format!("failed to write stub to {}", output.display()))?;
    println!("Generated stub → {}", output.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_wit_path_defaults_to_wit_dir() {
        assert_eq!(resolve_wit_path(None), PathBuf::from("./wit"));
    }

    #[test]
    fn resolve_wit_path_returns_provided_value() {
        let custom = PathBuf::from("/some/custom/wit");
        assert_eq!(resolve_wit_path(Some(custom.clone())), custom);
    }

    #[test]
    fn build_python_errors_when_script_has_no_filename() {
        let err = build_python(
            Path::new("/"),
            Path::new("/tmp/out.wasm"),
            Path::new("./wit"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("no filename"));
    }

    #[test]
    fn generate_stub_writes_output_file() {
        let tmp = tempfile::tempdir().unwrap();
        let wit_path = tmp.path().join("task.wit");
        std::fs::write(&wit_path, "record task-input {}\nrecord task-output {}\n").unwrap();
        let output = tmp.path().join("stub.py");

        generate_stub(&wit_path, &output, false).unwrap();

        let contents = std::fs::read_to_string(&output).unwrap();
        assert!(contents.contains("class TaskInput"));
        assert!(contents.contains("class TaskOutput"));
    }

    #[test]
    fn generate_stub_errors_on_missing_wit_file() {
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("nope.wit");
        let output = tmp.path().join("stub.py");
        let err = generate_stub(&missing, &output, false).unwrap_err();
        assert!(err.to_string().contains("failed to generate stub"));
    }

    #[test]
    fn generate_stub_refuses_to_overwrite_nonempty_file_without_force() {
        let tmp = tempfile::tempdir().unwrap();
        let wit_path = tmp.path().join("task.wit");
        std::fs::write(&wit_path, "record task-input {}\nrecord task-output {}\n").unwrap();
        let output = tmp.path().join("task.py");
        std::fs::write(&output, "def process(input): ...  # hand-written impl").unwrap();

        let err = generate_stub(&wit_path, &output, false).unwrap_err();
        assert!(err.to_string().contains("already exists"));
        assert_eq!(
            std::fs::read_to_string(&output).unwrap(),
            "def process(input): ...  # hand-written impl",
            "existing file must be left untouched"
        );
    }

    #[test]
    fn generate_stub_overwrites_with_force() {
        let tmp = tempfile::tempdir().unwrap();
        let wit_path = tmp.path().join("task.wit");
        std::fs::write(&wit_path, "record task-input {}\nrecord task-output {}\n").unwrap();
        let output = tmp.path().join("task.py");
        std::fs::write(&output, "old content").unwrap();

        generate_stub(&wit_path, &output, true).unwrap();

        let contents = std::fs::read_to_string(&output).unwrap();
        assert!(contents.contains("class TaskInput"));
    }

    #[test]
    fn generate_stub_overwrites_empty_existing_file_without_force() {
        let tmp = tempfile::tempdir().unwrap();
        let wit_path = tmp.path().join("task.wit");
        std::fs::write(&wit_path, "record task-input {}\nrecord task-output {}\n").unwrap();
        let output = tmp.path().join("task.py");
        std::fs::write(&output, "").unwrap();

        generate_stub(&wit_path, &output, false).unwrap();

        let contents = std::fs::read_to_string(&output).unwrap();
        assert!(contents.contains("class TaskInput"));
    }
}

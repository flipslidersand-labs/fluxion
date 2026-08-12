use std::time::Duration;

use anyhow::Result;
use serde::Serialize;

use crate::registry::RegistryStore;
use crate::workflow::JobDefinition;

#[derive(Debug, Serialize)]
pub struct RunResult {
    pub run_id: String,
    pub workflow_name: String,
    pub jobs: Vec<JobResult>,
    pub total_elapsed_ms: u64,
    pub succeeded: usize,
    pub total: usize,
    pub success: bool,
}

#[derive(Debug, Serialize)]
pub struct JobResult {
    pub job_id: String,
    pub status: String,
    pub elapsed_ms: u64,
    pub reason: Option<String>,
    pub skipped: bool,
    /// Wasm compile phase (0 = not measured or skipped job).
    pub compile_us: u64,
    /// Component instantiate phase.
    pub instantiate_us: u64,
    /// Guest execute phase.
    pub execute_us: u64,
}

impl RunResult {
    pub fn summary(&self) -> String {
        format!(
            "Run {} — {}/{} jobs succeeded in {:.2}s",
            self.run_id,
            self.succeeded,
            self.total,
            self.total_elapsed_ms as f64 / 1000.0
        )
    }
}

impl JobResult {
    pub fn from_succeeded(job_id: String, elapsed: Duration, skipped: bool) -> Self {
        Self {
            job_id,
            status: "succeeded".into(),
            elapsed_ms: elapsed.as_millis() as u64,
            reason: None,
            skipped,
            compile_us: 0,
            instantiate_us: 0,
            execute_us: 0,
        }
    }

    pub fn from_succeeded_with_metrics(
        job_id: String,
        elapsed: Duration,
        compile_us: u64,
        instantiate_us: u64,
        execute_us: u64,
    ) -> Self {
        Self {
            job_id,
            status: "succeeded".into(),
            elapsed_ms: elapsed.as_millis() as u64,
            reason: None,
            skipped: false,
            compile_us,
            instantiate_us,
            execute_us,
        }
    }

    pub fn from_skipped(job_id: String) -> Self {
        Self {
            job_id,
            status: "skipped".into(),
            elapsed_ms: 0,
            reason: None,
            skipped: true,
            compile_us: 0,
            instantiate_us: 0,
            execute_us: 0,
        }
    }

    pub fn from_failed(job_id: String, elapsed: Duration, reason: String) -> Self {
        Self {
            job_id,
            status: "failed".into(),
            elapsed_ms: elapsed.as_millis() as u64,
            reason: Some(reason),
            skipped: false,
            compile_us: 0,
            instantiate_us: 0,
            execute_us: 0,
        }
    }
}

/// Resolve the wasm component path for a job.
///
/// Priority:
/// 1. `job.component` if non-empty (local path, backward-compatible)
/// 2. `job.oci_ref` looked up via `RegistryStore::resolve()` (registry-backed)
///
/// Returns `Err` when neither is available or the OCI ref is not in the registry.
pub fn resolve_component_path(job: &JobDefinition, store: &RegistryStore) -> Result<String> {
    if !job.component.is_empty() {
        return Ok(job.component.clone());
    }
    if let Some(oci_ref) = &job.oci_ref {
        if let Some(entry) = store.resolve(oci_ref)? {
            return Ok(entry.wasm_path);
        }
        anyhow::bail!(
            "OCI ref '{}' is not in the local registry — run `fluxion registry pull` first",
            oci_ref.to_string_repr()
        );
    }
    anyhow::bail!("job has neither `component` nor `oci_ref`")
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use super::*;
    use crate::registry::{OciRef, RegistryStore};
    use crate::workflow::JobDefinition;

    fn store() -> RegistryStore {
        RegistryStore::from_conn(Connection::open_in_memory().unwrap()).unwrap()
    }

    fn job(component: &str, oci_ref: Option<OciRef>) -> JobDefinition {
        JobDefinition {
            component: component.to_string(),
            depends_on: vec![],
            input: None,
            permissions: Default::default(),
            worker: None,
            env: Default::default(),
            when: None,
            foreach: None,
            input_from: None,
            max_parallel: None,
            output_size_limit_mb: None,
            fail_fast: false,
            component_sha256: None,
            reduce: None,
            oci_ref,
        }
    }

    #[test]
    fn resolve_prefers_component_path() {
        let s = store();
        let j = job("/local/x.wasm", Some(OciRef::parse("ghcr.io/org/x:v1").unwrap()));
        assert_eq!(resolve_component_path(&j, &s).unwrap(), "/local/x.wasm");
    }

    #[test]
    fn resolve_falls_back_to_registry() {
        let s = store();
        let oci = OciRef::parse("ghcr.io/org/hello:v1").unwrap();
        s.register(&oci, "/cached/hello.wasm").unwrap();

        let j = job("", Some(oci));
        assert_eq!(resolve_component_path(&j, &s).unwrap(), "/cached/hello.wasm");
    }

    #[test]
    fn resolve_errors_when_oci_not_in_registry() {
        let s = store();
        let j = job("", Some(OciRef::parse("ghcr.io/org/missing:v1").unwrap()));
        assert!(resolve_component_path(&j, &s).is_err());
    }

    #[test]
    fn resolve_errors_when_neither_set() {
        let s = store();
        let j = job("", None);
        assert!(resolve_component_path(&j, &s).is_err());
    }
}

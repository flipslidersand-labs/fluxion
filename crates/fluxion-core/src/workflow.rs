use anyhow::{Context, Result};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workflow {
    pub name: String,
    pub jobs: IndexMap<String, JobDefinition>,
    /// Remote worker configs for distributed execution (round-robin if job has no explicit worker).
    #[serde(default)]
    pub workers: Vec<WorkerConfig>,
    /// Maximum number of jobs that may execute concurrently. None = unbounded.
    #[serde(default)]
    pub max_parallel: Option<usize>,
}

/// Per-worker configuration. Supports both a plain URL string and an extended
/// form with optional mTLS certificate paths and load-balancing weight.
///
/// ```yaml
/// workers:
///   - http://worker-a:7777          # plain form (no TLS, weight=1)
///   - url: https://worker-b:7778    # extended form
///     weight: 3                     # receives 3x the traffic of weight-1 workers
///     tls:
///       cert: /etc/fluxion/client.crt
///       key:  /etc/fluxion/client.key
///       ca:   /etc/fluxion/ca.crt
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum WorkerConfig {
    Simple(String),
    Full {
        url: String,
        #[serde(default)]
        tls: Option<TlsConfig>,
        /// Load-balancing weight for weighted round-robin. Defaults to 1.
        #[serde(default = "default_weight")]
        weight: u32,
    },
}

fn default_weight() -> u32 {
    1
}

impl WorkerConfig {
    pub fn url(&self) -> &str {
        match self {
            Self::Simple(s) => s,
            Self::Full { url, .. } => url,
        }
    }

    pub fn tls(&self) -> Option<&TlsConfig> {
        match self {
            Self::Simple(_) => None,
            Self::Full { tls, .. } => tls.as_ref(),
        }
    }

    /// Load-balancing weight. Plain-form workers always have weight 1.
    pub fn weight(&self) -> u32 {
        match self {
            Self::Simple(_) => 1,
            Self::Full { weight, .. } => *weight,
        }
    }
}

/// mTLS certificate paths used by the scheduler when connecting to a worker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    /// Path to the PEM-encoded client certificate.
    pub cert: PathBuf,
    /// Path to the PEM-encoded client private key.
    pub key: PathBuf,
    /// Path to the PEM-encoded CA certificate used to verify the server.
    pub ca: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobDefinition {
    pub component: String,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub input: Option<String>,
    #[serde(default)]
    pub permissions: PermissionSet,
    /// Pin this job to a specific worker URL. Overrides round-robin assignment.
    #[serde(default)]
    pub worker: Option<String>,
    /// Environment variables injected into the Wasm component via WASI.
    #[serde(default)]
    pub env: std::collections::HashMap<String, String>,
    /// Optional conditional expression. If present and evaluates to false, the job is SKIPPED.
    /// Example: `"validate.status == 'SUCCESS'"`
    #[serde(default)]
    pub when: Option<String>,
    /// JSONPath expression. Expands input JSON array to fan-out child jobs
    /// named `<job_id>.0`, `<job_id>.1`, …
    #[serde(default)]
    pub foreach: Option<String>,
    /// Fan-in: collect all outputs from the named foreach job as a JSON array
    /// and pass it as this job's input.
    #[serde(default)]
    pub input_from: Option<String>,
    /// Per-job parallelism cap (overrides workflow-level max_parallel).
    #[serde(default)]
    pub max_parallel: Option<usize>,
    /// Maximum allowed output size in megabytes. Defaults to 64 MB.
    /// Outputs exceeding this limit cause the job to fail with a clear error.
    #[serde(default)]
    pub output_size_limit_mb: Option<u64>,
    /// When `true`, any foreach child failure immediately cancels remaining siblings.
    /// When `false` (default), siblings run to completion before the fan-in is cancelled.
    #[serde(default)]
    pub fail_fast: bool,
    /// Optional SHA-256 hex digest of the component `.wasm` file.
    /// When present, the file is verified before execution; mismatch causes the job to fail.
    #[serde(default)]
    pub component_sha256: Option<String>,
}

// ── Permission types ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PermissionSet {
    #[serde(default)]
    pub filesystem: FilesystemPermission,
    #[serde(default)]
    pub network: NetworkPermission,
    #[serde(default)]
    pub limits: ResourceLimits,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FilesystemPermission {
    /// Directories the component may read from (guest path = host path).
    #[serde(default)]
    pub read: Vec<PathBuf>,
    /// Directories the component may read and write.
    #[serde(default)]
    pub write: Vec<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NetworkPermission {
    /// Allowlist of host:port strings. Empty = deny all.
    #[serde(default)]
    pub allow: Vec<String>,
}

impl NetworkPermission {
    pub fn allows(&self, addr: &str) -> bool {
        self.allow.iter().any(|h| addr.starts_with(h.as_str()))
    }
    pub fn is_deny_all(&self) -> bool {
        self.allow.is_empty()
    }
}

fn default_memory_mb() -> u64 {
    256
}
fn default_timeout_secs() -> u64 {
    60
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceLimits {
    #[serde(default = "default_memory_mb")]
    pub memory_mb: u64,
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            memory_mb: 256,
            timeout_secs: 60,
        }
    }
}

// ── Workflow impl ─────────────────────────────────────────────────────────────

impl Workflow {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let src = std::fs::read_to_string(path.as_ref())
            .with_context(|| format!("Failed to read {:?}", path.as_ref()))?;
        let wf: Self =
            serde_yaml::from_str(&src).with_context(|| "Failed to parse workflow YAML")?;
        wf.validate()?;
        Ok(wf)
    }

    fn validate(&self) -> Result<()> {
        for (job_id, def) in &self.jobs {
            for dep in &def.depends_on {
                anyhow::ensure!(
                    self.jobs.contains_key(dep),
                    "Job '{}' depends on unknown job '{}'",
                    job_id,
                    dep
                );
            }
            // Validate input_from references a foreach job
            if let Some(src) = &def.input_from {
                let src_def = self.jobs.get(src.as_str()).ok_or_else(|| {
                    anyhow::anyhow!(
                        "Job '{}' has input_from '{}' which does not exist",
                        job_id,
                        src
                    )
                })?;
                anyhow::ensure!(
                    src_def.foreach.is_some(),
                    "Job '{}' has input_from '{}' but '{}' does not have foreach",
                    job_id,
                    src,
                    src
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parse_network_sandbox_yaml() {
        let src = r#"
name: network-sandbox
jobs:
  connect-allowed:
    component: foo.wasm
    input: "127.0.0.1:19999"
    permissions:
      network:
        allow: ["127.0.0.1:19999"]
      limits:
        timeout_secs: 5
  connect-denied:
    component: foo.wasm
    input: "127.0.0.1:19999"
    depends_on: [connect-allowed]
"#;
        let result: Result<Workflow, _> = serde_yaml::from_str(src);
        println!("result: {result:?}");
        assert!(result.is_ok());
    }

    #[test]
    fn parse_foreach_yaml() {
        let src = r#"
name: fanout-test
jobs:
  process:
    component: transform.wasm
    foreach: "$.items"
    max_parallel: 4
  aggregate:
    component: merge.wasm
    depends_on: [process]
    input_from: process
"#;
        // validate() checks input_from → foreach, which passes here
        let wf: Workflow = serde_yaml::from_str(src).unwrap();
        assert!(wf.jobs["process"].foreach.is_some());
        assert_eq!(wf.jobs["aggregate"].input_from.as_deref(), Some("process"));
    }
}

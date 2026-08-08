use anyhow::{Context, Result};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workflow {
    pub name: String,
    pub jobs: IndexMap<String, JobDefinition>,
    /// Remote worker URLs for distributed execution (round-robin if job has no explicit worker).
    #[serde(default)]
    pub workers: Vec<String>,
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
}

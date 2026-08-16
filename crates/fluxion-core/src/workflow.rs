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
    /// DNS SRV record name for automatic worker discovery (e.g. "_fluxion._tcp.internal").
    /// Resolved at startup and merged with the static `workers:` list.
    /// On SRV query failure, the static list is used as-is.
    #[serde(default)]
    pub workers_srv: Option<String>,
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
    /// Aggregation mode for fan-in jobs. Must be used together with `input_from`.
    #[serde(default)]
    pub reduce: Option<ReduceMode>,
    /// Execution backend for this job.
    /// `local` (default) runs the Wasm component in-process via wasmtime.
    /// `remote` dispatches the job to an HTTP worker via fluxion-worker.
    #[serde(default)]
    pub executor: ExecutorKind,
    /// When `true` and `executor: remote`, uses POST /jobs (fire-and-poll)
    /// instead of the synchronous POST /run.
    #[serde(default)]
    pub async_dispatch: bool,
    /// OCI reference (`registry/repo:tag` or `registry/repo@sha256:…`).
    /// When set, the component is pulled from the OCI registry at runtime
    /// and the `component` field is ignored.
    #[serde(default)]
    pub oci_ref: Option<String>,
}

// ── ExecutorKind ──────────────────────────────────────────────────────────────

/// Execution backend for a job.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecutorKind {
    /// Run the Wasm component in-process via wasmtime (default).
    #[default]
    Local,
    /// Dispatch to an HTTP remote worker (fluxion-worker).
    Remote,
}

// ── ReduceMode ────────────────────────────────────────────────────────────────

/// How a fan-in job aggregates outputs from its foreach source.
/// Only valid when `input_from` is set.
///
/// YAML examples:
/// ```yaml
/// reduce: json_array
/// reduce:
///   custom: /path/to/reducer.wasm
/// ```
#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ReduceMode {
    /// Concatenate raw bytes from all child outputs.
    Concat,
    /// Wrap each child output as an element of a JSON array.
    JsonArray,
    /// Deep-merge child outputs as JSON objects (last-write-wins on key conflict).
    JsonMerge,
    /// Pass outputs to an external Wasm component for custom reduction.
    /// The value is the path to the `.wasm` component.
    Custom(String),
}

// serde_yaml 0.9 cannot deserialize externally-tagged enums with non-unit
// variants from YAML mappings; implement manually to handle both forms:
//   reduce: json_array          (string → unit variant)
//   reduce:
//     custom: /path/to/r.wasm  (map → Custom newtype)
impl<'de> serde::Deserialize<'de> for ReduceMode {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        use serde::de::{self, MapAccess, Visitor};

        struct RMVisitor;
        impl<'de> Visitor<'de> for RMVisitor {
            type Value = ReduceMode;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(
                    f,
                    r#""concat", "json_array", "json_merge", or {{custom: <path>}}"#
                )
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                match v {
                    "concat" => Ok(ReduceMode::Concat),
                    "json_array" => Ok(ReduceMode::JsonArray),
                    "json_merge" => Ok(ReduceMode::JsonMerge),
                    other => Err(E::unknown_variant(
                        other,
                        &["concat", "json_array", "json_merge", "custom"],
                    )),
                }
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let key: String = map
                    .next_key()?
                    .ok_or_else(|| de::Error::custom("empty map for reduce mode"))?;
                match key.as_str() {
                    "custom" => {
                        let component: String = map.next_value()?;
                        while map.next_key::<de::IgnoredAny>()?.is_some() {
                            let _: de::IgnoredAny = map.next_value()?;
                        }
                        Ok(ReduceMode::Custom(component))
                    }
                    other => Err(de::Error::unknown_variant(
                        other,
                        &["concat", "json_array", "json_merge", "custom"],
                    )),
                }
            }
        }
        de.deserialize_any(RMVisitor)
    }
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

// ── Validation types ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum ValidationError {
    UnknownDependency { job: String, dep: String },
    UnknownInputFrom { job: String, src: String },
    InputFromNotForeach { job: String, src: String },
    CyclicDependency,
    ComponentNotFound { job: String, path: String },
    ReduceWithoutInputFrom { job: String },
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownDependency { job, dep } => {
                write!(f, "Job '{job}' depends on unknown job '{dep}'")
            }
            Self::UnknownInputFrom { job, src } => {
                write!(f, "Job '{job}' has input_from '{src}' which does not exist")
            }
            Self::InputFromNotForeach { job, src } => {
                write!(
                    f,
                    "Job '{job}' has input_from '{src}' but '{src}' does not have foreach"
                )
            }
            Self::CyclicDependency => write!(f, "Workflow contains a circular dependency"),
            Self::ComponentNotFound { job, path } => {
                write!(f, "Job '{job}': component not found at '{path}'")
            }
            Self::ReduceWithoutInputFrom { job } => {
                write!(f, "Job '{job}': `reduce` requires `input_from`")
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct ValidationReport {
    pub errors: Vec<ValidationError>,
}

impl ValidationReport {
    pub fn is_ok(&self) -> bool {
        self.errors.is_empty()
    }

    pub fn into_result(self) -> Result<()> {
        if self.errors.is_empty() {
            return Ok(());
        }
        let msgs: Vec<String> = self.errors.iter().map(|e| e.to_string()).collect();
        Err(anyhow::anyhow!("{}", msgs.join("\n")))
    }
}

impl std::fmt::Display for ValidationReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.errors.is_empty() {
            return write!(f, "Validation passed");
        }
        writeln!(f, "Validation failed ({} error(s)):", self.errors.len())?;
        for err in &self.errors {
            writeln!(f, "  - {err}")?;
        }
        Ok(())
    }
}

// DFS cycle detection — kept in workflow.rs to avoid a circular import with dag.rs.
fn has_cycle(jobs: &IndexMap<String, JobDefinition>) -> bool {
    use std::collections::HashSet;

    fn dfs<'a>(
        node: &'a str,
        jobs: &'a IndexMap<String, JobDefinition>,
        visited: &mut HashSet<&'a str>,
        stack: &mut HashSet<&'a str>,
    ) -> bool {
        if stack.contains(node) {
            return true;
        }
        if visited.contains(node) {
            return false;
        }
        visited.insert(node);
        stack.insert(node);
        if let Some(def) = jobs.get(node) {
            for dep in &def.depends_on {
                if dfs(dep.as_str(), jobs, visited, stack) {
                    return true;
                }
            }
        }
        stack.remove(node);
        false
    }

    let mut visited = std::collections::HashSet::new();
    let mut stack = std::collections::HashSet::new();
    jobs.keys()
        .any(|id| dfs(id.as_str(), jobs, &mut visited, &mut stack))
}

// ── Workflow impl ─────────────────────────────────────────────────────────────

impl Workflow {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let src = std::fs::read_to_string(path.as_ref())
            .with_context(|| format!("Failed to read {:?}", path.as_ref()))?;
        let wf: Self =
            serde_yaml::from_str(&src).with_context(|| "Failed to parse workflow YAML")?;
        wf.validate().into_result()?;
        Ok(wf)
    }

    /// Check that every `component:` path exists on disk.
    /// Not called by `from_file` — only used by the `validate` CLI subcommand.
    pub fn check_component_paths(&self) -> ValidationReport {
        let mut report = ValidationReport::default();
        for (job_id, def) in &self.jobs {
            if std::fs::metadata(&def.component).is_err() {
                report.errors.push(ValidationError::ComponentNotFound {
                    job: job_id.clone(),
                    path: def.component.clone(),
                });
            }
        }
        report
    }

    pub fn validate(&self) -> ValidationReport {
        let mut report = ValidationReport::default();

        for (job_id, def) in &self.jobs {
            for dep in &def.depends_on {
                if !self.jobs.contains_key(dep) {
                    report.errors.push(ValidationError::UnknownDependency {
                        job: job_id.clone(),
                        dep: dep.clone(),
                    });
                }
            }
            if let Some(src) = &def.input_from {
                match self.jobs.get(src.as_str()) {
                    None => report.errors.push(ValidationError::UnknownInputFrom {
                        job: job_id.clone(),
                        src: src.clone(),
                    }),
                    Some(src_def) if src_def.foreach.is_none() => {
                        report.errors.push(ValidationError::InputFromNotForeach {
                            job: job_id.clone(),
                            src: src.clone(),
                        })
                    }
                    _ => {}
                }
            }
            if def.reduce.is_some() && def.input_from.is_none() {
                report.errors.push(ValidationError::ReduceWithoutInputFrom {
                    job: job_id.clone(),
                });
            }
        }

        if has_cycle(&self.jobs) {
            report.errors.push(ValidationError::CyclicDependency);
        }

        report
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
        let wf: Workflow = serde_yaml::from_str(src).unwrap();
        assert!(wf.jobs["process"].foreach.is_some());
        assert_eq!(wf.jobs["aggregate"].input_from.as_deref(), Some("process"));
    }

    fn make_wf(yaml: &str) -> Workflow {
        serde_yaml::from_str(yaml).unwrap()
    }

    #[test]
    fn validate_ok_simple() {
        let wf = make_wf(
            r#"
name: ok
jobs:
  a:
    component: a.wasm
  b:
    component: b.wasm
    depends_on: [a]
"#,
        );
        let report = wf.validate();
        assert!(report.is_ok(), "{report}");
    }

    #[test]
    fn validate_unknown_dep() {
        let wf = make_wf(
            r#"
name: bad
jobs:
  a:
    component: a.wasm
    depends_on: [nonexistent]
"#,
        );
        let report = wf.validate();
        assert!(!report.is_ok());
        assert!(matches!(
            report.errors[0],
            ValidationError::UnknownDependency { .. }
        ));
    }

    #[test]
    fn validate_unknown_input_from() {
        let wf = make_wf(
            r#"
name: bad
jobs:
  a:
    component: a.wasm
    input_from: ghost
"#,
        );
        let report = wf.validate();
        assert!(!report.is_ok());
        assert!(matches!(
            report.errors[0],
            ValidationError::UnknownInputFrom { .. }
        ));
    }

    #[test]
    fn validate_input_from_not_foreach() {
        let wf = make_wf(
            r#"
name: bad
jobs:
  producer:
    component: p.wasm
  consumer:
    component: c.wasm
    input_from: producer
"#,
        );
        let report = wf.validate();
        assert!(!report.is_ok());
        assert!(matches!(
            report.errors[0],
            ValidationError::InputFromNotForeach { .. }
        ));
    }

    #[test]
    fn validate_cycle_detected() {
        let wf = make_wf(
            r#"
name: cycle
jobs:
  a:
    component: a.wasm
    depends_on: [b]
  b:
    component: b.wasm
    depends_on: [a]
"#,
        );
        let report = wf.validate();
        assert!(!report.is_ok());
        assert!(matches!(
            report.errors.last(),
            Some(ValidationError::CyclicDependency)
        ));
    }

    #[test]
    fn validate_collects_multiple_errors() {
        let wf = make_wf(
            r#"
name: multi-error
jobs:
  a:
    component: a.wasm
    depends_on: [missing1]
  b:
    component: b.wasm
    depends_on: [missing2]
"#,
        );
        let report = wf.validate();
        assert!(!report.is_ok());
        assert_eq!(report.errors.len(), 2);
    }

    #[test]
    fn validation_report_display_ok() {
        let report = ValidationReport::default();
        assert_eq!(report.to_string(), "Validation passed");
    }

    #[test]
    fn validation_report_display_errors() {
        let mut report = ValidationReport::default();
        report.errors.push(ValidationError::CyclicDependency);
        assert!(report.to_string().contains("1 error(s)"));
    }

    #[test]
    fn into_result_ok() {
        assert!(ValidationReport::default().into_result().is_ok());
    }

    #[test]
    fn into_result_err() {
        let mut report = ValidationReport::default();
        report.errors.push(ValidationError::CyclicDependency);
        assert!(report.into_result().is_err());
    }

    #[test]
    fn parse_reduce_yaml() {
        let src = r#"
name: reduce-test
jobs:
  process:
    component: transform.wasm
    foreach: "$.items"
  aggregate:
    component: merge.wasm
    depends_on: [process]
    input_from: process
    reduce: json_array
"#;
        let wf: Workflow = serde_yaml::from_str(src).unwrap();
        assert_eq!(wf.jobs["aggregate"].reduce, Some(ReduceMode::JsonArray));
    }

    #[test]
    fn parse_reduce_custom_yaml() {
        let src = r#"
name: reduce-custom
jobs:
  process:
    component: transform.wasm
    foreach: "$.items"
  aggregate:
    component: merge.wasm
    depends_on: [process]
    input_from: process
    reduce:
      custom: /path/to/reducer.wasm
"#;
        let wf: Workflow = serde_yaml::from_str(src).unwrap();
        assert_eq!(
            wf.jobs["aggregate"].reduce,
            Some(ReduceMode::Custom("/path/to/reducer.wasm".to_string()))
        );
    }

    #[test]
    fn validate_reduce_without_input_from() {
        let wf = make_wf(
            r#"
name: bad-reduce
jobs:
  a:
    component: a.wasm
    reduce: concat
"#,
        );
        let report = wf.validate();
        assert!(!report.is_ok());
        assert!(
            report
                .errors
                .iter()
                .any(|e| matches!(e, ValidationError::ReduceWithoutInputFrom { .. }))
        );
    }

    #[test]
    fn validate_reduce_with_input_from_is_ok() {
        let wf = make_wf(
            r#"
name: good-reduce
jobs:
  source:
    component: a.wasm
    foreach: "$.items"
  sink:
    component: b.wasm
    depends_on: [source]
    input_from: source
    reduce: json_merge
"#,
        );
        assert!(wf.validate().is_ok());
    }

    // ── ExecutorKind ──────────────────────────────────────────────────────────

    #[test]
    fn executor_kind_defaults_to_local() {
        let wf: Workflow = serde_yaml::from_str(
            r#"
name: test
jobs:
  step:
    component: x.wasm
"#,
        )
        .expect("parse");
        assert_eq!(wf.jobs["step"].executor, ExecutorKind::Local);
    }

    #[test]
    fn executor_kind_parses_remote() {
        let wf: Workflow = serde_yaml::from_str(
            r#"
name: test
jobs:
  step:
    component: x.wasm
    executor: remote
"#,
        )
        .expect("parse");
        assert_eq!(wf.jobs["step"].executor, ExecutorKind::Remote);
    }

    #[test]
    fn executor_kind_serializes_as_snake_case() {
        assert_eq!(
            serde_json::to_string(&ExecutorKind::Local).unwrap(),
            "\"local\""
        );
        assert_eq!(
            serde_json::to_string(&ExecutorKind::Remote).unwrap(),
            "\"remote\""
        );
    }
}

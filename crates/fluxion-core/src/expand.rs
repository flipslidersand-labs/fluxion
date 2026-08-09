//! Foreach fan-out expansion.
//!
//! Transforms a `Workflow` that contains `foreach:` jobs into a fully-expanded
//! `Workflow` where each array element becomes its own numbered child job
//! (`job_id.0`, `job_id.1`, …).  `input_from:` fan-in jobs have their
//! `depends_on` rewritten to wait for every child.

use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use indexmap::IndexMap;
use serde_json::Value;

use crate::workflow::{JobDefinition, Workflow};

/// Result of expanding a workflow's foreach jobs.
pub struct ExpandedWorkflow {
    /// The rewritten workflow with no foreach jobs remaining.
    pub workflow: Workflow,
    /// Maps each original foreach job ID → its expanded child IDs.
    /// Non-foreach jobs are absent from this map.
    pub foreach_map: HashMap<String, Vec<String>>,
}

/// Expand all `foreach:` jobs in `wf`.
///
/// If the workflow contains no foreach jobs this is essentially a clone.
pub fn expand_foreach(wf: &Workflow, input_override: Option<&str>) -> Result<ExpandedWorkflow> {
    let mut new_jobs: IndexMap<String, JobDefinition> = IndexMap::new();
    let mut foreach_map: HashMap<String, Vec<String>> = HashMap::new();

    for (job_id, def) in &wf.jobs {
        if let Some(path) = &def.foreach {
            // Determine the input JSON to expand
            let input_str = input_override
                .map(|s| s.to_string())
                .or_else(|| def.input.clone())
                .unwrap_or_default();

            let items = extract_jsonpath_array(path, &input_str)
                .with_context(|| format!("Job '{}': failed to extract foreach array", job_id))?;

            let mut child_ids: Vec<String> = Vec::with_capacity(items.len());
            for (i, item) in items.into_iter().enumerate() {
                let child_id = format!("{}.{}", job_id, i);
                let item_input = serde_json::to_string(&item)
                    .with_context(|| format!("Job '{}': cannot serialise item {}", job_id, i))?;

                let child = JobDefinition {
                    component: def.component.clone(),
                    depends_on: def.depends_on.clone(),
                    input: Some(item_input),
                    permissions: def.permissions.clone(),
                    worker: def.worker.clone(),
                    env: def.env.clone(),
                    when: None,
                    foreach: None,
                    input_from: None,
                    max_parallel: def.max_parallel,
                    output_size_limit_mb: def.output_size_limit_mb,
                };
                new_jobs.insert(child_id.clone(), child);
                child_ids.push(child_id);
            }
            foreach_map.insert(job_id.clone(), child_ids);
        } else {
            new_jobs.insert(job_id.clone(), def.clone());
        }
    }

    // Rewrite depends_on references that point to a former foreach job.
    // Also rewrite input_from jobs: their depends_on becomes all children,
    // and their input will be assembled at runtime from child outputs.
    for (job_id, def) in new_jobs.iter_mut() {
        // Rewrite depends_on
        let new_deps: Vec<String> = def
            .depends_on
            .iter()
            .flat_map(|dep| {
                if let Some(children) = foreach_map.get(dep) {
                    children.clone()
                } else {
                    vec![dep.clone()]
                }
            })
            .collect();
        def.depends_on = new_deps;

        // input_from: add all children as additional deps if not already present
        if let Some(src) = &def.input_from.clone() {
            if let Some(children) = foreach_map.get(src) {
                for child in children {
                    if !def.depends_on.contains(child) {
                        def.depends_on.push(child.clone());
                    }
                }
                // Mark that this job needs fan-in input assembly (leave input_from intact)
            } else {
                bail!(
                    "Job '{}': input_from '{}' is not a foreach job or does not exist",
                    job_id,
                    src
                );
            }
        }
    }

    let workflow = Workflow {
        name: wf.name.clone(),
        jobs: new_jobs,
        workers: wf.workers.clone(),
        max_parallel: wf.max_parallel,
    };

    Ok(ExpandedWorkflow {
        workflow,
        foreach_map,
    })
}

/// Extract a JSON array from `input` at `path`.
///
/// Supported path forms:
/// - `"$"` or `"$[*]"` — use the top-level value as-is (must be an array)
/// - `"$.field"` — get a field from a JSON object
pub fn extract_jsonpath_array(path: &str, input: &str) -> Result<Vec<Value>> {
    // Empty input → treat as empty array for idempotency.
    if input.trim().is_empty() {
        return Ok(vec![]);
    }

    let root: Value = serde_json::from_str(input)
        .with_context(|| format!("foreach input is not valid JSON: {:?}", input))?;

    let val = if path == "$" || path == "$[*]" {
        root
    } else if let Some(field) = path.strip_prefix("$.") {
        root.get(field).cloned().with_context(|| {
            format!(
                "foreach path '{}': field '{}' not found in JSON",
                path, field
            )
        })?
    } else {
        bail!(
            "Unsupported foreach JSONPath '{}'. Supported: '$', '$[*]', '$.field'",
            path
        );
    };

    match val {
        Value::Array(arr) => Ok(arr),
        other => bail!(
            "foreach path '{}' resolved to {}, expected an array",
            path,
            other.type_str()
        ),
    }
}

trait TypeStr {
    fn type_str(&self) -> &'static str;
}

impl TypeStr for Value {
    fn type_str(&self) -> &'static str {
        match self {
            Value::Null => "null",
            Value::Bool(_) => "bool",
            Value::Number(_) => "number",
            Value::String(_) => "string",
            Value::Array(_) => "array",
            Value::Object(_) => "object",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_dollar_array() {
        let items = extract_jsonpath_array("$", r#"["a","b","c"]"#).unwrap();
        assert_eq!(items.len(), 3);
    }

    #[test]
    fn extract_dollar_star_array() {
        let items = extract_jsonpath_array("$[*]", r#"[1,2,3]"#).unwrap();
        assert_eq!(items.len(), 3);
    }

    #[test]
    fn extract_field_array() {
        let items = extract_jsonpath_array("$.items", r#"{"items":[{"id":1},{"id":2}]}"#).unwrap();
        assert_eq!(items.len(), 2);
    }

    #[test]
    fn empty_input_returns_empty() {
        let items = extract_jsonpath_array("$.items", "").unwrap();
        assert!(items.is_empty());
    }

    #[test]
    fn unsupported_path_errors() {
        assert!(extract_jsonpath_array("$.a.b.c", r#"{"a":{"b":{"c":[]}}}"#).is_err());
    }

    #[test]
    fn expand_foreach_creates_child_jobs() {
        use crate::workflow::JobDefinition;
        use indexmap::IndexMap;

        let mut jobs = IndexMap::new();
        jobs.insert(
            "process".to_string(),
            JobDefinition {
                component: "t.wasm".to_string(),
                depends_on: vec![],
                input: Some(r#"{"items":[1,2,3]}"#.to_string()),
                permissions: Default::default(),
                worker: None,
                env: Default::default(),
                when: None,
                foreach: Some("$.items".to_string()),
                input_from: None,
                max_parallel: None,
                output_size_limit_mb: None,
            },
        );
        jobs.insert(
            "aggregate".to_string(),
            JobDefinition {
                component: "m.wasm".to_string(),
                depends_on: vec!["process".to_string()],
                input: None,
                permissions: Default::default(),
                worker: None,
                env: Default::default(),
                when: None,
                foreach: None,
                input_from: Some("process".to_string()),
                max_parallel: None,
                output_size_limit_mb: None,
            },
        );

        let wf = Workflow {
            name: "test".to_string(),
            jobs,
            workers: vec![],
            max_parallel: None,
        };

        let expanded = expand_foreach(&wf, None).unwrap();
        let fw = &expanded.workflow;

        assert!(fw.jobs.contains_key("process.0"));
        assert!(fw.jobs.contains_key("process.1"));
        assert!(fw.jobs.contains_key("process.2"));
        assert!(!fw.jobs.contains_key("process"));

        let agg = &fw.jobs["aggregate"];
        assert!(agg.depends_on.contains(&"process.0".to_string()));
        assert!(agg.depends_on.contains(&"process.1".to_string()));
        assert!(agg.depends_on.contains(&"process.2".to_string()));

        let map = &expanded.foreach_map;
        assert_eq!(map["process"], vec!["process.0", "process.1", "process.2"]);
    }
}

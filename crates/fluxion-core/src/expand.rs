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

/// Returns true when `def` is a dynamic foreach job — has `foreach:` but no static `input:`.
/// These are expanded at runtime after their dependencies produce output.
pub fn is_dynamic_foreach(def: &JobDefinition) -> bool {
    def.foreach.is_some() && def.input.is_none()
}

/// Expand all **static** `foreach:` jobs in `wf`.
///
/// Jobs where `foreach` is set but `input` is absent are *dynamic* — they will be
/// expanded at runtime by [`expand_foreach_dynamic`] once their dependencies complete.
/// Those jobs are left untouched in the returned workflow.
///
/// If the workflow contains no static foreach jobs this is essentially a clone.
pub fn expand_foreach(wf: &Workflow, input_override: Option<&str>) -> Result<ExpandedWorkflow> {
    let mut new_jobs: IndexMap<String, JobDefinition> = IndexMap::new();
    let mut foreach_map: HashMap<String, Vec<String>> = HashMap::new();

    for (job_id, def) in &wf.jobs {
        if let Some(path) = &def.foreach {
            // Dynamic foreach (no static input) — leave the job in place; the scheduler
            // will expand it at runtime once its dependencies produce output.
            if is_dynamic_foreach(def) {
                new_jobs.insert(job_id.clone(), def.clone());
                continue;
            }

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
                    fail_fast: false,
                    component_sha256: def.component_sha256.clone(),
                    reduce: None,
                    executor: def.executor.clone(),
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
        workers_srv: wf.workers_srv.clone(),
    };

    Ok(ExpandedWorkflow {
        workflow,
        foreach_map,
    })
}

/// Expand a single dynamic foreach job at runtime, using the output bytes of a
/// completed dependency as the JSON input for the JSONPath expression.
///
/// Returns the child job IDs (e.g. `["process.0", "process.1", ...]`) and the
/// new child `JobDefinition`s to inject into the workflow.
///
/// The caller is responsible for removing the original dynamic foreach job from the
/// workflow and DAG and registering the children there.
pub fn expand_foreach_dynamic(
    job_id: &str,
    def: &JobDefinition,
    dep_output: &[u8],
) -> Result<(Vec<String>, IndexMap<String, JobDefinition>)> {
    let path = def
        .foreach
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("Job '{}': not a foreach job", job_id))?;

    let input_str = String::from_utf8_lossy(dep_output).into_owned();
    let items = extract_jsonpath_array(path, &input_str)
        .with_context(|| format!("Job '{}': failed to extract dynamic foreach array", job_id))?;

    let mut children: IndexMap<String, JobDefinition> = IndexMap::new();
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
            fail_fast: false,
            component_sha256: def.component_sha256.clone(),
            reduce: None,
        };
        children.insert(child_id.clone(), child);
        child_ids.push(child_id);
    }

    Ok((child_ids, children))
}

/// Extract items from `input` using a JSONPath `path` expression.
///
/// The full RFC 9535 JSONPath syntax is supported, including nested paths,
/// wildcards, slice notation, and filter expressions:
///
/// - `"$"` — root value (must be an array; its elements are returned)
/// - `"$[*]"` / `"$.items[*]"` — each matched node is returned as an item
/// - `"$.items[*].id"` — nested path; one item per matching leaf
/// - `"$.data[?(@.active==true)]"` — filter expression
///
/// When the query returns a single match that is a JSON array, the array is
/// unwrapped so callers receive the individual elements — this preserves
/// backward-compatible behaviour for paths like `"$"` and `"$.field"`.
pub fn extract_jsonpath_array(path: &str, input: &str) -> Result<Vec<Value>> {
    // Empty input → treat as empty array for idempotency.
    if input.trim().is_empty() {
        return Ok(vec![]);
    }

    let root: Value = serde_json::from_str(input)
        .with_context(|| format!("foreach input is not valid JSON: {:?}", input))?;

    let jpath = serde_json_path::JsonPath::parse(path)
        .with_context(|| format!("invalid JSONPath expression: '{}'", path))?;

    let matches: Vec<&Value> = jpath.query(&root).all();

    // Backward compat: `$` and `$.field` produce a single match that IS the
    // array. Unwrap it so callers get the individual elements.
    if matches.len() == 1
        && let Value::Array(arr) = matches[0]
    {
        return Ok(arr.clone());
    }

    Ok(matches.into_iter().cloned().collect())
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
    fn nested_path_supported() {
        let items = extract_jsonpath_array("$.a.b.c", r#"{"a":{"b":{"c":[1,2,3]}}}"#).unwrap();
        assert_eq!(items.len(), 3);
    }

    #[test]
    fn wildcard_nested_extracts_leaves() {
        let items = extract_jsonpath_array(
            "$.items[*].id",
            r#"{"items":[{"id":"x"},{"id":"y"},{"id":"z"}]}"#,
        )
        .unwrap();
        assert_eq!(items.len(), 3);
        assert_eq!(items[0], serde_json::json!("x"));
        assert_eq!(items[2], serde_json::json!("z"));
    }

    #[test]
    fn filter_expression_matches_active() {
        let items = extract_jsonpath_array(
            "$.data[?(@.active==true)]",
            r#"{"data":[{"id":1,"active":true},{"id":2,"active":false},{"id":3,"active":true}]}"#,
        )
        .unwrap();
        assert_eq!(items.len(), 2);
    }

    #[test]
    fn filter_expression_returns_empty_when_none_match() {
        let items = extract_jsonpath_array(
            "$.data[?(@.active==true)]",
            r#"{"data":[{"id":1,"active":false}]}"#,
        )
        .unwrap();
        assert!(items.is_empty());
    }

    #[test]
    fn invalid_jsonpath_syntax_errors() {
        assert!(extract_jsonpath_array("not-a-path", r#"[]"#).is_err());
    }

    #[test]
    fn is_dynamic_foreach_detects_missing_input() {
        let def = JobDefinition {
            component: "t.wasm".to_string(),
            depends_on: vec![],
            input: None,
            permissions: Default::default(),
            worker: None,
            env: Default::default(),
            when: None,
            foreach: Some("$.items".to_string()),
            input_from: None,
            max_parallel: None,
            output_size_limit_mb: None,
            fail_fast: false,
            component_sha256: None,
            reduce: None,
        };
        assert!(is_dynamic_foreach(&def));
        let static_def = JobDefinition {
            input: Some(r#"{"items":[1]}"#.to_string()),
            ..def
        };
        assert!(!is_dynamic_foreach(&static_def));
    }

    #[test]
    fn expand_foreach_dynamic_expands_from_dep_output() {
        let def = JobDefinition {
            component: "process.wasm".to_string(),
            depends_on: vec!["fetch".to_string()],
            input: None,
            permissions: Default::default(),
            worker: None,
            env: Default::default(),
            when: None,
            foreach: Some("$.items".to_string()),
            input_from: None,
            max_parallel: None,
            output_size_limit_mb: None,
            fail_fast: false,
            component_sha256: None,
            reduce: None,
        };
        let dep_output = br#"{"items":["x","y","z"]}"#;
        let (child_ids, children) = expand_foreach_dynamic("process", &def, dep_output).unwrap();

        assert_eq!(child_ids, vec!["process.0", "process.1", "process.2"]);
        assert_eq!(children.len(), 3);
        assert_eq!(children["process.0"].input.as_deref(), Some("\"x\""));
        assert_eq!(children["process.1"].input.as_deref(), Some("\"y\""));
        assert_eq!(children["process.2"].input.as_deref(), Some("\"z\""));
        // Children inherit the parent's deps
        assert_eq!(children["process.0"].depends_on, vec!["fetch".to_string()]);
        // Children are not foreach themselves
        assert!(children["process.0"].foreach.is_none());
    }

    #[test]
    fn expand_foreach_dynamic_empty_output_gives_zero_children() {
        let def = JobDefinition {
            component: "t.wasm".to_string(),
            depends_on: vec![],
            input: None,
            permissions: Default::default(),
            worker: None,
            env: Default::default(),
            when: None,
            foreach: Some("$.items".to_string()),
            input_from: None,
            max_parallel: None,
            output_size_limit_mb: None,
            fail_fast: false,
            component_sha256: None,
            reduce: None,
        };
        let (ids, children) = expand_foreach_dynamic("job", &def, b"").unwrap();
        assert!(ids.is_empty());
        assert!(children.is_empty());
    }

    #[test]
    fn expand_foreach_skips_dynamic_jobs() {
        use indexmap::IndexMap;

        let mut jobs = IndexMap::new();
        // Static foreach — gets expanded
        jobs.insert(
            "static_job".to_string(),
            JobDefinition {
                component: "t.wasm".to_string(),
                depends_on: vec![],
                input: Some(r#"{"items":[1,2]}"#.to_string()),
                permissions: Default::default(),
                worker: None,
                env: Default::default(),
                when: None,
                foreach: Some("$.items".to_string()),
                input_from: None,
                max_parallel: None,
                output_size_limit_mb: None,
                fail_fast: false,
                component_sha256: None,
                reduce: None,
            },
        );
        // Dynamic foreach — must NOT be expanded, stays as-is
        jobs.insert(
            "dynamic_job".to_string(),
            JobDefinition {
                component: "t.wasm".to_string(),
                depends_on: vec!["static_job".to_string()],
                input: None,
                permissions: Default::default(),
                worker: None,
                env: Default::default(),
                when: None,
                foreach: Some("$.data".to_string()),
                input_from: None,
                max_parallel: None,
                output_size_limit_mb: None,
                fail_fast: false,
                component_sha256: None,
                reduce: None,
            },
        );
        let wf = Workflow { name: "t".to_string(), jobs, workers: vec![], max_parallel: None, workers_srv: None };
        let expanded = expand_foreach(&wf, None).unwrap();
        let fw = &expanded.workflow;

        // Static job expanded into 2 children
        assert!(fw.jobs.contains_key("static_job.0"));
        assert!(fw.jobs.contains_key("static_job.1"));
        assert!(!fw.jobs.contains_key("static_job"));

        // Dynamic job remains unexpanded
        assert!(fw.jobs.contains_key("dynamic_job"));
        assert!(fw.jobs["dynamic_job"].foreach.is_some());
        assert!(fw.jobs["dynamic_job"].input.is_none());
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
                fail_fast: false,
                component_sha256: None,
                reduce: None,
                executor: Default::default(),
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
                fail_fast: false,
                component_sha256: None,
                reduce: None,
                executor: Default::default(),
            },
        );

        let wf = Workflow {
            name: "test".to_string(),
            jobs,
            workers: vec![],
            max_parallel: None,
            workers_srv: None,
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

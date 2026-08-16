//! Foreach fan-out expansion.
//!
//! Transforms a `Workflow` that contains `foreach:` jobs into a fully-expanded
//! `Workflow` where each array element becomes its own numbered child job
//! (`job_id.0`, `job_id.1`, …).  `input_from:` fan-in jobs have their
//! `depends_on` rewritten to wait for every child.

use std::collections::{HashMap, HashSet};

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
///
/// Jobs with `foreach:` but no `input:` are **dynamic foreach templates**: they are
/// kept in the workflow as-is and expanded at runtime when a predecessor completes.
/// Callers can detect them by checking `job.foreach.is_some() && job.input.is_none()`.
pub fn expand_foreach(wf: &Workflow, input_override: Option<&str>) -> Result<ExpandedWorkflow> {
    let mut new_jobs: IndexMap<String, JobDefinition> = IndexMap::new();
    let mut foreach_map: HashMap<String, Vec<String>> = HashMap::new();

    // Pass 1: expand static foreach jobs; keep dynamic templates as-is.
    let mut dynamic_templates: HashSet<String> = HashSet::new();
    for (job_id, def) in &wf.jobs {
        if let Some(path) = &def.foreach {
            let maybe_input = input_override
                .map(|s| s.to_string())
                .or_else(|| def.input.clone());

            if maybe_input.is_none() {
                // No static input → dynamic template; expand at runtime.
                dynamic_templates.insert(job_id.clone());
                new_jobs.insert(job_id.clone(), def.clone());
                continue;
            }

            let input_str = maybe_input.unwrap();
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
                    async_dispatch: def.async_dispatch,
                    oci_ref: def.oci_ref.clone(),
                };
                new_jobs.insert(child_id.clone(), child);
                child_ids.push(child_id);
            }
            foreach_map.insert(job_id.clone(), child_ids);
        } else {
            new_jobs.insert(job_id.clone(), def.clone());
        }
    }

    // Pass 2: rewrite depends_on and input_from references.
    //
    // - Static foreach: deps pointing to the template are replaced with its children.
    // - Dynamic foreach: deps pointing to the template are kept as-is (runtime will
    //   replace them with the actual children once expansion occurs).
    for (job_id, def) in new_jobs.iter_mut() {
        // Rewrite depends_on
        let new_deps: Vec<String> = def
            .depends_on
            .iter()
            .flat_map(|dep| {
                if let Some(children) = foreach_map.get(dep) {
                    // Statically-expanded foreach → point to all children.
                    children.clone()
                } else {
                    // Either a regular job or a dynamic template → keep as-is.
                    vec![dep.clone()]
                }
            })
            .collect();
        def.depends_on = new_deps;

        // input_from: add children as additional deps so the fan-in job waits for all.
        if let Some(src) = &def.input_from.clone() {
            if let Some(children) = foreach_map.get(src) {
                // Static foreach fan-in: add all children to depends_on.
                for child in children {
                    if !def.depends_on.contains(child) {
                        def.depends_on.push(child.clone());
                    }
                }
            } else if dynamic_templates.contains(src) {
                // Dynamic foreach fan-in: keep the dep on the template.
                // execute() will rewrite depends_on when it expands the template at runtime.
                if !def.depends_on.contains(src) {
                    def.depends_on.push(src.to_string());
                }
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

/// Dynamically expand a single foreach job using runtime output from a completed
/// predecessor.
///
/// This is the runtime analogue of `expand_foreach`. It is called when a foreach
/// job has no static `input:` field — the foreach array is discovered at runtime
/// from `completed_output`.
///
/// Returns `(child_id, child_def)` pairs. The caller is responsible for inserting
/// these into the running workflow and DAG.
pub fn expand_foreach_dynamic(
    foreach_job_id: &str,
    def: &JobDefinition,
    completed_output: &[u8],
) -> Result<Vec<(String, JobDefinition)>> {
    let path = def
        .foreach
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("job '{}' has no foreach field", foreach_job_id))?;

    let output_str = String::from_utf8_lossy(completed_output);
    let items = extract_jsonpath_array(path, &output_str)
        .with_context(|| format!("job '{}': dynamic foreach expansion failed", foreach_job_id))?;

    let mut children = Vec::with_capacity(items.len());
    for (i, item) in items.into_iter().enumerate() {
        let child_id = format!("{}.{}", foreach_job_id, i);
        let item_input = serde_json::to_string(&item)
            .with_context(|| format!("job '{}': cannot serialise item {}", foreach_job_id, i))?;
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
            async_dispatch: def.async_dispatch,
            oci_ref: def.oci_ref.clone(),
        };
        children.push((child_id, child));
    }
    Ok(children)
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
                async_dispatch: false,
                oci_ref: None,
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
                async_dispatch: false,
                oci_ref: None,
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

    fn make_dynamic_foreach_job(depends_on: Vec<String>) -> JobDefinition {
        JobDefinition {
            component: "process.wasm".to_string(),
            depends_on,
            input: None, // no static input → dynamic
            permissions: Default::default(),
            worker: None,
            env: Default::default(),
            when: None,
            foreach: Some("$.items[*]".to_string()),
            input_from: None,
            max_parallel: None,
            output_size_limit_mb: None,
            fail_fast: false,
            component_sha256: None,
            reduce: None,
            executor: Default::default(),
            async_dispatch: false,
            oci_ref: None,
        }
    }

    #[test]
    fn dynamic_foreach_template_kept_in_workflow() {
        use indexmap::IndexMap;

        let mut jobs = IndexMap::new();
        jobs.insert(
            "fetch".to_string(),
            JobDefinition {
                component: "fetch.wasm".to_string(),
                depends_on: vec![],
                input: Some("{}".to_string()),
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
                executor: Default::default(),
                async_dispatch: false,
                oci_ref: None,
            },
        );
        jobs.insert(
            "process".to_string(),
            make_dynamic_foreach_job(vec!["fetch".to_string()]),
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

        // Template must stay in the expanded workflow.
        assert!(
            fw.jobs.contains_key("process"),
            "dynamic template should be preserved"
        );
        assert!(
            fw.jobs["process"].foreach.is_some(),
            "foreach field must be intact"
        );
        assert!(fw.jobs["process"].input.is_none(), "input must remain None");
        // Not in foreach_map — signals it's dynamic.
        assert!(!expanded.foreach_map.contains_key("process"));
    }

    #[test]
    fn dynamic_foreach_fan_in_keeps_template_dep() {
        use indexmap::IndexMap;

        let mut jobs = IndexMap::new();
        jobs.insert(
            "fetch".to_string(),
            JobDefinition {
                component: "fetch.wasm".to_string(),
                depends_on: vec![],
                input: Some("{}".to_string()),
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
                executor: Default::default(),
                async_dispatch: false,
                oci_ref: None,
            },
        );
        jobs.insert(
            "process".to_string(),
            make_dynamic_foreach_job(vec!["fetch".to_string()]),
        );
        jobs.insert(
            "agg".to_string(),
            JobDefinition {
                component: "agg.wasm".to_string(),
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
                async_dispatch: false,
                oci_ref: None,
            },
        );

        let wf = Workflow {
            name: "dyn-fanin".to_string(),
            jobs,
            workers: vec![],
            max_parallel: None,
            workers_srv: None,
        };

        let expanded = expand_foreach(&wf, None).unwrap();
        let fw = &expanded.workflow;

        // Fan-in job must still depend on the template (not phantom children).
        assert!(
            fw.jobs["agg"].depends_on.contains(&"process".to_string()),
            "agg should keep dep on dynamic template"
        );
    }

    #[test]
    fn expand_foreach_dynamic_expands_from_predecessor_output() {
        let def = make_dynamic_foreach_job(vec!["fetch".to_string()]);
        let output = br#"{"items":["a","b","c"]}"#;
        let children = super::expand_foreach_dynamic("process", &def, output).unwrap();

        assert_eq!(children.len(), 3);
        assert_eq!(children[0].0, "process.0");
        assert_eq!(children[1].0, "process.1");
        assert_eq!(children[2].0, "process.2");
        // Each child has the static input for its item.
        assert_eq!(children[0].1.input.as_deref(), Some("\"a\""));
        assert_eq!(children[1].1.input.as_deref(), Some("\"b\""));
    }

    #[test]
    fn expand_foreach_dynamic_empty_output_gives_no_children() {
        let def = make_dynamic_foreach_job(vec![]);
        let children = super::expand_foreach_dynamic("proc", &def, b"").unwrap();
        assert!(children.is_empty());
    }
}

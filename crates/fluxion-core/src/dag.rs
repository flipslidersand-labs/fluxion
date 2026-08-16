use anyhow::{Result, bail};
use std::collections::{HashMap, VecDeque};

use crate::workflow::Workflow;

pub struct Dag {
    /// Job IDs in dependency-first topological order.
    pub topo_order: Vec<String>,
    /// job → its direct dependencies (must finish before job starts).
    pub deps: HashMap<String, Vec<String>>,
    /// job → jobs that are waiting on it.
    pub dependents: HashMap<String, Vec<String>>,
}

impl Dag {
    pub fn build(wf: &Workflow) -> Result<Self> {
        let deps: HashMap<String, Vec<String>> = wf
            .jobs
            .iter()
            .map(|(id, def)| (id.clone(), def.depends_on.clone()))
            .collect();

        let mut dependents: HashMap<String, Vec<String>> =
            wf.jobs.keys().map(|k| (k.clone(), vec![])).collect();
        for (job_id, job_deps) in &deps {
            for dep in job_deps {
                dependents
                    .entry(dep.clone())
                    .or_default()
                    .push(job_id.clone());
            }
        }

        let topo_order = kahn_sort(&deps)?;
        Ok(Self {
            topo_order,
            deps,
            dependents,
        })
    }

    /// Jobs with no dependencies — can start immediately.
    pub fn roots(&self) -> Vec<String> {
        self.deps
            .iter()
            .filter(|(_, d)| d.is_empty())
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Insert new job nodes into an existing DAG (used for dynamic foreach expansion).
    ///
    /// `new_deps` must only reference jobs that are already in the DAG or other
    /// entries in `new_deps`. New jobs are appended to `topo_order` in the order
    /// they appear in `new_jobs`.
    pub fn insert_nodes(&mut self, new_jobs: &[String], new_deps: &HashMap<String, Vec<String>>) {
        for job_id in new_jobs {
            let deps = new_deps.get(job_id).cloned().unwrap_or_default();
            for dep in &deps {
                self.dependents
                    .entry(dep.clone())
                    .or_default()
                    .push(job_id.clone());
            }
            self.deps.insert(job_id.clone(), deps);
            self.dependents.entry(job_id.clone()).or_default();
            self.topo_order.push(job_id.clone());
        }
    }
}

/// Kahn's algorithm: topological sort. Errors on cycle.
fn kahn_sort(deps: &HashMap<String, Vec<String>>) -> Result<Vec<String>> {
    // indegree[job] = number of unmet dependencies
    let mut indegree: HashMap<&str, usize> =
        deps.iter().map(|(k, v)| (k.as_str(), v.len())).collect();

    // notify[dep] = jobs that are waiting for `dep` to finish
    let mut notify: HashMap<&str, Vec<&str>> = HashMap::new();
    for (job, job_deps) in deps {
        for dep in job_deps {
            notify.entry(dep.as_str()).or_default().push(job.as_str());
        }
    }

    let mut queue: VecDeque<&str> = indegree
        .iter()
        .filter(|&(_, d)| *d == 0)
        .map(|(&id, _)| id)
        .collect();

    let mut order = Vec::new();
    while let Some(job) = queue.pop_front() {
        order.push(job.to_string());
        for &waiter in notify.get(job).into_iter().flatten() {
            let deg = indegree.get_mut(waiter).unwrap();
            *deg -= 1;
            if *deg == 0 {
                queue.push_back(waiter);
            }
        }
    }

    if order.len() != deps.len() {
        bail!("Workflow contains a circular dependency");
    }
    Ok(order)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_deps(pairs: &[(&str, &[&str])]) -> HashMap<String, Vec<String>> {
        pairs
            .iter()
            .map(|(k, vs)| (k.to_string(), vs.iter().map(|s| s.to_string()).collect()))
            .collect()
    }

    #[test]
    fn linear_chain_is_ordered() {
        let deps = make_deps(&[("a", &[]), ("b", &["a"]), ("c", &["b"])]);
        let order = kahn_sort(&deps).unwrap();
        assert_eq!(order, ["a", "b", "c"]);
    }

    #[test]
    fn parallel_roots_end_with_merge() {
        let deps = make_deps(&[("a", &[]), ("b", &[]), ("c", &["a", "b"])]);
        let order = kahn_sort(&deps).unwrap();
        assert_eq!(order.len(), 3);
        assert_eq!(order.last().unwrap(), "c");
    }

    #[test]
    fn cycle_is_detected() {
        let deps = make_deps(&[("a", &["b"]), ("b", &["a"])]);
        assert!(kahn_sort(&deps).is_err());
    }
}

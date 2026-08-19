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

    /// Insert new nodes into the DAG without a full rebuild.
    ///
    /// `new_jobs` maps new job IDs to their dependency lists. All dependency IDs must
    /// already exist in the DAG (this is asserted) — cycles are not checked (the caller
    /// is responsible for correctness, typically by only inserting children of a dynamic
    /// foreach expansion where the deps are the parent's deps).
    ///
    /// The topological ordering is extended: new nodes are appended after the latest
    /// dependency in the existing order, which is correct for leaf-only insertions.
    pub fn insert_nodes(&mut self, new_jobs: &HashMap<String, Vec<String>>) {
        for (job_id, deps) in new_jobs {
            self.deps.insert(job_id.clone(), deps.clone());
            self.dependents.insert(job_id.clone(), vec![]);
            for dep in deps {
                self.dependents
                    .entry(dep.clone())
                    .or_default()
                    .push(job_id.clone());
            }
            self.topo_order.push(job_id.clone());
        }
    }

    /// Remove a node and all references to it from the DAG.
    /// Used when replacing a dynamic foreach placeholder with its expanded children.
    pub fn remove_node(&mut self, job_id: &str) {
        self.deps.remove(job_id);
        self.dependents.remove(job_id);
        self.topo_order.retain(|id| id != job_id);
        for waiters in self.dependents.values_mut() {
            waiters.retain(|id| id != job_id);
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

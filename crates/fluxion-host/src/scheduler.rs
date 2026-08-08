use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use fluxion_core::{
    dag::Dag,
    runner::{JobResult, RunResult},
    state::JobStatus,
    store::RunStore,
    workflow::Workflow,
};
use tokio::sync::{Semaphore, mpsc};
use tracing::{Instrument, info_span};

use crate::{FluxionHost, remote};

/// Run a workflow from scratch, printing progress to stdout.
pub async fn run(wf: &Workflow, workflow_path: &Path, host: Arc<FluxionHost>) -> Result<RunResult> {
    run_inner(wf, workflow_path, host, HashMap::new(), true).await
}

/// Run a workflow silently (no stdout) — for MCP / programmatic use.
pub async fn run_silent(
    wf: &Workflow,
    workflow_path: &Path,
    host: Arc<FluxionHost>,
) -> Result<RunResult> {
    run_inner(wf, workflow_path, host, HashMap::new(), false).await
}

/// Retry a previous run, re-executing `from_job` and all downstream dependents.
pub async fn retry(
    wf: &Workflow,
    workflow_path: &Path,
    host: Arc<FluxionHost>,
    prev_run_id: &str,
    from_job: &str,
) -> Result<RunResult> {
    retry_inner(wf, workflow_path, host, prev_run_id, from_job, true).await
}

/// Retry silently — for MCP / programmatic use.
pub async fn retry_silent(
    wf: &Workflow,
    workflow_path: &Path,
    host: Arc<FluxionHost>,
    prev_run_id: &str,
    from_job: &str,
) -> Result<RunResult> {
    retry_inner(wf, workflow_path, host, prev_run_id, from_job, false).await
}

async fn run_inner(
    wf: &Workflow,
    workflow_path: &Path,
    host: Arc<FluxionHost>,
    pre_succeeded: HashMap<String, JobStatus>,
    print_progress: bool,
) -> Result<RunResult> {
    let store = RunStore::open()?;
    let run_id = RunStore::new_run_id();
    store.create_run(&run_id, &wf.name, workflow_path)?;
    if print_progress {
        println!("Run ID: {run_id}");
    }

    let permits = wf.max_parallel.unwrap_or(Semaphore::MAX_PERMITS);
    let sem = Arc::new(Semaphore::new(permits));

    let span = info_span!("fluxion.run", run_id = %run_id, workflow = %wf.name);
    let result = execute(wf, host, &store, &run_id, pre_succeeded, print_progress, sem)
        .instrument(span)
        .await?;
    store.complete_run(&run_id, result.success)?;
    Ok(result)
}

async fn retry_inner(
    wf: &Workflow,
    workflow_path: &Path,
    host: Arc<FluxionHost>,
    prev_run_id: &str,
    from_job: &str,
    print_progress: bool,
) -> Result<RunResult> {
    let store = RunStore::open()?;
    let (_, prev_states) = store.load_run(prev_run_id)?;
    let dag = Dag::build(wf)?;
    let replay_set = downstream_inclusive(&dag, from_job);

    let pre_succeeded: HashMap<String, JobStatus> = prev_states
        .into_iter()
        .filter(|(id, status)| {
            matches!(status, JobStatus::Succeeded { .. }) && !replay_set.contains(id.as_str())
        })
        .collect();

    let run_id = RunStore::new_run_id();
    store.create_run(&run_id, &wf.name, workflow_path)?;
    if print_progress {
        println!(
            "Retry run ID: {run_id}  (from '{from_job}', skipping {} pre-succeeded jobs)",
            pre_succeeded.len()
        );
    }

    let permits = wf.max_parallel.unwrap_or(Semaphore::MAX_PERMITS);
    let sem = Arc::new(Semaphore::new(permits));

    let span = info_span!("fluxion.run", run_id = %run_id, workflow = %wf.name, retry = true);
    let result = execute(wf, host, &store, &run_id, pre_succeeded, print_progress, sem)
        .instrument(span)
        .await?;
    store.complete_run(&run_id, result.success)?;
    Ok(result)
}

/// Core execution loop. Returns a structured RunResult.
async fn execute(
    wf: &Workflow,
    host: Arc<FluxionHost>,
    store: &RunStore,
    run_id: &str,
    pre_succeeded: HashMap<String, JobStatus>,
    print_progress: bool,
    sem: Arc<Semaphore>,
) -> Result<RunResult> {
    let dag = Dag::build(wf)?;
    let pad = wf.jobs.keys().map(|k| k.len()).max().unwrap_or(0);

    let mut statuses: HashMap<String, JobStatus> = wf
        .jobs
        .keys()
        .map(|k| (k.clone(), JobStatus::Pending))
        .collect();

    let mut job_results: Vec<JobResult> = Vec::new();

    // Seed pre-succeeded jobs
    for (id, status) in &pre_succeeded {
        store.upsert_job(run_id, id, status)?;
        statuses.insert(id.clone(), status.clone());
        if let JobStatus::Succeeded { elapsed } = status {
            if print_progress {
                println!(
                    "[skip] {:<pad$}  SUCCESS  {:.2}s  (previous run)",
                    id,
                    elapsed.as_secs_f64(),
                    pad = pad
                );
            }
            job_results.push(JobResult::from_succeeded(id.clone(), *elapsed, true));
        }
    }

    let (tx, mut rx) = mpsc::unbounded_channel::<JobEvent>();
    let rr = Arc::new(AtomicUsize::new(0));

    let mut in_flight = 0usize;
    for job_id in dag.roots() {
        if pre_succeeded.contains_key(&job_id) {
            continue;
        }
        if print_progress {
            print_running(&job_id, pad);
        }
        store.upsert_job(run_id, &job_id, &JobStatus::Running)?;
        let worker_url = resolve_worker(&job_id, wf, &rr);
        launch(&job_id, wf, host.clone(), tx.clone(), worker_url, sem.clone());
        statuses.insert(job_id, JobStatus::Running);
        in_flight += 1;
    }

    for (job_id, job) in &wf.jobs {
        if pre_succeeded.contains_key(job_id) || dag.roots().contains(job_id) {
            continue;
        }
        if job.depends_on.iter().all(|d| pre_succeeded.contains_key(d)) {
            if print_progress {
                print_running(job_id, pad);
            }
            store.upsert_job(run_id, job_id, &JobStatus::Running)?;
            let worker_url = resolve_worker(job_id, wf, &rr);
            launch(job_id, wf, host.clone(), tx.clone(), worker_url, sem.clone());
            statuses.insert(job_id.clone(), JobStatus::Running);
            in_flight += 1;
        }
    }

    let workflow_start = Instant::now();
    let mut overall_success = true;

    while in_flight > 0 {
        let Some(event) = rx.recv().await else { break };

        if print_progress {
            print_result(&event, pad);
        }
        store.upsert_job(run_id, &event.job_id, &event.status)?;
        statuses.insert(event.job_id.clone(), event.status.clone());
        in_flight -= 1;

        match &event.status {
            JobStatus::Succeeded { elapsed } => {
                job_results.push(JobResult::from_succeeded_with_metrics(
                    event.job_id.clone(),
                    *elapsed,
                    event.compile_us,
                    event.instantiate_us,
                    event.execute_us,
                ));
            }
            JobStatus::Failed { elapsed, reason } => {
                overall_success = false;
                job_results.push(JobResult::from_failed(
                    event.job_id.clone(),
                    *elapsed,
                    reason.clone(),
                ));
                if print_progress {
                    eprintln!(
                        "\nReason:\n  {}\n\nRetry:\n  fluxion retry {} --from {}",
                        reason, run_id, event.job_id
                    );
                }
                break;
            }
            _ => {}
        }

        if overall_success {
            for dep in dag.dependents.get(&event.job_id).into_iter().flatten() {
                if pre_succeeded.contains_key(dep) {
                    continue;
                }
                let all_done = dag.deps[dep]
                    .iter()
                    .all(|d| matches!(statuses[d], JobStatus::Succeeded { .. }));
                if all_done {
                    if print_progress {
                        print_running(dep, pad);
                    }
                    store.upsert_job(run_id, dep, &JobStatus::Running)?;
                    let worker_url = resolve_worker(dep, wf, &rr);
                    launch(dep, wf, host.clone(), tx.clone(), worker_url, sem.clone());
                    statuses.insert(dep.clone(), JobStatus::Running);
                    in_flight += 1;
                }
            }
        }
    }

    let total_elapsed_ms = workflow_start.elapsed().as_millis() as u64;
    let succeeded = job_results
        .iter()
        .filter(|j| j.status == "succeeded")
        .count();
    let total = dag.topo_order.len();

    if print_progress {
        println!(
            "\nCompleted {}/{} jobs in {:.2}s",
            succeeded,
            total,
            total_elapsed_ms as f64 / 1000.0
        );
    }

    Ok(RunResult {
        run_id: run_id.to_string(),
        workflow_name: wf.name.clone(),
        jobs: job_results,
        total_elapsed_ms,
        succeeded,
        total,
        success: overall_success,
    })
}

struct JobEvent {
    job_id: String,
    status: JobStatus,
    /// Phase metrics for succeeded jobs; all zeros for failures/skips.
    compile_us: u64,
    instantiate_us: u64,
    execute_us: u64,
}

/// Resolve which worker URL to use for a job.
/// Priority: job.worker → round-robin from wf.workers → None (local).
fn resolve_worker(job_id: &str, wf: &Workflow, rr: &AtomicUsize) -> Option<String> {
    if let Some(url) = &wf.jobs[job_id].worker {
        return Some(url.clone());
    }
    if wf.workers.is_empty() {
        return None;
    }
    let idx = rr.fetch_add(1, Ordering::Relaxed) % wf.workers.len();
    Some(wf.workers[idx].clone())
}

fn launch(
    job_id: &str,
    wf: &Workflow,
    host: Arc<FluxionHost>,
    tx: mpsc::UnboundedSender<JobEvent>,
    worker_url: Option<String>,
    sem: Arc<Semaphore>,
) {
    let job_id = job_id.to_string();
    let component = wf.jobs[&job_id].component.clone();
    let input = wf.jobs[&job_id]
        .input
        .clone()
        .unwrap_or_default()
        .into_bytes();
    let perms = wf.jobs[&job_id].permissions.clone();
    let env = wf.jobs[&job_id].env.clone();
    let timeout_secs = perms.limits.timeout_secs;

    let span = info_span!("fluxion.job", job.id = %job_id, component = %component);

    tokio::spawn(
        async move {
            let _permit = sem.acquire_owned().await.expect("semaphore closed");
            let start = Instant::now();

            let run_result: anyhow::Result<(Vec<u8>, crate::JobMetrics)> =
                if let Some(ref url) = worker_url {
                    remote::run_remote(url, &component, input, &perms, &env).await
                } else {
                    let c = component.clone();
                    let p = perms.clone();
                    let e = env.clone();
                    match tokio::time::timeout(
                        Duration::from_secs(timeout_secs),
                        tokio::task::spawn_blocking(move || {
                            host.run_component_measured(&c, input, &p, &e)
                        }),
                    )
                    .await
                    {
                        Err(_) => Err(anyhow::anyhow!("Timeout after {}s", timeout_secs)),
                        Ok(Err(e)) => Err(anyhow::anyhow!("{}", e)),
                        Ok(Ok(r)) => r,
                    }
                };

            let elapsed = start.elapsed();
            let (status, compile_us, instantiate_us, execute_us) = match run_result {
                Ok((_, m)) => (
                    JobStatus::Succeeded { elapsed },
                    m.compile.as_micros() as u64,
                    m.instantiate.as_micros() as u64,
                    m.execute.as_micros() as u64,
                ),
                Err(e) => (
                    JobStatus::Failed { elapsed, reason: e.to_string() },
                    0,
                    0,
                    0,
                ),
            };

            tracing::info!(
                status = status.label(),
                elapsed_ms = elapsed.as_millis() as u64,
                compile_us,
                worker = worker_url.as_deref().unwrap_or("local"),
                "job finished"
            );

            let _ = tx.send(JobEvent {
                job_id,
                status,
                compile_us,
                instantiate_us,
                execute_us,
            });
        }
        .instrument(span),
    );
}

fn downstream_inclusive<'a>(dag: &'a Dag, start: &'a str) -> std::collections::HashSet<&'a str> {
    let mut visited = std::collections::HashSet::new();
    let mut queue = std::collections::VecDeque::new();
    queue.push_back(start);
    while let Some(node) = queue.pop_front() {
        if visited.insert(node) {
            for dep in dag.dependents.get(node).into_iter().flatten() {
                queue.push_back(dep.as_str());
            }
        }
    }
    visited
}

fn timestamp() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let h = (secs / 3600) % 24;
    let m = (secs / 60) % 60;
    let s = secs % 60;
    format!("{:02}:{:02}:{:02}", h, m, s)
}

fn print_running(job_id: &str, pad: usize) {
    println!("[{}] {:<pad$}  RUNNING", timestamp(), job_id, pad = pad);
}

// ── #31 max_parallel ─────────────────────────────────────────────────────────
// Tests live here once Workflow gains `max_parallel: Option<usize>`.
//
// Verification approach (to be added in the PR):
//   1. Add `max_parallel: Option<usize>` to Workflow struct.
//   2. Wrap execute()'s job-launch in Arc<Semaphore> acquired from max_parallel.
//   3. In tests: use an AtomicUsize to track peak concurrency and assert ≤ N.
//
// CLI-level smoke test is in crates/fluxion-cli/tests/cli_tests.rs (#31).

fn print_result(event: &JobEvent, pad: usize) {
    match &event.status {
        JobStatus::Succeeded { elapsed } => println!(
            "[{}] {:<pad$}  SUCCESS  {:.2}s",
            timestamp(),
            event.job_id,
            elapsed.as_secs_f64(),
            pad = pad
        ),
        JobStatus::Failed { elapsed, reason: _ } => println!(
            "[{}] {:<pad$}  FAILED   {:.2}s",
            timestamp(),
            event.job_id,
            elapsed.as_secs_f64(),
            pad = pad
        ),
        _ => {}
    }
}

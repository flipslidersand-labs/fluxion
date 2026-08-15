use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use fluxion_core::{
    dag::Dag,
    expand::expand_foreach,
    runner::{JobResult, RunResult},
    state::JobStatus,
    store::RunStore,
    workflow::{ExecutorKind, PermissionSet, ReduceMode, TlsConfig, Workflow},
};
use tokio::sync::{Semaphore, mpsc};
use tracing::{Instrument, info_span};

use crate::{FluxionHost, remote};

/// Load-balancing strategy for distributing jobs across remote workers.
#[derive(Debug, Clone, Default, clap::ValueEnum)]
pub enum LbStrategy {
    /// Plain round-robin: cycle through workers in order (default).
    #[default]
    RoundRobin,
    /// Weighted round-robin: distribute proportionally to each worker's `weight`.
    Weighted,
    /// Least-connections: query each worker's `/health` endpoint and send the next
    /// job to the worker reporting the fewest active jobs.
    LeastConn,
}

/// Run a workflow from scratch, printing progress to stdout.
pub async fn run(wf: &Workflow, workflow_path: &Path, host: Arc<FluxionHost>) -> Result<RunResult> {
    run_inner(
        wf,
        workflow_path,
        host,
        HashMap::new(),
        true,
        LbStrategy::default(),
    )
    .await
}

/// Run a workflow from scratch with the specified load-balancing strategy.
pub async fn run_with_strategy(
    wf: &Workflow,
    workflow_path: &Path,
    host: Arc<FluxionHost>,
    strategy: LbStrategy,
) -> Result<RunResult> {
    run_inner(wf, workflow_path, host, HashMap::new(), true, strategy).await
}

/// Run a workflow silently (no stdout) — for MCP / programmatic use.
pub async fn run_silent(
    wf: &Workflow,
    workflow_path: &Path,
    host: Arc<FluxionHost>,
) -> Result<RunResult> {
    run_inner(
        wf,
        workflow_path,
        host,
        HashMap::new(),
        false,
        LbStrategy::default(),
    )
    .await
}

/// Run a workflow silently with the specified load-balancing strategy.
pub async fn run_silent_with_strategy(
    wf: &Workflow,
    workflow_path: &Path,
    host: Arc<FluxionHost>,
    strategy: LbStrategy,
) -> Result<RunResult> {
    run_inner(wf, workflow_path, host, HashMap::new(), false, strategy).await
}

/// Retry a previous run, re-executing `from_job` and all downstream dependents.
pub async fn retry(
    wf: &Workflow,
    workflow_path: &Path,
    host: Arc<FluxionHost>,
    prev_run_id: &str,
    from_job: &str,
) -> Result<RunResult> {
    retry_inner(
        wf,
        workflow_path,
        host,
        prev_run_id,
        from_job,
        true,
        LbStrategy::default(),
    )
    .await
}

/// Retry silently — for MCP / programmatic use.
pub async fn retry_silent(
    wf: &Workflow,
    workflow_path: &Path,
    host: Arc<FluxionHost>,
    prev_run_id: &str,
    from_job: &str,
) -> Result<RunResult> {
    retry_inner(
        wf,
        workflow_path,
        host,
        prev_run_id,
        from_job,
        false,
        LbStrategy::default(),
    )
    .await
}

/// Perform GET /health on each candidate URL concurrently.
/// Returns the subset that responded successfully, updating the DB health status.
async fn health_check_workers(candidates: &[String]) -> Vec<String> {
    if candidates.is_empty() {
        return Vec::new();
    }
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest client");

    let store = RunStore::open().ok();

    let checks = candidates.iter().map(|url| {
        let url = url.clone();
        let client = client.clone();
        async move {
            let health_url = format!("{url}/health");
            let ok = client
                .get(&health_url)
                .send()
                .await
                .map(|r| r.status().is_success())
                .unwrap_or(false);
            (url, ok)
        }
    });
    let results = futures::future::join_all(checks).await;

    let healthy: Vec<String> = results
        .iter()
        .filter_map(|(url, ok)| if *ok { Some(url.clone()) } else { None })
        .collect();

    if let Some(s) = &store {
        for (url, ok) in &results {
            let _ = s.update_worker_health(url, *ok);
        }
    }

    healthy
}

/// Resolve the effective worker list for a workflow run.
///
/// 1. Start with static `workers:` from YAML.
/// 2. If `workers_srv:` is set, resolve SRV and merge results.
/// 3. If both are empty, fall back to DB-registered workers.
///
/// Returns only health-checked workers.
async fn effective_workers(wf: &Workflow) -> Vec<String> {
    let mut candidates: Vec<String> = wf.workers.iter().map(|w| w.url().to_string()).collect();

    // Merge SRV-discovered workers (failures are silently ignored).
    if let Some(srv) = &wf.workers_srv {
        let mut srv_urls = crate::resolve_srv_workers(srv).await;
        candidates.append(&mut srv_urls);
    }

    if candidates.is_empty() {
        candidates = RunStore::open()
            .and_then(|s| s.registered_worker_urls())
            .unwrap_or_default();
    }
    health_check_workers(&candidates).await
}

/// Resolve effective workers and return them as `WorkerInfo`, preserving TLS config.
async fn effective_workers_info(wf: &Workflow) -> Vec<WorkerInfo> {
    let healthy_urls = effective_workers(wf).await;
    healthy_urls
        .into_iter()
        .map(|url| {
            let tls = wf
                .workers
                .iter()
                .find(|w| w.url() == url.as_str())
                .and_then(|w| w.tls().cloned());
            WorkerInfo { url, tls }
        })
        .collect()
}

async fn run_inner(
    wf: &Workflow,
    workflow_path: &Path,
    host: Arc<FluxionHost>,
    pre_succeeded: HashMap<String, JobStatus>,
    print_progress: bool,
    strategy: LbStrategy,
) -> Result<RunResult> {
    let store = RunStore::open()?;
    let run_id = RunStore::new_run_id();
    store.create_run(&run_id, &wf.name, workflow_path)?;
    if print_progress {
        println!("Run ID: {run_id}");
    }

    let workers = effective_workers_info(wf).await;
    let permits = wf.max_parallel.unwrap_or(Semaphore::MAX_PERMITS);
    let sem = Arc::new(Semaphore::new(permits));

    let span = info_span!("fluxion.run", run_id = %run_id, workflow = %wf.name);
    let result = execute(
        wf,
        ExecOpts {
            host,
            store: &store,
            run_id: &run_id,
            pre_succeeded,
            print_progress,
            sem,
            strategy,
            workers,
        },
    )
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
    strategy: LbStrategy,
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

    let workers = effective_workers_info(wf).await;
    let permits = wf.max_parallel.unwrap_or(Semaphore::MAX_PERMITS);
    let sem = Arc::new(Semaphore::new(permits));

    let span = info_span!("fluxion.run", run_id = %run_id, workflow = %wf.name, retry = true);
    let result = execute(
        wf,
        ExecOpts {
            host,
            store: &store,
            run_id: &run_id,
            pre_succeeded,
            print_progress,
            sem,
            strategy,
            workers,
        },
    )
    .instrument(span)
    .await?;
    store.complete_run(&run_id, result.success)?;
    Ok(result)
}

struct ExecOpts<'a> {
    host: Arc<FluxionHost>,
    store: &'a RunStore,
    run_id: &'a str,
    pre_succeeded: HashMap<String, JobStatus>,
    print_progress: bool,
    sem: Arc<Semaphore>,
    strategy: LbStrategy,
    workers: Vec<WorkerInfo>,
}

/// Core execution loop. Returns a structured RunResult.
async fn execute(wf: &Workflow, opts: ExecOpts<'_>) -> Result<RunResult> {
    let ExecOpts {
        host,
        store,
        run_id,
        pre_succeeded,
        print_progress,
        sem,
        strategy,
        workers,
    } = opts;
    // ── Expand foreach jobs ─────────────────────────────────────────────────
    let expanded = expand_foreach(wf, None)?;
    let wf = &expanded.workflow;
    let foreach_map = &expanded.foreach_map;

    let dag = Dag::build(wf)?;

    // Validate: executor:remote jobs require at least one worker configured.
    for (job_id, job) in &wf.jobs {
        if job.executor == ExecutorKind::Remote && job.worker.is_none() && workers.is_empty() {
            anyhow::bail!(
                "job '{}' uses executor:remote but no workers are configured",
                job_id
            );
        }
    }

    let pad = wf.jobs.keys().map(|k| k.len()).max().unwrap_or(0);

    let mut statuses: HashMap<String, JobStatus> = wf
        .jobs
        .keys()
        .map(|k| (k.clone(), JobStatus::Pending))
        .collect();

    let mut job_results: Vec<JobResult> = Vec::new();
    // Per-job output bytes (only stored when needed for fan-in).
    let mut job_outputs: HashMap<String, Vec<u8>> = HashMap::new();

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
        let job_workers = resolve_workers(&job_id, wf, &workers, &rr, &strategy).await;
        launch(
            &job_id,
            wf,
            host.clone(),
            tx.clone(),
            job_workers,
            sem.clone(),
            None,
        );
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
            let job_workers = resolve_workers(job_id, wf, &workers, &rr, &strategy).await;
            let fanin_input =
                build_fanin_input(job_id, wf, foreach_map, &job_outputs, host.clone()).await;
            launch(
                job_id,
                wf,
                host.clone(),
                tx.clone(),
                job_workers,
                sem.clone(),
                fanin_input,
            );
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
                crate::metrics::ACTIVE_JOBS.dec();
                crate::metrics::JOBS_TOTAL
                    .with_label_values(&["succeeded", &event.job_id])
                    .inc();
                crate::metrics::JOB_DURATION
                    .with_label_values(&[&event.job_id])
                    .observe(elapsed.as_secs_f64());
                // Store output if this job is a foreach child (needed for fan-in)
                if let Some(output) = event.output.clone() {
                    job_outputs.insert(event.job_id.clone(), output);
                }
                job_results.push(JobResult::from_succeeded_with_metrics(
                    event.job_id.clone(),
                    *elapsed,
                    event.compile_us,
                    event.instantiate_us,
                    event.execute_us,
                ));

                // Print foreach group progress if applicable
                if print_progress {
                    print_foreach_progress(&event.job_id, foreach_map, &statuses, pad);
                }
            }
            JobStatus::Failed { elapsed, reason } => {
                crate::metrics::ACTIVE_JOBS.dec();
                crate::metrics::JOBS_TOTAL
                    .with_label_values(&["failed", &event.job_id])
                    .inc();
                crate::metrics::JOB_DURATION
                    .with_label_values(&[&event.job_id])
                    .observe(elapsed.as_secs_f64());
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

                // Check if the failed job is a foreach child.
                let foreach_parent = foreach_map
                    .iter()
                    .find(|(_, children)| children.contains(&event.job_id))
                    .map(|(parent, children)| (parent.clone(), children.clone()));

                match foreach_parent {
                    Some((parent_id, siblings)) => {
                        let fail_fast = wf.jobs.get(&parent_id).is_some_and(|j| j.fail_fast);
                        if fail_fast {
                            // Cancel all pending siblings immediately.
                            for sibling in &siblings {
                                if sibling == &event.job_id {
                                    continue;
                                }
                                if matches!(
                                    statuses.get(sibling.as_str()),
                                    Some(JobStatus::Pending) | Some(JobStatus::Ready)
                                ) {
                                    let cancel = JobStatus::Cancelled;
                                    store.upsert_job(run_id, sibling, &cancel)?;
                                    statuses.insert(sibling.clone(), cancel);
                                    if print_progress {
                                        println!(
                                            "[{}] {:<pad$}  CANCELLED (fail_fast)",
                                            timestamp(),
                                            sibling,
                                            pad = pad
                                        );
                                    }
                                }
                            }
                            break;
                        }
                        // fail_fast=false: let remaining siblings run to completion;
                        // the fan-in cancellation happens in the dep-ready check above.
                    }
                    None => break, // non-foreach job failure → stop immediately
                }
            }
            _ => {}
        }

        if overall_success {
            for dep in dag.dependents.get(&event.job_id).into_iter().flatten() {
                if pre_succeeded.contains_key(dep) {
                    continue;
                }
                // All deps must be in a terminal done state (Succeeded OR Skipped).
                let all_done = dag.deps[dep].iter().all(|d| {
                    matches!(
                        statuses[d],
                        JobStatus::Succeeded { .. } | JobStatus::Skipped | JobStatus::Cancelled
                    )
                });
                if all_done {
                    let job_def = &wf.jobs[dep];

                    // Cancel fan-in if any foreach child failed.
                    let foreach_child_failed = if let Some(src) = &job_def.input_from {
                        if let Some(children) = foreach_map.get(src) {
                            children
                                .iter()
                                .any(|c| matches!(statuses.get(c), Some(JobStatus::Failed { .. })))
                        } else {
                            false
                        }
                    } else {
                        false
                    };

                    if foreach_child_failed {
                        let cancel_status = JobStatus::Cancelled;
                        store.upsert_job(run_id, dep, &cancel_status)?;
                        statuses.insert(dep.clone(), cancel_status);
                        if print_progress {
                            println!(
                                "[{}] {:<pad$}  CANCELLED (foreach child failed)",
                                timestamp(),
                                dep,
                                pad = pad
                            );
                        }
                        continue;
                    }

                    // Check if any dep is Skipped and no `when:` guard is present → propagate skip.
                    let any_dep_skipped = dag.deps[dep]
                        .iter()
                        .any(|d| matches!(statuses[d], JobStatus::Skipped));

                    let should_skip = if let Some(when_expr) = &job_def.when {
                        !eval_when(when_expr, &statuses)
                    } else {
                        any_dep_skipped
                    };

                    if should_skip {
                        if print_progress {
                            println!("[{}] {:<pad$}  SKIPPED", timestamp(), dep, pad = pad);
                        }
                        store.upsert_job(run_id, dep, &JobStatus::Skipped)?;
                        statuses.insert(dep.clone(), JobStatus::Skipped);
                        job_results.push(JobResult::from_skipped(dep.to_string()));
                        // BFS cascade: propagate Skipped to all downstream jobs.
                        let mut cascade_queue: std::collections::VecDeque<String> =
                            std::collections::VecDeque::new();
                        cascade_queue.push_back(dep.clone());
                        while let Some(skipped_id) = cascade_queue.pop_front() {
                            for downstream in dag.dependents.get(&skipped_id).into_iter().flatten()
                            {
                                if pre_succeeded.contains_key(downstream) {
                                    continue;
                                }
                                if matches!(statuses[downstream.as_str()], JobStatus::Pending) {
                                    let all_cascade_done =
                                        dag.deps[downstream.as_str()].iter().all(|d| {
                                            matches!(
                                                statuses[d],
                                                JobStatus::Succeeded { .. }
                                                    | JobStatus::Skipped
                                                    | JobStatus::Cancelled
                                            )
                                        });
                                    if all_cascade_done {
                                        let downstream_def = &wf.jobs[downstream.as_str()];
                                        let cascade_skip =
                                            if let Some(when_expr) = &downstream_def.when {
                                                !eval_when(when_expr, &statuses)
                                            } else {
                                                true
                                            };
                                        if cascade_skip {
                                            if print_progress {
                                                println!(
                                                    "[{}] {:<pad$}  SKIPPED",
                                                    timestamp(),
                                                    downstream,
                                                    pad = pad
                                                );
                                            }
                                            store.upsert_job(
                                                run_id,
                                                downstream,
                                                &JobStatus::Skipped,
                                            )?;
                                            statuses.insert(downstream.clone(), JobStatus::Skipped);
                                            job_results
                                                .push(JobResult::from_skipped(downstream.clone()));
                                            cascade_queue.push_back(downstream.clone());
                                        } else {
                                            if print_progress {
                                                print_running(downstream, pad);
                                            }
                                            store.upsert_job(
                                                run_id,
                                                downstream,
                                                &JobStatus::Running,
                                            )?;
                                            let dw_workers = resolve_workers(
                                                downstream, wf, &workers, &rr, &strategy,
                                            )
                                            .await;
                                            let fanin_input = build_fanin_input(
                                                downstream,
                                                wf,
                                                foreach_map,
                                                &job_outputs,
                                                host.clone(),
                                            )
                                            .await;
                                            launch(
                                                downstream,
                                                wf,
                                                host.clone(),
                                                tx.clone(),
                                                dw_workers,
                                                sem.clone(),
                                                fanin_input,
                                            );
                                            statuses.insert(downstream.clone(), JobStatus::Running);
                                            in_flight += 1;
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        if print_progress {
                            print_running(dep, pad);
                        }
                        store.upsert_job(run_id, dep, &JobStatus::Running)?;
                        let dep_workers = resolve_workers(dep, wf, &workers, &rr, &strategy).await;
                        let fanin_input =
                            build_fanin_input(dep, wf, foreach_map, &job_outputs, host.clone())
                                .await;
                        launch(
                            dep,
                            wf,
                            host.clone(),
                            tx.clone(),
                            dep_workers,
                            sem.clone(),
                            fanin_input,
                        );
                        statuses.insert(dep.clone(), JobStatus::Running);
                        in_flight += 1;
                    }
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

/// Build fan-in input for a job with `input_from`.  Returns `None` if the job
/// has no `input_from`, or if outputs are not yet available (falls back to the
/// YAML `input` field).
///
/// Aggregation mode is controlled by the job's `reduce` field:
/// - `None` / `JsonArray`: wrap each child output as a JSON array element (default)
/// - `Concat`: concatenate raw bytes from all children
/// - `JsonMerge`: deep-merge child JSON objects (last-write-wins on key conflict)
/// - `Custom(path)`: invoke a Wasm component with the JSON array of child outputs
async fn build_fanin_input(
    job_id: &str,
    wf: &Workflow,
    foreach_map: &HashMap<String, Vec<String>>,
    job_outputs: &HashMap<String, Vec<u8>>,
    host: Arc<FluxionHost>,
) -> Option<Vec<u8>> {
    let def = wf.jobs.get(job_id)?;
    let src = def.input_from.as_deref()?;
    let children = foreach_map.get(src)?;

    let raw_outputs: Vec<Vec<u8>> = children
        .iter()
        .map(|child_id| job_outputs.get(child_id).cloned().unwrap_or_default())
        .collect();

    match def.reduce.as_ref() {
        None | Some(ReduceMode::JsonArray) => {
            let values: Vec<serde_json::Value> = raw_outputs
                .iter()
                .map(|b| serde_json::from_slice(b).unwrap_or(serde_json::Value::Null))
                .collect();
            serde_json::to_vec(&values).ok()
        }
        Some(ReduceMode::Concat) => Some(raw_outputs.into_iter().flatten().collect()),
        Some(ReduceMode::JsonMerge) => {
            let mut merged = serde_json::Map::new();
            for bytes in &raw_outputs {
                if let Ok(serde_json::Value::Object(obj)) = serde_json::from_slice(bytes) {
                    merged.extend(obj);
                }
            }
            serde_json::to_vec(&serde_json::Value::Object(merged)).ok()
        }
        Some(ReduceMode::Custom(component_path)) => {
            let values: Vec<serde_json::Value> = raw_outputs
                .iter()
                .map(|b| serde_json::from_slice(b).unwrap_or(serde_json::Value::Null))
                .collect();
            let input = serde_json::to_vec(&values).ok()?;
            let path = component_path.clone();
            let perms = PermissionSet::default();
            tokio::task::spawn_blocking(move || host.run_component(&path, input, &perms).ok())
                .await
                .ok()
                .flatten()
        }
    }
}

/// Print foreach group progress when a child job finishes.
fn print_foreach_progress(
    job_id: &str,
    foreach_map: &HashMap<String, Vec<String>>,
    statuses: &HashMap<String, JobStatus>,
    pad: usize,
) {
    // Find which foreach group this job belongs to (if any)
    for (parent, children) in foreach_map {
        if children.contains(&job_id.to_string()) {
            let done = children
                .iter()
                .filter(|c| matches!(statuses.get(*c), Some(JobStatus::Succeeded { .. })))
                .count();
            let total = children.len();
            println!(
                "[{}] {:<pad$}  ({}/{} done)",
                timestamp(),
                parent,
                done,
                total,
                pad = pad
            );
            return;
        }
    }
}

struct JobEvent {
    job_id: String,
    status: JobStatus,
    /// Captured wasm output bytes (for fan-in assembly).
    output: Option<Vec<u8>>,
    /// Phase metrics for succeeded jobs; all zeros for failures/skips.
    compile_us: u64,
    instantiate_us: u64,
    execute_us: u64,
}

/// A resolved worker target: URL plus optional mTLS config.
#[derive(Clone)]
struct WorkerInfo {
    url: String,
    tls: Option<TlsConfig>,
}

/// Resolve the ordered list of workers to try for a job, including failover targets.
/// - `job.worker` set → `[that url, TLS from wf if available]` (pinned; no failover)
/// - `workers` non-empty → pick primary via strategy, then remaining workers as failover targets
/// - otherwise → empty (run locally)
async fn resolve_workers(
    job_id: &str,
    wf: &Workflow,
    workers: &[WorkerInfo],
    rr: &AtomicUsize,
    strategy: &LbStrategy,
) -> Vec<WorkerInfo> {
    if let Some(url) = &wf.jobs[job_id].worker {
        return vec![WorkerInfo {
            url: url.clone(),
            tls: None,
        }];
    }
    if workers.is_empty() {
        return Vec::new();
    }
    let n = workers.len();
    let start = match strategy {
        LbStrategy::RoundRobin => rr.fetch_add(1, Ordering::Relaxed) % n,
        LbStrategy::Weighted => pick_weighted(&wf.workers, rr),
        LbStrategy::LeastConn => pick_least_conn(&wf.workers).await,
    };
    (0..n).map(|i| workers[(start + i) % n].clone()).collect()
}

/// Weighted round-robin: picks the worker index proportionally to its weight.
/// Uses a global counter so successive calls cycle through the weight-space evenly.
fn pick_weighted(workers: &[fluxion_core::workflow::WorkerConfig], rr: &AtomicUsize) -> usize {
    let total_weight: u32 = workers.iter().map(|w| w.weight()).sum();
    if total_weight == 0 {
        return rr.fetch_add(1, Ordering::Relaxed) % workers.len();
    }
    let slot = (rr.fetch_add(1, Ordering::Relaxed) as u32) % total_weight;
    let mut acc = 0u32;
    for (i, w) in workers.iter().enumerate() {
        acc += w.weight();
        if slot < acc {
            return i;
        }
    }
    0
}

/// Least-connections: query each worker's `/health` endpoint and return the
/// index of the worker with the fewest active jobs.
/// Workers that fail to respond are skipped (treated as having max load).
async fn pick_least_conn(workers: &[fluxion_core::workflow::WorkerConfig]) -> usize {
    let mut best_idx = 0usize;
    let mut best_count = u64::MAX;
    for (i, w) in workers.iter().enumerate() {
        let url = format!("{}/health", w.url().trim_end_matches('/'));
        if let Ok(resp) = reqwest::get(&url).await
            && let Ok(json) = resp.json::<serde_json::Value>().await
        {
            let active = json
                .get("active_jobs")
                .and_then(|v| v.as_u64())
                .unwrap_or(u64::MAX);
            if active < best_count {
                best_count = active;
                best_idx = i;
            }
        }
    }
    best_idx
}

/// Try each worker in order. Reachability failures (`RemoteError::Unreachable`)
/// fail over to the next worker; a worker that answers with an execution error
/// stops immediately (retrying elsewhere would reproduce the same component bug).
async fn run_with_failover(
    workers: &[WorkerInfo],
    component: &str,
    input: &[u8],
    perms: &PermissionSet,
    env: &HashMap<String, String>,
) -> anyhow::Result<(Vec<u8>, crate::JobMetrics)> {
    let mut tried: Vec<String> = Vec::new();
    for (i, worker) in workers.iter().enumerate() {
        let last = i + 1 == workers.len();
        match remote::run_remote(
            &worker.url,
            component,
            input.to_vec(),
            perms,
            env,
            worker.tls.as_ref(),
        )
        .await
        {
            Ok(r) => {
                crate::metrics::WORKER_HEALTH
                    .with_label_values(&[&worker.url])
                    .set(1.0);
                return Ok(r);
            }
            Err(e) => {
                tried.push(format!("{}: {e}", worker.url));
                if e.is_failover() && !last {
                    crate::metrics::WORKER_HEALTH
                        .with_label_values(&[&worker.url])
                        .set(0.0);
                    tracing::warn!(worker = %worker.url, error = %e, "worker unreachable, failing over");
                    continue;
                }
                return Err(anyhow::anyhow!(
                    "job dispatch failed [{}]",
                    tried.join("; ")
                ));
            }
        }
    }
    unreachable!("resolve_workers never returns an empty list here")
}

/// Async variant of `run_with_failover` — uses POST /jobs + polling GET /jobs/:id.
/// The timeout is embedded inside `run_remote_async` (perms.limits.timeout_secs + 30).
async fn run_with_failover_async(
    workers: &[WorkerInfo],
    component: &str,
    input: &[u8],
    perms: &PermissionSet,
    env: &HashMap<String, String>,
) -> anyhow::Result<(Vec<u8>, crate::JobMetrics)> {
    let mut tried: Vec<String> = Vec::new();
    for (i, worker) in workers.iter().enumerate() {
        let last = i + 1 == workers.len();
        match remote::run_remote_async(
            &worker.url,
            component,
            input.to_vec(),
            perms,
            env,
            worker.tls.as_ref(),
        )
        .await
        {
            Ok(r) => {
                crate::metrics::WORKER_HEALTH
                    .with_label_values(&[&worker.url])
                    .set(1.0);
                return Ok(r);
            }
            Err(e) => {
                tried.push(format!("{}: {e}", worker.url));
                if e.is_failover() && !last {
                    crate::metrics::WORKER_HEALTH
                        .with_label_values(&[&worker.url])
                        .set(0.0);
                    tracing::warn!(worker = %worker.url, error = %e, "async worker unreachable, failing over");
                    continue;
                }
                return Err(anyhow::anyhow!(
                    "async job dispatch failed [{}]",
                    tried.join("; ")
                ));
            }
        }
    }
    unreachable!("resolve_workers never returns an empty list here")
}

fn launch(
    job_id: &str,
    wf: &Workflow,
    host: Arc<FluxionHost>,
    tx: mpsc::UnboundedSender<JobEvent>,
    workers: Vec<WorkerInfo>,
    sem: Arc<Semaphore>,
    // Override input bytes (used for fan-in assembly). If None, falls back to wf.jobs[job_id].input.
    input_override: Option<Vec<u8>>,
) {
    crate::metrics::ACTIVE_JOBS.inc();
    let job_id = job_id.to_string();
    let executor = wf.jobs[&job_id].executor.clone();
    let async_dispatch = wf.jobs[&job_id].async_dispatch;
    let component = wf.jobs[&job_id].component.clone();
    let input = input_override.unwrap_or_else(|| {
        wf.jobs[&job_id]
            .input
            .clone()
            .unwrap_or_default()
            .into_bytes()
    });
    let perms = wf.jobs[&job_id].permissions.clone();
    let env = wf.jobs[&job_id].env.clone();
    let output_size_limit = wf.jobs[&job_id].output_size_limit_mb.unwrap_or(64) * 1024 * 1024;
    let component_sha256 = wf.jobs[&job_id].component_sha256.clone();
    let timeout_secs = perms.limits.timeout_secs;

    let span = info_span!("fluxion.job", job.id = %job_id, component = %component);

    tokio::spawn(
        async move {
            let _permit = sem.acquire_owned().await.expect("semaphore closed");
            let start = Instant::now();

            // Resolve hostnames in the network allowlist before entering spawn_blocking.
            // Pure IP entries pass through unchanged; hostname entries are expanded to IPs
            // (TTL-cached 60 s). Failed lookups are dropped → deny-all for those entries.
            let perms = if !perms.network.allow.is_empty() {
                let resolved = crate::resolve_network_allow(&perms.network.allow).await;
                fluxion_core::workflow::PermissionSet {
                    network: fluxion_core::workflow::NetworkPermission { allow: resolved },
                    ..perms
                }
            } else {
                perms
            };

            // Verify SHA-256 digest before loading the component (supply-chain protection).
            if let Err(e) = crate::verify_component_digest(&component, component_sha256.as_deref())
            {
                let elapsed = start.elapsed();
                let _ = tx.send(JobEvent {
                    job_id: job_id.clone(),
                    status: JobStatus::Failed {
                        elapsed,
                        reason: format!("digest verification failed: {e}"),
                    },
                    output: None,
                    compile_us: 0,
                    instantiate_us: 0,
                    execute_us: 0,
                });
                crate::metrics::ACTIVE_JOBS.dec();
                return;
            }

            let run_result: anyhow::Result<(Vec<u8>, crate::JobMetrics)> = match executor {
                ExecutorKind::Remote => {
                    if async_dispatch {
                        run_with_failover_async(&workers, &component, &input, &perms, &env).await
                    } else {
                        match tokio::time::timeout(
                            Duration::from_secs(timeout_secs),
                            run_with_failover(&workers, &component, &input, &perms, &env),
                        )
                        .await
                        {
                            Err(_) => Err(anyhow::anyhow!("Timeout after {}s", timeout_secs)),
                            Ok(r) => r,
                        }
                    }
                }
                ExecutorKind::Local => {
                    let c = component.clone();
                    let p = perms.clone();
                    let e = env.clone();
                    let i = input.clone();
                    match tokio::time::timeout(
                        Duration::from_secs(timeout_secs),
                        tokio::task::spawn_blocking(move || {
                            host.run_component_measured(&c, i, &p, &e)
                        }),
                    )
                    .await
                    {
                        Err(_) => Err(anyhow::anyhow!("Timeout after {}s", timeout_secs)),
                        Ok(Err(e)) => Err(anyhow::anyhow!("{}", e)),
                        Ok(Ok(r)) => r,
                    }
                }
            };

            let elapsed = start.elapsed();
            let (status, output, compile_us, instantiate_us, execute_us) = match run_result {
                Ok((out, m)) => {
                    if out.len() as u64 > output_size_limit {
                        (
                            JobStatus::Failed {
                                elapsed,
                                reason: format!(
                                    "output size {} bytes exceeds limit of {} MB",
                                    out.len(),
                                    output_size_limit / (1024 * 1024)
                                ),
                            },
                            None,
                            m.compile.as_micros() as u64,
                            m.instantiate.as_micros() as u64,
                            m.execute.as_micros() as u64,
                        )
                    } else {
                        (
                            JobStatus::Succeeded { elapsed },
                            Some(out),
                            m.compile.as_micros() as u64,
                            m.instantiate.as_micros() as u64,
                            m.execute.as_micros() as u64,
                        )
                    }
                }
                Err(e) => (
                    JobStatus::Failed {
                        elapsed,
                        reason: e.to_string(),
                    },
                    None,
                    0,
                    0,
                    0,
                ),
            };

            tracing::info!(
                status = status.label(),
                elapsed_ms = elapsed.as_millis() as u64,
                compile_us,
                worker = if workers.is_empty() {
                    "local"
                } else {
                    workers[0].url.as_str()
                },
                "job finished"
            );

            let _ = tx.send(JobEvent {
                job_id,
                status,
                output,
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

/// Evaluate a simple conditional expression like `"validate.status == 'SUCCESS'"`.
/// Returns `true` if the condition holds (job should run), `false` if the job should be skipped.
/// Unknown / malformed expressions default to `true` (run the job).
fn eval_when(expr: &str, statuses: &HashMap<String, JobStatus>) -> bool {
    let parts: Vec<&str> = expr.splitn(3, ' ').collect();
    if parts.len() != 3 {
        return true;
    }
    let (lhs, op, rhs) = (parts[0], parts[1], parts[2].trim_matches('\''));
    if let Some((job_id, attr)) = lhs.rsplit_once('.')
        && attr == "status"
    {
        let status_str = statuses
            .get(job_id)
            .map(|s| match s {
                JobStatus::Succeeded { .. } => "SUCCESS",
                JobStatus::Failed { .. } => "FAILED",
                JobStatus::Skipped => "SKIPPED",
                _ => "UNKNOWN",
            })
            .unwrap_or("UNKNOWN");
        return match op {
            "==" => status_str == rhs,
            "!=" => status_str != rhs,
            _ => true,
        };
    }
    true
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
        JobStatus::Skipped => println!(
            "[{}] {:<pad$}  SKIPPED",
            timestamp(),
            event.job_id,
            pad = pad
        ),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
    use std::io::Write as _;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    // Build a Workflow with a single job "j" via JSON so we don't depend on
    // indexmap directly in this crate's tests.
    fn wf(workers: &[&str], job_worker: Option<&str>) -> Workflow {
        let workers_json = serde_json::to_string(workers).unwrap();
        let job = match job_worker {
            Some(w) => format!(r#"{{"component":"x.wasm","worker":"{w}"}}"#),
            None => r#"{"component":"x.wasm"}"#.to_string(),
        };
        let s = format!(r#"{{"name":"t","jobs":{{"j":{job}}},"workers":{workers_json}}}"#);
        serde_json::from_str(&s).unwrap()
    }

    fn worker_urls(workers: &[WorkerInfo]) -> Vec<&str> {
        workers.iter().map(|w| w.url.as_str()).collect()
    }

    fn make_worker_infos(urls: &[&str]) -> Vec<WorkerInfo> {
        urls.iter()
            .map(|u| WorkerInfo {
                url: u.to_string(),
                tls: None,
            })
            .collect()
    }

    #[tokio::test]
    async fn pinned_worker_has_no_failover_targets() {
        // An explicit `worker:` must yield exactly that URL — never fail over.
        let w = wf(&["http://a", "http://b"], Some("http://pinned"));
        let rr = AtomicUsize::new(0);
        let ew = make_worker_infos(&["http://a", "http://b"]);
        assert_eq!(
            worker_urls(&resolve_workers("j", &w, &ew, &rr, &LbStrategy::RoundRobin).await),
            vec!["http://pinned"]
        );
    }

    #[tokio::test]
    async fn no_workers_runs_locally() {
        let w = wf(&[], None);
        let rr = AtomicUsize::new(0);
        assert!(
            resolve_workers("j", &w, &[], &rr, &LbStrategy::RoundRobin)
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn round_robin_lists_all_workers_as_failover_targets() {
        let w = wf(&["http://a", "http://b", "http://c"], None);
        let ew = make_worker_infos(&["http://a", "http://b", "http://c"]);
        let rr = AtomicUsize::new(0);
        assert_eq!(
            worker_urls(&resolve_workers("j", &w, &ew, &rr, &LbStrategy::RoundRobin).await),
            vec!["http://a", "http://b", "http://c"]
        );
        // The next job rotates the start but still lists every worker as a target.
        assert_eq!(
            worker_urls(&resolve_workers("j", &w, &ew, &rr, &LbStrategy::RoundRobin).await),
            vec!["http://b", "http://c", "http://a"]
        );
    }

    // ── weighted round-robin tests ────────────────────────────────────────────

    /// Build a Workflow with full-form workers carrying explicit weights.
    fn wf_weighted(weights: &[(u32, &str)]) -> Workflow {
        let workers_json: String = {
            let entries: Vec<String> = weights
                .iter()
                .map(|(w, url)| format!(r#"{{"url":"{url}","weight":{w}}}"#))
                .collect();
            format!("[{}]", entries.join(","))
        };
        let s = format!(
            r#"{{"name":"t","jobs":{{"j":{{"component":"x.wasm"}}}},"workers":{workers_json}}}"#
        );
        serde_json::from_str(&s).unwrap()
    }

    #[test]
    fn weighted_rr_distributes_proportionally() {
        // Workers with weights [2, 1]: over 30 slots, A should get ~20, B ~10.
        let w = wf_weighted(&[(2, "http://a"), (1, "http://b")]);
        let rr = AtomicUsize::new(0);
        let mut counts = std::collections::HashMap::new();
        for _ in 0..30 {
            let idx = pick_weighted(&w.workers, &rr);
            *counts.entry(idx).or_insert(0usize) += 1;
        }
        let a = counts.get(&0).copied().unwrap_or(0);
        let b = counts.get(&1).copied().unwrap_or(0);
        // Exact: 2/3 of 30 = 20 for A, 1/3 = 10 for B (deterministic modular arithmetic)
        assert_eq!(a, 20, "worker A (weight 2) should get 20/30 slots, got {a}");
        assert_eq!(b, 10, "worker B (weight 1) should get 10/30 slots, got {b}");
    }

    #[test]
    fn weighted_rr_equal_weights_acts_like_round_robin() {
        // Both weight=1: should distribute exactly 50/50 over 10 calls.
        let w = wf_weighted(&[(1, "http://a"), (1, "http://b")]);
        let rr = AtomicUsize::new(0);
        let mut counts = [0usize; 2];
        for _ in 0..10 {
            counts[pick_weighted(&w.workers, &rr)] += 1;
        }
        assert_eq!(counts[0], 5);
        assert_eq!(counts[1], 5);
    }

    #[test]
    fn weighted_rr_single_worker_always_picked() {
        let w = wf_weighted(&[(5, "http://only")]);
        let rr = AtomicUsize::new(0);
        for _ in 0..10 {
            assert_eq!(pick_weighted(&w.workers, &rr), 0);
        }
    }

    // Bind then drop so the port is guaranteed to refuse connections.
    async fn closed_port_url() -> String {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        drop(l);
        format!("http://{addr}")
    }

    fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
        hay.windows(needle.len()).position(|w| w == needle)
    }

    // Minimal HTTP server that answers any /run POST with `output`.
    async fn spawn_mock_worker(output: Vec<u8>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                let out_b64 = B64.encode(&output);
                tokio::spawn(async move {
                    // Drain the request (up to Content-Length) so the client's
                    // write completes before we reply.
                    let mut buf = Vec::new();
                    let mut tmp = [0u8; 8192];
                    let mut header_end = None;
                    let mut content_len = None;
                    loop {
                        let n = match sock.read(&mut tmp).await {
                            Ok(n) => n,
                            Err(_) => break,
                        };
                        if n == 0 {
                            break;
                        }
                        buf.extend_from_slice(&tmp[..n]);
                        if header_end.is_none() {
                            if let Some(p) = find(&buf, b"\r\n\r\n") {
                                header_end = Some(p + 4);
                                let head = String::from_utf8_lossy(&buf[..p]).to_lowercase();
                                for line in head.lines() {
                                    if let Some(v) = line.strip_prefix("content-length:") {
                                        content_len = v.trim().parse::<usize>().ok();
                                    }
                                }
                            }
                        }
                        if let (Some(he), Some(cl)) = (header_end, content_len) {
                            if buf.len() >= he + cl {
                                break;
                            }
                        }
                    }
                    let body = format!(
                        "{{\"output\":\"{out_b64}\",\"compile_ms\":0,\"instantiate_ms\":0,\"execute_ms\":0}}"
                    );
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                    let _ = sock.flush().await;
                });
            }
        });
        format!("http://{addr}")
    }

    fn tmp_wasm() -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(b"\0asm").unwrap();
        f
    }

    fn plain_workers(urls: &[String]) -> Vec<WorkerInfo> {
        urls.iter()
            .map(|u| WorkerInfo {
                url: u.clone(),
                tls: None,
            })
            .collect()
    }

    #[tokio::test]
    async fn fails_over_to_healthy_worker() {
        // First worker is unreachable; the job must succeed on the second.
        let down = closed_port_url().await;
        let up = spawn_mock_worker(b"ok-output".to_vec()).await;
        let f = tmp_wasm();
        let path = f.path().to_string_lossy().into_owned();
        let workers = plain_workers(&[down, up]);
        let (out, _) = run_with_failover(
            &workers,
            &path,
            b"in",
            &PermissionSet::default(),
            &HashMap::new(),
        )
        .await
        .expect("should fail over to the healthy worker");
        assert_eq!(out, b"ok-output");
    }

    #[tokio::test]
    async fn all_workers_down_lists_every_url() {
        let d1 = closed_port_url().await;
        let d2 = closed_port_url().await;
        let f = tmp_wasm();
        let path = f.path().to_string_lossy().into_owned();
        let workers = plain_workers(&[d1.clone(), d2.clone()]);
        let err = run_with_failover(
            &workers,
            &path,
            b"in",
            &PermissionSet::default(),
            &HashMap::new(),
        )
        .await
        .expect_err("all workers down → error");
        let msg = err.to_string();
        assert!(msg.contains(&d1), "error should list {d1}: {msg}");
        assert!(msg.contains(&d2), "error should list {d2}: {msg}");
    }

    // ── build_fanin_input reduce mode tests ──────────────────────────────────

    fn fanin_wf(reduce_json: Option<&str>) -> Workflow {
        let reduce_field = match reduce_json {
            Some(r) => format!(r#","reduce":{r}"#),
            None => String::new(),
        };
        let s = format!(
            r#"{{"name":"fanin-test","jobs":{{"process":{{"component":"process.wasm","foreach":"$.items"}},"aggregate":{{"component":"agg.wasm","input_from":"process"{reduce_field}}}}}}}"#
        );
        serde_json::from_str(&s).unwrap()
    }

    // ── ExecutorKind dispatch validation ─────────────────────────────────────

    fn wf_with_executor(executor: &str, has_workers: bool) -> Workflow {
        let workers_json = if has_workers {
            r#"[{"url":"http://w1"}]"#.to_string()
        } else {
            "[]".to_string()
        };
        let s = format!(
            r#"{{"name":"t","jobs":{{"j":{{"component":"x.wasm","executor":"{executor}"}}}},"workers":{workers_json}}}"#
        );
        serde_json::from_str(&s).unwrap()
    }

    fn fanin_map() -> HashMap<String, Vec<String>> {
        let mut m = HashMap::new();
        m.insert(
            "process".to_string(),
            vec!["process[0]".to_string(), "process[1]".to_string()],
        );
        m
    }

    fn fanin_outputs(a: &[u8], b: &[u8]) -> HashMap<String, Vec<u8>> {
        let mut m = HashMap::new();
        m.insert("process[0]".to_string(), a.to_vec());
        m.insert("process[1]".to_string(), b.to_vec());
        m
    }

    #[tokio::test]
    async fn fanin_json_array_wraps_outputs() {
        let wf = fanin_wf(Some(r#""json_array""#));
        let foreach_map = fanin_map();
        let outputs = fanin_outputs(b"1", b"2");
        let host = Arc::new(FluxionHost::new().unwrap());
        let result = build_fanin_input("aggregate", &wf, &foreach_map, &outputs, host)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(v, serde_json::json!([1, 2]));
    }

    #[tokio::test]
    async fn fanin_default_behaves_like_json_array() {
        let wf = fanin_wf(None);
        let foreach_map = fanin_map();
        let outputs = fanin_outputs(b"\"x\"", b"\"y\"");
        let host = Arc::new(FluxionHost::new().unwrap());
        let result = build_fanin_input("aggregate", &wf, &foreach_map, &outputs, host)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(v, serde_json::json!(["x", "y"]));
    }

    #[tokio::test]
    async fn fanin_concat_joins_raw_bytes() {
        let wf = fanin_wf(Some(r#""concat""#));
        let foreach_map = fanin_map();
        let outputs = fanin_outputs(b"hello", b" world");
        let host = Arc::new(FluxionHost::new().unwrap());
        let result = build_fanin_input("aggregate", &wf, &foreach_map, &outputs, host)
            .await
            .unwrap();
        assert_eq!(result, b"hello world");
    }

    #[tokio::test]
    async fn fanin_json_merge_merges_objects() {
        let wf = fanin_wf(Some(r#""json_merge""#));
        let foreach_map = fanin_map();
        let outputs = fanin_outputs(br#"{"a":1,"b":2}"#, br#"{"b":99,"c":3}"#);
        let host = Arc::new(FluxionHost::new().unwrap());
        let result = build_fanin_input("aggregate", &wf, &foreach_map, &outputs, host)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(v, serde_json::json!({"a": 1, "b": 99, "c": 3}));
    }

    #[tokio::test]
    async fn remote_executor_without_workers_is_rejected() {
        let host = Arc::new(FluxionHost::new().unwrap());
        let wf = wf_with_executor("remote", false);
        let tmp = tempfile::tempdir().unwrap();
        // SAFETY: single-threaded test context.
        unsafe { std::env::set_var("HOME", tmp.path()) };
        let store = RunStore::open().unwrap();
        let opts = ExecOpts {
            host,
            store: &store,
            run_id: "test-run",
            pre_succeeded: HashMap::new(),
            print_progress: false,
            sem: Arc::new(Semaphore::new(4)),
            strategy: LbStrategy::RoundRobin,
            workers: vec![],
        };
        let err = execute(&wf, opts).await.expect_err("should fail");
        assert!(
            err.to_string().contains("executor:remote"),
            "error should mention executor:remote: {err}"
        );
    }

    #[tokio::test]
    async fn local_executor_without_workers_is_accepted_at_validation() {
        let host = Arc::new(FluxionHost::new().unwrap());
        let wf = wf_with_executor("local", false);
        let tmp = tempfile::tempdir().unwrap();
        // SAFETY: single-threaded test context.
        unsafe { std::env::set_var("HOME", tmp.path()) };
        let store = RunStore::open().unwrap();
        let opts = ExecOpts {
            host,
            store: &store,
            run_id: "test-run2",
            pre_succeeded: HashMap::new(),
            print_progress: false,
            sem: Arc::new(Semaphore::new(4)),
            strategy: LbStrategy::RoundRobin,
            workers: vec![],
        };
        let result = execute(&wf, opts).await;
        if let Err(ref e) = result {
            assert!(
                !e.to_string().contains("executor:remote"),
                "local executor should not trigger remote-worker validation: {e}"
            );
        }
    }
}

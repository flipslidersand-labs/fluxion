use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use fluxion_core::{
    dag::Dag,
    expand::{ExpandedWorkflow, expand_foreach, expand_foreach_dynamic, is_dynamic_foreach},
    runner::{JobResult, RunResult},
    state::JobStatus,
    store::{MAX_OUTPUT_BYTES, MAX_RUN_OUTPUT_BYTES, RunStore, SaveOutcome},
    workflow::{ExecutorKind, JobDefinition, PermissionSet, ReduceMode, TlsConfig, Workflow},
};
use tokio::sync::{Semaphore, mpsc};
use tracing::{Instrument, info_span};

use crate::{FluxionHost, remote};

/// Load-balancing strategy for distributing jobs across remote workers.
#[derive(Debug, Clone, Default)]
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
#[deprecated(note = "use `run_with_options` with `RunOptions::default()`")]
pub async fn run(wf: &Workflow, workflow_path: &Path, host: Arc<FluxionHost>) -> Result<RunResult> {
    run_with_options(wf, workflow_path, host, &RunOptions::default()).await
}

/// Run a workflow from scratch with the specified load-balancing strategy.
#[deprecated(note = "use `run_with_options` with `RunOptions { strategy, .. }`")]
pub async fn run_with_strategy(
    wf: &Workflow,
    workflow_path: &Path,
    host: Arc<FluxionHost>,
    strategy: LbStrategy,
) -> Result<RunResult> {
    let opts = RunOptions {
        strategy,
        progress: true,
    };
    run_with_options(wf, workflow_path, host, &opts).await
}

/// Run a workflow silently (no stdout) — for MCP / programmatic use.
#[deprecated(note = "use `run_with_options` with `RunOptions { progress: false, .. }`")]
pub async fn run_silent(
    wf: &Workflow,
    workflow_path: &Path,
    host: Arc<FluxionHost>,
) -> Result<RunResult> {
    let opts = RunOptions {
        progress: false,
        ..RunOptions::default()
    };
    run_with_options(wf, workflow_path, host, &opts).await
}

/// Run a workflow silently with the specified load-balancing strategy.
#[deprecated(note = "use `run_with_options` with `RunOptions { strategy, progress: false }`")]
pub async fn run_silent_with_strategy(
    wf: &Workflow,
    workflow_path: &Path,
    host: Arc<FluxionHost>,
    strategy: LbStrategy,
) -> Result<RunResult> {
    let opts = RunOptions {
        strategy,
        progress: false,
    };
    run_with_options(wf, workflow_path, host, &opts).await
}

/// Retry a previous run, re-executing `from_job` and all downstream dependents.
#[deprecated(note = "use `retry_with_options` with `RunOptions::default()`")]
pub async fn retry(
    wf: &Workflow,
    workflow_path: &Path,
    host: Arc<FluxionHost>,
    prev_run_id: &str,
    from_job: &str,
) -> Result<RunResult> {
    retry_with_options(
        wf,
        workflow_path,
        host,
        prev_run_id,
        from_job,
        &RunOptions::default(),
    )
    .await
}

/// Retry silently — for MCP / programmatic use.
#[deprecated(note = "use `retry_with_options` with `RunOptions { progress: false, .. }`")]
pub async fn retry_silent(
    wf: &Workflow,
    workflow_path: &Path,
    host: Arc<FluxionHost>,
    prev_run_id: &str,
    from_job: &str,
) -> Result<RunResult> {
    let opts = RunOptions {
        progress: false,
        ..RunOptions::default()
    };
    retry_with_options(wf, workflow_path, host, prev_run_id, from_job, &opts).await
}

/// Options shared by [`run_with_options`] and [`retry_with_options`].
#[derive(Debug, Clone)]
pub struct RunOptions {
    /// Load-balancing strategy for distributing jobs across remote workers.
    pub strategy: LbStrategy,
    /// Print progress to stdout/stderr (`false` for MCP / programmatic use).
    pub progress: bool,
}

impl Default for RunOptions {
    fn default() -> Self {
        Self {
            strategy: LbStrategy::default(),
            progress: true,
        }
    }
}

/// Run a workflow from scratch with the given [`RunOptions`], recording the run
/// in the default [`RunStore`] (see [`fluxion_core::paths::data_dir`]).
pub async fn run_with_options(
    wf: &Workflow,
    workflow_path: &Path,
    host: Arc<FluxionHost>,
    opts: &RunOptions,
) -> Result<RunResult> {
    let store = RunStore::open()?;
    run_with_store(wf, workflow_path, host, opts, &store).await
}

/// Like [`run_with_options`], but records the run (and worker health, and the
/// registered-worker fallback) in the caller-supplied `store`.
pub async fn run_with_store(
    wf: &Workflow,
    workflow_path: &Path,
    host: Arc<FluxionHost>,
    opts: &RunOptions,
    store: &RunStore,
) -> Result<RunResult> {
    run_inner(
        wf,
        workflow_path,
        host,
        HashMap::new(),
        opts.progress,
        opts.strategy.clone(),
        store,
    )
    .await
}

/// Retry a previous run, re-executing `from_job` and its dependents, with the
/// given [`RunOptions`], using the default [`RunStore`].
pub async fn retry_with_options(
    wf: &Workflow,
    workflow_path: &Path,
    host: Arc<FluxionHost>,
    prev_run_id: &str,
    from_job: &str,
    opts: &RunOptions,
) -> Result<RunResult> {
    let store = RunStore::open()?;
    retry_with_store(wf, workflow_path, host, prev_run_id, from_job, opts, &store).await
}

/// Like [`retry_with_options`], but reads the previous run from and records the
/// retry in the caller-supplied `store`.
pub async fn retry_with_store(
    wf: &Workflow,
    workflow_path: &Path,
    host: Arc<FluxionHost>,
    prev_run_id: &str,
    from_job: &str,
    opts: &RunOptions,
    store: &RunStore,
) -> Result<RunResult> {
    retry_inner(wf, workflow_path, host, prev_run_id, from_job, opts, store).await
}

/// Perform GET /health on each candidate URL concurrently.
/// Returns the subset that responded successfully, updating the DB health status.
async fn health_check_workers(candidates: &[String], store: &RunStore) -> Vec<String> {
    if candidates.is_empty() {
        return Vec::new();
    }
    let results = probe_workers(candidates).await;

    let healthy: Vec<String> = results
        .iter()
        .filter_map(|(url, ok)| if *ok { Some(url.clone()) } else { None })
        .collect();

    for (url, ok) in &results {
        let _ = store.update_worker_health(url, *ok);
    }

    healthy
}

/// Concurrently `GET {url}/health` on each candidate; a worker is healthy only
/// on a 2xx response. Pure network probe (no DB access).
async fn probe_workers(candidates: &[String]) -> Vec<(String, bool)> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest client");

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
    futures::future::join_all(checks).await
}

/// Resolve the effective worker list for a workflow run.
///
/// 1. Start with static `workers:` from YAML.
/// 2. If `workers_srv:` is set, resolve SRV and merge results.
/// 3. If both are empty, fall back to DB-registered workers.
///
/// Returns only health-checked workers.
async fn effective_workers(wf: &Workflow, store: &RunStore) -> Vec<String> {
    let mut candidates: Vec<String> = wf.workers.iter().map(|w| w.url().to_string()).collect();

    // Merge SRV-discovered workers (failures are silently ignored).
    if let Some(srv) = &wf.workers_srv {
        let mut srv_urls = crate::resolve_srv_workers(srv).await;
        candidates.append(&mut srv_urls);
    }

    if candidates.is_empty() {
        candidates = store.registered_worker_urls().unwrap_or_default();
    }
    health_check_workers(&candidates, store).await
}

/// Look up the mTLS config for `url` among the workflow's static `workers:`.
fn tls_for(wf: &Workflow, url: &str) -> Option<TlsConfig> {
    wf.workers
        .iter()
        .find(|w| w.url() == url)
        .and_then(|w| w.tls().cloned())
}

/// Resolve effective workers and return them as `WorkerInfo`, preserving TLS config.
async fn effective_workers_info(wf: &Workflow, store: &RunStore) -> Vec<WorkerInfo> {
    let healthy_urls = effective_workers(wf, store).await;
    worker_infos_for(wf, healthy_urls)
}

/// Attach TLS config and weight from `wf.workers` (matched by URL) to each URL.
/// Workers not listed in YAML (SRV/DB-discovered) get no TLS and weight 1.
fn worker_infos_for(wf: &Workflow, urls: Vec<String>) -> Vec<WorkerInfo> {
    urls.into_iter()
        .map(|url| {
            let cfg = wf.workers.iter().find(|w| w.url() == url.as_str());
            let tls = cfg.and_then(|w| w.tls().cloned());
            let weight = cfg.map_or(1, |w| w.weight());
            WorkerInfo { url, tls, weight }
        })
        .collect()
}

/// Run-time guard for workflows that did not go through `validate` (API/DB
/// paths): SRV-resolved workers get no TLS, so refuse the combination (#412).
fn ensure_srv_without_tls(wf: &Workflow) -> Result<()> {
    anyhow::ensure!(
        !wf.srv_with_tls(),
        "{}",
        fluxion_core::workflow::SRV_WITH_TLS_MSG
    );
    Ok(())
}

async fn run_inner(
    wf: &Workflow,
    workflow_path: &Path,
    host: Arc<FluxionHost>,
    pre_succeeded: HashMap<String, JobStatus>,
    print_progress: bool,
    strategy: LbStrategy,
    store: &RunStore,
) -> Result<RunResult> {
    ensure_srv_without_tls(wf)?;
    let run_id = RunStore::new_run_id();
    store.create_run(&run_id, &wf.name, workflow_path)?;
    if print_progress {
        println!("Run ID: {run_id}");
    }

    let workers = effective_workers_info(wf, store).await;
    let permits = wf.max_parallel.unwrap_or(Semaphore::MAX_PERMITS);
    let sem = Arc::new(Semaphore::new(permits));

    let span = info_span!("fluxion.run", run_id = %run_id, workflow = %wf.name);
    let result = execute(
        wf,
        ExecOpts {
            host,
            store,
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
    opts: &RunOptions,
    store: &RunStore,
) -> Result<RunResult> {
    ensure_srv_without_tls(wf)?;
    let (print_progress, strategy) = (opts.progress, opts.strategy.clone());
    let (_, prev_states) = store.load_run(prev_run_id)?;
    // Outputs saved by the previous run (#387). Missing (old run, size limits) or
    // unreadable outputs just mean "re-run the whole group", as before.
    let prev_outputs = store.load_outputs(prev_run_id).unwrap_or_else(|e| {
        tracing::warn!(run = %prev_run_id, error = %e, "cannot read saved job outputs; re-running whole groups");
        HashMap::new()
    });
    // Compute the replay set on the *statically expanded* DAG (#288): previous
    // run states are keyed by expanded child ids (`job.0`, ...), which the
    // unexpanded DAG does not contain.
    let expanded = expand_foreach(wf, None)?;
    let dag = Dag::build(&expanded.workflow)?;
    let starts: Vec<String> = if let Some(children) = expanded.foreach_map.get(from_job) {
        children.clone() // static foreach parent -> all of its children
    } else if expanded.workflow.jobs.contains_key(from_job) {
        vec![from_job.to_string()]
    } else if let Some(group) = dynamic_group_of(wf, from_job) {
        vec![group] // child of a dynamic foreach -> re-run the whole group (#315)
    } else {
        anyhow::bail!("unknown job '{from_job}' in workflow '{}'", wf.name);
    };
    let mut replay_set: std::collections::HashSet<String> = std::collections::HashSet::new();
    for start in &starts {
        replay_set.extend(
            downstream_inclusive(&dag, start)
                .into_iter()
                .map(String::from),
        );
    }
    // A fan-in re-run with a carried-over child needs that child's output, or it
    // would receive null for it. So when a child or the fan-in of a static
    // foreach group is replayed, reuse the previous children only if the saved
    // outputs of ALL of them are available (#387); otherwise replay the whole group.
    for (parent, children) in &expanded.foreach_map {
        let fanin_replayed = wf
            .jobs
            .iter()
            .any(|(id, d)| d.input_from.as_deref() == Some(parent) && replay_set.contains(id));
        if !(fanin_replayed || children.iter().any(|c| replay_set.contains(c))) {
            continue;
        }
        let carried_output_missing = children.iter().any(|c| {
            !replay_set.contains(c)
                && matches!(prev_states.get(c), Some(JobStatus::Succeeded { .. }))
                && !prev_outputs.contains_key(c)
        });
        if carried_output_missing {
            for child in children {
                replay_set.extend(
                    downstream_inclusive(&dag, child)
                        .into_iter()
                        .map(String::from),
                );
            }
        }
    }

    // Dynamic foreach groups (#315, #387). A group that is untouched and fully
    // succeeded is carried over. A touched one is re-run whole (dependency,
    // children, fan-in) unless the saved outputs allow reuse: then the
    // dependency and the previous children are kept, the group is re-expanded
    // from the saved dependency output, and only the named child / the fan-in
    // re-run.
    let mut rerun_groups: Vec<String> = Vec::new();
    let mut carried_over: Vec<String> = Vec::new();
    let mut replayed_children: std::collections::HashSet<String> = std::collections::HashSet::new();
    for (id, def) in wf.jobs.iter().filter(|(_, d)| is_dynamic_foreach(d)) {
        let prefix = format!("{id}.");
        let mut kids = prev_states.iter().filter(|(k, _)| k.starts_with(&prefix));
        let prev_ok = kids.clone().next().is_some()
            && kids.all(|(_, st)| matches!(st, JobStatus::Succeeded { .. }));
        let touched = replay_set.contains(id)
            || dag
                .dependents
                .get(id)
                .into_iter()
                .flatten()
                .any(|d| replay_set.contains(d));
        if !touched && prev_ok {
            carried_over.push(id.clone());
        } else if let Some(children) =
            reusable_dynamic_group(id, def, from_job, &replay_set, &prev_states, &prev_outputs)
        {
            carried_over.push(id.clone());
            replayed_children.extend(children);
        } else {
            rerun_groups.push(id.clone());
            replay_set.extend(def.depends_on.iter().cloned());
        }
    }
    for id in &rerun_groups {
        let down: Vec<String> = downstream_inclusive(&dag, id)
            .into_iter()
            .map(String::from)
            .collect();
        replay_set.extend(down);
    }

    let mut pre_succeeded: HashMap<String, JobStatus> = prev_states
        .into_iter()
        .filter(|(id, status)| {
            matches!(status, JobStatus::Succeeded { .. })
                && !replay_set.contains(id.as_str())
                && !replayed_children.contains(id.as_str())
                && !rerun_groups.iter().any(|g| {
                    id.strip_prefix(g.as_str())
                        .is_some_and(|r| r.starts_with('.'))
                })
        })
        .collect();
    for id in carried_over {
        pre_succeeded.insert(
            id,
            JobStatus::Succeeded {
                elapsed: Duration::ZERO,
            },
        );
    }

    let run_id = RunStore::new_run_id();
    store.create_run(&run_id, &wf.name, workflow_path)?;
    // Carry the saved outputs of reused jobs over into the new run: `execute`
    // seeds fan-in inputs from them, and a later retry of this run can reuse them
    // too. They fit the limits (they already fit in the previous run), so a failed
    // copy is an error, never a silent null in a fan-in input.
    for id in pre_succeeded.keys() {
        if let Some(out) = prev_outputs.get(id) {
            let saved = store.save_output(&run_id, id, out)?;
            anyhow::ensure!(
                saved == SaveOutcome::Saved,
                "cannot carry over the saved output of '{id}' ({saved:?})"
            );
        }
    }
    if print_progress {
        println!(
            "Retry run ID: {run_id}  (from '{from_job}', skipping {} pre-succeeded jobs)",
            pre_succeeded.len()
        );
    }

    let workers = effective_workers_info(wf, store).await;
    let permits = wf.max_parallel.unwrap_or(Semaphore::MAX_PERMITS);
    let sem = Arc::new(Semaphore::new(permits));

    let span = info_span!("fluxion.run", run_id = %run_id, workflow = %wf.name, retry = true);
    let result = execute(
        wf,
        ExecOpts {
            host,
            store,
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

/// When the run stops early on a failure, any job still recorded as
/// `Running` was already dispatched (`launch()` spawned its task and it has
/// not reported back yet) but will never be waited on again once the event
/// loop breaks. Left as `Running`, its RunStore entry would look stuck
/// forever — breaking `fluxion status`/`fluxion retry --from` and silently
/// leaking the semaphore permit it holds until the detached task eventually
/// finishes on its own (#243).
///
/// Marks every such job `Cancelled` in both the store and the in-memory
/// `statuses` map so the persisted history reflects "abandoned due to
/// upstream failure" rather than a lie of still-in-progress.
fn cancel_orphaned_running_jobs(
    store: &RunStore,
    run_id: &str,
    statuses: &mut HashMap<String, JobStatus>,
    print_progress: bool,
    pad: usize,
) -> Result<()> {
    let running: Vec<String> = statuses
        .iter()
        .filter(|(_, status)| matches!(status, JobStatus::Running))
        .map(|(id, _)| id.clone())
        .collect();

    for job_id in running {
        store.upsert_job(run_id, &job_id, &JobStatus::Cancelled)?;
        statuses.insert(job_id.clone(), JobStatus::Cancelled);
        if print_progress {
            println!(
                "[{}] {:<pad$}  CANCELLED (abandoned after upstream failure)",
                timestamp(),
                job_id,
                pad = pad
            );
        }
    }
    Ok(())
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
    // Keep the pre-expansion workflow: static foreach parents (and their
    // `fail_fast`) only exist there; expanded children always carry `false` (#272).
    let original_wf = wf;
    let expanded = expand_foreach(wf, None)?;
    // Own the (expanded) job table and DAG so dynamic children can be registered
    // at runtime (#313). Helpers borrow them via `&wf` / `&foreach_map`.
    let ExpandedWorkflow {
        workflow: wf,
        foreach_map,
    } = expanded;

    let dag = Dag::build(&wf)?;

    validate_remote_workers(&wf, &workers)?;

    let pad = wf.jobs.keys().map(|k| k.len()).max().unwrap_or(0);

    let statuses: HashMap<String, JobStatus> = wf
        .jobs
        .keys()
        .map(|k| (k.clone(), JobStatus::Pending))
        .collect();
    let mut st = RunState {
        wf,
        dag,
        foreach_map,
        statuses,
        job_outputs: HashMap::new(),
        job_results: Vec::new(),
        in_flight: 0,
        overall_success: true,
    };

    seed_pre_succeeded(&mut st, store, run_id, &pre_succeeded, print_progress, pad)?;
    expand_reused_dynamic_groups(&mut st, &pre_succeeded)?;

    let (tx, mut rx) = mpsc::unbounded_channel::<JobEvent>();
    let rr = Arc::new(AtomicUsize::new(0));
    let handler = EventHandler {
        store,
        run_id,
        original_wf,
        print_progress,
        pad,
    };
    let starter = JobStarter {
        store,
        run_id,
        host: &host,
        tx: &tx,
        sem: &sem,
        workers: &workers,
        rr: &rr,
        strategy: &strategy,
        print_progress,
        pad,
    };

    let scheduler = DependentScheduler {
        store,
        run_id,
        pre_succeeded: &pre_succeeded,
        starter: &starter,
        print_progress,
        pad,
    };

    for job_id in st.dag.roots() {
        if pre_succeeded.contains_key(&job_id) {
            continue;
        }
        if is_dynamic_foreach(&st.wf.jobs[&job_id]) {
            anyhow::bail!(
                "dynamic foreach job '{job_id}' needs a dependency to take its array from"
            );
        }
        starter.start(&job_id, &mut st).await?;
    }

    let job_ids: Vec<String> = st.wf.jobs.keys().cloned().collect();
    for job_id in &job_ids {
        if pre_succeeded.contains_key(job_id) || st.dag.roots().contains(job_id) {
            continue;
        }
        let job = &st.wf.jobs[job_id];
        if job.depends_on.iter().all(|d| pre_succeeded.contains_key(d)) {
            if is_dynamic_foreach(job) {
                anyhow::bail!(
                    "dynamic foreach job '{job_id}': outputs of its dependency from the previous run are not retained"
                );
            }
            starter.start(job_id, &mut st).await?;
        }
    }

    let workflow_start = Instant::now();

    while st.in_flight > 0 {
        let Some(event) = rx.recv().await else { break };

        st.in_flight -= 1;
        if !handler.on_event(&event, &mut st)? {
            break;
        }
        if !scheduler
            .schedule_dependents(&event.job_id, &mut st)
            .await?
        {
            break;
        }
    }

    finalize(st, store, run_id, workflow_start, print_progress)
}

/// Validate: executor:remote jobs require at least one worker configured.
fn validate_remote_workers(wf: &Workflow, workers: &[WorkerInfo]) -> Result<()> {
    for (job_id, job) in &wf.jobs {
        if job.executor == ExecutorKind::Remote && job.worker.is_none() && workers.is_empty() {
            anyhow::bail!(
                "job '{}' uses executor:remote but no workers are configured",
                job_id
            );
        }
    }
    Ok(())
}

/// Seed jobs that succeeded in the previous run (retry): persist them, mark
/// them done in `st` and report them as carried over.
fn seed_pre_succeeded(
    st: &mut RunState,
    store: &RunStore,
    run_id: &str,
    pre_succeeded: &HashMap<String, JobStatus>,
    print_progress: bool,
    pad: usize,
) -> Result<()> {
    // Outputs of carried-over jobs (copied into this run by `retry_inner`). A
    // fresh run has none, and must not depend on the outputs table at all.
    if !pre_succeeded.is_empty() {
        for (id, out) in store.load_outputs(run_id)? {
            if pre_succeeded.contains_key(&id) {
                st.job_outputs.insert(id, out);
            }
        }
    }
    for (id, status) in pre_succeeded {
        store.upsert_job(run_id, id, status)?;
        st.statuses.insert(id.clone(), status.clone());
        if let JobStatus::Succeeded { elapsed } = status {
            if print_progress {
                println!(
                    "[skip] {:<pad$}  SUCCESS  {:.2}s  (previous run)",
                    id,
                    elapsed.as_secs_f64(),
                    pad = pad
                );
            }
            st.job_results
                .push(JobResult::from_succeeded(id.clone(), *elapsed, true));
        }
    }
    Ok(())
}

/// On retry, a reused dynamic foreach group (its dependency was carried over with
/// a saved output) is expanded up front from that saved output, so the kept
/// children, the re-run children and the fan-in wiring exist before anything
/// starts (#387). Groups whose dependency output is unavailable are left alone.
fn expand_reused_dynamic_groups(
    st: &mut RunState,
    pre_succeeded: &HashMap<String, JobStatus>,
) -> Result<()> {
    let ids: Vec<String> = st
        .wf
        .jobs
        .iter()
        .filter(|(_, d)| {
            is_dynamic_foreach(d)
                && matches!(d.depends_on.as_slice(), [dep]
                    if pre_succeeded.contains_key(dep) && st.job_outputs.contains_key(dep))
        })
        .map(|(id, _)| id.clone())
        .collect();
    for id in ids {
        let (children, _) = expand_dynamic_job(
            &id,
            &mut st.wf,
            &mut st.dag,
            &mut st.foreach_map,
            &st.job_outputs,
        )?;
        st.statuses.remove(&id);
        for child in children {
            st.statuses.entry(child).or_insert(JobStatus::Pending);
        }
    }
    Ok(())
}

/// Terminal state a dependent may proceed past (Succeeded / Skipped / Cancelled).
fn is_done(status: &JobStatus) -> bool {
    matches!(
        status,
        JobStatus::Succeeded { .. } | JobStatus::Skipped | JobStatus::Cancelled
    )
}

/// End of run: no job may stay Pending (#287), then build the [`RunResult`].
fn finalize(
    mut st: RunState,
    store: &RunStore,
    run_id: &str,
    workflow_start: Instant,
    print_progress: bool,
) -> Result<RunResult> {
    // Safety net (#287): whatever never became runnable (e.g. downstream of a
    // failure that stopped the run) must not stay Pending in the store.
    for (id, status) in st.statuses.iter_mut() {
        if matches!(status, JobStatus::Pending) {
            store.upsert_job(run_id, id, &JobStatus::Cancelled)?;
            *status = JobStatus::Cancelled;
        }
    }

    let total_elapsed_ms = workflow_start.elapsed().as_millis() as u64;
    let succeeded = st
        .job_results
        .iter()
        .filter(|j| j.status == "succeeded")
        .count();
    let total = st.dag.topo_order.len();

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
        workflow_name: st.wf.name.clone(),
        jobs: st.job_results,
        total_elapsed_ms,
        succeeded,
        total,
        success: st.overall_success,
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
    /// Load-balancing weight (1 for workers not listed in `wf.workers`).
    weight: u32,
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
            tls: tls_for(wf, url),
            weight: 1,
        }];
    }
    if workers.is_empty() {
        return Vec::new();
    }
    let n = workers.len();
    let start = match strategy {
        LbStrategy::RoundRobin => rr.fetch_add(1, Ordering::Relaxed) % n,
        LbStrategy::Weighted => pick_weighted(workers, rr),
        LbStrategy::LeastConn => pick_least_conn(workers).await,
    };
    (0..n).map(|i| workers[(start + i) % n].clone()).collect()
}

/// Weighted round-robin: picks the worker index proportionally to its weight.
/// Uses a global counter so successive calls cycle through the weight-space evenly.
fn pick_weighted(workers: &[WorkerInfo], rr: &AtomicUsize) -> usize {
    let total_weight: u32 = workers.iter().map(|w| w.weight).sum();
    if total_weight == 0 {
        return rr.fetch_add(1, Ordering::Relaxed) % workers.len();
    }
    let slot = (rr.fetch_add(1, Ordering::Relaxed) as u32) % total_weight;
    let mut acc = 0u32;
    for (i, w) in workers.iter().enumerate() {
        acc += w.weight;
        if slot < acc {
            return i;
        }
    }
    0
}

/// Least-connections: query each worker's `/health` endpoint and return the
/// index of the worker with the fewest active jobs.
/// Workers that fail to respond are skipped (treated as having max load).
async fn pick_least_conn(workers: &[WorkerInfo]) -> usize {
    let mut best_idx = 0usize;
    let mut best_count = u64::MAX;
    for (i, w) in workers.iter().enumerate() {
        let url = format!("{}/health", w.url.trim_end_matches('/'));
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

/// RAII guard for the `fluxion_active_jobs` gauge: `inc` on creation, `dec` on
/// drop, so the gauge stays balanced regardless of which path a job exits by.
struct ActiveJobGuard;

impl ActiveJobGuard {
    fn new() -> Self {
        crate::metrics::ACTIVE_JOBS.inc();
        Self
    }
}

impl Drop for ActiveJobGuard {
    fn drop(&mut self) {
        crate::metrics::ACTIVE_JOBS.dec();
    }
}

/// Keep the runtime job table and the DAG in sync when a dynamic foreach
/// placeholder is replaced by its expanded `children` (#313, groundwork for
/// #295/#314).
///
/// Inserts every child into `wf.jobs` and `dag` (with its `depends_on`), then
/// removes the placeholder from both. Called by [`expand_dynamic_job`].
fn register_dynamic_children(
    wf: &mut Workflow,
    dag: &mut Dag,
    placeholder: &str,
    children: Vec<(String, JobDefinition)>,
) {
    let new_deps: HashMap<String, Vec<String>> = children
        .iter()
        .map(|(id, def)| (id.clone(), def.depends_on.clone()))
        .collect();
    for (id, def) in children {
        wf.jobs.insert(id, def);
    }
    dag.insert_nodes(&new_deps);
    wf.jobs.shift_remove(placeholder);
    dag.remove_node(placeholder);
}

/// Expand the dynamic foreach job `job_id` (#314) using the output of its single
/// dependency, then rewrite the job table / DAG / `foreach_map`:
/// children replace the placeholder, and every job that depended on the
/// placeholder now depends on all children instead.
///
/// Returns the child ids and the ids of the jobs that depended on the placeholder.
fn expand_dynamic_job(
    job_id: &str,
    wf: &mut Workflow,
    dag: &mut Dag,
    foreach_map: &mut HashMap<String, Vec<String>>,
    job_outputs: &HashMap<String, Vec<u8>>,
) -> Result<(Vec<String>, Vec<String>)> {
    let def = wf.jobs[job_id].clone();
    let [dep] = def.depends_on.as_slice() else {
        anyhow::bail!(
            "dynamic foreach job '{job_id}' must have exactly one dependency (found {})",
            def.depends_on.len()
        );
    };
    let output = job_outputs.get(dep).ok_or_else(|| {
        anyhow::anyhow!("dynamic foreach job '{job_id}': no output available from '{dep}'")
    })?;
    let (child_ids, children) = expand_foreach_dynamic(job_id, &def, output, 1)?;

    let dependents = dag.dependents.get(job_id).cloned().unwrap_or_default();
    register_dynamic_children(wf, dag, job_id, children.into_iter().collect());
    for d in &dependents {
        let Some(downstream) = wf.jobs.get_mut(d) else {
            continue;
        };
        downstream.depends_on = downstream
            .depends_on
            .iter()
            .flat_map(|x| {
                if x == job_id {
                    child_ids.clone()
                } else {
                    vec![x.clone()]
                }
            })
            .collect();
        dag.deps.insert(d.clone(), downstream.depends_on.clone());
        for c in &child_ids {
            dag.dependents.entry(c.clone()).or_default().push(d.clone());
        }
    }
    foreach_map.insert(job_id.to_string(), child_ids.clone());
    Ok((child_ids, dependents))
}

/// Mutable state of one `execute()` run, shared by the start / event /
/// scheduling steps (#293). The job table and DAG live here because dynamic
/// foreach expansion rewrites them mid-run.
struct RunState {
    wf: Workflow,
    dag: Dag,
    foreach_map: HashMap<String, Vec<String>>,
    statuses: HashMap<String, JobStatus>,
    /// Per-job output bytes (only stored when needed for fan-in).
    job_outputs: HashMap<String, Vec<u8>>,
    job_results: Vec<JobResult>,
    in_flight: usize,
    overall_success: bool,
}

/// Decides what becomes runnable / skipped / cancelled after a job finished.
struct DependentScheduler<'a> {
    store: &'a RunStore,
    run_id: &'a str,
    pre_succeeded: &'a HashMap<String, JobStatus>,
    starter: &'a JobStarter<'a>,
    print_progress: bool,
    pad: usize,
}

impl DependentScheduler<'_> {
    /// Examine the dependents of the finished job `finished`: cancel (upstream
    /// failure), skip (+ BFS cascade), expand (dynamic foreach) or start them.
    /// Returns `Ok(false)` when the run must stop.
    async fn schedule_dependents(&self, finished: &str, st: &mut RunState) -> Result<bool> {
        let (store, run_id, pad) = (self.store, self.run_id, self.pad);
        let (print_progress, pre_succeeded) = (self.print_progress, self.pre_succeeded);
        // Owned worklist: dynamic expansion rewrites `dag` mid-loop, and a
        // zero-child expansion re-queues the placeholder's dependents (#314).
        let mut candidates: Vec<String> =
            st.dag.dependents.get(finished).cloned().unwrap_or_default();
        let mut next = 0;
        while next < candidates.len() {
            let dep_owned = candidates[next].clone();
            let dep = &dep_owned;
            next += 1;
            if pre_succeeded.contains_key(dep) {
                continue;
            }
            // All deps must be in a terminal done state (Succeeded OR Skipped).
            // A Failed foreach child is terminal too (fail_fast=false lets its
            // siblings finish): the dependents are then Cancelled below (#287).
            let all_done = st.dag.deps[dep].iter().all(|d| {
                is_done(&st.statuses[d])
                    || (matches!(st.statuses[d], JobStatus::Failed { .. })
                        && st.foreach_map.values().any(|c| c.contains(d)))
            });
            if all_done {
                let job_def = &st.wf.jobs[dep];

                // Cancel the job (fan-in and everything downstream of it) if a
                // foreach child failed or an upstream job was Cancelled.
                let upstream_failed = st.dag.deps[dep].iter().any(|d| {
                    matches!(
                        st.statuses[d],
                        JobStatus::Failed { .. } | JobStatus::Cancelled
                    )
                });

                if upstream_failed {
                    let cancel_status = JobStatus::Cancelled;
                    store.upsert_job(run_id, dep, &cancel_status)?;
                    st.statuses.insert(dep.clone(), cancel_status);
                    if print_progress {
                        println!(
                            "[{}] {:<pad$}  CANCELLED (foreach child failed)",
                            timestamp(),
                            dep,
                            pad = pad
                        );
                    }
                    candidates.extend(st.dag.dependents.get(dep).cloned().unwrap_or_default());
                    continue;
                }

                // Check if any dep is Skipped and no `when:` guard is present → propagate skip.
                let any_dep_skipped = st.dag.deps[dep]
                    .iter()
                    .any(|d| matches!(st.statuses[d], JobStatus::Skipped));

                let should_skip = if let Some(when_expr) = &job_def.when {
                    !eval_when(when_expr, &st.statuses)
                } else {
                    any_dep_skipped
                };

                if should_skip {
                    if print_progress {
                        println!("[{}] {:<pad$}  SKIPPED", timestamp(), dep, pad = pad);
                    }
                    store.upsert_job(run_id, dep, &JobStatus::Skipped)?;
                    st.statuses.insert(dep.clone(), JobStatus::Skipped);
                    st.job_results
                        .push(JobResult::from_skipped(dep.to_string()));
                    self.cascade_skip(dep, st).await?;
                } else if is_dynamic_foreach(&st.wf.jobs[dep]) {
                    match expand_dynamic_job(
                        dep,
                        &mut st.wf,
                        &mut st.dag,
                        &mut st.foreach_map,
                        &st.job_outputs,
                    ) {
                        Ok((child_ids, dependents)) => {
                            st.statuses.remove(dep);
                            if child_ids.is_empty() {
                                candidates.extend(dependents);
                            }
                            for child in &child_ids {
                                self.starter.start(child, st).await?;
                            }
                        }
                        Err(e) => {
                            let failed = JobStatus::Failed {
                                elapsed: Duration::ZERO,
                                reason: format!("{e:#}"),
                            };
                            store.upsert_job(run_id, dep, &failed)?;
                            st.statuses.insert(dep.clone(), failed);
                            st.job_results.push(JobResult::from_failed(
                                dep.clone(),
                                Duration::ZERO,
                                format!("{e:#}"),
                            ));
                            st.overall_success = false;
                            if print_progress {
                                eprintln!("\nReason:\n  {e:#}");
                            }
                            cancel_orphaned_running_jobs(
                                store,
                                run_id,
                                &mut st.statuses,
                                print_progress,
                                pad,
                            )?;
                            return Ok(false);
                        }
                    }
                } else {
                    self.starter.start(dep, st).await?;
                }
            }
        }
        Ok(true)
    }

    /// BFS cascade: after `from` was skipped, skip (or, when its `when:` holds,
    /// start) every downstream job whose dependencies are all done.
    async fn cascade_skip(&self, from: &str, st: &mut RunState) -> Result<()> {
        let (store, run_id, pad) = (self.store, self.run_id, self.pad);
        let (print_progress, pre_succeeded) = (self.print_progress, self.pre_succeeded);
        // BFS cascade: propagate Skipped to all downstream jobs.
        let mut cascade_queue: std::collections::VecDeque<String> =
            std::collections::VecDeque::new();
        cascade_queue.push_back(from.to_string());
        while let Some(skipped_id) = cascade_queue.pop_front() {
            let downs: Vec<String> = st
                .dag
                .dependents
                .get(&skipped_id)
                .cloned()
                .unwrap_or_default();
            for downstream in &downs {
                if pre_succeeded.contains_key(downstream) {
                    continue;
                }
                if matches!(st.statuses[downstream.as_str()], JobStatus::Pending) {
                    let all_cascade_done = st.dag.deps[downstream.as_str()]
                        .iter()
                        .all(|d| is_done(&st.statuses[d]));
                    if all_cascade_done {
                        let downstream_def = &st.wf.jobs[downstream.as_str()];
                        let cascade_skip = if let Some(when_expr) = &downstream_def.when {
                            !eval_when(when_expr, &st.statuses)
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
                            store.upsert_job(run_id, downstream, &JobStatus::Skipped)?;
                            st.statuses.insert(downstream.clone(), JobStatus::Skipped);
                            st.job_results
                                .push(JobResult::from_skipped(downstream.clone()));
                            cascade_queue.push_back(downstream.clone());
                        } else {
                            self.starter.start(downstream, st).await?;
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

/// Immutable context for handling job-completion events in [`execute`] (#293).
struct EventHandler<'a> {
    store: &'a RunStore,
    run_id: &'a str,
    original_wf: &'a Workflow,
    print_progress: bool,
    pad: usize,
}

impl EventHandler<'_> {
    /// Save a finished job's output in the store. Purely best effort: skipped
    /// (over the size limits) or failed saves only log a warning and never
    /// affect the run; a later `retry` just re-runs such a job. Logs the job id
    /// and size, never the output itself.
    fn persist_output(&self, job_id: &str, output: &[u8]) {
        match self.store.save_output(self.run_id, job_id, output) {
            Ok(SaveOutcome::Saved) => {
                tracing::info!(job = %job_id, bytes = output.len(), "saved job output");
            }
            Ok(SaveOutcome::TooLarge) => tracing::warn!(
                job = %job_id,
                bytes = output.len(),
                limit = MAX_OUTPUT_BYTES,
                "job output too large to keep; retry will re-run this job"
            ),
            Ok(SaveOutcome::RunBudgetExceeded) => tracing::warn!(
                job = %job_id,
                bytes = output.len(),
                limit = MAX_RUN_OUTPUT_BYTES,
                "run output budget exceeded; retry will re-run this job"
            ),
            Err(e) => tracing::warn!(job = %job_id, error = %e, "could not save job output"),
        }
    }

    /// Record a finished job (store, `statuses`, metrics, outputs, results) and
    /// apply the failure policy (`fail_fast`, orphan cancellation).
    /// Returns `Ok(false)` when the run must stop (the caller leaves the event loop).
    fn on_event(&self, event: &JobEvent, st: &mut RunState) -> Result<bool> {
        let RunState {
            wf,
            foreach_map,
            statuses,
            job_outputs,
            job_results,
            overall_success,
            ..
        } = st;
        if self.print_progress {
            print_result(event, self.pad);
        }
        self.store
            .upsert_job(self.run_id, &event.job_id, &event.status)?;
        statuses.insert(event.job_id.clone(), event.status.clone());

        match &event.status {
            JobStatus::Succeeded { elapsed } => {
                crate::metrics::JOBS_TOTAL
                    .with_label_values(&["succeeded", &event.job_id])
                    .inc();
                crate::metrics::JOB_DURATION
                    .with_label_values(&[&event.job_id])
                    .observe(elapsed.as_secs_f64());
                // Keep the output in memory (fan-in / dynamic expansion) and in the
                // store (so a later `retry` can reuse it, #387).
                if let Some(output) = event.output.clone() {
                    self.persist_output(&event.job_id, &output);
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
                if self.print_progress {
                    print_foreach_progress(&event.job_id, foreach_map, statuses, self.pad);
                }
            }
            JobStatus::Failed { elapsed, reason } => {
                crate::metrics::JOBS_TOTAL
                    .with_label_values(&["failed", &event.job_id])
                    .inc();
                crate::metrics::JOB_DURATION
                    .with_label_values(&[&event.job_id])
                    .observe(elapsed.as_secs_f64());
                *overall_success = false;
                job_results.push(JobResult::from_failed(
                    event.job_id.clone(),
                    *elapsed,
                    reason.clone(),
                ));
                if self.print_progress {
                    eprintln!(
                        "\nReason:\n  {}\n\nRetry:\n  fluxion retry {} --from {}",
                        reason, self.run_id, event.job_id
                    );
                }

                // Check if the failed job is a foreach child.
                let foreach_parent = foreach_map
                    .iter()
                    .find(|(_, children)| children.contains(&event.job_id))
                    .map(|(parent, children)| (parent.clone(), children.clone()));

                match foreach_parent {
                    Some((parent_id, siblings)) => {
                        let fail_fast = self
                            .original_wf
                            .jobs
                            .get(&parent_id)
                            .or_else(|| wf.jobs.get(&parent_id))
                            .is_some_and(|j| j.fail_fast);
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
                                    self.store.upsert_job(self.run_id, sibling, &cancel)?;
                                    statuses.insert(sibling.clone(), cancel);
                                    if self.print_progress {
                                        println!(
                                            "[{}] {:<pad$}  CANCELLED (fail_fast)",
                                            timestamp(),
                                            sibling,
                                            pad = self.pad
                                        );
                                    }
                                }
                            }
                            cancel_orphaned_running_jobs(
                                self.store,
                                self.run_id,
                                statuses,
                                self.print_progress,
                                self.pad,
                            )?;
                            return Ok(false);
                        }
                        // fail_fast=false: let remaining siblings run to completion;
                        // the fan-in cancellation happens in the dep-ready check above.
                    }
                    None => {
                        // non-foreach job failure → stop immediately
                        cancel_orphaned_running_jobs(
                            self.store,
                            self.run_id,
                            statuses,
                            self.print_progress,
                            self.pad,
                        )?;
                        return Ok(false);
                    }
                }
            }
            _ => {}
        }
        Ok(true)
    }
}

/// Everything the job-start sequence needs, shared by the launch sites of
/// [`execute`] (#293). The workflow and mutable run state are passed per call
/// because dynamic foreach expansion rewrites them mid-run.
struct JobStarter<'a> {
    store: &'a RunStore,
    run_id: &'a str,
    host: &'a Arc<FluxionHost>,
    tx: &'a mpsc::UnboundedSender<JobEvent>,
    sem: &'a Arc<Semaphore>,
    workers: &'a [WorkerInfo],
    rr: &'a AtomicUsize,
    strategy: &'a LbStrategy,
    print_progress: bool,
    pad: usize,
}

impl JobStarter<'_> {
    /// Mark `id` Running (store + `statuses`), resolve its workers, assemble the
    /// fan-in input (`None` for jobs without `input_from`), spawn it and count
    /// it in `in_flight`.
    async fn start(&self, id: &str, st: &mut RunState) -> Result<()> {
        let RunState {
            wf,
            foreach_map,
            job_outputs,
            statuses,
            in_flight,
            ..
        } = st;
        if self.print_progress {
            print_running(id, self.pad);
        }
        self.store
            .upsert_job(self.run_id, id, &JobStatus::Running)?;
        let job_workers = resolve_workers(id, wf, self.workers, self.rr, self.strategy).await;
        let fanin_input =
            build_fanin_input(id, wf, foreach_map, job_outputs, self.host.clone()).await;
        launch(
            id,
            wf,
            self.host.clone(),
            self.tx.clone(),
            job_workers,
            self.sem.clone(),
            fanin_input,
        );
        statuses.insert(id.to_string(), JobStatus::Running);
        *in_flight += 1;
        Ok(())
    }
}

/// Report a job that failed before it could run (OCI pull, digest check, ...).
fn fail_early(tx: &mpsc::UnboundedSender<JobEvent>, job_id: &str, start: Instant, reason: String) {
    let _ = tx.send(JobEvent {
        job_id: job_id.to_string(),
        status: JobStatus::Failed {
            elapsed: start.elapsed(),
            reason,
        },
        output: None,
        compile_us: 0,
        instantiate_us: 0,
        execute_us: 0,
    });
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
    // Balanced by Drop on every exit path of the spawned task (#296).
    let active = ActiveJobGuard::new();
    let job_id = job_id.to_string();
    let executor = wf.jobs[&job_id].executor.clone();
    let async_dispatch = wf.jobs[&job_id].async_dispatch;
    let component = wf.jobs[&job_id].component.clone();
    let oci_ref = wf.jobs[&job_id].oci_ref.clone();
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
            let _active = active;
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

            // OCI auto-pull: if oci_ref is set, pull the component and write it to
            // a temp file, then replace `component` for the rest of this invocation.
            // The existing SHA-256 cache keys on wasm bytes, so the component is
            // cached automatically after the first pull.
            let (component, _oci_tmpfile) = if let Some(ref r) = oci_ref {
                match crate::oci::pull(r).await {
                    Ok(bytes) => {
                        use std::io::Write as _;
                        match tempfile::Builder::new()
                            .suffix(".wasm")
                            .tempfile()
                            .and_then(|mut f| {
                                f.write_all(&bytes)?;
                                Ok(f)
                            }) {
                            Ok(f) => {
                                let path = f.path().to_string_lossy().into_owned();
                                (path, Some(f))
                            }
                            Err(e) => {
                                fail_early(
                                    &tx,
                                    &job_id,
                                    start,
                                    format!("OCI tmp write failed: {e}"),
                                );
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        fail_early(
                            &tx,
                            &job_id,
                            start,
                            format!("OCI pull failed for '{}': {e}", r),
                        );
                        return;
                    }
                }
            } else {
                (component, None)
            };

            // Verify SHA-256 digest before loading the component (supply-chain protection).
            if let Err(e) = crate::verify_component_digest(&component, component_sha256.as_deref())
            {
                fail_early(
                    &tx,
                    &job_id,
                    start,
                    format!("digest verification failed: {e}"),
                );
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
                    // Rely solely on the wasmtime epoch deadline (set inside
                    // run_component_measured with this same timeout_secs) to
                    // bound execution time. An outer tokio::time::timeout
                    // around spawn_blocking cannot cancel the underlying OS
                    // thread — spawn_blocking tasks always run to completion
                    // regardless of whether anything is still awaiting them
                    // (a documented tokio limitation). Wrapping it here only
                    // gave up waiting early while the runaway guest kept
                    // occupying a blocking-pool slot until its own epoch
                    // trap eventually fired, silently reducing the pool's
                    // effective capacity under repeated timeouts (#244).
                    match tokio::task::spawn_blocking(move || {
                        host.run_component_measured(&c, i, &p, &e)
                    })
                    .await
                    {
                        Err(e) => Err(anyhow::anyhow!("{}", e)),
                        Ok(r) => r,
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

/// Decide whether a touched dynamic foreach group can be reused on retry (#387)
/// instead of being re-run whole. Returns the children to re-run, or `None` to
/// fall back to the whole-group re-run. Reuse requires: the group's single
/// dependency is not being re-run, succeeded and has a saved output; expanding
/// that output yields exactly the previous children; and every previous child
/// that is kept succeeded and has a saved output.
fn reusable_dynamic_group(
    id: &str,
    def: &JobDefinition,
    from_job: &str,
    replay_set: &std::collections::HashSet<String>,
    prev_states: &HashMap<String, JobStatus>,
    prev_outputs: &HashMap<String, Vec<u8>>,
) -> Option<Vec<String>> {
    let [dep] = def.depends_on.as_slice() else {
        return None;
    };
    if replay_set.contains(dep)
        || !matches!(prev_states.get(dep), Some(JobStatus::Succeeded { .. }))
    {
        return None;
    }
    let (child_ids, _) = expand_foreach_dynamic(id, def, prev_outputs.get(dep)?, 1).ok()?;
    let prefix = format!("{id}.");
    let prev_ids: std::collections::HashSet<&str> = prev_states
        .keys()
        .filter(|k| {
            k.strip_prefix(&prefix)
                .is_some_and(|n| n.parse::<usize>().is_ok())
        })
        .map(String::as_str)
        .collect();
    if prev_ids != child_ids.iter().map(String::as_str).collect() {
        return None; // the expansion no longer matches the previous run
    }
    let replay: Vec<String> = if from_job == id {
        child_ids.clone()
    } else if child_ids.iter().any(|c| c == from_job) {
        vec![from_job.to_string()]
    } else {
        Vec::new()
    };
    let kept_ok = child_ids.iter().all(|c| {
        replay.contains(c)
            || !matches!(prev_states.get(c), Some(JobStatus::Succeeded { .. }))
            || prev_outputs.contains_key(c)
    });
    kept_ok.then_some(replay)
}

/// If `id` looks like `<group>.<n>` where `<group>` is a dynamic foreach job in
/// `wf`, return `<group>` (dynamic children only exist at run time).
fn dynamic_group_of(wf: &Workflow, id: &str) -> Option<String> {
    let (group, n) = id.rsplit_once('.')?;
    (n.parse::<usize>().is_ok() && wf.jobs.get(group).is_some_and(is_dynamic_foreach))
        .then(|| group.to_string())
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
    use fluxion_core::workflow::ResourceLimits;
    use std::io::Write as _;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[test]
    fn register_dynamic_children_syncs_table_and_dag() {
        let mut w: Workflow = serde_json::from_str(
            r#"{"name":"t","jobs":{
                "a":{"component":"x.wasm"},
                "p":{"component":"x.wasm","depends_on":["a"]},
                "z":{"component":"x.wasm","depends_on":["p"]}}}"#,
        )
        .unwrap();
        let mut dag = Dag::build(&w).unwrap();
        let child: JobDefinition =
            serde_json::from_str(r#"{"component":"x.wasm","depends_on":["a"]}"#).unwrap();
        register_dynamic_children(&mut w, &mut dag, "p", vec![("p.0".into(), child)]);

        assert!(w.jobs.contains_key("p.0") && !w.jobs.contains_key("p"));
        assert_eq!(dag.deps["p.0"], vec!["a".to_string()]);
        assert!(dag.dependents["a"].contains(&"p.0".to_string()));
        assert!(!dag.deps.contains_key("p") && !dag.topo_order.contains(&"p".to_string()));
        assert!(!dag.dependents["a"].contains(&"p".to_string()));
    }

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

    /// A host whose compiled-component cache lives in a tempdir (kept alive by
    /// the returned guard) instead of the user's real cache location (#403).
    fn test_host() -> (tempfile::TempDir, Arc<FluxionHost>) {
        let dir = tempfile::tempdir().unwrap();
        let host = Arc::new(FluxionHost::with_cache_dir(dir.path()).unwrap());
        (dir, host)
    }

    fn make_worker_infos(urls: &[&str]) -> Vec<WorkerInfo> {
        urls.iter()
            .map(|u| WorkerInfo {
                url: u.to_string(),
                tls: None,
                weight: 1,
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

    /// #279: a pinned `worker:` URL must inherit the mTLS config declared in `workers:`.
    #[tokio::test]
    async fn pinned_worker_inherits_tls_from_workers() {
        let s = r#"{"name":"t","jobs":{"j":{"component":"x.wasm","worker":"https://w1"}},
            "workers":[{"url":"https://w1","tls":{"cert":"c.pem","key":"k.pem","ca":"ca.pem"}},"https://w2"]}"#;
        let w: Workflow = serde_json::from_str(s).unwrap();
        let rr = AtomicUsize::new(0);
        let got = resolve_workers("j", &w, &[], &rr, &LbStrategy::RoundRobin).await;
        assert_eq!(worker_urls(&got), vec!["https://w1"]);
        assert!(got[0].tls.is_some(), "pinned worker lost its TLS config");

        // A pinned URL not declared in `workers:` has no TLS.
        let s2 = s.replace(r#""worker":"https://w1""#, r#""worker":"https://other""#);
        let w2: Workflow = serde_json::from_str(&s2).unwrap();
        let got2 = resolve_workers("j", &w2, &[], &rr, &LbStrategy::RoundRobin).await;
        assert!(got2[0].tls.is_none());
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

    /// Effective-worker list with explicit weights (mirrors `effective_workers_info`).
    fn weighted_infos(weights: &[(u32, &str)]) -> Vec<WorkerInfo> {
        weights
            .iter()
            .map(|(weight, url)| WorkerInfo {
                url: url.to_string(),
                tls: None,
                weight: *weight,
            })
            .collect()
    }

    #[test]
    fn weighted_rr_distributes_proportionally() {
        // Workers with weights [2, 1]: over 30 slots, A should get ~20, B ~10.
        let ew = weighted_infos(&[(2, "http://a"), (1, "http://b")]);
        let rr = AtomicUsize::new(0);
        let mut counts = std::collections::HashMap::new();
        for _ in 0..30 {
            let idx = pick_weighted(&ew, &rr);
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
        let ew = weighted_infos(&[(1, "http://a"), (1, "http://b")]);
        let rr = AtomicUsize::new(0);
        let mut counts = [0usize; 2];
        for _ in 0..10 {
            counts[pick_weighted(&ew, &rr)] += 1;
        }
        assert_eq!(counts[0], 5);
        assert_eq!(counts[1], 5);
    }

    #[test]
    fn weighted_rr_single_worker_always_picked() {
        let ew = weighted_infos(&[(5, "http://only")]);
        let rr = AtomicUsize::new(0);
        for _ in 0..10 {
            assert_eq!(pick_weighted(&ew, &rr), 0);
        }
    }

    /// #271: `workers_srv:` / DB-registered workers only → `wf.workers` is empty.
    /// Weighted selection must not panic (previously `% 0`).
    #[tokio::test]
    async fn weighted_with_empty_static_workers_does_not_panic() {
        let w = wf(&[], None);
        assert!(w.workers.is_empty());
        let ew = weighted_infos(&[(1, "http://srv-only")]);
        let rr = AtomicUsize::new(0);
        assert_eq!(
            worker_urls(&resolve_workers("j", &w, &ew, &rr, &LbStrategy::Weighted).await),
            vec!["http://srv-only"]
        );
    }

    /// #271: when a static worker is filtered out (unhealthy), the remaining
    /// workers keep their own weights instead of inheriting by index.
    #[tokio::test]
    async fn weighted_applies_weight_to_matching_url_after_filtering() {
        // YAML: a(w=1), b(w=9), c(w=1). `a` is unhealthy → effective = [b, c].
        let w = wf_weighted(&[(1, "http://a"), (9, "http://b"), (1, "http://c")]);
        let ew = worker_infos_for(&w, vec!["http://b".into(), "http://c".into()]);
        assert_eq!(ew[0].weight, 9);
        assert_eq!(ew[1].weight, 1);
        let rr = AtomicUsize::new(0);
        let mut first_b = 0;
        for _ in 0..10 {
            let order = resolve_workers("j", &w, &ew, &rr, &LbStrategy::Weighted).await;
            if order[0].url == "http://b" {
                first_b += 1;
            }
        }
        assert_eq!(first_b, 9, "b (weight 9) should lead 9 of 10 times");
    }

    // Distinct privileged ports (1, 2, ...): an unprivileged test process cannot
    // bind them, so no parallel test's ephemeral listener can ever take the port
    // over, and nothing listens there so connections are refused. (A
    // bind-then-drop ephemeral port races with other tests' mock servers.)
    async fn closed_port_url() -> String {
        use std::sync::atomic::{AtomicU16, Ordering};
        static NEXT: AtomicU16 = AtomicU16::new(1);
        let port = NEXT.fetch_add(1, Ordering::Relaxed);
        format!("http://127.0.0.1:{port}")
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
                    //
                    // #220: run_remote() issues a `HEAD /components/{sha256}`
                    // CAS check before the actual `/run` POST. A HEAD (and any
                    // other request with no body) has no `Content-Length`
                    // header, so `content_len` stays `None` forever and the
                    // old logic kept calling `sock.read()` waiting for a body
                    // that would never arrive — while the client was itself
                    // waiting for our response on the same connection. Real
                    // deadlock (both sides blocked), not merely slow. Once
                    // headers are parsed, treat "no Content-Length" as "no
                    // body" and stop draining immediately.
                    let mut buf = Vec::new();
                    let mut tmp = [0u8; 8192];
                    let mut header_end = None;
                    let mut content_len = None;
                    while let Ok(n) = sock.read(&mut tmp).await {
                        if n == 0 {
                            break;
                        }
                        buf.extend_from_slice(&tmp[..n]);
                        if header_end.is_none()
                            && let Some(p) = find(&buf, b"\r\n\r\n")
                        {
                            header_end = Some(p + 4);
                            let head = String::from_utf8_lossy(&buf[..p]).to_lowercase();
                            for line in head.lines() {
                                if let Some(v) = line.strip_prefix("content-length:") {
                                    content_len = v.trim().parse::<usize>().ok();
                                }
                            }
                            if content_len.is_none() {
                                break;
                            }
                        }
                        if let (Some(he), Some(cl)) = (header_end, content_len)
                            && buf.len() >= he + cl
                        {
                            break;
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
                weight: 1,
            })
            .collect()
    }

    /// #220: `PermissionSet::default()` の `timeout_secs` (60s) をそのまま
    /// フェイルオーバーテストに使うと、`remote::run_remote` が
    /// `timeout_secs + 10` (=70s) をタイムアウトとして使うため、down worker
    /// 1件あたり最大70秒待つことになる。テストでは down worker への到達不能を
    /// 高速に検出させるため短いタイムアウトを使う。
    ///
    /// ただし調査の結果、この reqwest 側タイムアウトを縮めても環境によっては
    /// （少なくともこの開発サンドボックスでは）依然として60秒超ハングする
    /// ことを確認した — 恐らく OS/ネットワーク層で closed port への
    /// connect() 自体が async タイムアウトの外側でブロックしている
    /// （sandbox 特有のネットワーク制限の可能性がある）。原因の完全特定は
    /// 未了のため、テスト側にも `tokio::time::timeout` を被せて「原因不明の
    /// まま無期限にハングする」から「10秒で明確に失敗する」に変える。
    fn fast_timeout_perms() -> PermissionSet {
        PermissionSet {
            limits: ResourceLimits {
                timeout_secs: 2,
                ..ResourceLimits::default()
            },
            ..PermissionSet::default()
        }
    }

    #[tokio::test]
    async fn fails_over_to_healthy_worker() {
        // First worker is unreachable; the job must succeed on the second.
        let down = closed_port_url().await;
        let up = spawn_mock_worker(b"ok-output".to_vec()).await;
        let f = tmp_wasm();
        let path = f.path().to_string_lossy().into_owned();
        let workers = plain_workers(&[down, up]);
        let (out, _) = tokio::time::timeout(
            Duration::from_secs(20),
            run_with_failover(
                &workers,
                &path,
                b"in",
                &fast_timeout_perms(),
                &HashMap::new(),
            ),
        )
        .await
        .expect("run_with_failover should not hang past 10s (#220)")
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
        let err = tokio::time::timeout(
            Duration::from_secs(20),
            run_with_failover(
                &workers,
                &path,
                b"in",
                &fast_timeout_perms(),
                &HashMap::new(),
            ),
        )
        .await
        .expect("run_with_failover should not hang past 10s (#220)")
        .expect_err("all workers down → error");
        let msg = err.to_string();
        assert!(msg.contains(&d1), "error should list {d1}: {msg}");
        assert!(msg.contains(&d2), "error should list {d2}: {msg}");
    }

    /// Run `run_with_failover` over `workers` with a hang guard; returns the error text.
    async fn failover_err(workers: &[WorkerInfo]) -> String {
        let f = tmp_wasm();
        let path = f.path().to_string_lossy().into_owned();
        tokio::time::timeout(
            Duration::from_secs(20),
            run_with_failover(
                workers,
                &path,
                b"in",
                &fast_timeout_perms(),
                &HashMap::new(),
            ),
        )
        .await
        .expect("run_with_failover should not hang (#220)")
        .expect_err("dispatch should fail")
        .to_string()
    }

    /// #285: a worker that answers 5xx is an Execution error — stop immediately,
    /// never fail over to the (healthy) next worker.
    #[tokio::test]
    async fn execution_error_5xx_does_not_fail_over() {
        let failing = spawn_failing_mock_worker().await;
        let healthy = spawn_mock_worker(b"ok-output".to_vec()).await;
        let msg = failover_err(&plain_workers(&[failing.clone(), healthy.clone()])).await;
        assert!(msg.contains(&failing), "error should name {failing}: {msg}");
        assert!(
            !msg.contains(&healthy),
            "must not have tried the second worker {healthy}: {msg}"
        );
    }

    /// #285: HTTP 200 but no `output` field is also an Execution error (no failover).
    #[tokio::test]
    async fn missing_output_field_does_not_fail_over() {
        let bad = spawn_canned_mock_worker("200 OK", "{\"compile_ms\":1}").await;
        let healthy = spawn_mock_worker(b"ok-output".to_vec()).await;
        let msg = failover_err(&plain_workers(&[bad.clone(), healthy.clone()])).await;
        assert!(msg.contains(&bad), "error should name {bad}: {msg}");
        assert!(msg.contains("missing output field"), "{msg}");
        assert!(!msg.contains(&healthy), "must not fail over: {msg}");
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
        let (_cache, host) = test_host();
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
        let (_cache, host) = test_host();
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
        let (_cache, host) = test_host();
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
        let (_cache, host) = test_host();
        let result = build_fanin_input("aggregate", &wf, &foreach_map, &outputs, host)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(v, serde_json::json!({"a": 1, "b": 99, "c": 3}));
    }

    #[tokio::test]
    async fn remote_executor_without_workers_is_rejected() {
        let (_cache, host) = test_host();
        let wf = wf_with_executor("remote", false);
        let tmp = tempfile::tempdir().unwrap();
        let store = RunStore::open_at(&tmp.path().join("runs.db")).unwrap();
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
        let (_cache, host) = test_host();
        let wf = wf_with_executor("local", false);
        let tmp = tempfile::tempdir().unwrap();
        let store = RunStore::open_at(&tmp.path().join("runs.db")).unwrap();
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

    // ── #243: orphaned in-flight jobs on non-foreach failure ─────────────────

    /// Like `spawn_mock_worker` but waits `delay` before replying, so the
    /// caller can assert on state while the job is still in flight.
    async fn spawn_delayed_mock_worker(output: Vec<u8>, delay: Duration) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                let out_b64 = B64.encode(&output);
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut tmp = [0u8; 8192];
                    let mut header_end = None;
                    let mut content_len = None;
                    while let Ok(n) = sock.read(&mut tmp).await {
                        if n == 0 {
                            break;
                        }
                        buf.extend_from_slice(&tmp[..n]);
                        if header_end.is_none()
                            && let Some(p) = find(&buf, b"\r\n\r\n")
                        {
                            header_end = Some(p + 4);
                            let head = String::from_utf8_lossy(&buf[..p]).to_lowercase();
                            for line in head.lines() {
                                if let Some(v) = line.strip_prefix("content-length:") {
                                    content_len = v.trim().parse::<usize>().ok();
                                }
                            }
                            if content_len.is_none() {
                                break;
                            }
                        }
                        if let (Some(he), Some(cl)) = (header_end, content_len)
                            && buf.len() >= he + cl
                        {
                            break;
                        }
                    }
                    tokio::time::sleep(delay).await;
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

    /// Answers every request with `500` immediately, so the job fails fast
    /// without depending on timeout behavior.
    async fn spawn_failing_mock_worker() -> String {
        spawn_canned_mock_worker("500 Internal Server Error", "{\"error\":\"boom\"}").await
    }

    /// Answers every request with the given status line and JSON body.
    async fn spawn_canned_mock_worker(status: &'static str, body: &'static str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut tmp = [0u8; 8192];
                    let mut header_end = None;
                    let mut content_len = None;
                    while let Ok(n) = sock.read(&mut tmp).await {
                        if n == 0 {
                            break;
                        }
                        buf.extend_from_slice(&tmp[..n]);
                        if header_end.is_none()
                            && let Some(p) = find(&buf, b"\r\n\r\n")
                        {
                            header_end = Some(p + 4);
                            let head = String::from_utf8_lossy(&buf[..p]).to_lowercase();
                            for line in head.lines() {
                                if let Some(v) = line.strip_prefix("content-length:") {
                                    content_len = v.trim().parse::<usize>().ok();
                                }
                            }
                            if content_len.is_none() {
                                break;
                            }
                        }
                        if let (Some(he), Some(cl)) = (header_end, content_len)
                            && buf.len() >= he + cl
                        {
                            break;
                        }
                    }
                    let resp = format!(
                        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len(),
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                    let _ = sock.flush().await;
                });
            }
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn non_foreach_failure_cancels_orphaned_running_siblings() {
        let slow_url =
            spawn_delayed_mock_worker(b"slow-output".to_vec(), Duration::from_millis(300)).await;
        let fail_url = spawn_failing_mock_worker().await;
        // Both jobs need a real (dummy) component file on disk — a
        // nonexistent path would fail at the local read step before the
        // mock worker is ever contacted, defeating the point of this test.
        let slow_wasm = tmp_wasm();
        let fail_wasm = tmp_wasm();
        let slow_path = slow_wasm.path().to_string_lossy();
        let fail_path = fail_wasm.path().to_string_lossy();

        let wf: Workflow = serde_json::from_str(&format!(
            r#"{{"name":"t","jobs":{{
                "slow_ok":{{"component":"{slow_path}","executor":"remote","worker":"{slow_url}"}},
                "fast_fail":{{"component":"{fail_path}","executor":"remote","worker":"{fail_url}"}}
            }}}}"#
        ))
        .unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let store = RunStore::open_at(&tmp.path().join("runs.db")).unwrap();
        let (_cache, host) = test_host();
        let run_id = "test-run-243";
        store.create_run(run_id, "t", Path::new("t.yaml")).unwrap();

        let opts = ExecOpts {
            host,
            store: &store,
            run_id,
            pre_succeeded: HashMap::new(),
            print_progress: false,
            sem: Arc::new(Semaphore::new(4)),
            strategy: LbStrategy::RoundRobin,
            workers: vec![],
        };

        let result = tokio::time::timeout(Duration::from_secs(10), execute(&wf, opts))
            .await
            .expect("execute must not hang")
            .expect("execute should return Ok even when a job fails");
        assert!(!result.success, "run must be reported as failed overall");

        // "slow_ok" was still in flight (300ms delay) when "fast_fail" reported
        // failure. It must be persisted as Cancelled, not left dangling as
        // Running forever.
        let (_, jobs) = store.load_run(run_id).unwrap();
        assert!(
            matches!(jobs.get("slow_ok"), Some(JobStatus::Cancelled)),
            "orphaned in-flight job must be marked Cancelled, got: {:?}",
            jobs.get("slow_ok")
        );
    }

    /// #272: `fail_fast: true` on a static foreach parent must cancel the
    /// remaining children once one fails, and end the run promptly.
    #[tokio::test]
    async fn static_foreach_fail_fast_cancels_siblings() {
        let slow_a = spawn_delayed_mock_worker(b"slow".to_vec(), Duration::from_secs(20)).await;
        let slow_b = spawn_delayed_mock_worker(b"slow".to_vec(), Duration::from_secs(20)).await;
        let fail_url = spawn_failing_mock_worker().await;
        let wasm = tmp_wasm();
        let path = wasm.path().to_string_lossy().into_owned();

        // Round-robin over [fail, slow_a, slow_b]: exactly one child lands on the
        // failing worker first (a 500 is an Execution error → no failover).
        let wf: Workflow = serde_json::from_value(serde_json::json!({
            "name": "t",
            "jobs": {"j": {
                "component": path,
                "executor": "remote",
                "foreach": "$.items",
                "input": "{\"items\":[1,2,3]}",
                "fail_fast": true
            }}
        }))
        .unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let store = RunStore::open_at(&tmp.path().join("runs.db")).unwrap();
        let (_cache, host) = test_host();
        let run_id = "test-run-272";
        store.create_run(run_id, "t", Path::new("t.yaml")).unwrap();

        let opts = ExecOpts {
            host,
            store: &store,
            run_id,
            pre_succeeded: HashMap::new(),
            print_progress: false,
            sem: Arc::new(Semaphore::new(4)),
            strategy: LbStrategy::RoundRobin,
            workers: make_worker_infos(&[&fail_url, &slow_a, &slow_b]),
        };

        let start = Instant::now();
        let result = tokio::time::timeout(Duration::from_secs(15), execute(&wf, opts))
            .await
            .expect("execute must not hang")
            .expect("execute should return Ok even when a job fails");
        assert!(!result.success);
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "run must end right after the failure, not wait for siblings ({:?})",
            start.elapsed()
        );

        let (_, jobs) = store.load_run(run_id).unwrap();
        let count = |f: fn(&JobStatus) -> bool| jobs.values().filter(|s| f(s)).count();
        assert_eq!(
            count(|s| matches!(s, JobStatus::Failed { .. })),
            1,
            "{jobs:?}"
        );
        assert_eq!(count(|s| matches!(s, JobStatus::Cancelled)), 2, "{jobs:?}");
        assert_eq!(
            count(|s| matches!(s, JobStatus::Succeeded { .. })),
            0,
            "{jobs:?}"
        );
    }

    /// #314: run fetch -> dynamic foreach `process` -> aggregate (the
    /// examples/dynamic-fanout.yaml shape) against mock workers and return the
    /// ids of the jobs that ran.
    async fn run_dynamic_fanout(fetch_out: &str, two_deps: bool) -> (bool, Vec<String>) {
        let fetch_url = spawn_mock_worker(fetch_out.as_bytes().to_vec()).await;
        let proc_url = spawn_mock_worker(b"\"ok\"".to_vec()).await;
        let agg_url = spawn_mock_worker(b"\"done\"".to_vec()).await;
        let wasm = tmp_wasm();
        let p = wasm.path().to_string_lossy().into_owned();
        let process_deps = if two_deps {
            vec!["fetch", "fetch2"]
        } else {
            vec!["fetch"]
        };
        let mut wf_json = serde_json::json!({
            "name": "t",
            "jobs": {
                "fetch": {"component": p, "executor": "remote", "worker": fetch_url,
                          "input": "{}"},
                "process": {"component": p, "executor": "remote", "worker": proc_url,
                            "foreach": "$.items", "depends_on": process_deps},
                "aggregate": {"component": p, "executor": "remote", "worker": agg_url,
                              "depends_on": ["process"], "input_from": "process"}
            }
        });
        if two_deps {
            wf_json["jobs"]["fetch2"] = serde_json::json!({
                "component": p, "executor": "remote", "worker": fetch_url, "input": "{}"
            });
        }
        let wf: Workflow = serde_json::from_value(wf_json).unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let store = RunStore::open_at(&tmp.path().join("runs.db")).unwrap();
        let (_cache, host) = test_host();
        store
            .create_run("test-run-314", "t", Path::new("t.yaml"))
            .unwrap();
        let opts = ExecOpts {
            host,
            store: &store,
            run_id: "test-run-314",
            pre_succeeded: HashMap::new(),
            print_progress: false,
            sem: Arc::new(Semaphore::new(4)),
            strategy: LbStrategy::RoundRobin,
            workers: vec![],
        };
        let result = tokio::time::timeout(Duration::from_secs(10), execute(&wf, opts))
            .await
            .expect("execute must not hang")
            .expect("execute should return Ok");
        let mut ids: Vec<String> = result.jobs.iter().map(|j| j.job_id.clone()).collect();
        ids.sort();
        (result.success, ids)
    }

    #[tokio::test]
    async fn dynamic_foreach_expands_to_one_child_per_element() {
        let (ok, ids) = run_dynamic_fanout(r#"{"items":["a","b","c"]}"#, false).await;
        assert!(ok, "{ids:?}");
        assert_eq!(
            ids,
            ["aggregate", "fetch", "process.0", "process.1", "process.2"]
        );
    }

    #[tokio::test]
    async fn dynamic_foreach_with_two_dependencies_fails_clearly() {
        let (ok, ids) = run_dynamic_fanout(r#"{"items":["a"]}"#, true).await;
        assert!(!ok, "{ids:?}");
        assert!(ids.contains(&"process".to_string()), "{ids:?}");
        assert!(!ids.contains(&"process.0".to_string()), "{ids:?}");
    }

    #[tokio::test]
    async fn dynamic_foreach_with_empty_array_runs_zero_children() {
        let (ok, ids) = run_dynamic_fanout(r#"{"items":[]}"#, false).await;
        assert!(ok, "{ids:?}");
        assert_eq!(ids, ["aggregate", "fetch"]);
    }

    /// #287: `fail_fast: false` foreach, 3 children, one fails. The other two
    /// finish, the fan-in and its downstream are Cancelled, the independent
    /// branch keeps running, and nothing is left Pending.
    async fn run_failfast_false(dynamic: bool) -> (bool, HashMap<String, JobStatus>) {
        let fail_url = spawn_failing_mock_worker().await;
        let ok_a = spawn_mock_worker(b"\"ok\"".to_vec()).await;
        let ok_b = spawn_mock_worker(b"\"ok\"".to_vec()).await;
        let pinned = spawn_mock_worker(br#"{"items":[1,2,3]}"#.to_vec()).await;
        let wasm = tmp_wasm();
        let p = wasm.path().to_string_lossy().into_owned();
        let pin = |extra: serde_json::Value| {
            let mut j = serde_json::json!({
                "component": p, "executor": "remote", "worker": pinned
            });
            j.as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            j
        };
        let mut jobs = serde_json::json!({
            "agg": pin(serde_json::json!({"depends_on": ["j"], "input_from": "j"})),
            "post": pin(serde_json::json!({"depends_on": ["agg"]})),
            "ind": pin(serde_json::json!({})),
            "ind_next": pin(serde_json::json!({"depends_on": ["ind"]})),
        });
        let foreach_job = if dynamic {
            jobs["fetch"] = pin(serde_json::json!({}));
            serde_json::json!({
                "component": p, "executor": "remote", "foreach": "$.items",
                "fail_fast": false, "depends_on": ["fetch"]
            })
        } else {
            serde_json::json!({
                "component": p, "executor": "remote", "foreach": "$.items",
                "fail_fast": false, "input": "{\"items\":[1,2,3]}"
            })
        };
        jobs["j"] = foreach_job;
        let wf: Workflow =
            serde_json::from_value(serde_json::json!({"name": "t", "jobs": jobs})).unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let store = RunStore::open_at(&tmp.path().join("runs.db")).unwrap();
        let (_cache, host) = test_host();
        store
            .create_run("test-run-287", "t", Path::new("t.yaml"))
            .unwrap();
        let opts = ExecOpts {
            host,
            store: &store,
            run_id: "test-run-287",
            pre_succeeded: HashMap::new(),
            print_progress: false,
            sem: Arc::new(Semaphore::new(4)),
            strategy: LbStrategy::RoundRobin,
            workers: make_worker_infos(&[&fail_url, &ok_a, &ok_b]),
        };
        let result = tokio::time::timeout(Duration::from_secs(10), execute(&wf, opts))
            .await
            .expect("execute must not hang")
            .expect("execute should return Ok even when a job fails");
        let (_, jobs) = store.load_run("test-run-287").unwrap();
        (result.success, jobs)
    }

    fn assert_failfast_false_outcome(ok: bool, jobs: &HashMap<String, JobStatus>) {
        assert!(!ok, "run must be reported as failed");
        let count = |f: fn(&JobStatus) -> bool| {
            jobs.iter()
                .filter(|(id, s)| id.starts_with("j.") && f(s))
                .count()
        };
        assert_eq!(
            count(|s| matches!(s, JobStatus::Failed { .. })),
            1,
            "{jobs:?}"
        );
        assert_eq!(
            count(|s| matches!(s, JobStatus::Succeeded { .. })),
            2,
            "{jobs:?}"
        );
        for id in ["agg", "post"] {
            assert!(
                matches!(jobs.get(id), Some(JobStatus::Cancelled)),
                "{id}: {jobs:?}"
            );
        }
        for id in ["ind", "ind_next"] {
            assert!(
                matches!(jobs.get(id), Some(JobStatus::Succeeded { .. })),
                "{id}: {jobs:?}"
            );
        }
        assert!(
            !jobs.values().any(|s| matches!(s, JobStatus::Pending)),
            "no job may stay Pending: {jobs:?}"
        );
    }

    #[tokio::test]
    async fn static_foreach_fail_fast_false_cancels_fanin_and_leaves_no_pending() {
        let (ok, jobs) = run_failfast_false(false).await;
        assert_failfast_false_outcome(ok, &jobs);
    }

    #[tokio::test]
    async fn dynamic_foreach_fail_fast_false_cancels_fanin_and_leaves_no_pending() {
        let (ok, jobs) = run_failfast_false(true).await;
        assert_failfast_false_outcome(ok, &jobs);
    }

    /// #315: first run of fetch -> dynamic `process` -> aggregate (+ an
    /// unrelated `ind`), then `retry --from <from>`. Returns the retry result.
    ///
    /// `prepare(store, first_run_id)` shapes the outputs the first run left in the
    /// store (#387). Also returns the inputs every `aggregate` execution received.
    async fn retry_dynamic_prepared(
        from: &str,
        prepare: impl FnOnce(&RunStore, &str),
    ) -> (RunResult, Vec<String>) {
        let fetch_url = spawn_mock_worker(br#"{"items":[1,2,3]}"#.to_vec()).await;
        let ok_url = spawn_mock_worker(b"\"ok\"".to_vec()).await;
        let agg_inputs = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let agg_url = spawn_capturing_worker(Arc::clone(&agg_inputs)).await;
        let wasm = tmp_wasm();
        let p = wasm.path().to_string_lossy().into_owned();
        let job = |worker: &str, extra: serde_json::Value| {
            let mut j = serde_json::json!({
                "component": p, "executor": "remote", "worker": worker
            });
            j.as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            j
        };
        let wf: Workflow = serde_json::from_value(serde_json::json!({"name": "t", "jobs": {
            "fetch": job(&fetch_url, serde_json::json!({})),
            "process": job(&ok_url, serde_json::json!({
                "foreach": "$.items", "depends_on": ["fetch"]})),
            "aggregate": job(&agg_url, serde_json::json!({
                "depends_on": ["process"], "input_from": "process"})),
            "ind": job(&ok_url, serde_json::json!({})),
        }}))
        .unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let store = RunStore::open_at(&tmp.path().join("runs.db")).unwrap();
        let (_cache, host) = test_host();
        let opts = RunOptions {
            progress: false,
            ..Default::default()
        };
        let first = run_with_store(&wf, Path::new("t.yaml"), host.clone(), &opts, &store)
            .await
            .expect("first run");
        assert!(first.success, "{:?}", first.jobs);
        prepare(&store, &first.run_id);
        let retried = tokio::time::timeout(
            Duration::from_secs(10),
            retry_with_store(
                &wf,
                Path::new("t.yaml"),
                host,
                &first.run_id,
                from,
                &opts,
                &store,
            ),
        )
        .await
        .expect("retry must not hang")
        .expect("retry must re-expand the dynamic foreach");
        let inputs = agg_inputs.lock().unwrap().clone();
        (retried, inputs)
    }

    /// Retry of an "old run" that saved no outputs: must fall back to re-running
    /// the whole dynamic group (the pre-#387 behaviour).
    async fn retry_dynamic_from(from: &str) -> RunResult {
        retry_dynamic_prepared(from, |store, run| {
            store.delete_run_outputs(run).unwrap();
        })
        .await
        .0
    }

    /// Make the saved child outputs distinguishable from a fresh re-run.
    fn mark_saved_dynamic_children(store: &RunStore, run: &str) {
        for n in 0..3 {
            store
                .save_output(
                    run,
                    &format!("process.{n}"),
                    format!("\"saved-{n}\"").as_bytes(),
                )
                .unwrap();
        }
    }

    #[tokio::test]
    async fn retry_from_aggregate_reuses_fetch_and_succeeded_children() {
        let (r, agg) = retry_dynamic_prepared("aggregate", mark_saved_dynamic_children).await;
        assert!(r.success, "{:?}", r.jobs);
        assert_eq!(job_ids(&r, false), ["aggregate"]);
        for kept in ["fetch", "process.0", "process.1", "process.2", "ind"] {
            assert!(
                job_ids(&r, true).contains(&kept.to_string()),
                "{kept} must be reused"
            );
        }
        let last = agg.last().expect("aggregate ran");
        assert!(!last.contains("null"), "{last}");
        assert_eq!(last, r#"["saved-0","saved-1","saved-2"]"#);
    }

    #[tokio::test]
    async fn retry_from_dynamic_child_reruns_only_that_child_and_the_fanin() {
        let (r, agg) = retry_dynamic_prepared("process.1", mark_saved_dynamic_children).await;
        assert!(r.success, "{:?}", r.jobs);
        assert_eq!(job_ids(&r, false), ["aggregate", "process.1"]);
        for kept in ["fetch", "process.0", "process.2"] {
            assert!(
                job_ids(&r, true).contains(&kept.to_string()),
                "{kept} must be reused"
            );
        }
        let last = agg.last().unwrap();
        assert!(!last.contains("null"), "{last}");
        assert_eq!(last, r#"["saved-0","ok","saved-2"]"#);
    }

    #[tokio::test]
    async fn retry_from_dynamic_parent_reuses_fetch_and_reruns_all_children() {
        let (r, agg) = retry_dynamic_prepared("process", mark_saved_dynamic_children).await;
        assert!(r.success, "{:?}", r.jobs);
        assert_eq!(
            job_ids(&r, false),
            ["aggregate", "process.0", "process.1", "process.2"]
        );
        assert!(job_ids(&r, true).contains(&"fetch".to_string()));
        assert_eq!(agg.last().unwrap(), r#"["ok","ok","ok"]"#);
    }

    #[tokio::test]
    async fn retry_dynamic_with_a_missing_child_output_reruns_the_whole_group() {
        let (r, agg) = retry_dynamic_prepared("process.1", |store, run| {
            mark_saved_dynamic_children(store, run);
            store.delete_run_outputs(run).unwrap();
            store
                .save_output(run, "fetch", br#"{"items":[1,2,3]}"#)
                .unwrap();
            store
                .save_output(run, "process.0", br#""saved-0""#)
                .unwrap();
        })
        .await;
        assert!(r.success, "{:?}", r.jobs);
        assert_eq!(
            job_ids(&r, false),
            ["aggregate", "fetch", "process.0", "process.1", "process.2"]
        );
        let last = agg.last().unwrap();
        assert!(!last.contains("null") && !last.contains("saved"), "{last}");
        assert_eq!(last, r#"["ok","ok","ok"]"#);
    }

    #[tokio::test]
    async fn retry_dynamic_with_a_changed_element_count_reruns_the_whole_group() {
        // The saved fetch output now expands to 2 children, the previous run had 3.
        let (r, agg) = retry_dynamic_prepared("aggregate", |store, run| {
            mark_saved_dynamic_children(store, run);
            store
                .save_output(run, "fetch", br#"{"items":[1,2]}"#)
                .unwrap();
        })
        .await;
        assert!(r.success, "{:?}", r.jobs);
        assert_eq!(
            job_ids(&r, false),
            ["aggregate", "fetch", "process.0", "process.1", "process.2"]
        );
        assert_eq!(agg.last().unwrap(), r#"["ok","ok","ok"]"#);
    }

    fn job_ids(r: &RunResult, skipped: bool) -> Vec<String> {
        let mut ids: Vec<String> = r
            .jobs
            .iter()
            .filter(|j| j.skipped == skipped)
            .map(|j| j.job_id.clone())
            .collect();
        ids.sort();
        ids
    }

    #[tokio::test]
    async fn retry_from_dynamic_foreach_job_reexpands_children() {
        let r = retry_dynamic_from("process").await;
        assert!(r.success, "{:?}", r.jobs);
        // dependency output is not retained, so fetch re-runs too.
        assert_eq!(
            job_ids(&r, false),
            ["aggregate", "fetch", "process.0", "process.1", "process.2"]
        );
        assert_eq!(job_ids(&r, true), ["ind"]);
    }

    #[tokio::test]
    async fn retry_from_fanin_of_dynamic_foreach_reruns_whole_group() {
        let r = retry_dynamic_from("aggregate").await;
        assert!(r.success, "{:?}", r.jobs);
        assert_eq!(
            job_ids(&r, false),
            ["aggregate", "fetch", "process.0", "process.1", "process.2"]
        );
    }

    #[tokio::test]
    async fn retry_from_unrelated_job_keeps_dynamic_group_skipped() {
        let r = retry_dynamic_from("ind").await;
        assert!(r.success, "{:?}", r.jobs);
        assert_eq!(job_ids(&r, false), ["ind"]);
        assert!(job_ids(&r, true).contains(&"aggregate".to_string()));
    }

    /// #315: `fail_fast: true` on a dynamic foreach cancels the remaining
    /// children once one fails (same path as the static case, #272).
    #[tokio::test]
    async fn dynamic_foreach_fail_fast_true_cancels_siblings() {
        let slow_a = spawn_delayed_mock_worker(b"slow".to_vec(), Duration::from_secs(20)).await;
        let slow_b = spawn_delayed_mock_worker(b"slow".to_vec(), Duration::from_secs(20)).await;
        let fail_url = spawn_failing_mock_worker().await;
        let fetch_url = spawn_mock_worker(br#"{"items":[1,2,3]}"#.to_vec()).await;
        let wasm = tmp_wasm();
        let p = wasm.path().to_string_lossy().into_owned();
        let wf: Workflow = serde_json::from_value(serde_json::json!({"name": "t", "jobs": {
            "fetch": {"component": p, "executor": "remote", "worker": fetch_url},
            "j": {"component": p, "executor": "remote", "foreach": "$.items",
                  "fail_fast": true, "depends_on": ["fetch"]},
            "agg": {"component": p, "executor": "remote", "worker": fetch_url,
                    "depends_on": ["j"], "input_from": "j"}
        }}))
        .unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let store = RunStore::open_at(&tmp.path().join("runs.db")).unwrap();
        let (_cache, host) = test_host();
        store
            .create_run("test-run-315", "t", Path::new("t.yaml"))
            .unwrap();
        let opts = ExecOpts {
            host,
            store: &store,
            run_id: "test-run-315",
            pre_succeeded: HashMap::new(),
            print_progress: false,
            sem: Arc::new(Semaphore::new(4)),
            strategy: LbStrategy::RoundRobin,
            workers: make_worker_infos(&[&fail_url, &slow_a, &slow_b]),
        };
        let start = Instant::now();
        let result = tokio::time::timeout(Duration::from_secs(15), execute(&wf, opts))
            .await
            .expect("execute must not hang")
            .expect("execute should return Ok even when a job fails");
        assert!(!result.success);
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "must end right after the failure ({:?})",
            start.elapsed()
        );
        let (_, jobs) = store.load_run("test-run-315").unwrap();
        let n = |f: fn(&JobStatus) -> bool| jobs.values().filter(|s| f(s)).count();
        assert_eq!(n(|s| matches!(s, JobStatus::Failed { .. })), 1, "{jobs:?}");
        assert_eq!(n(|s| matches!(s, JobStatus::Cancelled)), 3, "{jobs:?}");
        // Only `fetch` (outside the foreach group) may have succeeded.
        assert_eq!(
            n(|s| matches!(s, JobStatus::Succeeded { .. })),
            1,
            "{jobs:?}"
        );
        assert!(
            matches!(jobs["fetch"], JobStatus::Succeeded { .. }),
            "{jobs:?}"
        );
        assert_eq!(n(|s| matches!(s, JobStatus::Pending)), 0, "{jobs:?}");
    }

    /// #288: first run of a static foreach `j` (3 children) -> fan-in `agg`,
    /// plus an unrelated `ind`; then `retry --from <from>`.
    ///
    /// Also returns the input bytes (as text) every `agg` execution received.
    ///
    /// `prepare(store, first_run_id)` runs between the first run and the retry, to
    /// shape which outputs the first run left in the store (#387).
    async fn retry_static_prepared(
        from: &str,
        prepare: impl FnOnce(&RunStore, &str),
    ) -> Result<(RunResult, Vec<String>)> {
        let ok_url = spawn_mock_worker(b"\"ok\"".to_vec()).await;
        let agg_inputs = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let agg_url = spawn_capturing_worker(Arc::clone(&agg_inputs)).await;
        let wasm = tmp_wasm();
        let p = wasm.path().to_string_lossy().into_owned();
        let job = |extra: serde_json::Value| {
            let mut j = serde_json::json!({
                "component": p, "executor": "remote", "worker": ok_url
            });
            j.as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            j
        };
        let wf: Workflow = serde_json::from_value(serde_json::json!({"name": "t", "jobs": {
            "j": job(serde_json::json!({
                "foreach": "$.items", "input": "{\"items\":[1,2,3]}"})),
            "agg": job(serde_json::json!({
                "depends_on": ["j"], "input_from": "j", "worker": agg_url})),
            "ind": job(serde_json::json!({})),
        }}))
        .unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let store = RunStore::open_at(&tmp.path().join("runs.db")).unwrap();
        let (_cache, host) = test_host();
        let opts = RunOptions {
            progress: false,
            ..Default::default()
        };
        let first = run_with_store(&wf, Path::new("t.yaml"), host.clone(), &opts, &store)
            .await
            .expect("first run");
        assert!(first.success, "{:?}", first.jobs);
        prepare(&store, &first.run_id);
        let retried = tokio::time::timeout(
            Duration::from_secs(10),
            retry_with_store(
                &wf,
                Path::new("t.yaml"),
                host,
                &first.run_id,
                from,
                &opts,
                &store,
            ),
        )
        .await
        .expect("retry must not hang")?;
        let inputs = agg_inputs.lock().unwrap().clone();
        Ok((retried, inputs))
    }

    /// Retry of an "old run" that saved no outputs: must fall back to re-running
    /// the whole foreach group (the pre-#387 behaviour).
    async fn retry_static_from(from: &str) -> Result<(RunResult, Vec<String>)> {
        retry_static_prepared(from, |store, run| {
            store.delete_run_outputs(run).unwrap();
        })
        .await
    }

    /// Mark the saved outputs of j.0 / j.2 so reuse is distinguishable from a re-run.
    fn mark_saved_children(store: &RunStore, run: &str) {
        store.save_output(run, "j.0", br#""saved-0""#).unwrap();
        store.save_output(run, "j.2", br#""saved-2""#).unwrap();
    }

    #[tokio::test]
    async fn retry_from_static_child_reuses_saved_outputs_of_the_other_children() {
        let (r, agg) = retry_static_prepared("j.1", mark_saved_children)
            .await
            .expect("retry");
        assert!(r.success, "{:?}", r.jobs);
        assert_eq!(job_ids(&r, false), ["agg", "j.1"]);
        assert_eq!(job_ids(&r, true), ["ind", "j.0", "j.2"]);
        // the reused children's SAVED outputs feed the fan-in; no null
        let last = agg.last().expect("agg ran");
        assert!(!last.contains("null"), "{last}");
        assert_eq!(last, r#"["saved-0","ok","saved-2"]"#);
    }

    #[tokio::test]
    async fn retry_from_static_fanin_reuses_all_saved_children() {
        let (r, agg) = retry_static_prepared("agg", |store, run| {
            mark_saved_children(store, run);
            store.save_output(run, "j.1", br#""saved-1""#).unwrap();
        })
        .await
        .expect("retry");
        assert!(r.success, "{:?}", r.jobs);
        assert_eq!(job_ids(&r, false), ["agg"]);
        assert_eq!(job_ids(&r, true), ["ind", "j.0", "j.1", "j.2"]);
        assert_eq!(agg.last().unwrap(), r#"["saved-0","saved-1","saved-2"]"#);
    }

    #[tokio::test]
    async fn retry_with_a_missing_child_output_reruns_the_whole_group() {
        // j.2's output was not saved (e.g. over the size limit): fall back.
        let (r, agg) = retry_static_prepared("j.1", |store, run| {
            mark_saved_children(store, run);
            store.delete_run_outputs(run).unwrap();
            store.save_output(run, "j.0", br#""saved-0""#).unwrap();
        })
        .await
        .expect("retry");
        assert!(r.success, "{:?}", r.jobs);
        assert_eq!(job_ids(&r, false), ["agg", "j.0", "j.1", "j.2"]);
        let last = agg.last().unwrap();
        assert!(!last.contains("null") && !last.contains("saved"), "{last}");
        assert_eq!(last, r#"["ok","ok","ok"]"#);
    }

    #[tokio::test]
    async fn retried_run_keeps_the_reused_outputs_for_the_next_retry() {
        // Outputs carried over into the retry run are themselves reusable.
        let ok = spawn_mock_worker(b"\"ok\"".to_vec()).await;
        let wasm = tmp_wasm();
        let p = wasm.path().to_string_lossy().into_owned();
        let wf: Workflow = serde_json::from_value(serde_json::json!({"name": "t", "jobs": {
            "j": pinned_job(&p, &ok, serde_json::json!({
                "foreach": "$.items", "input": "{\"items\":[1,2]}"})),
            "agg": pinned_job(&p, &ok, serde_json::json!({
                "depends_on": ["j"], "input_from": "j"})),
        }}))
        .unwrap();
        let (_dir, store) = temp_store();
        let (_cache, host) = test_host();
        let opts = RunOptions {
            progress: false,
            ..Default::default()
        };
        let first = run_with_store(&wf, Path::new("t.yaml"), host.clone(), &opts, &store)
            .await
            .unwrap();
        let second = retry_with_store(
            &wf,
            Path::new("t.yaml"),
            host,
            &first.run_id,
            "j.1",
            &opts,
            &store,
        )
        .await
        .unwrap();
        let carried = store.load_outputs(&second.run_id).unwrap();
        assert!(carried.contains_key("j.0"), "{:?}", carried.keys());
    }

    /// Worker that records the `input` of every `/run` request (decoded).
    async fn spawn_capturing_worker(inputs: Arc<std::sync::Mutex<Vec<String>>>) -> String {
        use axum::{
            Json, Router,
            http::StatusCode,
            routing::{get, head, post},
        };
        let app = Router::new()
            .route("/health", get(|| async { StatusCode::OK }))
            .route("/components/:sha", head(|| async { StatusCode::OK }))
            .route(
                "/run",
                post(move |Json(body): Json<serde_json::Value>| async move {
                    let raw = B64
                        .decode(body["input"].as_str().unwrap_or_default())
                        .unwrap_or_default();
                    inputs
                        .lock()
                        .unwrap()
                        .push(String::from_utf8_lossy(&raw).into_owned());
                    Json(serde_json::json!({
                        "output": B64.encode(b"\"done\""),
                        "compile_ms": 0, "instantiate_ms": 0, "execute_ms": 0
                    }))
                }),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}")
    }

    /// The retried fan-in must receive every child's output, never a null.
    fn assert_fanin_got_all_outputs(agg_inputs: &[String]) {
        let last = agg_inputs.last().expect("agg ran");
        assert!(!last.contains("null"), "fan-in input has a null: {last}");
        assert_eq!(last, r#"["ok","ok","ok"]"#);
    }

    #[tokio::test]
    async fn retry_from_static_foreach_parent_reruns_all_children_and_fanin() {
        let (r, agg) = retry_static_from("j").await.expect("retry");
        assert!(r.success, "{:?}", r.jobs);
        assert_eq!(job_ids(&r, false), ["agg", "j.0", "j.1", "j.2"]);
        assert_eq!(job_ids(&r, true), ["ind"]);
        assert_fanin_got_all_outputs(&agg);
    }

    #[tokio::test]
    async fn retry_from_static_foreach_child_reruns_whole_group() {
        // Child outputs are not retained across runs (#387), so naming one child
        // re-runs the whole group; a carried-over child would feed null to agg.
        let (r, agg) = retry_static_from("j.1").await.expect("retry");
        assert!(r.success, "{:?}", r.jobs);
        assert_eq!(job_ids(&r, false), ["agg", "j.0", "j.1", "j.2"]);
        assert_eq!(job_ids(&r, true), ["ind"]);
        assert_fanin_got_all_outputs(&agg);
    }

    #[tokio::test]
    async fn retry_from_static_fanin_reruns_whole_group() {
        let (r, agg) = retry_static_from("agg").await.expect("retry");
        assert!(r.success, "{:?}", r.jobs);
        assert_eq!(job_ids(&r, false), ["agg", "j.0", "j.1", "j.2"]);
        assert_fanin_got_all_outputs(&agg);
    }

    #[tokio::test]
    async fn retry_from_unknown_job_is_an_error() {
        let err = retry_static_from("nope").await.expect_err("must fail");
        assert!(err.to_string().contains("nope"), "{err:#}");
    }

    #[tokio::test]
    async fn retry_from_dynamic_child_id_reruns_the_group() {
        let r = retry_dynamic_from("process.1").await;
        assert!(r.success, "{:?}", r.jobs);
        assert_eq!(
            job_ids(&r, false),
            ["aggregate", "fetch", "process.0", "process.1", "process.2"]
        );
    }

    // ── #293: characterization of `when:` / skip cascade ─────────────────────
    //
    // These pin the CURRENT behaviour of execute() before the skip cascade is
    // refactored. Tests marked #[ignore] describe behaviour that looks wrong;
    // they assert the intuitive outcome and are not fixed here.

    /// Run `jobs` (component/executor/worker are filled in) through `execute`.
    /// `"worker": "FAIL"` pins a job to a failing worker; foreach jobs are left
    /// unpinned and spread round-robin over [fail, ok, ok] when `rr_foreach`.
    async fn run_jobs(
        mut jobs: serde_json::Value,
        rr_foreach: bool,
    ) -> (RunResult, HashMap<String, JobStatus>) {
        let ok = spawn_mock_worker(b"\"ok\"".to_vec()).await;
        let fail = spawn_failing_mock_worker().await;
        let wasm = tmp_wasm();
        let p = wasm.path().to_string_lossy().into_owned();
        for (_, j) in jobs.as_object_mut().unwrap() {
            let o = j.as_object_mut().unwrap();
            o.insert("component".into(), p.clone().into());
            o.insert("executor".into(), "remote".into());
            let pinned = if o.get("worker").and_then(|w| w.as_str()) == Some("FAIL") {
                Some(fail.clone())
            } else if rr_foreach && o.contains_key("foreach") {
                None
            } else {
                Some(ok.clone())
            };
            match pinned {
                Some(w) => o.insert("worker".into(), w.into()),
                None => o.remove("worker"),
            };
        }
        let wf: Workflow =
            serde_json::from_value(serde_json::json!({"name": "t", "jobs": jobs})).unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let store = RunStore::open_at(&tmp.path().join("runs.db")).unwrap();
        let (_cache, host) = test_host();
        store
            .create_run("test-run-293", "t", Path::new("t.yaml"))
            .unwrap();
        let opts = ExecOpts {
            host,
            store: &store,
            run_id: "test-run-293",
            pre_succeeded: HashMap::new(),
            print_progress: false,
            sem: Arc::new(Semaphore::new(4)),
            strategy: LbStrategy::RoundRobin,
            workers: make_worker_infos(&[&fail, &ok, &ok]),
        };
        let result = tokio::time::timeout(Duration::from_secs(10), execute(&wf, opts))
            .await
            .expect("execute must not hang")
            .expect("execute should return Ok");
        let (_, states) = store.load_run("test-run-293").unwrap();
        (result, states)
    }

    fn is_skipped(s: &HashMap<String, JobStatus>, id: &str) -> bool {
        matches!(s.get(id), Some(JobStatus::Skipped))
    }
    fn is_ok(s: &HashMap<String, JobStatus>, id: &str) -> bool {
        matches!(s.get(id), Some(JobStatus::Succeeded { .. }))
    }
    fn is_cancelled(s: &HashMap<String, JobStatus>, id: &str) -> bool {
        matches!(s.get(id), Some(JobStatus::Cancelled))
    }
    fn none_pending(s: &HashMap<String, JobStatus>) -> bool {
        !s.values().any(|v| matches!(v, JobStatus::Pending))
    }

    #[tokio::test]
    async fn skip_when_false_marks_job_skipped_and_run_still_succeeds() {
        let (r, s) = run_jobs(
            serde_json::json!({
                "a": {},
                "b": {"depends_on": ["a"], "when": "a.status != 'SUCCESS'"},
                "ind": {},
            }),
            false,
        )
        .await;
        assert!(r.success);
        assert!(is_ok(&s, "a") && is_ok(&s, "ind"), "{s:?}");
        assert!(is_skipped(&s, "b"), "{s:?}");
        let b = r.jobs.iter().find(|j| j.job_id == "b").unwrap();
        assert_eq!(b.status, "skipped");
        assert!(none_pending(&s));
    }

    #[tokio::test]
    async fn skip_propagates_to_all_downstream_jobs_without_when() {
        let (r, s) = run_jobs(
            serde_json::json!({
                "a": {},
                "b": {"depends_on": ["a"], "when": "a.status != 'SUCCESS'"},
                "c": {"depends_on": ["b"]},
                "d": {"depends_on": ["c"]},
                "ind": {},
            }),
            false,
        )
        .await;
        assert!(r.success);
        for id in ["b", "c", "d"] {
            assert!(is_skipped(&s, id), "{id}: {s:?}");
        }
        assert!(is_ok(&s, "ind"));
        assert!(none_pending(&s));
    }

    #[tokio::test]
    async fn downstream_with_true_when_runs_even_if_its_dependency_was_skipped() {
        let (r, s) = run_jobs(
            serde_json::json!({
                "a": {},
                "b": {"depends_on": ["a"], "when": "a.status != 'SUCCESS'"},
                "c": {"depends_on": ["b"], "when": "b.status == 'SKIPPED'"},
                "d": {"depends_on": ["c"]},
            }),
            false,
        )
        .await;
        assert!(r.success);
        assert!(is_skipped(&s, "b"));
        // an explicit `when:` overrides skip propagation; its own dependents follow it
        assert!(is_ok(&s, "c") && is_ok(&s, "d"), "{s:?}");
    }

    #[tokio::test]
    async fn fanin_with_a_skipped_extra_dependency_is_skipped_despite_finished_children() {
        // Pins current "any skipped dependency skips a job without `when`" rule,
        // even for a fan-in whose children all succeeded.
        let (r, s) = run_jobs(
            serde_json::json!({
                "a": {},
                "sk": {"depends_on": ["a"], "when": "a.status != 'SUCCESS'"},
                "j": {"foreach": "$.items", "input": "{\"items\":[1,2]}"},
                "agg": {"depends_on": ["j", "sk"], "input_from": "j"},
            }),
            false,
        )
        .await;
        assert!(r.success);
        assert!(is_ok(&s, "j.0") && is_ok(&s, "j.1"), "{s:?}");
        assert!(is_skipped(&s, "agg"), "{s:?}");
    }

    #[tokio::test]
    async fn skipped_branch_stays_skipped_while_failed_foreach_cancels_its_fanin() {
        // #385: fail_fast=false failure -> fan-in Cancelled (not Skipped);
        // the unrelated skipped branch keeps Skipped. Nothing stays Pending.
        let (r, s) = run_jobs(
            serde_json::json!({
                "a": {},
                "sk": {"depends_on": ["a"], "when": "a.status != 'SUCCESS'"},
                "sk2": {"depends_on": ["sk"]},
                "j": {"foreach": "$.items", "input": "{\"items\":[1,2,3]}",
                      "fail_fast": false, "depends_on": ["a"]},
                "agg": {"depends_on": ["j"], "input_from": "j"},
            }),
            true,
        )
        .await;
        assert!(!r.success);
        assert!(is_skipped(&s, "sk") && is_skipped(&s, "sk2"), "{s:?}");
        assert!(is_cancelled(&s, "agg"), "{s:?}");
        assert!(none_pending(&s), "{s:?}");
    }

    #[tokio::test]
    async fn fail_fast_true_keeps_earlier_skips_and_cancels_the_rest() {
        let (r, s) = run_jobs(
            serde_json::json!({
                "a": {},
                "sk": {"depends_on": ["a"], "when": "a.status != 'SUCCESS'"},
                "j": {"foreach": "$.items", "input": "{\"items\":[1,2,3]}",
                      "fail_fast": true, "depends_on": ["a"]},
                "agg": {"depends_on": ["j"], "input_from": "j"},
            }),
            true,
        )
        .await;
        assert!(!r.success);
        assert!(is_skipped(&s, "sk"), "{s:?}");
        assert!(is_cancelled(&s, "agg"), "{s:?}");
        assert!(none_pending(&s), "{s:?}");
    }

    #[tokio::test]
    async fn when_status_of_cancelled_or_unfinished_job_reads_as_unknown() {
        let st = HashMap::from([("x".to_string(), JobStatus::Cancelled)]);
        assert!(!eval_when("x.status == 'SUCCESS'", &st));
        assert!(eval_when("x.status == 'UNKNOWN'", &st));
        assert!(eval_when("missing.status == 'UNKNOWN'", &st));
    }

    // #392: failure handlers (`when: <job>.status == 'FAILED'`) are not supported.
    // `Workflow::validate` rejects them (core tests); the tests below pin the
    // runtime behaviour that makes them impossible, for a plain job and for a
    // foreach group with fail_fast true / false (execute() does not validate).

    #[tokio::test]
    async fn failed_status_handler_is_rejected_by_validate() {
        let wf: Workflow = serde_json::from_value(serde_json::json!({"name": "t", "jobs": {
            "a": {"component": "a.wasm"},
            "handler": {"component": "h.wasm", "depends_on": ["a"],
                        "when": "a.status == 'FAILED'"},
        }}))
        .unwrap();
        let report = wf.validate();
        assert!(!report.is_ok());
        assert!(
            report
                .to_string()
                .contains("failure handlers are not supported"),
            "{report}"
        );
    }

    #[tokio::test]
    async fn handler_of_a_failed_plain_job_is_cancelled() {
        let (r, s) = run_jobs(
            serde_json::json!({
                "a": {"worker": "FAIL"},
                "handler": {"depends_on": ["a"], "when": "a.status == 'FAILED'"},
            }),
            false,
        )
        .await;
        assert!(!r.success);
        assert!(is_cancelled(&s, "handler"), "{s:?}");
    }

    /// Exactly one child lands on the failing worker (round-robin [fail, ok, ok]);
    /// which one depends on the start order, so the tests must not assume j.0.
    async fn failed_child_handler_states(fail_fast: bool) -> HashMap<String, JobStatus> {
        let (r, s) = run_jobs(
            serde_json::json!({
                "j": {"foreach": "$.items", "input": "{\"items\":[1,2,3]}",
                      "fail_fast": fail_fast},
                "handler": {"depends_on": ["j"], "when": "j.0.status == 'FAILED'"},
            }),
            true,
        )
        .await;
        assert!(!r.success);
        s
    }

    fn count_children(s: &HashMap<String, JobStatus>, f: fn(&JobStatus) -> bool) -> usize {
        s.iter()
            .filter(|(id, st)| id.starts_with("j.") && f(st))
            .count()
    }

    #[tokio::test]
    async fn handler_of_a_failed_foreach_child_is_cancelled_with_fail_fast_false() {
        let s = failed_child_handler_states(false).await;
        assert_eq!(
            count_children(&s, |st| matches!(st, JobStatus::Failed { .. })),
            1,
            "{s:?}"
        );
        assert_eq!(
            count_children(&s, |st| matches!(st, JobStatus::Succeeded { .. })),
            2,
            "{s:?}"
        );
        assert!(is_cancelled(&s, "handler"), "{s:?}");
        assert!(none_pending(&s));
    }

    #[tokio::test]
    async fn handler_of_a_failed_foreach_child_is_cancelled_with_fail_fast_true() {
        let s = failed_child_handler_states(true).await;
        assert_eq!(
            count_children(&s, |st| matches!(st, JobStatus::Failed { .. })),
            1,
            "{s:?}"
        );
        assert!(is_cancelled(&s, "handler"), "{s:?}");
        assert!(none_pending(&s));
    }

    /// #393: `when:` on a foreach job is inherited by every child (static expansion).
    #[tokio::test]
    async fn foreach_job_with_false_when_is_skipped() {
        let (_r, s) = run_jobs(
            serde_json::json!({
                "a": {},
                "j": {"foreach": "$.items", "input": "{\"items\":[1,2]}",
                      "depends_on": ["a"], "when": "a.status != 'SUCCESS'"},
            }),
            false,
        )
        .await;
        assert!(is_skipped(&s, "j.0") && is_skipped(&s, "j.1"), "{s:?}");
        assert!(none_pending(&s), "{s:?}");
    }

    #[tokio::test]
    async fn foreach_job_with_true_when_runs_all_children() {
        let (_r, s) = run_jobs(
            serde_json::json!({
                "a": {},
                "j": {"foreach": "$.items", "input": "{\"items\":[1,2]}",
                      "depends_on": ["a"], "when": "a.status == 'SUCCESS'"},
            }),
            false,
        )
        .await;
        assert!(is_ok(&s, "j.0") && is_ok(&s, "j.1"), "{s:?}");
    }

    /// fetch -> dynamic foreach `j` (with `when`) -> fan-in `agg`; returns the
    /// stored job states.
    async fn run_dynamic_when(when: &str) -> HashMap<String, JobStatus> {
        let fetch_url = spawn_mock_worker(br#"{"items":[1,2]}"#.to_vec()).await;
        let ok_url = spawn_mock_worker(b"\"ok\"".to_vec()).await;
        let wasm = tmp_wasm();
        let p = wasm.path().to_string_lossy().into_owned();
        let wf: Workflow = serde_json::from_value(serde_json::json!({"name": "t", "jobs": {
            "fetch": {"component": p, "executor": "remote", "worker": fetch_url},
            "j": {"component": p, "executor": "remote", "worker": ok_url,
                  "foreach": "$.items", "depends_on": ["fetch"], "when": when},
            "agg": {"component": p, "executor": "remote", "worker": ok_url,
                    "depends_on": ["j"], "input_from": "j"}
        }}))
        .unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let store = RunStore::open_at(&tmp.path().join("runs.db")).unwrap();
        let (_cache, host) = test_host();
        store
            .create_run("test-run-393", "t", Path::new("t.yaml"))
            .unwrap();
        let opts = ExecOpts {
            host,
            store: &store,
            run_id: "test-run-393",
            pre_succeeded: HashMap::new(),
            print_progress: false,
            sem: Arc::new(Semaphore::new(4)),
            strategy: LbStrategy::RoundRobin,
            workers: vec![],
        };
        tokio::time::timeout(Duration::from_secs(10), execute(&wf, opts))
            .await
            .expect("execute must not hang")
            .expect("execute should return Ok");
        store.load_run("test-run-393").unwrap().1
    }

    /// #393: same behaviour for a dynamic foreach (children created at run time).
    #[tokio::test]
    async fn dynamic_foreach_with_false_when_skips_children_and_fan_in() {
        let s = run_dynamic_when("fetch.status != 'SUCCESS'").await;
        // The placeholder is skipped before expansion, so no child runs at all.
        assert!(is_skipped(&s, "j"), "{s:?}");
        assert!(!is_ok(&s, "j.0") && !is_ok(&s, "j.1"), "{s:?}");
        assert!(is_skipped(&s, "agg"), "{s:?}");
        assert!(none_pending(&s), "{s:?}");
    }

    #[tokio::test]
    async fn dynamic_foreach_with_true_when_runs_children_and_fan_in() {
        let s = run_dynamic_when("fetch.status == 'SUCCESS'").await;
        assert!(is_ok(&s, "j.0") && is_ok(&s, "j.1"), "{s:?}");
        assert!(is_ok(&s, "agg"), "{s:?}");
    }

    // ── #387: job outputs are saved by the scheduler ─────────────────────────

    fn pinned_job(wasm: &str, worker: &str, extra: serde_json::Value) -> serde_json::Value {
        let mut j = serde_json::json!({
            "component": wasm, "executor": "remote", "worker": worker
        });
        j.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        j
    }

    async fn run_saving(jobs: serde_json::Value, store: &RunStore) -> RunResult {
        let wf: Workflow =
            serde_json::from_value(serde_json::json!({"name": "t", "jobs": jobs})).unwrap();
        let opts = RunOptions {
            progress: false,
            ..Default::default()
        };
        let cache = tempfile::tempdir().unwrap();
        let host = Arc::new(FluxionHost::with_cache_dir(cache.path()).unwrap());
        run_with_store(&wf, Path::new("t.yaml"), host, &opts, store)
            .await
            .expect("run")
    }

    #[tokio::test]
    async fn successful_job_outputs_are_saved_in_the_store() {
        let url = spawn_mock_worker(b"\"ok\"".to_vec()).await;
        let wasm = tmp_wasm();
        let p = wasm.path().to_string_lossy().into_owned();
        let (_dir, store) = temp_store();
        let r = run_saving(
            serde_json::json!({
                "a": pinned_job(&p, &url, serde_json::json!({})),
                "b": pinned_job(&p, &url, serde_json::json!({"depends_on": ["a"]})),
            }),
            &store,
        )
        .await;
        assert!(r.success);
        let saved = store.load_outputs(&r.run_id).unwrap();
        assert_eq!(saved.len(), 2, "{:?}", saved.keys());
        assert_eq!(saved["a"], b"\"ok\"");
        assert_eq!(saved["b"], b"\"ok\"");
    }

    #[tokio::test]
    async fn oversized_output_is_not_saved_but_the_run_succeeds() {
        let big = spawn_mock_worker(vec![b'x'; MAX_OUTPUT_BYTES + 1]).await;
        let small = spawn_mock_worker(b"\"ok\"".to_vec()).await;
        let wasm = tmp_wasm();
        let p = wasm.path().to_string_lossy().into_owned();
        let (_dir, store) = temp_store();
        let r = run_saving(
            serde_json::json!({
                "big": pinned_job(&p, &big, serde_json::json!({})),
                "small": pinned_job(&p, &small, serde_json::json!({})),
            }),
            &store,
        )
        .await;
        assert!(r.success, "{:?}", r.jobs);
        let saved = store.load_outputs(&r.run_id).unwrap();
        assert!(
            !saved.contains_key("big"),
            "oversized output must not be kept"
        );
        assert!(saved.contains_key("small"));
        let (_, states) = store.load_run(&r.run_id).unwrap();
        assert!(matches!(
            states.get("big"),
            Some(JobStatus::Succeeded { .. })
        ));
    }

    #[tokio::test]
    async fn dynamic_foreach_source_and_children_outputs_are_saved() {
        let fetch = spawn_mock_worker(br#"{"items":[1,2,3]}"#.to_vec()).await;
        let ok = spawn_mock_worker(b"\"ok\"".to_vec()).await;
        let wasm = tmp_wasm();
        let p = wasm.path().to_string_lossy().into_owned();
        let (_dir, store) = temp_store();
        let r = run_saving(
            serde_json::json!({
                "fetch": pinned_job(&p, &fetch, serde_json::json!({})),
                "process": pinned_job(&p, &ok, serde_json::json!({
                    "foreach": "$.items", "depends_on": ["fetch"]})),
                "aggregate": pinned_job(&p, &ok, serde_json::json!({
                    "depends_on": ["process"], "input_from": "process"})),
            }),
            &store,
        )
        .await;
        assert!(r.success, "{:?}", r.jobs);
        let saved = store.load_outputs(&r.run_id).unwrap();
        let mut ids: Vec<&str> = saved.keys().map(String::as_str).collect();
        ids.sort();
        assert_eq!(
            ids,
            ["aggregate", "fetch", "process.0", "process.1", "process.2"]
        );
        assert_eq!(saved["fetch"], br#"{"items":[1,2,3]}"#);
    }

    #[tokio::test]
    async fn a_failing_output_save_does_not_fail_the_run() {
        // A store whose DB has no `job_outputs` table: every save_output errors.
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE runs (id TEXT PRIMARY KEY, workflow_name TEXT NOT NULL,
                workflow_path TEXT NOT NULL, started_at INTEGER NOT NULL,
                completed_at INTEGER, status TEXT NOT NULL);
             CREATE TABLE job_states (run_id TEXT NOT NULL, job_id TEXT NOT NULL,
                status TEXT NOT NULL, elapsed_ms INTEGER, reason TEXT,
                PRIMARY KEY (run_id, job_id));
             CREATE TABLE workers (url TEXT PRIMARY KEY, registered_at INTEGER NOT NULL,
                last_health TEXT);",
        )
        .unwrap();
        let store = RunStore::from_conn(conn);
        assert!(store.save_output("r", "j", b"x").is_err(), "precondition");
        let url = spawn_mock_worker(b"\"ok\"".to_vec()).await;
        let wasm = tmp_wasm();
        let p = wasm.path().to_string_lossy().into_owned();
        let r = run_saving(
            serde_json::json!({"a": pinned_job(&p, &url, serde_json::json!({}))}),
            &store,
        )
        .await;
        assert!(r.success, "{:?}", r.jobs);
        let (_, states) = store.load_run(&r.run_id).unwrap();
        assert!(matches!(states.get("a"), Some(JobStatus::Succeeded { .. })));
    }

    // ── #292: store injection (no env access; tempdir + explicit store) ──────

    fn temp_store() -> (tempfile::TempDir, RunStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = RunStore::open_at(&dir.path().join("runs.db")).unwrap();
        (dir, store)
    }

    fn one_remote_job_wf(worker_url: &str, component: &str) -> Workflow {
        serde_json::from_value(serde_json::json!({"name": "t", "jobs": {
            "a": {"component": component, "executor": "remote", "worker": worker_url}
        }}))
        .unwrap()
    }

    #[tokio::test]
    async fn run_with_store_records_the_run_in_the_injected_store() {
        let url = spawn_mock_worker(b"\"ok\"".to_vec()).await;
        let wasm = tmp_wasm();
        let wf = one_remote_job_wf(&url, &wasm.path().to_string_lossy());
        let (_dir, store) = temp_store();
        let opts = RunOptions {
            progress: false,
            ..Default::default()
        };
        let (_cache, host) = test_host();
        let r = run_with_store(&wf, Path::new("t.yaml"), host, &opts, &store)
            .await
            .expect("run");
        assert!(r.success, "{:?}", r.jobs);
        let (_, states) = store
            .load_run(&r.run_id)
            .expect("run is in the injected store");
        assert!(
            matches!(states.get("a"), Some(JobStatus::Succeeded { .. })),
            "{states:?}"
        );
    }

    #[tokio::test]
    async fn retry_with_store_reads_and_writes_the_injected_store() {
        let url = spawn_mock_worker(b"\"ok\"".to_vec()).await;
        let wasm = tmp_wasm();
        let wf = one_remote_job_wf(&url, &wasm.path().to_string_lossy());
        let (_dir, store) = temp_store();
        let opts = RunOptions {
            progress: false,
            ..Default::default()
        };
        let (_cache, host) = test_host();
        let first = run_with_store(&wf, Path::new("t.yaml"), host.clone(), &opts, &store)
            .await
            .expect("first run");
        let retried = retry_with_store(
            &wf,
            Path::new("t.yaml"),
            host,
            &first.run_id,
            "a",
            &opts,
            &store,
        )
        .await
        .expect("retry");
        assert!(retried.success);
        assert_ne!(retried.run_id, first.run_id);
        assert!(store.load_run(&retried.run_id).is_ok());
    }

    #[tokio::test]
    async fn run_and_retry_refuse_workers_srv_with_tls() {
        let (_dir, store) = temp_store();
        let (_cache, host) = test_host();
        let wf: Workflow = serde_json::from_value(serde_json::json!({
            "name": "t",
            "jobs": {"a": {"component": "x.wasm"}},
            "workers_srv": "_f._tcp.example",
            "workers": [{"url": "https://w:1", "tls": {"cert": "c", "key": "k", "ca": "a"}}]
        }))
        .unwrap();
        let e = run_inner(
            &wf,
            Path::new("t.yaml"),
            Arc::clone(&host),
            HashMap::new(),
            false,
            LbStrategy::RoundRobin,
            &store,
        )
        .await
        .unwrap_err();
        assert!(e.to_string().contains("SRV and mTLS"), "{e}");
        let e = retry_inner(
            &wf,
            Path::new("t.yaml"),
            host,
            "no-such-run",
            "a",
            &RunOptions::default(),
            &store,
        )
        .await
        .unwrap_err();
        assert!(e.to_string().contains("SRV and mTLS"), "{e}");
    }

    #[tokio::test]
    async fn effective_workers_falls_back_to_the_injected_stores_registered_workers() {
        let url = spawn_mock_worker(b"x".to_vec()).await;
        let (_dir, store) = temp_store();
        store.register_worker(&url).unwrap();
        // no static `workers:` and no SRV -> DB fallback must use *this* store
        let wf: Workflow =
            serde_json::from_str(r#"{"name":"t","jobs":{"a":{"component":"x.wasm"}}}"#).unwrap();
        assert_eq!(effective_workers(&wf, &store).await, vec![url.clone()]);
        let health = store.list_workers().unwrap();
        assert_eq!(health[0].last_health.as_deref(), Some("healthy"));
    }

    #[tokio::test]
    async fn health_check_records_unreachable_workers_in_the_injected_store() {
        // port 1 is closed -> probe fails -> recorded as unreachable
        let dead = "http://127.0.0.1:1".to_string();
        let (_dir, store) = temp_store();
        store.register_worker(&dead).unwrap();
        assert!(
            health_check_workers(std::slice::from_ref(&dead), &store)
                .await
                .is_empty()
        );
        let w = store.list_workers().unwrap();
        assert_eq!(w[0].last_health.as_deref(), Some("unreachable"));
    }

    /// Counts POST requests (job executions) per worker; `/health` GETs are
    /// answered but not counted.
    async fn spawn_counting_mock_worker(hits: Arc<AtomicUsize>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                let hits = Arc::clone(&hits);
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut tmp = [0u8; 8192];
                    let mut need = None;
                    while let Ok(n) = sock.read(&mut tmp).await {
                        if n == 0 {
                            break;
                        }
                        buf.extend_from_slice(&tmp[..n]);
                        if need.is_none()
                            && let Some(p) = find(&buf, b"\r\n\r\n")
                        {
                            let head = String::from_utf8_lossy(&buf[..p]).to_lowercase();
                            let cl = head
                                .lines()
                                .find_map(|l| l.strip_prefix("content-length:"))
                                .and_then(|v| v.trim().parse::<usize>().ok())
                                .unwrap_or(0);
                            need = Some(p + 4 + cl);
                        }
                        if need.is_some_and(|n| buf.len() >= n) {
                            break;
                        }
                    }
                    if buf.starts_with(b"POST") {
                        hits.fetch_add(1, Ordering::SeqCst);
                    }
                    let body =
                        "{\"output\":\"\",\"compile_ms\":0,\"instantiate_ms\":0,\"execute_ms\":0}";
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

    /// #317: `retry_with_options` must hand `opts.strategy` to `retry_inner`.
    /// Workers weighted [2, 1] and two sequential jobs: Weighted sends both
    /// jobs to worker A (slots 0 and 1), RoundRobin would send the second to B.
    #[tokio::test]
    async fn retry_with_options_propagates_weighted_strategy() {
        let hits_a = Arc::new(AtomicUsize::new(0));
        let hits_b = Arc::new(AtomicUsize::new(0));
        let url_a = spawn_counting_mock_worker(Arc::clone(&hits_a)).await;
        let url_b = spawn_counting_mock_worker(Arc::clone(&hits_b)).await;
        let wasm = tmp_wasm();
        let p = wasm.path().to_string_lossy();
        let wf: Workflow = serde_json::from_str(&format!(
            r#"{{"name":"t","workers":[{{"url":"{url_a}","weight":2}},{{"url":"{url_b}","weight":1}}],
                "jobs":{{
                    "a":{{"component":"{p}","executor":"remote"}},
                    "b":{{"component":"{p}","executor":"remote","depends_on":["a"]}}
                }}}}"#
        ))
        .unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let store = RunStore::open_at(&tmp.path().join("runs.db")).unwrap();
        store
            .create_run("prev-317", "t", Path::new("t.yaml"))
            .unwrap();
        let (_cache, host) = test_host();

        let opts = RunOptions {
            strategy: LbStrategy::Weighted,
            progress: false,
        };
        let result = tokio::time::timeout(
            Duration::from_secs(20),
            retry_with_store(
                &wf,
                Path::new("t.yaml"),
                host,
                "prev-317",
                "a",
                &opts,
                &store,
            ),
        )
        .await
        .expect("retry must not hang")
        .expect("retry should succeed");
        assert!(result.success);
        assert_eq!(hits_a.load(Ordering::SeqCst), 2);
        assert_eq!(hits_b.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn run_options_default_is_round_robin_with_progress() {
        let d = RunOptions::default();
        assert!(matches!(d.strategy, LbStrategy::RoundRobin));
        assert!(d.progress);
    }

    // ── downstream_inclusive ────────────────────────────────────────────────

    fn dag_from(edges: &[(&str, &[&str])]) -> Dag {
        // edges: (job_id, deps)
        let jobs_json: HashMap<&str, serde_json::Value> = edges
            .iter()
            .map(|(id, deps)| {
                (
                    *id,
                    serde_json::json!({"component": "x.wasm", "depends_on": deps}),
                )
            })
            .collect();
        let s = serde_json::json!({"name": "t", "jobs": jobs_json}).to_string();
        let wf: Workflow = serde_json::from_str(&s).unwrap();
        Dag::build(&wf).unwrap()
    }

    #[test]
    fn downstream_inclusive_includes_start_and_all_descendants() {
        // a -> b -> c, a -> d
        let dag = dag_from(&[("a", &[]), ("b", &["a"]), ("c", &["b"]), ("d", &["a"])]);
        let result = downstream_inclusive(&dag, "a");
        assert_eq!(
            result,
            std::collections::HashSet::from(["a", "b", "c", "d"])
        );
    }

    #[test]
    fn downstream_inclusive_leaf_returns_only_itself() {
        let dag = dag_from(&[("a", &[]), ("b", &["a"])]);
        let result = downstream_inclusive(&dag, "b");
        assert_eq!(result, std::collections::HashSet::from(["b"]));
    }

    // ── eval_when ────────────────────────────────────────────────────────────

    #[test]
    fn eval_when_malformed_expr_defaults_true() {
        let statuses = HashMap::new();
        assert!(eval_when("not-a-valid-expr", &statuses));
    }

    #[test]
    fn eval_when_matches_success_status() {
        let mut statuses = HashMap::new();
        statuses.insert(
            "validate".to_string(),
            JobStatus::Succeeded {
                elapsed: std::time::Duration::from_millis(1),
            },
        );
        assert!(eval_when("validate.status == 'SUCCESS'", &statuses));
        assert!(!eval_when("validate.status != 'SUCCESS'", &statuses));
        assert!(!eval_when("validate.status == 'FAILED'", &statuses));
    }

    #[test]
    fn eval_when_matches_failed_status() {
        let mut statuses = HashMap::new();
        statuses.insert(
            "build".to_string(),
            JobStatus::Failed {
                elapsed: std::time::Duration::from_millis(1),
                reason: "boom".to_string(),
            },
        );
        assert!(eval_when("build.status == 'FAILED'", &statuses));
    }

    #[test]
    fn eval_when_unknown_job_defaults_unknown() {
        let statuses: HashMap<String, JobStatus> = HashMap::new();
        assert!(!eval_when("missing.status == 'SUCCESS'", &statuses));
        assert!(eval_when("missing.status == 'UNKNOWN'", &statuses));
    }

    #[test]
    fn eval_when_non_status_attr_defaults_true() {
        let statuses = HashMap::new();
        assert!(eval_when("job.other == 'x'", &statuses));
    }

    // ── probe_workers (health probe; replaces the removed worker_registry) ──

    async fn spawn_status_server(status_line: &'static str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                let mut buf = [0u8; 1024];
                let _ = sock.read(&mut buf).await;
                let resp = format!("HTTP/1.1 {status_line}\r\ncontent-length: 0\r\n\r\n");
                let _ = sock.write_all(resp.as_bytes()).await;
            }
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn probe_workers_marks_2xx_healthy_and_5xx_or_closed_unhealthy() {
        let ok = spawn_status_server("200 OK").await;
        let err5xx = spawn_status_server("503 Service Unavailable").await;
        let closed = {
            let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = l.local_addr().unwrap();
            drop(l);
            format!("http://{addr}")
        };
        let results = probe_workers(&[ok.clone(), err5xx.clone(), closed.clone()]).await;
        assert_eq!(results, vec![(ok, true), (err5xx, false), (closed, false)]);
    }
}

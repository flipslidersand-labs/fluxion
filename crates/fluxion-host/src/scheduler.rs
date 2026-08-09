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
    workflow::{PermissionSet, TlsConfig, Workflow},
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
    let result = execute(
        wf,
        host,
        &store,
        &run_id,
        pre_succeeded,
        print_progress,
        sem,
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
    let result = execute(
        wf,
        host,
        &store,
        &run_id,
        pre_succeeded,
        print_progress,
        sem,
    )
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
        let workers = resolve_workers(&job_id, wf, &rr);
        launch(&job_id, wf, host.clone(), tx.clone(), workers, sem.clone());
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
            let workers = resolve_workers(job_id, wf, &rr);
            launch(job_id, wf, host.clone(), tx.clone(), workers, sem.clone());
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
                job_results.push(JobResult::from_succeeded_with_metrics(
                    event.job_id.clone(),
                    *elapsed,
                    event.compile_us,
                    event.instantiate_us,
                    event.execute_us,
                ));
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
                    let workers = resolve_workers(dep, wf, &rr);
                    launch(dep, wf, host.clone(), tx.clone(), workers, sem.clone());
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

/// A resolved worker target: URL plus optional mTLS config.
#[derive(Clone)]
struct WorkerInfo {
    url: String,
    tls: Option<TlsConfig>,
}

/// Resolve the ordered list of workers to try for a job, including failover targets.
/// - `job.worker` set → `[that url, no TLS]` (pinned; no failover — respect the explicit choice)
/// - `wf.workers` non-empty → round-robin start, then the remaining workers as failover targets
/// - otherwise → empty (run locally)
fn resolve_workers(job_id: &str, wf: &Workflow, rr: &AtomicUsize) -> Vec<WorkerInfo> {
    if let Some(url) = &wf.jobs[job_id].worker {
        return vec![WorkerInfo {
            url: url.clone(),
            tls: None,
        }];
    }
    if wf.workers.is_empty() {
        return Vec::new();
    }
    let n = wf.workers.len();
    let start = rr.fetch_add(1, Ordering::Relaxed) % n;
    (0..n)
        .map(|i| {
            let cfg = &wf.workers[(start + i) % n];
            WorkerInfo {
                url: cfg.url().to_string(),
                tls: cfg.tls().cloned(),
            }
        })
        .collect()
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
                crate::metrics::WORKER_HEALTH.with_label_values(&[&worker.url]).set(1.0);
                return Ok(r);
            }
            Err(e) => {
                tried.push(format!("{}: {e}", worker.url));
                if e.is_failover() && !last {
                    crate::metrics::WORKER_HEALTH.with_label_values(&[&worker.url]).set(0.0);
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

fn launch(
    job_id: &str,
    wf: &Workflow,
    host: Arc<FluxionHost>,
    tx: mpsc::UnboundedSender<JobEvent>,
    workers: Vec<WorkerInfo>,
    sem: Arc<Semaphore>,
) {
    crate::metrics::ACTIVE_JOBS.inc();
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

            let run_result: anyhow::Result<(Vec<u8>, crate::JobMetrics)> = if workers.is_empty() {
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
            } else {
                run_with_failover(&workers, &component, &input, &perms, &env).await
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
                    JobStatus::Failed {
                        elapsed,
                        reason: e.to_string(),
                    },
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

    #[test]
    fn pinned_worker_has_no_failover_targets() {
        // An explicit `worker:` must yield exactly that URL — never fail over.
        let w = wf(&["http://a", "http://b"], Some("http://pinned"));
        let rr = AtomicUsize::new(0);
        assert_eq!(worker_urls(&resolve_workers("j", &w, &rr)), vec!["http://pinned"]);
    }

    #[test]
    fn no_workers_runs_locally() {
        let w = wf(&[], None);
        let rr = AtomicUsize::new(0);
        assert!(resolve_workers("j", &w, &rr).is_empty());
    }

    #[test]
    fn round_robin_lists_all_workers_as_failover_targets() {
        let w = wf(&["http://a", "http://b", "http://c"], None);
        let rr = AtomicUsize::new(0);
        assert_eq!(
            worker_urls(&resolve_workers("j", &w, &rr)),
            vec!["http://a", "http://b", "http://c"]
        );
        // The next job rotates the start but still lists every worker as a target.
        assert_eq!(
            worker_urls(&resolve_workers("j", &w, &rr)),
            vec!["http://b", "http://c", "http://a"]
        );
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
            .map(|u| WorkerInfo { url: u.clone(), tls: None })
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
}

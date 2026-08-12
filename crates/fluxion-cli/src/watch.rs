use anyhow::Result;
use fluxion_core::runner::JobResult;
use fluxion_core::workflow::Workflow;
use fluxion_host::scheduler;
use notify::Watcher;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Watch a workflow YAML file for changes and re-run on save.
/// Debounces re-execution to avoid multiple triggers from rapid file changes.
pub async fn watch_and_run(path: PathBuf, debounce_ms: u64) -> Result<()> {
    // Validate the file exists and is parseable before entering the watch loop
    if !path.exists() {
        anyhow::bail!("{}: No such file or directory", path.display());
    }
    Workflow::from_file(&path)?;

    let path_display = path.display().to_string();
    println!(
        "[watch] Monitoring {} (debounce {}ms)",
        path_display, debounce_ms
    );
    println!("[watch] Press Ctrl+C to stop");

    // Create host once for the loop
    let host = Arc::new(fluxion_host::FluxionHost::new()?);

    // Flag to signal shutdown
    let shutdown = Arc::new(Mutex::new(false));
    let shutdown_clone = Arc::clone(&shutdown);

    // Channel to receive file change events
    let (tx, mut rx) = tokio::sync::mpsc::channel(100);

    // Spawn blocking task to run the file watcher (notify is !Send)
    let watch_path = path.clone();
    tokio::task::spawn_blocking(move || {
        if let Err(e) = run_watcher(watch_path, tx, shutdown_clone) {
            eprintln!("[watch] Watcher error: {}", e);
        }
    });

    // Main loop: wait for debounced file changes, then re-run
    let mut last_change: Option<Instant> = None;
    let mut prev_result: Option<Vec<JobResult>> = None;

    loop {
        tokio::select! {
            // File change event received
            Some(_) = rx.recv() => {
                last_change = Some(Instant::now());
            }
            // Debounce timer fires: if we haven't seen a change for debounce_ms, trigger re-run
            _ = tokio::time::sleep(std::time::Duration::from_millis(debounce_ms)), if last_change.is_some() => {
                if let Some(last) = last_change {
                    if last.elapsed() >= std::time::Duration::from_millis(debounce_ms) {
                        println!("[{}] File changed, running workflow...", fmt_time());

                        // Parse workflow
                        match Workflow::from_file(&path) {
                            Ok(wf) => {
                                // Run workflow
                                match tokio::task::block_in_place(|| {
                                    tokio::runtime::Handle::current().block_on(async {
                                        let wf_path = PathBuf::from(&path)
                                            .canonicalize()
                                            .unwrap_or(PathBuf::from(&path));
                                        scheduler::run_with_strategy(&wf, &wf_path, Arc::clone(&host), scheduler::LbStrategy::RoundRobin).await
                                    })
                                }) {
                                    Ok(result) => {
                                        print_result(&result.jobs, &prev_result);
                                        prev_result = Some(result.jobs);
                                        println!("[{}] Run complete", fmt_time());
                                    }
                                    Err(e) => {
                                        eprintln!("[{}] Run failed: {}", fmt_time(), e);
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("[{}] Failed to parse workflow: {}", fmt_time(), e);
                            }
                        }
                        last_change = None;
                    }
                }
            }
            // Ctrl+C or SIGTERM signal
            _ = tokio::signal::ctrl_c() => {
                println!("\n[watch] Received SIGINT, shutting down");
                {
                    let mut should_shutdown = shutdown.lock().unwrap();
                    *should_shutdown = true;
                }
                break;
            }
        }
    }

    Ok(())
}

/// Runs the file watcher in a blocking context.
/// Watches the target YAML file and all files it depends on (future enhancement).
fn run_watcher(
    path: std::path::PathBuf,
    tx: tokio::sync::mpsc::Sender<()>,
    shutdown: Arc<Mutex<bool>>,
) -> Result<()> {
    use notify::RecursiveMode;

    let path = Arc::new(path);
    let (watcher_tx, watcher_rx) = std::sync::mpsc::channel();
    let path_clone = Arc::clone(&path);
    let mut watcher = notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
        match res {
            Ok(event) => {
                // Only care about modify/create events on our target file
                if matches!(
                    event.kind,
                    notify::EventKind::Modify(_)
                        | notify::EventKind::Create(_)
                        | notify::EventKind::Access(_)
                ) {
                    for path_buf in &event.paths {
                        if path_buf.ends_with(path_clone.file_name().unwrap_or_default()) {
                            let _ = watcher_tx.send(());
                            break;
                        }
                    }
                }
            }
            Err(e) => eprintln!("[watch] Error: {}", e),
        }
    })?;

    // Watch the directory containing the file (notify doesn't watch files directly)
    let watch_dir = path.parent().unwrap_or_else(|| std::path::Path::new("."));
    watcher.watch(watch_dir, RecursiveMode::NonRecursive)?;

    // Wait for watcher events or shutdown signal
    loop {
        if let Ok(true) = shutdown.lock().map(|s| *s) {
            break;
        }

        // Try to receive an event with a timeout to allow checking shutdown
        match watcher_rx.recv_timeout(std::time::Duration::from_millis(100)) {
            Ok(()) => {
                // Forward to async channel (best-effort; ignore send errors)
                let _ = tx.blocking_send(());
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                // Continue checking shutdown flag
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                break;
            }
        }
    }

    Ok(())
}

/// Format current time as HH:MM:SS
fn fmt_time() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let h = (now / 3600) % 24;
    let m = (now / 60) % 60;
    let s = now % 60;
    format!("{:02}:{:02}:{:02}", h, m, s)
}

/// Print job results with color highlighting.
/// Compare against previous results: success→failure is red, failure→success is green.
fn print_result(current: &[JobResult], prev: &Option<Vec<JobResult>>) {
    const GREEN: &str = "\x1b[32m";
    const RED: &str = "\x1b[31m";
    const RESET: &str = "\x1b[0m";

    let prev_map = prev
        .as_ref()
        .map(|jobs| {
            jobs.iter()
                .map(|j| (j.job_id.as_str(), j.status.as_str()))
                .collect::<std::collections::HashMap<_, _>>()
        })
        .unwrap_or_default();

    let pad = current.iter().map(|j| j.job_id.len()).max().unwrap_or(0);
    println!();
    println!("  {:<pad$}  STATUS", "JOB", pad = pad);
    println!("  {}", "-".repeat(pad + 20));

    for job in current {
        let status = job.status.as_str();
        let prev_status = prev_map.get(job.job_id.as_str()).copied();

        // Color if status changed
        let colored = if let Some(prev_s) = prev_status {
            if prev_s != status {
                if status == "SUCCESS" {
                    format!("{}{}{}", GREEN, status.to_uppercase(), RESET)
                } else if status == "FAILED" {
                    format!("{}{}{}", RED, status.to_uppercase(), RESET)
                } else {
                    status.to_uppercase()
                }
            } else {
                status.to_uppercase()
            }
        } else {
            status.to_uppercase()
        };

        println!("  {:<pad$}  {}", job.job_id, colored, pad = pad);
    }
    println!();
}

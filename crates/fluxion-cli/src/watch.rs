use anyhow::Result;
use notify::Watcher;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Watch a workflow YAML file for changes and re-run on save.
/// Debounces re-execution to avoid multiple triggers from rapid file changes.
pub async fn watch_and_run(path: PathBuf, debounce_ms: u64) -> Result<()> {
    let path_display = path.display().to_string();
    println!(
        "[watch] Monitoring {} (debounce {}ms)",
        path_display, debounce_ms
    );
    println!("[watch] Press Ctrl+C to stop");

    // Flag to signal shutdown
    let shutdown = Arc::new(Mutex::new(false));
    let shutdown_clone = Arc::clone(&shutdown);

    // Channel to receive file change events
    let (tx, mut rx) = tokio::sync::mpsc::channel(100);

    // Spawn blocking task to run the file watcher (notify is !Send)
    let watch_path = path.clone();
    tokio::task::spawn_blocking(move || {
        if let Err(e) = run_watcher(&watch_path, tx, shutdown_clone) {
            eprintln!("[watch] Watcher error: {}", e);
        }
    });

    // Main loop: wait for debounced file changes, then re-run
    let mut last_change: Option<Instant> = None;

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
                        println!("[watch] File changed, re-running...");
                        // TODO: call scheduler::run_with_strategy here (phase 3 — #99)
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
    path: &std::path::Path,
    tx: tokio::sync::mpsc::Sender<()>,
    shutdown: Arc<Mutex<bool>>,
) -> Result<()> {
    use notify::RecursiveMode;

    let (watcher_tx, watcher_rx) = std::sync::mpsc::channel();
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
                        if path_buf.ends_with(path.file_name().unwrap_or_default()) {
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

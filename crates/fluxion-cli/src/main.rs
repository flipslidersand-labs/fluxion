mod mcp;
mod telemetry;
mod watch;

use anyhow::Result;
use chrono::Utc;
use clap::{Parser, Subcommand};
use fluxion_core::{
    dag::Dag,
    store::RunStore,
    workflow::{PermissionSet, ReduceMode, Workflow},
};
use fluxion_host::{FluxionHost, scheduler, scheduler::LbStrategy};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Parser)]
#[command(name = "fluxion", about = "Safe Wasm-based job execution engine")]
struct Cli {
    /// Emit OpenTelemetry spans to stdout
    #[arg(long, global = true)]
    trace: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run a YAML workflow (DAG of Wasm components)
    Run {
        /// Path to the workflow YAML file
        path: String,
        /// Print per-job compile/instantiate/execute breakdown after the run
        #[arg(long)]
        metrics: bool,
        /// Parse the workflow and show execution order without running anything
        #[arg(long)]
        dry_run: bool,
        /// Expose Prometheus /metrics on this port while the workflow runs
        #[arg(long)]
        metrics_port: Option<u16>,
        /// Load-balancing strategy for distributing jobs across remote workers
        #[arg(long, value_enum, default_value_t = LbStrategy::RoundRobin)]
        lb_strategy: LbStrategy,
        /// Default reduce strategy for fan-in jobs without an explicit `reduce:` in YAML
        #[arg(long, value_enum, default_value_t = CliReduceMode::JsonArray)]
        reduce_mode: CliReduceMode,
    },
    /// Retry a previous run from a specific job
    Retry {
        /// Run ID from the previous execution
        run_id: String,
        /// Re-execute this job and all its downstream dependents
        #[arg(long)]
        from: String,
    },
    /// Show detailed status of a previous run
    Status { run_id: String },
    /// Show job timeline and failure reasons for a previous run
    Logs {
        run_id: String,
        /// Output structured JSON instead of human-readable text
        #[arg(long)]
        json: bool,
    },
    /// Show interface and capability requirements of a Wasm component
    Inspect {
        /// Path to the .wasm component file
        path: String,
    },
    /// Execute a single Wasm component
    Component {
        #[command(subcommand)]
        action: ComponentCommands,
    },
    /// Manage run history
    Runs {
        #[command(subcommand)]
        action: RunsCommands,
    },
    /// Benchmark a Wasm component — compile / instantiate / execute breakdown
    Bench {
        /// Path to the .wasm component file
        path: String,
        /// Number of timed runs
        #[arg(long, default_value = "50")]
        runs: usize,
        /// Number of warmup runs (excluded from stats)
        #[arg(long, default_value = "3")]
        warmup: usize,
        /// Input payload passed to the component
        #[arg(long, default_value = "")]
        input: String,
    },
    /// Emit a Graphviz DOT graph of a workflow's job dependency DAG
    Dot {
        /// Path to the workflow YAML file
        path: String,
    },
    /// Start a remote worker HTTP server
    Worker {
        #[command(subcommand)]
        action: WorkerCommands,
    },
    /// Watch a YAML workflow file and re-run on every save
    Watch {
        /// Path to the workflow YAML file
        path: String,
        /// Debounce delay in milliseconds before re-running after a change
        #[arg(long, default_value = "500")]
        debounce: u64,
    },
    /// Validate a YAML workflow without executing it
    Validate {
        /// Path to the workflow YAML file
        path: String,
        /// Output structured JSON instead of human-readable text
        #[arg(long)]
        json: bool,
        /// Skip checking whether component .wasm files exist on disk
        #[arg(long)]
        skip_wasm_check: bool,
    },
    /// Start the MCP server (stdio transport)
    McpServe,
    /// Manage workflow schedules (cron-based recurring execution)
    Schedule {
        #[command(subcommand)]
        action: ScheduleCommands,
    },
}

#[derive(Subcommand)]
enum ScheduleCommands {
    /// Register a workflow to run on a cron schedule
    Add {
        /// Path to the workflow YAML file
        path: String,
        /// Cron expression (6 fields: sec min hour day month weekday), e.g. "0 0 * * * *" (every hour)
        #[arg(long)]
        cron: String,
    },
    /// List all registered schedules with their next run time
    List,
    /// Remove a schedule by its ID
    Remove {
        /// Schedule ID (from `fluxion schedule list`)
        id: String,
    },
    /// Run the schedule daemon — fires due workflows in a loop
    Daemon,
}

#[derive(Subcommand)]
enum WorkerCommands {
    /// Listen for remote job dispatch requests
    Serve {
        /// Port to listen on
        #[arg(long, default_value = "7777")]
        port: u16,
        /// Expose Prometheus /metrics on this port
        #[arg(long)]
        metrics_port: Option<u16>,
        /// Path to the PEM-encoded server TLS certificate (enables TLS/mTLS)
        #[arg(long, requires = "tls_key", requires = "ca_cert")]
        tls_cert: Option<PathBuf>,
        /// Path to the PEM-encoded server TLS private key
        #[arg(long, requires = "tls_cert", requires = "ca_cert")]
        tls_key: Option<PathBuf>,
        /// Path to the PEM-encoded CA certificate used to verify clients (mTLS)
        #[arg(long, requires = "tls_cert", requires = "tls_key")]
        ca_cert: Option<PathBuf>,
    },
    /// Register a worker URL in the local registry
    Register {
        /// Worker URL (e.g. http://host:7777)
        url: String,
    },
    /// Remove a worker URL from the local registry
    Remove {
        /// Worker URL to deregister
        url: String,
    },
    /// List registered workers and their health status
    List,
}

#[derive(Subcommand)]
enum ComponentCommands {
    /// Run a Wasm component with optional input
    Run {
        /// Path to the .wasm component file
        path: String,
        /// Input string passed to the component (mutually exclusive with --input-file)
        #[arg(long, conflicts_with = "input_file")]
        input: Option<String>,
        /// Read input from a file (mutually exclusive with --input)
        #[arg(long, conflicts_with = "input")]
        input_file: Option<PathBuf>,
    },
}

/// Reduce strategy for fan-in jobs (`--reduce-mode` CLI flag).
#[derive(Clone, Copy, Debug, Default, PartialEq, clap::ValueEnum)]
enum CliReduceMode {
    /// Collect each child output as an element in a JSON array (default).
    #[default]
    JsonArray,
    /// Concatenate text outputs as a single JSON string.
    Concat,
    /// Deep-merge JSON object outputs by key (last writer wins).
    JsonMerge,
}

impl From<CliReduceMode> for ReduceMode {
    fn from(m: CliReduceMode) -> Self {
        match m {
            CliReduceMode::JsonArray => ReduceMode::JsonArray,
            CliReduceMode::Concat => ReduceMode::Concat,
            CliReduceMode::JsonMerge => ReduceMode::JsonMerge,
        }
    }
}

/// Set `reduce` on all fan-in jobs that don't already have an explicit mode.
fn apply_default_reduce_mode(mut wf: Workflow, default: CliReduceMode) -> Workflow {
    let mode: ReduceMode = default.into();
    for def in wf.jobs.values_mut() {
        if def.input_from.is_some() && def.reduce.is_none() {
            def.reduce = Some(mode.clone());
        }
    }
    wf
}

#[derive(Subcommand)]
enum RunsCommands {
    /// List recent runs
    List {
        #[arg(long, default_value = "20")]
        limit: usize,
    },
    /// Delete runs older than N days
    Prune {
        /// Delete runs older than this many days (default: 30)
        #[arg(long, default_value = "30")]
        before: u64,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // MCP server must not init tracing (stdout is used for the protocol)
    let provider = if matches!(cli.command, Commands::McpServe) {
        None
    } else {
        telemetry::init(cli.trace)
    };

    let result = run(cli.command).await;

    telemetry::shutdown(provider);
    result
}

async fn run(command: Commands) -> Result<()> {
    match command {
        Commands::Run {
            path,
            metrics,
            dry_run,
            metrics_port,
            lb_strategy,
            reduce_mode,
        } => {
            let wf = Workflow::from_file(&path)
                .map_err(|e| anyhow::anyhow!("Failed to load '{}': {}", path, e))?;
            let wf = apply_default_reduce_mode(wf, reduce_mode);
            if dry_run {
                cmd_dry_run(&wf)?;
                return Ok(());
            }
            if let Some(port) = metrics_port {
                tokio::spawn(fluxion_host::metrics::serve(port));
            }
            let workflow_path = PathBuf::from(&path)
                .canonicalize()
                .unwrap_or(PathBuf::from(&path));
            let host = Arc::new(FluxionHost::new()?);
            let result =
                scheduler::run_with_strategy(&wf, &workflow_path, host, lb_strategy).await?;
            if metrics {
                print_metrics_table(&result.jobs);
            }
        }

        Commands::Retry { run_id, from } => {
            let store = RunStore::open()?;
            let (workflow_path, _) = store.load_run(&run_id)?;
            let wf = Workflow::from_file(&workflow_path).map_err(|e| {
                anyhow::anyhow!("Failed to load workflow '{}': {}", workflow_path, e)
            })?;
            let wp = PathBuf::from(&workflow_path);
            let host = Arc::new(FluxionHost::new()?);
            scheduler::retry(&wf, &wp, host, &run_id, &from).await?;
        }

        Commands::Status { run_id } => {
            cmd_status(&run_id)?;
        }

        Commands::Logs { run_id, json } => {
            cmd_logs(&run_id, json)?;
        }

        Commands::Inspect { path } => {
            cmd_inspect(&path)?;
        }

        Commands::Component { action } => match action {
            ComponentCommands::Run {
                path,
                input,
                input_file,
            } => {
                let input_bytes = resolve_input(input, input_file)?;
                let host = FluxionHost::new()?;
                let output = host
                    .run_component(&path, input_bytes, &PermissionSet::default())
                    .map_err(|e| anyhow::anyhow!("Failed to run '{}': {}", path, e))?;
                println!("{}", String::from_utf8_lossy(&output));
            }
        },

        Commands::Runs { action } => match action {
            RunsCommands::List { limit } => {
                let store = RunStore::open()?;
                let runs = store.list_runs(limit)?;
                if runs.is_empty() {
                    println!("No runs found.");
                } else {
                    println!("{:<28}  {:<20}  STATUS", "RUN ID", "WORKFLOW");
                    println!("{}", "-".repeat(60));
                    for r in runs {
                        println!("{:<28}  {:<20}  {}", r.id, r.workflow_name, r.status);
                    }
                }
            }
            RunsCommands::Prune { before } => {
                let store = RunStore::open()?;
                let deleted = store.prune(before)?;
                println!("Pruned {} run(s) older than {} days.", deleted, before);
            }
        },

        Commands::Bench {
            path,
            runs,
            warmup,
            input,
        } => {
            cmd_bench(&path, runs, warmup, &input)?;
        }

        Commands::Dot { path } => {
            cmd_dot(&path)?;
        }

        Commands::Worker { action } => match action {
            WorkerCommands::Serve {
                port,
                metrics_port,
                tls_cert,
                tls_key,
                ca_cert,
            } => {
                let tls = match (tls_cert, tls_key, ca_cert) {
                    (Some(cert), Some(key), Some(ca)) => {
                        Some(fluxion_worker::WorkerTls { cert, key, ca })
                    }
                    _ => None,
                };
                fluxion_worker::serve(port, metrics_port, tls).await?;
            }
            WorkerCommands::Register { url } => {
                let store = RunStore::open()?;
                store.register_worker(&url)?;
                println!("Registered: {url}");
            }
            WorkerCommands::Remove { url } => {
                let store = RunStore::open()?;
                let n = store.remove_worker(&url)?;
                if n == 0 {
                    println!("Not found: {url}");
                } else {
                    println!("Removed: {url}");
                }
            }
            WorkerCommands::List => {
                let store = RunStore::open()?;
                let workers = store.list_workers()?;
                if workers.is_empty() {
                    println!("No workers registered.");
                } else {
                    println!("{:<40}  STATUS", "URL");
                    println!("{}", "-".repeat(52));
                    for w in workers {
                        let status = w.last_health.as_deref().unwrap_or("unknown");
                        println!("{:<40}  {}", w.url, status.to_uppercase());
                    }
                }
            }
        },

        Commands::Watch { path, debounce } => {
            watch::watch_and_run(PathBuf::from(&path), debounce).await?;
        }

        Commands::Validate {
            path,
            json,
            skip_wasm_check,
        } => {
            cmd_validate(&path, json, skip_wasm_check);
        }

        Commands::McpServe => {
            mcp::serve().await?;
        }

        Commands::Schedule { action } => match action {
            ScheduleCommands::Add { path, cron } => {
                cmd_schedule_add(&path, &cron)?;
            }
            ScheduleCommands::List => {
                cmd_schedule_list()?;
            }
            ScheduleCommands::Remove { id } => {
                cmd_schedule_remove(&id)?;
            }
            ScheduleCommands::Daemon => {
                cmd_schedule_daemon().await?;
            }
        },
    }

    Ok(())
}

// ── input resolution (#33 --input-file / #37 stdin) ──────────────────────────

fn resolve_input(input: Option<String>, input_file: Option<PathBuf>) -> Result<Vec<u8>> {
    if let Some(s) = input {
        return Ok(s.into_bytes());
    }
    if let Some(path) = input_file {
        return std::fs::read(&path)
            .map_err(|e| anyhow::anyhow!("Cannot read '{}': {}", path.display(), e));
    }
    // Neither flag given: read stdin if it is a pipe/redirect, otherwise return empty.
    use std::io::IsTerminal;
    if !std::io::stdin().is_terminal() {
        let mut buf = Vec::new();
        std::io::Read::read_to_end(&mut std::io::stdin(), &mut buf)?;
        return Ok(buf);
    }
    Ok(vec![])
}

// ── fluxion run --dry-run (#29) ───────────────────────────────────────────────

fn cmd_dry_run(wf: &Workflow) -> Result<()> {
    let dag = Dag::build(wf)?;
    println!("Workflow: {} ({} jobs)\n", wf.name, dag.topo_order.len());
    println!("Execution order:");
    for (i, job_id) in dag.topo_order.iter().enumerate() {
        let deps = &dag.deps[job_id];
        if deps.is_empty() {
            println!("  {}. {}  (no deps)", i + 1, job_id);
        } else {
            println!("  {}. {}  (depends: {})", i + 1, job_id, deps.join(", "));
        }
    }
    Ok(())
}

// ── fluxion dot ───────────────────────────────────────────────────────────────

fn cmd_dot(path: &str) -> Result<()> {
    let wf = Workflow::from_file(path)
        .map_err(|e| anyhow::anyhow!("Failed to load '{}': {}", path, e))?;
    let dag = Dag::build(&wf)?;

    println!("digraph {} {{", dot_id(&wf.name));
    for job_id in &dag.topo_order {
        println!("  {};", dot_id(job_id));
    }
    for (job_id, deps) in &dag.deps {
        for dep in deps {
            println!("  {} -> {};", dot_id(dep), dot_id(job_id));
        }
    }
    println!("}}");
    Ok(())
}

// ── fluxion validate ──────────────────────────────────────────────────────────

fn cmd_validate(path: &str, json: bool, skip_wasm_check: bool) {
    // Step 1: read file
    let src = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) => {
            let msg = format!("Cannot read '{path}': {e}");
            if json {
                eprintln!("{}", serde_json::json!({"ok": false, "errors": [msg]}));
            } else {
                eprintln!("ERROR: {msg}");
            }
            std::process::exit(1);
        }
    };

    // Step 2: parse YAML (separate from validation so we get a clear parse error)
    let wf: Workflow = match serde_yaml::from_str(&src) {
        Ok(w) => w,
        Err(e) => {
            let msg = format!("YAML parse error: {e}");
            if json {
                eprintln!("{}", serde_json::json!({"ok": false, "errors": [msg]}));
            } else {
                eprintln!("ERROR: {msg}");
            }
            std::process::exit(1);
        }
    };

    // Step 3: structural validation (deps, cycles, input_from)
    let mut report = wf.validate();

    // Step 4: component path existence check (opt-out with --skip-wasm-check)
    if !skip_wasm_check {
        let path_report = wf.check_component_paths();
        report.errors.extend(path_report.errors);
    }

    if json {
        let out = serde_json::json!({
            "ok": report.is_ok(),
            "errors": report.errors.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
        });
        println!("{}", serde_json::to_string_pretty(&out).unwrap());
    } else if report.is_ok() {
        println!("✓ {path}: Validation passed");
    } else {
        for err in &report.errors {
            eprintln!("ERROR: {err}");
        }
    }

    if !report.is_ok() {
        std::process::exit(1);
    }
}

fn dot_id(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\\\""))
}

// ── fluxion run --metrics ─────────────────────────────────────────────────────

fn print_metrics_table(jobs: &[fluxion_core::runner::JobResult]) {
    let measured: Vec<_> = jobs.iter().filter(|j| !j.skipped).collect();
    if measured.is_empty() {
        return;
    }

    let pad = measured.iter().map(|j| j.job_id.len()).max().unwrap_or(0);

    println!();
    println!(
        "  {:<pad$}  {:>10}  {:>10}  {:>10}  {:>10}",
        "job",
        "compile",
        "instantiate",
        "execute",
        "total",
        pad = pad
    );
    println!("  {}", "-".repeat(pad + 46));

    for j in &measured {
        let total_us = j.compile_us + j.instantiate_us + j.execute_us;
        println!(
            "  {:<pad$}  {:>10}  {:>10}  {:>10}  {:>10}",
            j.job_id,
            fmt_us(j.compile_us),
            fmt_us(j.instantiate_us),
            fmt_us(j.execute_us),
            fmt_us(total_us),
            pad = pad
        );
    }
}

// ── fluxion bench ─────────────────────────────────────────────────────────────

fn cmd_bench(path: &str, runs: usize, warmup: usize, input: &str) -> Result<()> {
    anyhow::ensure!(runs > 0, "--runs must be at least 1");

    let host = FluxionHost::new()?;
    let perms = PermissionSet::default();
    let input_bytes = input.as_bytes().to_vec();

    println!(
        "Benchmarking: {}  ({} runs, {} warmup)\n",
        path, runs, warmup
    );

    for i in 0..warmup {
        print!("\r  warmup {}/{} ...", i + 1, warmup);
        let _ = std::io::Write::flush(&mut std::io::stdout());
        host.run_component_measured(path, input_bytes.clone(), &perms, &Default::default())
            .map_err(|e| anyhow::anyhow!("warmup run failed: {}", e))?;
    }
    if warmup > 0 {
        println!("\r  warmup done           ");
    }

    let mut compile_us: Vec<u64> = Vec::with_capacity(runs);
    let mut instantiate_us: Vec<u64> = Vec::with_capacity(runs);
    let mut execute_us: Vec<u64> = Vec::with_capacity(runs);

    for i in 0..runs {
        print!("\r  run {}/{} ...", i + 1, runs);
        let _ = std::io::Write::flush(&mut std::io::stdout());
        let (_, m) = host
            .run_component_measured(path, input_bytes.clone(), &perms, &Default::default())
            .map_err(|e| anyhow::anyhow!("run {} failed: {}", i + 1, e))?;
        compile_us.push(m.compile.as_micros() as u64);
        instantiate_us.push(m.instantiate.as_micros() as u64);
        execute_us.push(m.execute.as_micros() as u64);
    }
    println!("\r  done ({} runs)         \n", runs);

    let total_us: Vec<u64> = compile_us
        .iter()
        .zip(&instantiate_us)
        .zip(&execute_us)
        .map(|((c, i), e)| c + i + e)
        .collect();

    // Header
    println!(
        "{:<12}  {:>9}  {:>9}  {:>9}  {:>9}  {:>9}  {:>9}",
        "phase", "avg", "p50", "p95", "p99", "min", "max"
    );
    println!("{}", "-".repeat(72));
    print_bench_row("compile", &compile_us);
    print_bench_row("instantiate", &instantiate_us);
    print_bench_row("execute", &execute_us);
    println!("{}", "-".repeat(72));
    print_bench_row("total", &total_us);

    Ok(())
}

fn print_bench_row(label: &str, us_values: &[u64]) {
    let mut sorted = us_values.to_vec();
    sorted.sort_unstable();
    let n = sorted.len();

    let avg = sorted.iter().sum::<u64>() / n as u64;
    let p50 = sorted[n / 2];
    let p95 = sorted[(n * 95 / 100).min(n - 1)];
    let p99 = sorted[(n * 99 / 100).min(n - 1)];
    let min = sorted[0];
    let max = sorted[n - 1];

    println!(
        "{:<12}  {:>9}  {:>9}  {:>9}  {:>9}  {:>9}  {:>9}",
        label,
        fmt_us(avg),
        fmt_us(p50),
        fmt_us(p95),
        fmt_us(p99),
        fmt_us(min),
        fmt_us(max),
    );
}

fn fmt_us(us: u64) -> String {
    if us >= 1_000_000 {
        format!("{:.2}s", us as f64 / 1_000_000.0)
    } else if us >= 1_000 {
        format!("{:.2}ms", us as f64 / 1_000.0)
    } else {
        format!("{}µs", us)
    }
}

// ── fluxion status ────────────────────────────────────────────────────────────

fn cmd_status(run_id: &str) -> Result<()> {
    let store = RunStore::open()?;
    let run = store.get_run(run_id)?;
    let jobs = store.get_run_jobs(run_id)?;

    let elapsed_s = run
        .completed_at
        .map(|end| (end - run.started_at) as f64)
        .unwrap_or(0.0);

    println!("Run:      {}", run.id);
    println!("Workflow: {}", run.workflow_name);
    println!("Started:  {}", fmt_unix(run.started_at));
    println!("Elapsed:  {:.2}s", elapsed_s);
    println!("Status:   {}", run.status.to_uppercase());

    if jobs.is_empty() {
        return Ok(());
    }

    let pad = jobs.iter().map(|j| j.job_id.len()).max().unwrap_or(0);
    println!();
    println!("  {:<pad$}  STATUS   ELAPSED", "JOB", pad = pad);
    println!("  {}", "-".repeat(pad + 20));
    for j in &jobs {
        let elapsed = j
            .elapsed_ms
            .map(|ms| format!("{:.2}s", ms as f64 / 1000.0))
            .unwrap_or_else(|| "-".to_string());
        println!(
            "  {:<pad$}  {:<8} {}",
            j.job_id,
            j.status.to_uppercase(),
            elapsed,
            pad = pad
        );
        if let Some(ref reason) = j.reason {
            println!("    Reason: {}", reason);
        }
    }
    Ok(())
}

// ── fluxion logs ──────────────────────────────────────────────────────────────

fn cmd_logs(run_id: &str, json: bool) -> Result<()> {
    let store = RunStore::open()?;
    let run = store.get_run(run_id)?;
    let jobs = store.get_run_jobs(run_id)?;

    if json {
        use serde_json::json;
        let out = json!({
            "run": {
                "id": run.id,
                "workflow_name": run.workflow_name,
                "workflow_path": run.workflow_path,
                "started_at": run.started_at,
                "completed_at": run.completed_at,
                "status": run.status,
            },
            "jobs": jobs.iter().map(|j| json!({
                "job_id": j.job_id,
                "status": j.status,
                "elapsed_ms": j.elapsed_ms,
                "reason": j.reason,
            })).collect::<Vec<_>>(),
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
        return Ok(());
    }

    let pad = jobs.iter().map(|j| j.job_id.len()).max().unwrap_or(0);

    // Reconstruct a timeline by accumulating elapsed from run start.
    let mut cursor = run.started_at;
    for j in &jobs {
        let elapsed_ms = j.elapsed_ms.unwrap_or(0);
        println!(
            "[{}] {:<pad$}  RUNNING",
            fmt_unix(cursor),
            j.job_id,
            pad = pad
        );
        cursor += elapsed_ms / 1000;
        let elapsed_s = elapsed_ms as f64 / 1000.0;
        println!(
            "[{}] {:<pad$}  {}  {:.2}s",
            fmt_unix(cursor),
            j.job_id,
            j.status.to_uppercase(),
            elapsed_s,
            pad = pad,
        );
        if let Some(ref reason) = j.reason {
            println!("  {}", reason);
        }
    }
    Ok(())
}

// ── fluxion inspect ───────────────────────────────────────────────────────────

fn cmd_inspect(path: &str) -> Result<()> {
    use wasmtime::component::Component;
    use wasmtime::{Config, Engine};

    let meta =
        std::fs::metadata(path).map_err(|e| anyhow::anyhow!("Cannot read '{}': {}", path, e))?;

    let mut config = Config::new();
    config.wasm_component_model(true);
    let engine = Engine::new(&config)?;

    let component = Component::from_file(&engine, path)
        .map_err(|e| anyhow::anyhow!("Not a valid Wasm component '{}': {}", path, e))?;

    let ct = component.component_type();

    println!("Path:  {}", path);
    println!(
        "Size:  {} bytes ({:.1} KB)",
        meta.len(),
        meta.len() as f64 / 1024.0
    );

    let exports: Vec<_> = ct.exports(&engine).collect();
    println!();
    println!("Exports ({}):", exports.len());
    for (name, item) in &exports {
        println!("  {}  [{}]", name, component_item_kind(item));
    }

    let imports: Vec<_> = ct.imports(&engine).collect();
    if !imports.is_empty() {
        println!();
        println!("Imports ({}):", imports.len());
        for (name, item) in &imports {
            println!("  {}  [{}]", name, component_item_kind(item));
        }
    }
    Ok(())
}

fn component_item_kind(item: &wasmtime::component::types::ComponentItem) -> &'static str {
    use wasmtime::component::types::ComponentItem;
    match item {
        ComponentItem::ComponentFunc(_) => "func",
        ComponentItem::CoreFunc(_) => "core-func",
        ComponentItem::Module(_) => "module",
        ComponentItem::Component(_) => "component",
        ComponentItem::ComponentInstance(_) => "instance",
        ComponentItem::Type(_) => "type",
        ComponentItem::Resource(_) => "resource",
    }
}

// ── helpers ───────────────────────────────────────────────────────────────────

fn fmt_unix(secs: u64) -> String {
    let h = (secs / 3600) % 24;
    let m = (secs / 60) % 60;
    let s = secs % 60;
    format!("{:02}:{:02}:{:02}", h, m, s)
}

// ── fluxion schedule ──────────────────────────────────────────────────────────

fn next_run_secs(cron_expr: &str) -> Result<u64> {
    use std::str::FromStr;
    let schedule = cron::Schedule::from_str(cron_expr)
        .map_err(|e| anyhow::anyhow!("Invalid cron expression '{}': {}", cron_expr, e))?;
    let next = schedule.upcoming(Utc).next().ok_or_else(|| {
        anyhow::anyhow!("Cron expression '{}' has no future occurrences", cron_expr)
    })?;
    Ok(next.timestamp() as u64)
}

fn cmd_schedule_add(path: &str, cron_expr: &str) -> Result<()> {
    // Validate the workflow can be parsed.
    let _wf = Workflow::from_file(path)
        .map_err(|e| anyhow::anyhow!("Cannot load workflow '{}': {}", path, e))?;
    let abs_path = std::fs::canonicalize(path)
        .map_err(|e| anyhow::anyhow!("Cannot resolve '{}': {}", path, e))?;
    let next = next_run_secs(cron_expr)?;
    let id = format!(
        "sched-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
    );
    let store = RunStore::open()?;
    store.add_schedule(&id, abs_path.to_string_lossy().as_ref(), cron_expr, next)?;
    let next_dt = chrono::DateTime::<Utc>::from_timestamp(next as i64, 0)
        .map(|d| d.to_rfc3339())
        .unwrap_or_else(|| next.to_string());
    println!("Schedule added: {id}");
    println!("  workflow : {}", abs_path.display());
    println!("  cron     : {cron_expr}");
    println!("  next run : {next_dt}");
    Ok(())
}

fn cmd_schedule_list() -> Result<()> {
    let store = RunStore::open()?;
    let schedules = store.list_schedules()?;
    if schedules.is_empty() {
        println!("No schedules registered.");
        return Ok(());
    }
    println!("{:<28}  {:<20}  NEXT RUN", "ID", "CRON");
    println!("{}", "-".repeat(72));
    for s in schedules {
        let next_dt = chrono::DateTime::<Utc>::from_timestamp(s.next_run_at as i64, 0)
            .map(|d| d.to_rfc3339())
            .unwrap_or_else(|| s.next_run_at.to_string());
        println!("{:<28}  {:<20}  {}", s.id, s.cron_expr, next_dt);
        println!("    {}", s.workflow_path);
    }
    Ok(())
}

fn cmd_schedule_remove(id: &str) -> Result<()> {
    let store = RunStore::open()?;
    let n = store.remove_schedule(id)?;
    if n == 0 {
        println!("Not found: {id}");
    } else {
        println!("Removed: {id}");
    }
    Ok(())
}

async fn cmd_schedule_daemon() -> Result<()> {
    use tokio::signal::unix::{SignalKind, signal};
    println!("fluxion schedule daemon starting (press Ctrl-C or send SIGTERM to stop)");
    let mut sigterm = signal(SignalKind::terminate())?;
    let host = Arc::new(FluxionHost::new()?);

    loop {
        tokio::select! {
            _ = sigterm.recv() => {
                println!("Received SIGTERM — shutting down daemon");
                break;
            }
            _ = tokio::signal::ctrl_c() => {
                println!("Received SIGINT — shutting down daemon");
                break;
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                fire_due_schedules(Arc::clone(&host)).await;
            }
        }
    }
    Ok(())
}

async fn fire_due_schedules(host: Arc<FluxionHost>) {
    // Collect due schedules then drop the store (rusqlite::Connection is !Send).
    let due = {
        let store = match RunStore::open() {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("store open failed: {e}");
                return;
            }
        };
        match store.due_schedules() {
            Ok(d) => d,
            Err(e) => {
                tracing::error!("due_schedules failed: {e}");
                return;
            }
        }
    };
    for sched in due {
        let cron_expr = sched.cron_expr.clone();
        let sched_id = sched.id.clone();

        // Optimistic lock: advance next_run_at before execution.
        // If another process already updated next_run_at, skip this schedule.
        let next = match next_run_secs(&cron_expr) {
            Ok(n) => n,
            Err(e) => {
                tracing::error!(id = %sched_id, "invalid cron: {e}");
                continue;
            }
        };
        let claimed = {
            let store = match RunStore::open() {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("store open failed: {e}");
                    continue;
                }
            };
            store
                .claim_schedule(&sched_id, sched.next_run_at, next)
                .unwrap_or(false)
        };
        if !claimed {
            tracing::debug!(id = %sched_id, "schedule already claimed by another process, skipping");
            continue;
        }

        let wf = match Workflow::from_file(&sched.workflow_path) {
            Ok(w) => w,
            Err(e) => {
                tracing::error!(id = %sched_id, "failed to load workflow: {e}");
                continue;
            }
        };
        let wf_path = PathBuf::from(&sched.workflow_path);
        tracing::info!(schedule = %sched_id, "firing scheduled workflow");
        let _ =
            scheduler::run_with_strategy(&wf, &wf_path, Arc::clone(&host), LbStrategy::RoundRobin)
                .await;

        // Record last_run_at (next_run_at is already updated by claim_schedule).
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if let Ok(s) = RunStore::open() {
            let _ = s.update_schedule_next(&sched_id, now, next);
        }
    }
}

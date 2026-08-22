use std::path::PathBuf;
use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use fluxion_core::dag::Dag;
use fluxion_core::workflow::{ExecutorKind, JobDefinition, Workflow};
use fluxion_host::cache::ComponentCache;
use fluxion_host::{FluxionHost, scheduler};
use indexmap::IndexMap;

// ── helpers ───────────────────────────────────────────────────────────────────

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root")
}

fn hello_wasm_bytes() -> Vec<u8> {
    let path = workspace_root().join("components/hello/target/wasm32-wasip1/debug/hello.wasm");
    std::fs::read(&path)
        .expect("hello.wasm not built — run `cargo component build` in components/hello")
}

fn hello_wasm_path() -> String {
    workspace_root()
        .join("components/hello/target/wasm32-wasip1/debug/hello.wasm")
        .to_string_lossy()
        .into_owned()
}

fn dummy_job(depends_on: Vec<String>) -> JobDefinition {
    JobDefinition {
        component: "/dummy.wasm".to_string(),
        depends_on,
        input: None,
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
        executor: ExecutorKind::Local,
    }
}

fn hello_job(depends_on: Vec<String>) -> JobDefinition {
    JobDefinition {
        component: hello_wasm_path(),
        depends_on,
        input: Some(r#""bench""#.to_string()),
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
        executor: ExecutorKind::Local,
    }
}

/// Build a sequential chain of N hello jobs: job_0 → job_1 → … → job_{n-1}
fn sequential_workflow(n: usize) -> Workflow {
    let mut jobs = IndexMap::new();
    for i in 0..n {
        let dep = if i > 0 {
            vec![format!("job_{}", i - 1)]
        } else {
            vec![]
        };
        jobs.insert(format!("job_{}", i), hello_job(dep));
    }
    Workflow {
        name: format!("seq-{n}"),
        jobs,
        workers: vec![],
        max_parallel: None,
        workers_srv: None,
    }
}

/// Build N independent hello jobs (all run in parallel).
fn parallel_workflow(n: usize) -> Workflow {
    let mut jobs = IndexMap::new();
    for i in 0..n {
        jobs.insert(format!("job_{}", i), hello_job(vec![]));
    }
    Workflow {
        name: format!("par-{n}"),
        jobs,
        workers: vec![],
        max_parallel: None,
        workers_srv: None,
    }
}

fn chain_workflow(n: usize) -> Workflow {
    let mut jobs = IndexMap::new();
    for i in 0..n {
        let dep = if i > 0 {
            vec![format!("job_{}", i - 1)]
        } else {
            vec![]
        };
        jobs.insert(format!("job_{}", i), dummy_job(dep));
    }
    Workflow {
        name: format!("chain-{n}"),
        jobs,
        workers: vec![],
        max_parallel: None,
        workers_srv: None,
    }
}

// ── ComponentCache benchmarks ─────────────────────────────────────────────────

fn bench_cache(c: &mut Criterion) {
    let host = FluxionHost::new().expect("FluxionHost::new");
    let mut cfg = wasmtime::Config::new();
    cfg.wasm_component_model(true);
    cfg.epoch_interruption(true);
    let engine = wasmtime::Engine::new(&cfg).expect("engine");

    let wasm_bytes = hello_wasm_bytes();

    // Warm the cache via a fresh ComponentCache backed by a temp dir.
    let mut cache = ComponentCache::new();
    cache.store(&engine, &wasm_bytes).expect("initial store");

    let mut group = c.benchmark_group("cache");

    group.bench_function("load_hit", |b| {
        b.iter(|| {
            cache.load(&engine, &wasm_bytes).expect("cache hit");
        })
    });

    // Cold-store benchmark: create a fresh cache each iteration so every
    // store is a compile (no pre-existing artifact).
    group.bench_function("store_cold", |b| {
        b.iter(|| {
            let mut fresh = ComponentCache::new();
            fresh.store(&engine, &wasm_bytes).expect("store");
        })
    });

    group.finish();
    drop(host);
}

// ── Dag::build benchmarks ─────────────────────────────────────────────────────

fn bench_dag_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("dag_build");

    for n in [50usize, 200] {
        let wf = chain_workflow(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &wf, |b, wf| {
            b.iter(|| Dag::build(wf).unwrap());
        });
    }

    group.finish();
}

// ── scheduler hot-path: run_component (warm cache) ───────────────────────────

fn bench_run_component(c: &mut Criterion) {
    let host = Arc::new(FluxionHost::new().expect("FluxionHost::new"));
    let wasm_bytes = hello_wasm_bytes();
    let wasm_path = workspace_root().join("components/hello/target/wasm32-wasip1/debug/hello.wasm");
    let perms = fluxion_core::workflow::PermissionSet::default();

    // Warm the component cache (mem + disk) before measuring.
    host.run_component(&wasm_path, b"bench".to_vec(), &perms)
        .expect("warm-up");

    let mut group = c.benchmark_group("run_component");
    group.sample_size(10); // wasmtime execution is slow in debug builds

    group.bench_function("hello_warm_cache", |b| {
        let h = Arc::clone(&host);
        let path = wasm_path.clone();
        let p = perms.clone();
        b.iter(|| {
            h.run_component(&path, b"bench".to_vec(), &p).expect("run");
        })
    });

    group.finish();
    drop(host);

    // ignore wasm_bytes (only used for cache warm-up reference)
    let _ = wasm_bytes;
}

// ── scheduler end-to-end: run full workflow (warm cache) ─────────────────────

fn bench_workflow_run(c: &mut Criterion) {
    // hello.wasm must be pre-built; skip gracefully if absent.
    let wasm = workspace_root().join("components/hello/target/wasm32-wasip1/debug/hello.wasm");
    if !wasm.exists() {
        eprintln!(
            "SKIP bench_workflow_run: hello.wasm not found (run `cargo component build` in components/hello)"
        );
        return;
    }

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    // Warm the component cache so measurement reflects post-JIT latency.
    let host = Arc::new(FluxionHost::new().expect("FluxionHost::new"));
    let perms = fluxion_core::workflow::PermissionSet::default();
    host.run_component(&wasm, b"warmup".to_vec(), &perms)
        .expect("warmup run");

    let wf_path = workspace_root().join("examples/three-stage.yaml");

    let mut group = c.benchmark_group("workflow_run");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(15));

    for n in [1usize, 3, 5] {
        // Sequential: job_0 → job_1 → … → job_{n-1}
        let seq_wf = sequential_workflow(n);
        let seq_path = wf_path.clone();
        let h = Arc::clone(&host);
        group.bench_with_input(BenchmarkId::new("sequential", n), &n, |b, _| {
            b.iter(|| {
                rt.block_on(scheduler::run_silent(&seq_wf, &seq_path, Arc::clone(&h)))
                    .expect("sequential run");
            });
        });

        // Parallel: N independent jobs (no deps)
        let par_wf = parallel_workflow(n);
        let par_path = wf_path.clone();
        let h = Arc::clone(&host);
        group.bench_with_input(BenchmarkId::new("parallel", n), &n, |b, _| {
            b.iter(|| {
                rt.block_on(scheduler::run_silent(&par_wf, &par_path, Arc::clone(&h)))
                    .expect("parallel run");
            });
        });
    }

    group.finish();
    drop(host);
}

criterion_group!(
    benches,
    bench_cache,
    bench_dag_build,
    bench_run_component,
    bench_workflow_run
);
criterion_main!(benches);

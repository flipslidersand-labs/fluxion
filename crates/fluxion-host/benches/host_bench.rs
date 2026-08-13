use std::path::PathBuf;
use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use fluxion_core::dag::Dag;
use fluxion_core::workflow::{JobDefinition, Workflow};
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
    let path = workspace_root()
        .join("components/hello/target/wasm32-wasip1/debug/hello.wasm");
    std::fs::read(&path).expect("hello.wasm not built — run `cargo component build` in components/hello")
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
    }
}

fn chain_workflow(n: usize) -> Workflow {
    let mut jobs = IndexMap::new();
    for i in 0..n {
        let dep = if i > 0 { vec![format!("job_{}", i - 1)] } else { vec![] };
        jobs.insert(format!("job_{}", i), dummy_job(dep));
    }
    Workflow { name: format!("chain-{n}"), jobs, workers: vec![], max_parallel: None, workers_srv: None }
}

// ── ComponentCache benchmarks ─────────────────────────────────────────────────

fn bench_cache(c: &mut Criterion) {
    let host = FluxionHost::new().expect("FluxionHost::new");
    // Expose the engine via cache internals — we need an Engine for cache ops.
    // Use a standalone engine matching FluxionHost's config.
    let mut cfg = wasmtime::Config::new();
    cfg.wasm_component_model(true);
    cfg.epoch_interruption(true);
    let engine = wasmtime::Engine::new(&cfg).expect("engine");

    let wasm_bytes = hello_wasm_bytes();
    let tmp = tempfile::tempdir().expect("tempdir");

    let mut cache = ComponentCache::new();
    cache.dir = tmp.path().to_path_buf();

    // Warm the cache (store once so subsequent loads are hits)
    cache.store(&engine, &wasm_bytes).expect("initial store");

    let mut group = c.benchmark_group("cache");

    group.bench_function("load_hit", |b| {
        b.iter(|| {
            cache.load(&engine, &wasm_bytes).expect("cache hit");
        })
    });

    group.bench_function("store", |b| {
        // Clear artifact before each iteration so we always measure cold compile.
        let path = cache.artifact_path(&wasm_bytes);
        b.iter(|| {
            std::fs::remove_file(&path).ok();
            cache.store(&engine, &wasm_bytes).expect("store");
        })
    });

    group.finish();

    // Keep host alive for the duration (its ticker thread must not stop early).
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
    let wasm_path = workspace_root()
        .join("components/hello/target/wasm32-wasip1/debug/hello.wasm");
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

// ── scheduler::run end-to-end benchmark ──────────────────────────────────────

fn serial_workflow(n: usize, component: &str) -> Workflow {
    let mut jobs = IndexMap::new();
    for i in 0..n {
        let dep = if i > 0 { vec![format!("job_{}", i - 1)] } else { vec![] };
        let mut job = dummy_job(dep);
        job.component = component.to_string();
        jobs.insert(format!("job_{}", i), job);
    }
    Workflow { name: format!("serial-{n}"), jobs, workers: vec![], max_parallel: None, workers_srv: None }
}

fn parallel_workflow(n: usize, component: &str) -> Workflow {
    let mut jobs = IndexMap::new();
    for i in 0..n {
        let mut job = dummy_job(vec![]);
        job.component = component.to_string();
        jobs.insert(format!("job_{}", i), job);
    }
    Workflow { name: format!("parallel-{n}"), jobs, workers: vec![], max_parallel: None, workers_srv: None }
}

fn bench_workflow_run(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let hello_path = workspace_root()
        .join("components/hello/target/wasm32-wasip1/debug/hello.wasm");
    let hello_str = hello_path.to_string_lossy().into_owned();

    // Warm wasmtime cache before measuring so we only measure scheduler overhead.
    let host = Arc::new(FluxionHost::new().expect("FluxionHost::new"));
    let perms = fluxion_core::workflow::PermissionSet::default();
    host.run_component(&hello_path, b"warm".to_vec(), &perms)
        .expect("warm-up");

    let mut group = c.benchmark_group("workflow_run");
    group.measurement_time(std::time::Duration::from_secs(15));
    group.sample_size(10);

    let tmp_wf_path = std::env::temp_dir().join("bench_wf.yaml");

    for n in [1usize, 3, 5] {
        let wf = serial_workflow(n, &hello_str);
        group.bench_with_input(
            BenchmarkId::new("serial", n),
            &(wf, tmp_wf_path.clone()),
            |b, (wf, wf_path)| {
                b.iter(|| {
                    rt.block_on(scheduler::run_silent(wf, wf_path, Arc::clone(&host)))
                        .expect("run_silent")
                });
            },
        );
    }

    for n in [1usize, 3, 5] {
        let wf = parallel_workflow(n, &hello_str);
        group.bench_with_input(
            BenchmarkId::new("parallel", n),
            &(wf, tmp_wf_path.clone()),
            |b, (wf, wf_path)| {
                b.iter(|| {
                    rt.block_on(scheduler::run_silent(wf, wf_path, Arc::clone(&host)))
                        .expect("run_silent")
                });
            },
        );
    }

    group.finish();
    drop(host);
}

criterion_group!(benches, bench_cache, bench_dag_build, bench_run_component, bench_workflow_run);
criterion_main!(benches);

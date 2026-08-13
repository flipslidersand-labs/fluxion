use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use fluxion_core::dag::Dag;
use fluxion_core::store::RunStore;
use fluxion_core::workflow::{JobDefinition, NetworkPermission, PermissionSet, Workflow};
use indexmap::IndexMap;

// ── workflow fixtures ─────────────────────────────────────────────────────────

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
    Workflow {
        name: format!("chain-{n}"),
        jobs,
        workers: vec![],
        max_parallel: None,
        workers_srv: None,
    }
}

fn parallel_workflow(n: usize) -> Workflow {
    let mut jobs = IndexMap::new();
    for i in 0..n - 1 {
        jobs.insert(format!("leaf_{}", i), dummy_job(vec![]));
    }
    let merge_deps = (0..n - 1).map(|i| format!("leaf_{}", i)).collect();
    jobs.insert("merge".to_string(), dummy_job(merge_deps));
    Workflow {
        name: format!("parallel-{n}"),
        jobs,
        workers: vec![],
        max_parallel: None,
        workers_srv: None,
    }
}

// ── DAG benchmarks ────────────────────────────────────────────────────────────

fn bench_dag_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("dag_build");
    for n in [10, 50, 200] {
        let chain = chain_workflow(n);
        group.bench_with_input(BenchmarkId::new("chain", n), &chain, |b, wf| {
            b.iter(|| Dag::build(wf).unwrap())
        });

        let parallel = parallel_workflow(n);
        group.bench_with_input(BenchmarkId::new("parallel", n), &parallel, |b, wf| {
            b.iter(|| Dag::build(wf).unwrap())
        });
    }
    group.finish();
}

// ── RunStore benchmarks ───────────────────────────────────────────────────────

fn bench_run_store(c: &mut Criterion) {
    // Use a temp dir as HOME so RunStore writes to /tmp, not ~/.fluxion
    let tmp = tempfile::tempdir().expect("tempdir");
    // SAFETY: bench runs single-threaded; no other threads read HOME concurrently.
    unsafe { std::env::set_var("HOME", tmp.path()) };

    let store = RunStore::open().expect("open store");
    let wf_path = std::path::Path::new("/tmp/bench.yaml");

    let mut group = c.benchmark_group("run_store");
    group.measurement_time(std::time::Duration::from_secs(5));

    group.bench_function("create_run", |b| {
        b.iter(|| {
            let run_id = RunStore::new_run_id();
            store.create_run(&run_id, "bench-wf", wf_path).unwrap();
        })
    });

    // Pre-create a run for upsert bench
    let run_id = RunStore::new_run_id();
    store.create_run(&run_id, "bench-wf", wf_path).unwrap();

    group.bench_function("upsert_job", |b| {
        use fluxion_core::state::JobStatus;
        b.iter(|| {
            store
                .upsert_job(&run_id, "job_0", &JobStatus::Running)
                .unwrap();
        })
    });

    group.finish();

    // Restore HOME (best-effort; benches run in isolation anyway)
    // SAFETY: single-threaded context.
    unsafe { let _ = std::env::remove_var("HOME"); }
}

// ── PermissionSet benchmarks ──────────────────────────────────────────────────

fn bench_permission_check(c: &mut Criterion) {
    let mut group = c.benchmark_group("permission_check");

    // deny-all: empty allowlist — the common fast path for sandboxed jobs
    let deny_all = PermissionSet::default();
    group.bench_function("network_deny_all", |b| {
        b.iter(|| deny_all.network.is_deny_all())
    });

    // allow-list with 5 entries — checks whether addr is in the list
    let mut allow5 = PermissionSet::default();
    allow5.network = NetworkPermission {
        allow: vec![
            "127.0.0.1:8080".to_string(),
            "10.0.0.1:443".to_string(),
            "api.example.com:443".to_string(),
            "db.internal:5432".to_string(),
            "cache.internal:6379".to_string(),
        ],
    };
    group.bench_function("network_allows_hit", |b| {
        b.iter(|| allow5.network.allows("db.internal:5432"))
    });
    group.bench_function("network_allows_miss", |b| {
        b.iter(|| allow5.network.allows("evil.example.com:1234"))
    });

    group.finish();
}

criterion_group!(benches, bench_dag_build, bench_run_store, bench_permission_check);
criterion_main!(benches);

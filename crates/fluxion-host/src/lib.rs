pub mod cache;
pub mod scheduler;

use anyhow::{Context, Result};
use cache::ComponentCache;
use fluxion_core::workflow::PermissionSet;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use wasmtime::component::{Component, Linker};
use wasmtime::{Config, Engine, Store, StoreLimitsBuilder};
use wasmtime_wasi::{DirPerms, FilePerms, ResourceTable, WasiCtx, WasiCtxBuilder, WasiView};

/// Per-invocation timing breakdown for a single component run.
#[derive(Debug, Clone)]
pub struct JobMetrics {
    /// Time to load and compile the .wasm file via wasmtime.
    pub compile: Duration,
    /// Time to link and instantiate the compiled component.
    pub instantiate: Duration,
    /// Time spent inside the guest `process()` call.
    pub execute: Duration,
}

impl JobMetrics {
    pub fn total(&self) -> Duration {
        self.compile + self.instantiate + self.execute
    }
}

// Epoch ticker resolution: 10 ticks per second → 100ms granularity.
const TICKS_PER_SEC: u64 = 10;

wasmtime::component::bindgen!({
    path: "../../wit/task.wit",
    world: "task-component",
});

struct HostState {
    ctx: WasiCtx,
    table: ResourceTable,
    limits: wasmtime::StoreLimits,
}

impl WasiView for HostState {
    fn ctx(&mut self) -> &mut WasiCtx {
        &mut self.ctx
    }
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
}

pub struct FluxionHost {
    engine: Engine,
    /// L2: disk-backed compiled artifact cache (~/.cache/fluxion/components/).
    disk_cache: ComponentCache,
    /// L1: in-memory compiled component cache — avoids disk I/O on hot paths.
    mem_cache: RwLock<HashMap<String, Arc<Component>>>,
    /// L0: pre-linked instantiation cache — skips linker setup on repeated instantiate calls.
    pre_cache: RwLock<HashMap<String, Arc<TaskComponentPre<HostState>>>>,
}

impl FluxionHost {
    pub fn new() -> Result<Self> {
        let mut config = Config::new();
        config.wasm_component_model(true);
        // Epoch interruption allows the host to kill a running Wasm guest at any
        // loop back-edge or function call — the only way to stop CPU-bound guests.
        config.epoch_interruption(true);
        let engine = Engine::new(&config)?;

        // Background thread advances the epoch counter every 100ms.
        // The ticker runs for the process lifetime (detached thread is fine here).
        let ticker = engine.clone();
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(Duration::from_millis(1000 / TICKS_PER_SEC));
                ticker.increment_epoch();
            }
        });

        Ok(Self {
            engine,
            disk_cache: ComponentCache::new(),
            mem_cache: RwLock::new(HashMap::new()),
            pre_cache: RwLock::new(HashMap::new()),
        })
    }

    pub fn run_component(
        &self,
        wasm_path: impl AsRef<Path>,
        input: Vec<u8>,
        perms: &PermissionSet,
    ) -> Result<Vec<u8>> {
        let (output, _) = self.run_component_measured(wasm_path, input, perms)?;
        Ok(output)
    }

    /// Like `run_component` but also returns per-phase timing metrics.
    pub fn run_component_measured(
        &self,
        wasm_path: impl AsRef<Path>,
        input: Vec<u8>,
        perms: &PermissionSet,
    ) -> Result<(Vec<u8>, JobMetrics)> {
        let ctx = build_wasi_ctx(perms)?;
        let limits = StoreLimitsBuilder::new()
            .memory_size(perms.limits.memory_mb as usize * 1024 * 1024)
            .build();

        let state = HostState {
            ctx,
            table: ResourceTable::new(),
            limits,
        };
        let mut store = Store::new(&self.engine, state);
        store.limiter(|s| &mut s.limits);

        // Set the epoch deadline so CPU-bound guests are killed after timeout_secs.
        // epoch_deadline_trap() makes the Wasm trap (propagated as Err) when the
        // deadline fires, which terminates the blocking thread instead of leaking it.
        store.set_epoch_deadline(perms.limits.timeout_secs * TICKS_PER_SEC);
        store.epoch_deadline_trap();

        let wasm_bytes = std::fs::read(wasm_path.as_ref())?;
        let key = cache::wasm_key(&wasm_bytes);

        // ── compile phase (L1 mem → L2 disk → full compile) ──────────────────
        let t0 = Instant::now();
        let component: Arc<Component> = {
            if let Some(c) = self.mem_cache.read().unwrap().get(&key) {
                Arc::clone(c)
            } else {
                let c = Arc::new(match self.disk_cache.load(&self.engine, &wasm_bytes) {
                    Some(c) => c,
                    None => self.disk_cache.store(&self.engine, &wasm_bytes)?,
                });
                self.mem_cache.write().unwrap().insert(key.clone(), Arc::clone(&c));
                c
            }
        };
        let compile = t0.elapsed();

        // ── instantiate phase (L0 pre_cache → build once per component) ──────
        let t1 = Instant::now();
        let pre: Arc<TaskComponentPre<HostState>> = {
            if let Some(p) = self.pre_cache.read().unwrap().get(&key) {
                Arc::clone(p)
            } else {
                let mut linker: Linker<HostState> = Linker::new(&self.engine);
                wasmtime_wasi::add_to_linker_sync(&mut linker)?;
                let p = Arc::new(TaskComponentPre::new(linker.instantiate_pre(&*component)?)?);
                self.pre_cache.write().unwrap().insert(key, Arc::clone(&p));
                p
            }
        };
        let instance = pre.instantiate(&mut store).map_err(|e| {
            if is_oom_error(&e) {
                anyhow::anyhow!(
                    "OOM: component exceeded memory_mb={} limit ({})",
                    perms.limits.memory_mb,
                    e
                )
            } else {
                e
            }
        })?;
        let instantiate = t1.elapsed();

        let task_input = exports::fluxion::task::processor::TaskInput {
            content: input,
            metadata: vec![],
        };

        let t2 = Instant::now();
        let call_result = instance
            .fluxion_task_processor()
            .call_process(&mut store, &task_input);
        let execute = t2.elapsed();

        let metrics = JobMetrics { compile, instantiate, execute };

        match call_result {
            // Clean component-level error (returned via Result<_, String>)
            Ok(Err(e)) => anyhow::bail!("Component error: {}", e),
            Ok(Ok(output)) => Ok((output.content, metrics)),
            // Trap from the Wasm runtime — distinguish timeout from other traps
            Err(trap) => {
                if is_epoch_trap(&trap) {
                    anyhow::bail!(
                        "Timeout: killed after {}s (epoch interrupt)",
                        perms.limits.timeout_secs
                    )
                } else {
                    Err(trap)
                }
            }
        }
    }
}

// Detects whether an error originates from a StoreLimits memory cap.
fn is_oom_error(e: &anyhow::Error) -> bool {
    let s = e.to_string();
    s.contains("exceeds memory limits") || s.contains("memory allocation failed")
}

// Detects whether an anyhow error originates from a wasmtime epoch interrupt trap.
fn is_epoch_trap(e: &anyhow::Error) -> bool {
    // wasmtime surfaces the epoch interrupt as Trap::Interrupt in the error chain.
    for cause in e.chain() {
        if let Some(trap) = cause.downcast_ref::<wasmtime::Trap>()
            && *trap == wasmtime::Trap::Interrupt
        {
            return true;
        }
    }
    false
}

// An entry in the network allowlist: either an exact IP:port or all ports on an IP.
#[derive(Debug)]
enum NetworkEntry {
    Exact(SocketAddr),
    AnyPort(IpAddr),
}

impl NetworkEntry {
    fn matches(&self, addr: SocketAddr) -> bool {
        match self {
            Self::Exact(a) => *a == addr,
            Self::AnyPort(ip) => *ip == addr.ip(),
        }
    }
}

fn parse_network_entry(s: &str) -> Option<NetworkEntry> {
    if let Ok(a) = s.parse::<SocketAddr>() {
        return Some(NetworkEntry::Exact(a));
    }
    if let Ok(ip) = s.parse::<IpAddr>() {
        return Some(NetworkEntry::AnyPort(ip));
    }
    None
}

fn build_wasi_ctx(perms: &PermissionSet) -> Result<WasiCtx> {
    let mut builder = WasiCtxBuilder::new();
    builder.inherit_stdout().inherit_stderr();

    // Filesystem: preopen read dirs
    for path in &perms.filesystem.read {
        if path.exists() {
            let guest = path.to_string_lossy().to_string();
            builder.preopened_dir(path, &guest, DirPerms::READ, FilePerms::READ)?;
        }
    }

    // Filesystem: preopen read-write dirs (created on demand)
    for path in &perms.filesystem.write {
        std::fs::create_dir_all(path)
            .with_context(|| format!("Failed to create write dir {:?}", path))?;
        let guest = path.to_string_lossy().to_string();
        builder.preopened_dir(
            path,
            &guest,
            DirPerms::READ | DirPerms::MUTATE,
            FilePerms::READ | FilePerms::WRITE,
        )?;
    }

    // Network capability gate.
    // SocketAddrCheck::default() already returns false for every address, so
    // deny-all requires no extra work. We only install a check when an explicit
    // allowlist is provided.
    if !perms.network.allow.is_empty() {
        let entries: Vec<NetworkEntry> = perms
            .network
            .allow
            .iter()
            .filter_map(|s| parse_network_entry(s))
            .collect();

        anyhow::ensure!(
            !entries.is_empty(),
            "network.allow has entries but none could be parsed as `IP` or `IP:port`"
        );

        // ip_name_lookup is false by default; we keep DNS off since the
        // allowlist is IP-based. Callers must specify resolved IPs.
        builder.socket_addr_check(move |addr, _use| {
            let ok = entries.iter().any(|e| e.matches(addr));
            Box::pin(async move { ok })
        });
    }

    Ok(builder.build())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_exact_addr() {
        let e = parse_network_entry("93.184.216.34:443").unwrap();
        assert!(matches!(e, NetworkEntry::Exact(_)));
    }

    #[test]
    fn parse_ip_only() {
        let e = parse_network_entry("93.184.216.34").unwrap();
        assert!(matches!(e, NetworkEntry::AnyPort(_)));
    }

    #[test]
    fn parse_invalid_returns_none() {
        assert!(parse_network_entry("example.com:443").is_none());
        assert!(parse_network_entry("not-an-addr").is_none());
    }

    #[test]
    fn exact_entry_matches_only_same_port() {
        let e = NetworkEntry::Exact("93.184.216.34:443".parse().unwrap());
        assert!(e.matches("93.184.216.34:443".parse().unwrap()));
        assert!(!e.matches("93.184.216.34:80".parse().unwrap()));
        assert!(!e.matches("1.2.3.4:443".parse().unwrap()));
    }

    #[test]
    fn any_port_entry_matches_all_ports() {
        let e = NetworkEntry::AnyPort("93.184.216.34".parse().unwrap());
        assert!(e.matches("93.184.216.34:443".parse().unwrap()));
        assert!(e.matches("93.184.216.34:80".parse().unwrap()));
        assert!(!e.matches("1.2.3.4:443".parse().unwrap()));
    }

    #[test]
    fn ipv6_exact_entry() {
        let e = parse_network_entry("[::1]:8080").unwrap();
        assert!(matches!(e, NetworkEntry::Exact(_)));
        assert!(e.matches("[::1]:8080".parse().unwrap()));
        assert!(!e.matches("[::1]:9090".parse().unwrap()));
    }
}

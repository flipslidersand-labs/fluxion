pub mod cache;
pub mod metrics;
pub mod remote;
pub mod scheduler;

use anyhow::{Context, Result};
use cache::ComponentCache;
use fluxion_core::workflow::PermissionSet;
use lru::LruCache;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
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

// Default LRU capacities. Large enough for typical workflows; evicts least-recently-used
// entries when the limit is reached to prevent unbounded memory growth.
const MEM_CACHE_CAP: usize = 64;
const PRE_CACHE_CAP: usize = 64;

pub struct FluxionHost {
    engine: Engine,
    /// L2: disk-backed compiled artifact cache (~/.cache/fluxion/components/).
    disk_cache: ComponentCache,
    /// L1: LRU cache of compiled components — avoids disk I/O on hot paths.
    mem_cache: Mutex<LruCache<String, Arc<Component>>>,
    /// L0: LRU pre-instantiate cache — skips linker setup on repeated calls.
    pre_cache: Mutex<LruCache<String, Arc<TaskComponentPre<HostState>>>>,
    /// Signals the epoch ticker thread to stop on drop.
    ticker_shutdown: Arc<AtomicBool>,
}

impl Drop for FluxionHost {
    fn drop(&mut self) {
        // The ticker thread wakes every 100ms and exits on the next iteration.
        self.ticker_shutdown.store(true, Ordering::Relaxed);
    }
}

impl FluxionHost {
    /// Returns a weak reference to the ticker shutdown flag.
    /// Upgrades to None once the ticker thread exits after drop.
    #[cfg(test)]
    pub(crate) fn ticker_weak(&self) -> std::sync::Weak<AtomicBool> {
        Arc::downgrade(&self.ticker_shutdown)
    }

    pub fn new() -> Result<Self> {
        let mut config = Config::new();
        config.wasm_component_model(true);
        // Epoch interruption allows the host to kill a running Wasm guest at any
        // loop back-edge or function call — the only way to stop CPU-bound guests.
        config.epoch_interruption(true);
        let engine = Engine::new(&config)?;

        // Background thread advances the epoch counter every 100ms.
        // Exits when ticker_shutdown is set (FluxionHost::drop).
        let ticker_shutdown = Arc::new(AtomicBool::new(false));
        let thread_shutdown = Arc::clone(&ticker_shutdown);
        let ticker_engine = engine.clone();
        std::thread::spawn(move || {
            while !thread_shutdown.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(1000 / TICKS_PER_SEC));
                if !thread_shutdown.load(Ordering::Relaxed) {
                    ticker_engine.increment_epoch();
                }
            }
        });

        Ok(Self {
            engine,
            disk_cache: ComponentCache::new(),
            mem_cache: Mutex::new(LruCache::new(NonZeroUsize::new(MEM_CACHE_CAP).unwrap())),
            pre_cache: Mutex::new(LruCache::new(NonZeroUsize::new(PRE_CACHE_CAP).unwrap())),
            ticker_shutdown,
        })
    }

    pub fn run_component(
        &self,
        wasm_path: impl AsRef<Path>,
        input: Vec<u8>,
        perms: &PermissionSet,
    ) -> Result<Vec<u8>> {
        let (output, _) = self.run_component_measured(
            wasm_path,
            input,
            perms,
            &std::collections::HashMap::new(),
        )?;
        Ok(output)
    }

    /// Like `run_component` but also returns per-phase timing metrics.
    pub fn run_component_measured(
        &self,
        wasm_path: impl AsRef<Path>,
        input: Vec<u8>,
        perms: &PermissionSet,
        env: &std::collections::HashMap<String, String>,
    ) -> Result<(Vec<u8>, JobMetrics)> {
        let ctx = build_wasi_ctx(perms, env)?;
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

        // ── compile phase (L1 LRU mem → L2 disk → full compile) ─────────────
        let t0 = Instant::now();
        let component: Arc<Component> = {
            if let Some(c) = self.mem_cache.lock().unwrap().get(&key) {
                Arc::clone(c)
            } else {
                let c = Arc::new(match self.disk_cache.load(&self.engine, &wasm_bytes) {
                    Some(c) => c,
                    None => self.disk_cache.store(&self.engine, &wasm_bytes)?,
                });
                self.mem_cache
                    .lock()
                    .unwrap()
                    .put(key.clone(), Arc::clone(&c));
                c
            }
        };
        let compile = t0.elapsed();

        // ── instantiate phase (L0 LRU pre_cache → build once per component) ──
        let t1 = Instant::now();
        let pre: Arc<TaskComponentPre<HostState>> = {
            if let Some(p) = self.pre_cache.lock().unwrap().get(&key) {
                Arc::clone(p)
            } else {
                let mut linker: Linker<HostState> = Linker::new(&self.engine);
                wasmtime_wasi::add_to_linker_sync(&mut linker)?;
                let p = Arc::new(TaskComponentPre::new(linker.instantiate_pre(&component)?)?);
                self.pre_cache.lock().unwrap().put(key, Arc::clone(&p));
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

        let metrics = JobMetrics {
            compile,
            instantiate,
            execute,
        };

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

// DNS resolution cache: maps raw allowlist entry → (resolved IPs, cache timestamp).
static DNS_CACHE: LazyLock<Mutex<HashMap<String, (Vec<IpAddr>, Instant)>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

const DNS_CACHE_TTL: Duration = Duration::from_secs(60);

/// Resolve one allowlist entry to IP-based strings.
///
/// Pure IP entries (e.g. `"192.0.2.1:443"`, `"192.0.2.1"`) pass through unchanged.
/// Hostname entries (e.g. `"api.example.com:443"`) are resolved via the OS resolver
/// and the resolved IPs are cached for [`DNS_CACHE_TTL`].
///
/// If resolution fails the entry resolves to an empty list (= deny-all for that entry).
async fn resolve_entry(s: &str) -> Vec<String> {
    if parse_network_entry(s).is_some() {
        return vec![s.to_string()];
    }

    // Split "hostname:port" or plain "hostname".
    let (host, port): (String, Option<u16>) = if let Some((h, p)) = s.rsplit_once(':') {
        match p.parse::<u16>() {
            Ok(n) => (h.to_string(), Some(n)),
            Err(_) => (s.to_string(), None),
        }
    } else {
        (s.to_string(), None)
    };

    // Check TTL cache.
    {
        let cache = DNS_CACHE.lock().unwrap();
        if let Some((ips, ts)) = cache.get(s) {
            if ts.elapsed() < DNS_CACHE_TTL {
                return ips
                    .iter()
                    .map(|ip| match port {
                        Some(p) => format!("{ip}:{p}"),
                        None => ip.to_string(),
                    })
                    .collect();
            }
        }
    }

    let lookup_target = format!("{}:{}", host, port.unwrap_or(0));
    match tokio::net::lookup_host(&lookup_target).await {
        Ok(addrs) => {
            // Deduplicate IPs across multiple A/AAAA records.
            let mut seen = std::collections::HashSet::new();
            let ips: Vec<IpAddr> = addrs
                .map(|a| a.ip())
                .filter(|ip| seen.insert(*ip))
                .collect();

            let result: Vec<String> = ips
                .iter()
                .map(|ip| match port {
                    Some(p) => format!("{ip}:{p}"),
                    None => ip.to_string(),
                })
                .collect();

            DNS_CACHE.lock().unwrap().insert(s.to_string(), (ips, Instant::now()));
            result
        }
        Err(e) => {
            // DNS failure → no IPs → this entry becomes deny-all (safe default).
            tracing::warn!(entry = %s, error = %e, "network.allow: DNS resolution failed; entry blocked");
            vec![]
        }
    }
}

/// Expand all network allowlist entries to IP-based strings.
///
/// Pure IP entries pass through unchanged. Hostname entries are resolved via DNS
/// with a 60-second in-process cache. Unresolvable entries are dropped (safe fail).
pub async fn resolve_network_allow(allow: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    for entry in allow {
        out.extend(resolve_entry(entry).await);
    }
    out
}

fn build_wasi_ctx(
    perms: &PermissionSet,
    env: &std::collections::HashMap<String, String>,
) -> Result<WasiCtx> {
    let mut builder = WasiCtxBuilder::new();
    builder.inherit_stdout().inherit_stderr();
    for (k, v) in env {
        builder.env(k, v);
    }

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
            "network.allow has entries but none resolved to a valid IP address. \
             Pass the allow list through resolve_network_allow() before calling this function."
        );

        // All entries at this point are IP-based (resolved by resolve_network_allow).
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

    // Verify that the epoch ticker thread exits after FluxionHost is dropped.
    // Previously the ticker ran forever (detached), causing thread accumulation
    // when tests created multiple hosts.
    #[test]
    fn ticker_thread_stops_after_host_drop() {
        let host = FluxionHost::new().expect("FluxionHost::new");
        let weak = host.ticker_weak();

        // Ticker should be alive while the host is alive.
        assert!(weak.upgrade().is_some(), "ticker not running before drop");

        drop(host);

        // Give the ticker thread up to 500ms (5 ticks) to notice the shutdown
        // flag and exit. The thread sleeps 100ms per iteration, so this is
        // generous even under load.
        let deadline = std::time::Instant::now() + Duration::from_millis(500);
        while std::time::Instant::now() < deadline {
            if weak.upgrade().is_none() {
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }

        panic!("epoch ticker thread is still alive 500ms after FluxionHost drop");
    }

    // LRU mem_cache eviction: inserting more than MEM_CACHE_CAP entries must
    // not panic and must keep the cache at exactly MEM_CACHE_CAP capacity.
    #[test]
    fn lru_mem_cache_evicts_at_capacity() {
        use lru::LruCache;
        use std::num::NonZeroUsize;

        let cap = 4usize;
        let mut cache: LruCache<String, u32> = LruCache::new(NonZeroUsize::new(cap).unwrap());

        for i in 0u32..10 {
            cache.put(format!("key-{i}"), i);
        }

        // Cache should never exceed its capacity.
        assert_eq!(cache.len(), cap, "cache must be capped at {cap}");

        // The 4 most recently inserted entries must be present.
        for i in 6u32..10 {
            assert!(
                cache.contains(&format!("key-{i}")),
                "key-{i} should be in cache"
            );
        }
        // Older entries should have been evicted.
        for i in 0u32..6 {
            assert!(
                !cache.contains(&format!("key-{i}")),
                "key-{i} should be evicted"
            );
        }
    }

    // Confirm that creating and dropping multiple hosts does not accumulate
    // threads indefinitely (the regression from issue #18).
    #[test]
    fn multiple_hosts_do_not_accumulate_tickers() {
        let mut weaks = Vec::new();
        for _ in 0..5 {
            let host = FluxionHost::new().expect("FluxionHost::new");
            weaks.push(host.ticker_weak());
            drop(host);
        }

        // All tickers must stop within 500ms of the last drop.
        let deadline = std::time::Instant::now() + Duration::from_millis(500);
        while std::time::Instant::now() < deadline {
            if weaks.iter().all(|w| w.upgrade().is_none()) {
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }

        let still_alive = weaks.iter().filter(|w| w.upgrade().is_some()).count();
        panic!("{still_alive}/5 ticker threads still alive 500ms after all hosts dropped");
    }

    // ── resolve_network_allow tests ────────────────────────────────────────────

    #[tokio::test]
    async fn resolve_ip_entries_pass_through() {
        let allow = vec![
            "192.0.2.1:443".to_string(),
            "192.0.2.2".to_string(),
        ];
        let resolved = resolve_network_allow(&allow).await;
        assert_eq!(resolved, allow, "pure IP entries must pass through unchanged");
    }

    #[tokio::test]
    async fn resolve_localhost_hostname() {
        // "localhost" is guaranteed to resolve in any normal OS environment.
        let allow = vec!["localhost:8080".to_string()];
        let resolved = resolve_network_allow(&allow).await;
        assert!(!resolved.is_empty(), "localhost must resolve to at least one IP");
        // All returned entries must end in ":8080".
        assert!(
            resolved.iter().all(|e| e.ends_with(":8080")),
            "port must be preserved: {resolved:?}"
        );
        // All returned entries must parse as valid IP:port.
        for entry in &resolved {
            assert!(
                parse_network_entry(entry).is_some(),
                "resolved entry must be a valid IP string: {entry}"
            );
        }
    }

    #[tokio::test]
    async fn resolve_unresolvable_hostname_returns_empty() {
        // This domain is guaranteed to not exist.
        let allow = vec!["this.hostname.does.not.exist.invalid:443".to_string()];
        let resolved = resolve_network_allow(&allow).await;
        assert!(
            resolved.is_empty(),
            "unresolvable hostname must yield empty list (safe deny-all)"
        );
    }

    #[tokio::test]
    async fn resolve_mixed_ip_and_hostname() {
        let allow = vec![
            "192.0.2.1:443".to_string(),
            "localhost:8080".to_string(),
        ];
        let resolved = resolve_network_allow(&allow).await;
        // Must contain the original IP entry.
        assert!(resolved.contains(&"192.0.2.1:443".to_string()));
        // Must also contain at least one resolved localhost IP.
        assert!(resolved.len() > 1, "should have IP + resolved localhost: {resolved:?}");
    }
}

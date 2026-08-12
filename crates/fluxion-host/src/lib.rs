pub mod api;
pub mod cache;
pub mod metrics;
pub mod ui;
pub mod remote;
pub mod scheduler;
pub mod worker_registry;

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
        // Emit OTel audit event for the permission set being applied.
        tracing::info!(
            target: "fluxion.audit",
            fs_read  = ?perms.filesystem.read,
            fs_write = ?perms.filesystem.write,
            net_allow = ?perms.network.allow,
            memory_mb = perms.limits.memory_mb,
            timeout_secs = perms.limits.timeout_secs,
            "permission_grant"
        );

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
#[allow(clippy::type_complexity)]
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
        #[allow(clippy::collapsible_if)]
        if let Some((ips, ts)) = cache.get(s)
            && ts.elapsed() < DNS_CACHE_TTL
        {
            return ips
                .iter()
                .map(|ip| match port {
                    Some(p) => format!("{ip}:{p}"),
                    None => ip.to_string(),
                })
                .collect();
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

            DNS_CACHE
                .lock()
                .unwrap()
                .insert(s.to_string(), (ips, Instant::now()));
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

/// Resolve a DNS SRV record to a list of `http://host:port` worker URLs.
///
/// Returns an empty `Vec` (not an error) when the SRV query fails, so the
/// caller can fall back to the static worker list without interruption.
pub async fn resolve_srv_workers(srv_name: &str) -> Vec<String> {
    let srv = srv_name.to_string();
    tokio::task::spawn_blocking(move || resolve_srv_workers_sync(&srv))
        .await
        .unwrap_or_default()
}

fn resolve_srv_workers_sync(srv_name: &str) -> Vec<String> {
    use hickory_resolver::Resolver;
    let resolver = match Resolver::from_system_conf() {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(%srv_name, error = %e, "DNS SRV: resolver creation failed, using static workers");
            return vec![];
        }
    };
    match resolver.srv_lookup(srv_name) {
        Ok(lookup) => {
            let mut urls = Vec::new();
            for record in lookup.iter() {
                let target = record.target().to_utf8();
                let target = target.trim_end_matches('.');
                let port = record.port();
                urls.push(format!("http://{}:{}", target, port));
            }
            tracing::info!(%srv_name, count = urls.len(), "DNS SRV: resolved workers");
            urls
        }
        Err(e) => {
            tracing::warn!(%srv_name, error = %e, "DNS SRV: query failed, using static workers");
            vec![]
        }
    }
}

/// Verify that `wasm_path` matches the expected SHA-256 hex digest.
///
/// Returns `Ok(())` when digest matches or `expected` is `None`.
/// Returns `Err` when the file cannot be read or the digest does not match.
pub fn verify_component_digest(wasm_path: impl AsRef<Path>, expected: Option<&str>) -> Result<()> {
    let Some(expected_hex) = expected else {
        return Ok(());
    };
    use sha2::{Digest, Sha256};
    let bytes = std::fs::read(wasm_path.as_ref())
        .with_context(|| format!("failed to read {:?} for digest check", wasm_path.as_ref()))?;
    let hash = Sha256::digest(&bytes);
    let actual: String = hash.iter().map(|b| format!("{:02x}", b)).collect();
    anyhow::ensure!(
        actual.eq_ignore_ascii_case(expected_hex),
        "SHA-256 mismatch for {:?}: expected {}, got {}",
        wasm_path.as_ref(),
        expected_hex,
        actual
    );
    Ok(())
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
            if ok {
                tracing::info!(target: "fluxion.audit", %addr, "network_allow");
            } else {
                tracing::warn!(target: "fluxion.audit", %addr, "network_deny");
            }
            Box::pin(async move { ok })
        });
    }

    Ok(builder.build())
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── #67 SHA-256 component digest verification ─────────────────────────────

    #[test]
    fn verify_digest_passes_when_none() {
        // No expected hash → verification always passes.
        assert!(verify_component_digest("/nonexistent/path.wasm", None).is_ok());
    }

    #[test]
    fn verify_digest_fails_on_missing_file() {
        let result = verify_component_digest("/nonexistent/missing.wasm", Some("deadbeef"));
        assert!(result.is_err(), "missing file must fail");
        let msg = format!("{}", result.unwrap_err());
        assert!(
            msg.contains("failed to read"),
            "error should mention read failure: {msg}"
        );
    }

    #[test]
    fn verify_digest_passes_on_correct_hash() {
        use sha2::{Digest, Sha256};
        use std::io::Write;
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(b"hello wasm").unwrap();
        let hash = Sha256::digest(b"hello wasm");
        let hex: String = hash.iter().map(|b| format!("{:02x}", b)).collect();
        assert!(verify_component_digest(f.path(), Some(&hex)).is_ok());
    }

    #[test]
    fn verify_digest_fails_on_wrong_hash() {
        use std::io::Write;
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(b"hello wasm").unwrap();
        let result = verify_component_digest(
            f.path(),
            Some("0000000000000000000000000000000000000000000000000000000000000000"),
        );
        assert!(result.is_err(), "wrong hash must fail");
        let msg = format!("{}", result.unwrap_err());
        assert!(
            msg.contains("mismatch"),
            "error must mention mismatch: {msg}"
        );
    }

    #[test]
    fn verify_digest_is_case_insensitive() {
        use sha2::{Digest, Sha256};
        use std::io::Write;
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(b"case test").unwrap();
        let hash = Sha256::digest(b"case test");
        let upper_hex: String = hash.iter().map(|b| format!("{:02X}", b)).collect();
        assert!(
            verify_component_digest(f.path(), Some(&upper_hex)).is_ok(),
            "uppercase hex must match"
        );
    }

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
        let allow = vec!["192.0.2.1:443".to_string(), "192.0.2.2".to_string()];
        let resolved = resolve_network_allow(&allow).await;
        assert_eq!(
            resolved, allow,
            "pure IP entries must pass through unchanged"
        );
    }

    #[tokio::test]
    async fn resolve_localhost_hostname() {
        // "localhost" is guaranteed to resolve in any normal OS environment.
        let allow = vec!["localhost:8080".to_string()];
        let resolved = resolve_network_allow(&allow).await;
        assert!(
            !resolved.is_empty(),
            "localhost must resolve to at least one IP"
        );
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
    async fn dns_cache_hit_is_faster_than_miss() {
        let allow = vec!["localhost:9999".to_string()];

        // Warm up cache (first call = DNS lookup).
        let t0 = Instant::now();
        let _ = resolve_network_allow(&allow).await;
        let miss_us = t0.elapsed().as_micros();

        // Second call should hit the in-process TTL cache.
        let t1 = Instant::now();
        let _ = resolve_network_allow(&allow).await;
        let hit_us = t1.elapsed().as_micros();

        println!("DNS miss: {miss_us}µs  cache hit: {hit_us}µs");
        assert!(
            hit_us < miss_us,
            "cache hit ({hit_us}µs) should be faster than DNS miss ({miss_us}µs)"
        );
    }

    #[tokio::test]
    async fn resolve_mixed_ip_and_hostname() {
        let allow = vec!["192.0.2.1:443".to_string(), "localhost:8080".to_string()];
        let resolved = resolve_network_allow(&allow).await;
        // Must contain the original IP entry.
        assert!(resolved.contains(&"192.0.2.1:443".to_string()));
        // Must also contain at least one resolved localhost IP.
        assert!(
            resolved.len() > 1,
            "should have IP + resolved localhost: {resolved:?}"
        );
    }

    // ── Phase 1: アドレス形式の基本解決 (#78) ────────────────────────────────

    #[tokio::test]
    async fn resolve_hostname_without_port() {
        // "localhost" without a port should resolve to IP-only strings (no ":port" suffix).
        let allow = vec!["localhost".to_string()];
        let resolved = resolve_network_allow(&allow).await;
        assert!(
            !resolved.is_empty(),
            "localhost must resolve to at least one IP"
        );
        for entry in &resolved {
            // Must parse as a valid IP (AnyPort variant, no port suffix).
            assert!(
                parse_network_entry(entry).is_some(),
                "resolved entry must be a valid IP string: {entry}"
            );
            // Must NOT contain a colon that would indicate an IP:port (IPv4 case).
            // IPv6 addresses may contain colons but parse_network_entry handles them.
            if let Ok(ip) = entry.parse::<IpAddr>() {
                // Plain IP — correct.
                let _ = ip;
            } else if entry.starts_with('[') {
                // IPv6 with brackets — also acceptable as AnyPort.
            } else {
                panic!("unexpected format for port-less entry: {entry}");
            }
        }
    }

    #[tokio::test]
    async fn resolve_ipv6_entries_pass_through() {
        let cases = vec!["[::1]:8080".to_string(), "::1".to_string()];
        for input in &cases {
            let resolved = resolve_network_allow(&[input.clone()]).await;
            assert_eq!(
                resolved,
                vec![input.clone()],
                "IPv6 entry {input:?} must pass through unchanged"
            );
            assert!(
                parse_network_entry(input).is_some(),
                "IPv6 entry must be parseable by parse_network_entry: {input}"
            );
        }
    }

    // ── Phase 2: 失敗・混在エントリの安全 fallback (#79) ─────────────────────

    #[tokio::test]
    async fn resolve_mixed_entries_with_failure() {
        let allow = vec![
            "192.0.2.1:443".to_string(),
            "localhost:8080".to_string(),
            "this.invalid.host.example:443".to_string(),
        ];
        let resolved = resolve_network_allow(&allow).await;

        // IP entry must be present unchanged.
        assert!(
            resolved.contains(&"192.0.2.1:443".to_string()),
            "IP entry must survive: {resolved:?}"
        );
        // At least one localhost IP:8080 must be present.
        assert!(
            resolved.iter().any(|e| e.ends_with(":8080")),
            "resolved localhost entry must be present: {resolved:?}"
        );
        // Unresolvable entry must NOT appear in any form.
        assert!(
            !resolved.iter().any(|e| e.contains("invalid.host")),
            "unresolvable hostname must be excluded: {resolved:?}"
        );
        // All returned entries must parse as valid IP addresses.
        for entry in &resolved {
            assert!(
                parse_network_entry(entry).is_some(),
                "every entry in result must be a valid IP string: {entry}"
            );
        }
    }

    // ── Phase 3: TTL キャッシュ期限切れ後の再解決 (#80) ──────────────────────

    #[tokio::test]
    async fn dns_cache_expires_after_ttl() {
        let key = "localhost:19191".to_string();
        let allow = vec![key.clone()];

        // First call — populates cache.
        let first = resolve_network_allow(&allow).await;
        assert!(!first.is_empty(), "first resolution must succeed");

        // Backdate the cache entry to simulate TTL expiry.
        {
            let mut cache = DNS_CACHE.lock().unwrap();
            if let Some(entry) = cache.get_mut(&key) {
                entry.1 = Instant::now() - DNS_CACHE_TTL - Duration::from_millis(1);
            }
        }

        // Second call must re-resolve (not serve stale data).
        let second = resolve_network_allow(&allow).await;
        assert!(
            !second.is_empty(),
            "re-resolution after TTL expiry must succeed"
        );

        // Verify the cache timestamp was refreshed.
        {
            let cache = DNS_CACHE.lock().unwrap();
            let age = cache
                .get(&key)
                .map(|(_, ts)| ts.elapsed())
                .unwrap_or(Duration::MAX);
            assert!(
                age < Duration::from_secs(5),
                "cache timestamp must be refreshed after TTL expiry, age={age:?}"
            );
        }
    }

    // ── Phase 4: 並行解決の Mutex 安全性 (#81) ───────────────────────────────

    #[tokio::test]
    async fn resolve_concurrent_same_host() {
        let allow = vec!["localhost:12345".to_string()];

        let handles: Vec<_> = (0..10)
            .map(|_| {
                let a = allow.clone();
                tokio::spawn(async move { resolve_network_allow(&a).await })
            })
            .collect();

        let results = futures::future::join_all(handles).await;
        for (i, r) in results.iter().enumerate() {
            let resolved = r.as_ref().expect("task must not panic");
            assert!(
                !resolved.is_empty(),
                "task {i} returned empty result — concurrent resolution failed"
            );
            // All entries must be valid IP:port strings.
            for entry in resolved {
                assert!(
                    parse_network_entry(entry).is_some(),
                    "task {i} returned invalid entry: {entry}"
                );
            }
        }

        // All tasks must agree on the same IP set.
        let first_set: std::collections::HashSet<_> =
            results[0].as_ref().unwrap().iter().cloned().collect();
        for (i, r) in results.iter().enumerate().skip(1) {
            let set: std::collections::HashSet<_> = r.as_ref().unwrap().iter().cloned().collect();
            assert_eq!(
                first_set, set,
                "task {i} returned different IPs than task 0: {set:?} vs {first_set:?}"
            );
        }
    }
}

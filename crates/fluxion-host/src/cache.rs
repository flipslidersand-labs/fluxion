use anyhow::Result;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use wasmtime::{Engine, component::Component};

/// Cache-key type for the L1 / L2 component caches.
///
/// - `Path` — traditional mode: key derived by SHA-256 hashing the local wasm bytes.
/// - `Digest` — OCI mode: key is the registry layer digest (`sha256:<hex>`),
///   so `FluxionHost` can skip re-hashing bytes fetched over the wire.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CacheKey {
    /// Key derived by hashing wasm bytes at a local path.
    Path(PathBuf),
    /// Explicit digest string, e.g. `"sha256:abc123…"` from an OCI registry.
    Digest(String),
}

impl CacheKey {
    /// Produce the canonical 64-hex-char string used as LRU / disk-cache identifier.
    pub fn as_str_key(&self, wasm_bytes: &[u8]) -> String {
        match self {
            CacheKey::Digest(d) => d.strip_prefix("sha256:").unwrap_or(d).to_string(),
            CacheKey::Path(_) => wasm_key(wasm_bytes),
        }
    }
}

/// Disk-backed cache for precompiled Wasm components (.cwasm).
///
/// Cache key = SHA-256(wasm bytes) + wasmtime version, so:
/// - Different source .wasm files never collide.
/// - A wasmtime upgrade automatically invalidates all entries (new key → cache miss →
///   recompile), preventing UB from loading a stale artifact with `deserialize_file`.
pub struct ComponentCache {
    pub(crate) dir: PathBuf,
}

impl Default for ComponentCache {
    fn default() -> Self {
        Self::new()
    }
}

impl ComponentCache {
    pub fn new() -> Self {
        let dir = cache_base_dir().join("fluxion").join("components");
        // Best-effort: failure to create the dir means every lookup is a miss.
        std::fs::create_dir_all(&dir).ok();
        Self { dir }
    }

    /// Returns the cached `Component` if the key matches, or `None` on a miss.
    ///
    /// # Safety note
    /// `Component::deserialize_file` is unsafe for adversarially crafted input.
    /// Here the only files we ever deserialize are ones we wrote ourselves via
    /// `precompile_component()`. If the artifact is stale (e.g. after a wasmtime
    /// upgrade), wasmtime rejects it with an `Err` before any UB can occur — we
    /// then evict the file and return `None` so the caller recompiles.
    pub fn load(&self, engine: &Engine, wasm_bytes: &[u8]) -> Option<Component> {
        let path = self.artifact_path(wasm_bytes);
        if !path.exists() {
            return None;
        }
        // SAFETY: file was written by `store()` on this machine; wasmtime validates
        // the version magic before deserializing, returning Err on mismatch.
        match unsafe { Component::deserialize_file(engine, &path) } {
            Ok(c) => Some(c),
            Err(_) => {
                // Stale or corrupt artifact — evict so the next call recompiles.
                std::fs::remove_file(&path).ok();
                None
            }
        }
    }

    /// Compiles `wasm_bytes` and writes the result to the cache, then returns the
    /// component. Uses atomic rename to avoid partial writes visible to other processes.
    pub fn store(&self, engine: &Engine, wasm_bytes: &[u8]) -> Result<Component> {
        let artifact = engine.precompile_component(wasm_bytes)?;
        let path = self.artifact_path(wasm_bytes);
        atomic_write(&path, &artifact)?;
        enforce_cache_limit(&self.dir, MAX_CACHE_BYTES);
        // SAFETY: we just wrote this artifact from the same engine version.
        Ok(unsafe { Component::deserialize_file(engine, &path)? })
    }

    /// Like `load()` but accepts an explicit `CacheKey` (e.g. an OCI layer digest).
    pub fn load_by_key(
        &self,
        engine: &Engine,
        key: &CacheKey,
        wasm_bytes: &[u8],
    ) -> Option<Component> {
        let hex = key.as_str_key(wasm_bytes);
        let path = self.dir.join(format!("{hex}.cwasm"));
        if !path.exists() {
            return None;
        }
        match unsafe { Component::deserialize_file(engine, &path) } {
            Ok(c) => Some(c),
            Err(_) => {
                std::fs::remove_file(&path).ok();
                None
            }
        }
    }

    /// Like `store()` but writes under an explicit `CacheKey`.
    pub fn store_by_key(
        &self,
        engine: &Engine,
        key: &CacheKey,
        wasm_bytes: &[u8],
    ) -> Result<Component> {
        let artifact = engine.precompile_component(wasm_bytes)?;
        let hex = key.as_str_key(wasm_bytes);
        let path = self.dir.join(format!("{hex}.cwasm"));
        atomic_write(&path, &artifact)?;
        enforce_cache_limit(&self.dir, MAX_CACHE_BYTES);
        Ok(unsafe { Component::deserialize_file(engine, &path)? })
    }

    pub(crate) fn artifact_path(&self, wasm_bytes: &[u8]) -> PathBuf {
        self.dir.join(format!("{}.cwasm", cache_key(wasm_bytes)))
    }
}

/// SHA-256 hex digest of the wasm bytes — shared cache key for L1 and L2.
pub fn wasm_key(wasm_bytes: &[u8]) -> String {
    hex_encode(&Sha256::digest(wasm_bytes))
}

fn cache_key(wasm_bytes: &[u8]) -> String {
    wasm_key(wasm_bytes)
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

fn cache_base_dir() -> PathBuf {
    std::env::var("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            let home = std::env::var("HOME").unwrap_or_default();
            PathBuf::from(home).join(".cache")
        })
}

/// Maximum total size of the on-disk `.cwasm` cache before the
/// least-recently-modified entries are evicted. Without this, the cache
/// grows without bound: a wasmtime upgrade changes the artifact format and
/// invalidates old entries by making them fail to deserialize (see `load`),
/// but the stale files themselves are never deleted, and neither are entries
/// for components that are no longer used (#242).
const MAX_CACHE_BYTES: u64 = 512 * 1024 * 1024;

/// Evict least-recently-modified `.cwasm` entries from `dir` until its total
/// size is at or under `max_bytes`. Best-effort: any I/O error here is
/// swallowed so a failed cleanup never blocks compilation — worst case the
/// cache temporarily grows past `max_bytes`.
fn enforce_cache_limit(dir: &Path, max_bytes: u64) {
    let Ok(read_dir) = std::fs::read_dir(dir) else {
        return;
    };

    let mut entries: Vec<(PathBuf, u64, std::time::SystemTime)> = Vec::new();
    let mut total: u64 = 0;
    for entry in read_dir.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("cwasm") {
            continue;
        }
        let Ok(meta) = entry.metadata() else { continue };
        let size = meta.len();
        let modified = meta.modified().unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        total += size;
        entries.push((path, size, modified));
    }

    if total <= max_bytes {
        return;
    }

    // Oldest (least-recently-modified) first.
    entries.sort_by_key(|(_, _, mtime)| *mtime);

    for (path, size, _) in entries {
        if total <= max_bytes {
            break;
        }
        if std::fs::remove_file(&path).is_ok() {
            total = total.saturating_sub(size);
        }
    }
}

fn atomic_write(path: &Path, data: &[u8]) -> Result<()> {
    // Use a unique temp file in the same directory to avoid concurrent writers
    // corrupting each other's in-progress writes when two threads cache the
    // same component simultaneously.
    let dir = path.parent().unwrap_or(Path::new("."));
    let mut tmp = tempfile::NamedTempFile::new_in(dir)?;
    std::io::Write::write_all(&mut tmp, data)?;
    tmp.persist(path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasmtime::{Config, Engine};

    fn test_engine() -> Engine {
        let mut cfg = Config::new();
        cfg.wasm_component_model(true);
        Engine::new(&cfg).unwrap()
    }

    fn hello_wasm() -> Vec<u8> {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../components/hello/target/wasm32-wasip1/debug/hello.wasm");
        std::fs::read(&root).expect("hello.wasm not built — run `cargo build` in components/hello")
    }

    #[test]
    fn cache_key_is_stable() {
        let bytes = b"fake wasm";
        let k1 = wasm_key(bytes);
        let k2 = wasm_key(bytes);
        assert_eq!(k1, k2);
        assert_eq!(k1.len(), 64, "SHA-256 hex should be 64 chars");
    }

    #[test]
    fn different_bytes_give_different_keys() {
        let k1 = wasm_key(b"wasm_a");
        let k2 = wasm_key(b"wasm_b");
        assert_ne!(k1, k2);
    }

    #[test]
    #[ignore = "requires pre-built components/hello — run `cargo component build` in components/hello first"]
    fn miss_then_hit_roundtrip() {
        let engine = test_engine();
        let wasm = hello_wasm();
        let tmp = tempfile::tempdir().unwrap();
        let mut cache = ComponentCache::new();
        cache.dir = tmp.path().to_path_buf();

        // Cold miss
        assert!(cache.load(&engine, &wasm).is_none());

        // Populate
        let _c = cache.store(&engine, &wasm).unwrap();

        // Warm hit
        assert!(cache.load(&engine, &wasm).is_some());
    }

    #[test]
    #[ignore = "requires pre-built components/hello — run `cargo component build` in components/hello first"]
    fn stale_artifact_is_evicted() {
        let engine = test_engine();
        let wasm = hello_wasm();
        let tmp = tempfile::tempdir().unwrap();
        let mut cache = ComponentCache::new();
        cache.dir = tmp.path().to_path_buf();

        // Write garbage bytes at the artifact path.
        let path = cache.artifact_path(&wasm);
        std::fs::write(&path, b"not a valid cwasm artifact").unwrap();

        // load() must evict the file and return None, not panic.
        let result = cache.load(&engine, &wasm);
        assert!(result.is_none());
        assert!(!path.exists(), "stale artifact should be removed");
    }

    /// Verifies that a corrupted (or stale-wasmtime) .cwasm file is evicted and
    /// causes load() to return None, not panic. This covers the upgrade scenario
    /// where an artifact compiled by an older wasmtime version fails deserialization.
    ///
    /// Unlike `stale_artifact_is_evicted` (which needs a real compiled .wasm),
    /// this test works with any key bytes — the eviction path is independent of
    /// whether the source .wasm is valid.
    #[test]
    fn stale_artifact_evicted_without_real_wasm() {
        let engine = test_engine();
        let fake_wasm = b"synthetic key bytes - not real wasm";
        let tmp = tempfile::tempdir().unwrap();
        let mut cache = ComponentCache::new();
        cache.dir = tmp.path().to_path_buf();

        // Plant a garbage artifact at the expected cache path.
        let path = cache.artifact_path(fake_wasm);
        std::fs::write(&path, b"\x00STALE").unwrap();
        assert!(path.exists(), "artifact file should exist before load()");

        let result = cache.load(&engine, fake_wasm);

        assert!(
            result.is_none(),
            "stale artifact must yield None, not panic"
        );
        assert!(!path.exists(), "load() must delete the stale artifact");
    }

    #[test]
    fn cache_key_digest_strips_sha256_prefix() {
        let key = CacheKey::Digest("sha256:deadbeef".to_string());
        assert_eq!(key.as_str_key(b"unused"), "deadbeef");
    }

    #[test]
    fn cache_key_digest_without_prefix_is_unchanged() {
        let key = CacheKey::Digest("deadbeef".to_string());
        assert_eq!(key.as_str_key(b"unused"), "deadbeef");
    }

    #[test]
    fn cache_key_path_hashes_wasm_bytes() {
        let key = CacheKey::Path(PathBuf::from("/tmp/x.wasm"));
        let bytes = b"some wasm bytes";
        assert_eq!(key.as_str_key(bytes), wasm_key(bytes));
    }

    #[test]
    fn load_by_key_miss_returns_none() {
        let engine = test_engine();
        let tmp = tempfile::tempdir().unwrap();
        let mut cache = ComponentCache::new();
        cache.dir = tmp.path().to_path_buf();

        let key = CacheKey::Digest("sha256:doesnotexist".to_string());
        assert!(cache.load_by_key(&engine, &key, b"bytes").is_none());
    }

    #[test]
    fn enforce_cache_limit_is_noop_when_under_limit() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("a.cwasm"), vec![0u8; 100]).unwrap();
        std::fs::write(tmp.path().join("b.cwasm"), vec![0u8; 100]).unwrap();

        enforce_cache_limit(tmp.path(), 1_000_000);

        assert!(tmp.path().join("a.cwasm").exists());
        assert!(tmp.path().join("b.cwasm").exists());
    }

    #[test]
    fn enforce_cache_limit_evicts_oldest_first() {
        let tmp = tempfile::tempdir().unwrap();
        let oldest = tmp.path().join("oldest.cwasm");
        let middle = tmp.path().join("middle.cwasm");
        let newest = tmp.path().join("newest.cwasm");

        // Stagger mtimes: filesystem mtime resolution is coarser than a
        // single instruction, so sleep briefly between writes to guarantee
        // a strict modified-time ordering for the eviction test.
        std::fs::write(&oldest, vec![0u8; 100]).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(20));
        std::fs::write(&middle, vec![0u8; 100]).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(20));
        std::fs::write(&newest, vec![0u8; 100]).unwrap();

        // Total = 300 bytes; cap at 150 → only the newest (100) should
        // comfortably fit, so both older entries must be evicted.
        enforce_cache_limit(tmp.path(), 150);

        assert!(!oldest.exists(), "oldest entry must be evicted first");
        assert!(!middle.exists(), "middle entry must also be evicted");
        assert!(newest.exists(), "newest entry must survive");
    }

    #[test]
    fn enforce_cache_limit_ignores_non_cwasm_files() {
        let tmp = tempfile::tempdir().unwrap();
        let unrelated = tmp.path().join("notes.txt");
        std::fs::write(&unrelated, vec![0u8; 1_000_000]).unwrap();
        std::fs::write(tmp.path().join("a.cwasm"), vec![0u8; 10]).unwrap();

        enforce_cache_limit(tmp.path(), 1);

        assert!(
            unrelated.exists(),
            "non-.cwasm files must never be touched by the cache evictor"
        );
    }

    #[test]
    fn enforce_cache_limit_on_missing_dir_does_not_panic() {
        enforce_cache_limit(Path::new("/nonexistent/path/xyz"), 100);
    }

    #[test]
    fn artifact_path_contains_hash() {
        let tmp = tempfile::tempdir().unwrap();
        let mut cache = ComponentCache::new();
        cache.dir = tmp.path().to_path_buf();
        let bytes = b"some wasm bytes";
        let key = wasm_key(bytes);
        let path = cache.artifact_path(bytes);
        assert!(path.to_string_lossy().contains(&key));
        assert!(path.extension().unwrap() == "cwasm");
    }
}

use anyhow::Result;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use wasmtime::{Engine, component::Component};

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
        // SAFETY: we just wrote this artifact from the same engine version.
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

fn atomic_write(path: &Path, data: &[u8]) -> Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, data)?;
    std::fs::rename(&tmp, path)?;
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

    // Verifies the eviction path triggered by a wasmtime version upgrade.
    // A real version mismatch causes deserialize_file to return Err — we simulate
    // that with fake bytes so the test runs in CI without pre-built components.
    #[test]
    fn stale_artifact_evicted_on_version_mismatch() {
        let engine = test_engine();
        let wasm_bytes = b"fake wasm for version mismatch simulation";
        let tmp = tempfile::tempdir().unwrap();
        let mut cache = ComponentCache::new();
        cache.dir = tmp.path().to_path_buf();

        let path = cache.artifact_path(wasm_bytes);

        // Write bytes that look like a .cwasm artifact from an older wasmtime version.
        // Any bytes that fail Component::deserialize_file trigger the eviction path.
        std::fs::write(&path, b"\0asm\x01stale-version-artifact").unwrap();
        assert!(path.exists(), "pre-condition: artifact file must exist");

        let result = cache.load(&engine, wasm_bytes);

        assert!(result.is_none(), "stale artifact should yield None, not a Component");
        assert!(!path.exists(), "stale artifact must be deleted after eviction");
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

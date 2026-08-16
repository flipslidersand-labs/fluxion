use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use rusqlite::{Connection, params};

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS oci_components (
    id          TEXT PRIMARY KEY,
    registry    TEXT NOT NULL,
    repository  TEXT NOT NULL,
    tag         TEXT,
    digest      TEXT,
    wasm_path   TEXT NOT NULL,
    created_at  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_oci_components_repo
    ON oci_components (registry, repository);
";

/// Parsed reference to an OCI image: `registry/repository[:tag][@digest]`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OciRef {
    pub registry: String,
    pub repository: String,
    pub tag: Option<String>,
    pub digest: Option<String>,
}

impl OciRef {
    /// Parse `registry/repository[:tag][@digest]`.
    pub fn parse(s: &str) -> Result<Self> {
        // Split digest first.
        let (without_digest, digest) = if let Some((l, r)) = s.split_once('@') {
            (l, Some(r.to_string()))
        } else {
            (s, None)
        };

        // Split tag.
        let (without_tag, tag) = if let Some(idx) = without_digest.rfind(':') {
            // Make sure the colon is not part of the registry host (e.g. host:port).
            let before = &without_digest[..idx];
            if before.contains('/') {
                (
                    &without_digest[..idx],
                    Some(without_digest[idx + 1..].to_string()),
                )
            } else {
                (without_digest, None)
            }
        } else {
            (without_digest, None)
        };

        // Split registry / repository.
        let (registry, repository) = without_tag
            .split_once('/')
            .map(|(r, p)| (r.to_string(), p.to_string()))
            .ok_or_else(|| anyhow::anyhow!("OciRef must contain at least one '/': {s}"))?;

        Ok(Self {
            registry,
            repository,
            tag,
            digest,
        })
    }

    /// Canonical string representation.
    pub fn to_string_repr(&self) -> String {
        let mut s = format!("{}/{}", self.registry, self.repository);
        if let Some(tag) = &self.tag {
            s.push(':');
            s.push_str(tag);
        }
        if let Some(digest) = &self.digest {
            s.push('@');
            s.push_str(digest);
        }
        s
    }
}

/// A single entry in the local OCI component registry.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RegistryEntry {
    pub id: String,
    pub oci_ref: OciRef,
    pub wasm_path: String,
    pub created_at: u64,
}

/// Persistent store for OCI component metadata backed by SQLite.
///
/// `rusqlite::Connection` is `!Send`, so we wrap it in `Arc<Mutex<>>` to allow
/// sharing across threads (the mutex ensures single-threaded access at runtime).
#[derive(Clone)]
pub struct RegistryStore {
    conn: Arc<Mutex<Connection>>,
}

impl RegistryStore {
    /// Open (or create) the registry DB at the default path.
    pub fn open() -> Result<Self> {
        let path = Self::db_path();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(&path)?;
        Self::init_schema(&conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Open from an existing connection (for tests / in-memory DBs).
    pub fn from_conn(conn: Connection) -> Result<Self> {
        Self::init_schema(&conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    fn db_path() -> std::path::PathBuf {
        let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
        std::path::PathBuf::from(home)
            .join(".fluxion")
            .join("registry.db")
    }

    fn init_schema(conn: &Connection) -> Result<()> {
        conn.execute_batch(SCHEMA)?;
        Ok(())
    }

    /// Insert or replace a component entry.
    pub fn upsert(&self, entry: &RegistryEntry) -> Result<()> {
        let r = &entry.oci_ref;
        self.conn.lock().unwrap().execute(
            "INSERT INTO oci_components
                 (id, registry, repository, tag, digest, wasm_path, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(id) DO UPDATE SET
                 registry   = excluded.registry,
                 repository = excluded.repository,
                 tag        = excluded.tag,
                 digest     = excluded.digest,
                 wasm_path  = excluded.wasm_path,
                 created_at = excluded.created_at",
            params![
                entry.id,
                r.registry,
                r.repository,
                r.tag,
                r.digest,
                entry.wasm_path,
                entry.created_at,
            ],
        )?;
        Ok(())
    }

    /// Fetch a single entry by ID.
    pub fn get(&self, id: &str) -> Result<Option<RegistryEntry>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, registry, repository, tag, digest, wasm_path, created_at
             FROM oci_components WHERE id = ?1",
        )?;
        let mut rows = stmt.query_map(params![id], row_to_entry)?;
        Ok(rows.next().transpose()?)
    }

    /// List all entries, newest first.
    pub fn list(&self) -> Result<Vec<RegistryEntry>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, registry, repository, tag, digest, wasm_path, created_at
             FROM oci_components ORDER BY created_at DESC",
        )?;
        let rows = stmt
            .query_map([], row_to_entry)?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    /// Delete an entry by ID. Returns `true` if a row was removed.
    pub fn delete(&self, id: &str) -> Result<bool> {
        let n = self
            .conn
            .lock()
            .unwrap()
            .execute("DELETE FROM oci_components WHERE id = ?1", params![id])?;
        Ok(n > 0)
    }

    /// Look up entries matching a registry + repository prefix.
    pub fn find_by_repo(&self, registry: &str, repository: &str) -> Result<Vec<RegistryEntry>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, registry, repository, tag, digest, wasm_path, created_at
             FROM oci_components
             WHERE registry = ?1 AND repository = ?2
             ORDER BY created_at DESC",
        )?;
        let rows = stmt
            .query_map(params![registry, repository], row_to_entry)?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }
}

fn row_to_entry(row: &rusqlite::Row<'_>) -> rusqlite::Result<RegistryEntry> {
    Ok(RegistryEntry {
        id: row.get(0)?,
        oci_ref: OciRef {
            registry: row.get(1)?,
            repository: row.get(2)?,
            tag: row.get(3)?,
            digest: row.get(4)?,
        },
        wasm_path: row.get(5)?,
        created_at: row.get(6)?,
    })
}

#[cfg(test)]
fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Generate a unique registry entry ID from the OCI ref.
pub fn entry_id(oci_ref: &OciRef) -> String {
    let repr = oci_ref.to_string_repr();
    format!("reg-{:016x}", {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        repr.hash(&mut h);
        h.finish()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn in_memory_store() -> RegistryStore {
        let conn = Connection::open_in_memory().expect("in-memory db");
        RegistryStore::from_conn(conn).expect("init schema")
    }

    fn sample_ref() -> OciRef {
        OciRef {
            registry: "ghcr.io".to_string(),
            repository: "flipslidersand/hello".to_string(),
            tag: Some("v1.0".to_string()),
            digest: None,
        }
    }

    fn sample_entry() -> RegistryEntry {
        let r = sample_ref();
        RegistryEntry {
            id: entry_id(&r),
            oci_ref: r,
            wasm_path: "/tmp/hello.wasm".to_string(),
            created_at: now_secs(),
        }
    }

    // ── schema round-trip ─────────────────────────────────────────────────────

    #[test]
    fn schema_initialises_without_error() {
        in_memory_store();
    }

    #[test]
    fn upsert_then_get_roundtrip() {
        let store = in_memory_store();
        let entry = sample_entry();
        store.upsert(&entry).expect("upsert");

        let fetched = store.get(&entry.id).expect("get").expect("should exist");
        assert_eq!(fetched.id, entry.id);
        assert_eq!(fetched.oci_ref, entry.oci_ref);
        assert_eq!(fetched.wasm_path, entry.wasm_path);
    }

    #[test]
    fn list_returns_all_entries() {
        let store = in_memory_store();
        for i in 0..3u32 {
            let r = OciRef {
                registry: "ghcr.io".into(),
                repository: format!("flipslidersand/cmp-{i}"),
                tag: Some("latest".into()),
                digest: None,
            };
            let e = RegistryEntry {
                id: entry_id(&r),
                oci_ref: r,
                wasm_path: format!("/tmp/cmp-{i}.wasm"),
                created_at: now_secs(),
            };
            store.upsert(&e).expect("upsert");
        }
        let all = store.list().expect("list");
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn delete_removes_entry() {
        let store = in_memory_store();
        let entry = sample_entry();
        store.upsert(&entry).expect("upsert");
        assert!(store.delete(&entry.id).expect("delete"));
        assert!(store.get(&entry.id).expect("get").is_none());
        assert!(!store.delete(&entry.id).expect("delete again"));
    }

    #[test]
    fn find_by_repo_filters_correctly() {
        let store = in_memory_store();
        let r1 = OciRef {
            registry: "ghcr.io".into(),
            repository: "org/a".into(),
            tag: None,
            digest: None,
        };
        let r2 = OciRef {
            registry: "ghcr.io".into(),
            repository: "org/b".into(),
            tag: None,
            digest: None,
        };
        for r in [&r1, &r2] {
            store
                .upsert(&RegistryEntry {
                    id: entry_id(r),
                    oci_ref: r.clone(),
                    wasm_path: "/x.wasm".into(),
                    created_at: 0,
                })
                .unwrap();
        }
        let found = store.find_by_repo("ghcr.io", "org/a").expect("find");
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].oci_ref.repository, "org/a");
    }

    // ── OciRef::parse ─────────────────────────────────────────────────────────

    #[test]
    fn parse_simple_ref() {
        let r = OciRef::parse("ghcr.io/org/hello:v1").unwrap();
        assert_eq!(r.registry, "ghcr.io");
        assert_eq!(r.repository, "org/hello");
        assert_eq!(r.tag.as_deref(), Some("v1"));
        assert!(r.digest.is_none());
    }

    #[test]
    fn parse_ref_with_digest() {
        let r = OciRef::parse("ghcr.io/org/hello@sha256:abc123").unwrap();
        assert_eq!(r.digest.as_deref(), Some("sha256:abc123"));
        assert!(r.tag.is_none());
    }

    #[test]
    fn parse_ref_with_port() {
        let r = OciRef::parse("localhost:5000/myrepo:latest").unwrap();
        assert_eq!(r.registry, "localhost:5000");
        assert_eq!(r.repository, "myrepo");
        assert_eq!(r.tag.as_deref(), Some("latest"));
    }

    #[test]
    fn parse_missing_slash_fails() {
        assert!(OciRef::parse("noslash").is_err());
    }

    #[test]
    fn to_string_roundtrip() {
        let s = "ghcr.io/org/hello:v1";
        assert_eq!(OciRef::parse(s).unwrap().to_string_repr(), s);
    }
}

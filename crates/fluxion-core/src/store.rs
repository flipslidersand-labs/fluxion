use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use rusqlite::{Connection, params};

use crate::state::JobStatus;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS schedules (
    id            TEXT PRIMARY KEY,
    workflow_path TEXT NOT NULL,
    cron_expr     TEXT NOT NULL,
    created_at    INTEGER NOT NULL,
    last_run_at   INTEGER,
    next_run_at   INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS runs (
    id            TEXT PRIMARY KEY,
    workflow_name TEXT NOT NULL,
    workflow_path TEXT NOT NULL,
    started_at    INTEGER NOT NULL,
    completed_at  INTEGER,
    status        TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS job_states (
    run_id    TEXT NOT NULL,
    job_id    TEXT NOT NULL,
    status    TEXT NOT NULL,
    elapsed_ms INTEGER,
    reason    TEXT,
    PRIMARY KEY (run_id, job_id)
);
CREATE TABLE IF NOT EXISTS workers (
    url            TEXT PRIMARY KEY,
    registered_at  INTEGER NOT NULL,
    last_health    TEXT
);
";

pub struct RunStore {
    conn: Connection,
}

impl RunStore {
    pub fn open() -> Result<Self> {
        let db_path = Self::db_path();
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(&db_path)?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self { conn })
    }

    fn db_path() -> PathBuf {
        let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
        PathBuf::from(home).join(".fluxion").join("runs.db")
    }

    pub fn new_run_id() -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        format!("run-{ms}-{pid}-{seq}")
    }

    pub fn create_run(
        &self,
        run_id: &str,
        workflow_name: &str,
        workflow_path: &Path,
    ) -> Result<()> {
        let now = now_secs();
        self.conn.execute(
            "INSERT INTO runs (id, workflow_name, workflow_path, started_at, status)
             VALUES (?1, ?2, ?3, ?4, 'running')",
            params![
                run_id,
                workflow_name,
                workflow_path.to_string_lossy().as_ref(),
                now
            ],
        )?;
        Ok(())
    }

    pub fn upsert_job(&self, run_id: &str, job_id: &str, status: &JobStatus) -> Result<()> {
        let (label, elapsed_ms, reason) = serialize_status(status);
        self.conn.execute(
            "INSERT INTO job_states (run_id, job_id, status, elapsed_ms, reason)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(run_id, job_id) DO UPDATE SET
               status = excluded.status,
               elapsed_ms = excluded.elapsed_ms,
               reason = excluded.reason",
            params![run_id, job_id, label, elapsed_ms, reason],
        )?;
        Ok(())
    }

    pub fn complete_run(&self, run_id: &str, success: bool) -> Result<()> {
        let status = if success { "succeeded" } else { "failed" };
        let now = now_secs();
        self.conn.execute(
            "UPDATE runs SET status = ?1, completed_at = ?2 WHERE id = ?3",
            params![status, now, run_id],
        )?;
        Ok(())
    }

    /// Returns the workflow path and a map of job_id → JobStatus for a previous run.
    pub fn load_run(&self, run_id: &str) -> Result<(String, HashMap<String, JobStatus>)> {
        let workflow_path: String = self
            .conn
            .query_row(
                "SELECT workflow_path FROM runs WHERE id = ?1",
                params![run_id],
                |row| row.get(0),
            )
            .map_err(|_| anyhow::anyhow!("Run '{}' not found", run_id))?;

        let mut stmt = self.conn.prepare(
            "SELECT job_id, status, elapsed_ms, reason FROM job_states WHERE run_id = ?1",
        )?;

        let jobs = stmt
            .query_map(params![run_id], |row| {
                let job_id: String = row.get(0)?;
                let status: String = row.get(1)?;
                let elapsed_ms: Option<u64> = row.get(2)?;
                let reason: Option<String> = row.get(3)?;
                Ok((job_id, status, elapsed_ms, reason))
            })?
            .filter_map(|r| r.ok())
            .map(|(job_id, status, elapsed_ms, reason)| {
                let elapsed = Duration::from_millis(elapsed_ms.unwrap_or(0));
                let js = match status.as_str() {
                    "succeeded" => JobStatus::Succeeded { elapsed },
                    "failed" => JobStatus::Failed {
                        elapsed,
                        reason: reason.unwrap_or_default(),
                    },
                    "cancelled" => JobStatus::Cancelled,
                    "skipped" => JobStatus::Skipped,
                    _ => JobStatus::Pending,
                };
                (job_id, js)
            })
            .collect();

        Ok((workflow_path, jobs))
    }

    /// Fetch metadata for a single run.
    pub fn get_run(&self, run_id: &str) -> Result<RunDetail> {
        self.conn
            .query_row(
                "SELECT id, workflow_name, workflow_path, started_at, completed_at, status
                 FROM runs WHERE id = ?1",
                params![run_id],
                |row| {
                    Ok(RunDetail {
                        id: row.get(0)?,
                        workflow_name: row.get(1)?,
                        workflow_path: row.get(2)?,
                        started_at: row.get(3)?,
                        completed_at: row.get(4)?,
                        status: row.get(5)?,
                    })
                },
            )
            .map_err(|_| anyhow::anyhow!("Run '{}' not found", run_id))
    }

    /// Fetch all job records for a run in insertion order.
    pub fn get_run_jobs(&self, run_id: &str) -> Result<Vec<JobDetail>> {
        let mut stmt = self.conn.prepare(
            "SELECT job_id, status, elapsed_ms, reason
             FROM job_states WHERE run_id = ?1 ORDER BY rowid",
        )?;
        let jobs = stmt
            .query_map(params![run_id], |row| {
                Ok(JobDetail {
                    job_id: row.get(0)?,
                    status: row.get(1)?,
                    elapsed_ms: row.get(2)?,
                    reason: row.get(3)?,
                })
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(jobs)
    }

    /// Delete runs older than `before_days` days and their job records.
    /// Returns the number of runs deleted.
    pub fn prune(&self, before_days: u64) -> Result<usize> {
        let cutoff = now_secs().saturating_sub(before_days * 86400);
        // Delete orphaned job_states first (no FK cascade in schema).
        self.conn.execute(
            "DELETE FROM job_states WHERE run_id IN (
                SELECT id FROM runs WHERE started_at < ?1
             )",
            params![cutoff],
        )?;
        let deleted = self
            .conn
            .execute("DELETE FROM runs WHERE started_at < ?1", params![cutoff])?;
        Ok(deleted)
    }

    /// List recent runs, newest first.
    pub fn list_runs(&self, limit: usize) -> Result<Vec<RunSummary>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, workflow_name, started_at, status FROM runs
             ORDER BY started_at DESC LIMIT ?1",
        )?;
        let rows = stmt
            .query_map(params![limit as i64], |row| {
                Ok(RunSummary {
                    id: row.get(0)?,
                    workflow_name: row.get(1)?,
                    started_at: row.get(2)?,
                    status: row.get(3)?,
                })
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }
}

// ── Worker registry ───────────────────────────────────────────────────────────

#[derive(serde::Serialize)]
pub struct WorkerEntry {
    pub url: String,
    pub registered_at: u64,
    pub last_health: Option<String>,
}

impl RunStore {
    pub fn register_worker(&self, url: &str) -> Result<()> {
        let now = now_secs();
        self.conn.execute(
            "INSERT INTO workers (url, registered_at, last_health)
             VALUES (?1, ?2, NULL)
             ON CONFLICT(url) DO NOTHING",
            params![url, now],
        )?;
        Ok(())
    }

    pub fn remove_worker(&self, url: &str) -> Result<usize> {
        let n = self
            .conn
            .execute("DELETE FROM workers WHERE url = ?1", params![url])?;
        Ok(n)
    }

    pub fn list_workers(&self) -> Result<Vec<WorkerEntry>> {
        let mut stmt = self.conn.prepare(
            "SELECT url, registered_at, last_health FROM workers ORDER BY registered_at",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok(WorkerEntry {
                    url: row.get(0)?,
                    registered_at: row.get(1)?,
                    last_health: row.get(2)?,
                })
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    pub fn update_worker_health(&self, url: &str, healthy: bool) -> Result<()> {
        let status = if healthy { "healthy" } else { "unreachable" };
        self.conn.execute(
            "UPDATE workers SET last_health = ?1 WHERE url = ?2",
            params![status, url],
        )?;
        Ok(())
    }

    /// Return URLs of all registered workers that are not marked unreachable.
    pub fn registered_worker_urls(&self) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare(
            "SELECT url FROM workers WHERE last_health IS NULL OR last_health = 'healthy'
             ORDER BY registered_at",
        )?;
        let urls = stmt
            .query_map([], |row| row.get(0))?
            .filter_map(|r| r.ok())
            .collect();
        Ok(urls)
    }
}

// ── Schedule registry ─────────────────────────────────────────────────────────

#[derive(serde::Serialize)]
pub struct ScheduleEntry {
    pub id: String,
    pub workflow_path: String,
    pub cron_expr: String,
    pub created_at: u64,
    pub last_run_at: Option<u64>,
    pub next_run_at: u64,
}

impl RunStore {
    pub fn add_schedule(
        &self,
        id: &str,
        workflow_path: &str,
        cron_expr: &str,
        next_run_at: u64,
    ) -> Result<()> {
        let now = now_secs();
        self.conn.execute(
            "INSERT INTO schedules (id, workflow_path, cron_expr, created_at, next_run_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![id, workflow_path, cron_expr, now, next_run_at],
        )?;
        Ok(())
    }

    pub fn remove_schedule(&self, id: &str) -> Result<usize> {
        let n = self
            .conn
            .execute("DELETE FROM schedules WHERE id = ?1", params![id])?;
        Ok(n)
    }

    pub fn list_schedules(&self) -> Result<Vec<ScheduleEntry>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, workflow_path, cron_expr, created_at, last_run_at, next_run_at
             FROM schedules ORDER BY next_run_at",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok(ScheduleEntry {
                    id: row.get(0)?,
                    workflow_path: row.get(1)?,
                    cron_expr: row.get(2)?,
                    created_at: row.get(3)?,
                    last_run_at: row.get(4)?,
                    next_run_at: row.get(5)?,
                })
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    /// Return schedules whose `next_run_at` ≤ `now_secs`.
    pub fn due_schedules(&self) -> Result<Vec<ScheduleEntry>> {
        let now = now_secs();
        let mut stmt = self.conn.prepare(
            "SELECT id, workflow_path, cron_expr, created_at, last_run_at, next_run_at
             FROM schedules WHERE next_run_at <= ?1 ORDER BY next_run_at",
        )?;
        let rows = stmt
            .query_map(params![now], |row| {
                Ok(ScheduleEntry {
                    id: row.get(0)?,
                    workflow_path: row.get(1)?,
                    cron_expr: row.get(2)?,
                    created_at: row.get(3)?,
                    last_run_at: row.get(4)?,
                    next_run_at: row.get(5)?,
                })
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    /// Update `last_run_at` and `next_run_at` after a schedule fires.
    pub fn update_schedule_next(&self, id: &str, last_run_at: u64, next_run_at: u64) -> Result<()> {
        self.conn.execute(
            "UPDATE schedules SET last_run_at = ?1, next_run_at = ?2 WHERE id = ?3",
            params![last_run_at, next_run_at, id],
        )?;
        Ok(())
    }

    /// Atomically claim a schedule for execution using optimistic locking.
    ///
    /// Updates `next_run_at` to `new_next` only if it still equals `old_next`.
    /// Returns `true` when this process won the claim, `false` when another
    /// process already advanced `next_run_at` (i.e. duplicate execution avoided).
    pub fn claim_schedule(&self, id: &str, old_next: u64, new_next: u64) -> Result<bool> {
        let affected = self.conn.execute(
            "UPDATE schedules SET next_run_at = ?1 WHERE id = ?2 AND next_run_at = ?3",
            params![new_next, id, old_next],
        )?;
        Ok(affected > 0)
    }
}

#[derive(serde::Serialize)]
pub struct RunSummary {
    pub id: String,
    pub workflow_name: String,
    pub started_at: u64,
    pub status: String,
}

#[derive(serde::Serialize)]
pub struct RunDetail {
    pub id: String,
    pub workflow_name: String,
    pub workflow_path: String,
    pub started_at: u64,
    pub completed_at: Option<u64>,
    pub status: String,
}

#[derive(serde::Serialize)]
pub struct JobDetail {
    pub job_id: String,
    pub status: String,
    pub elapsed_ms: Option<u64>,
    pub reason: Option<String>,
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn open_tmp() -> RunStore {
        // Each test gets its own in-file DB via tempfile (rusqlite needs a real file for bundled).
        let f = NamedTempFile::new().unwrap();
        let conn = Connection::open(f.path()).unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        // Keep tempfile alive for the duration of the test by leaking it intentionally.
        // tempfile deletes on drop; we need the file to outlive the connection.
        std::mem::forget(f);
        RunStore { conn }
    }

    // ── #30 prune — before: method does not exist → compile error
    //               after:  old rows deleted, recent rows kept ───────────────

    // ── #30 prune — before: prune() method does not exist
    //               after:  old rows deleted, recent rows kept ───────────────
    //
    // These tests call store.prune() which is added in the #30 PR.
    // Enable (remove todo!/panic) once the method is implemented.

    #[test]
    fn prune_deletes_old_runs_and_keeps_recent() {
        let store = open_tmp();
        let old_id = "run-old";
        let new_id = "run-new";

        let old_ts = now_secs() - 31 * 86400;
        store
            .conn
            .execute(
                "INSERT INTO runs (id, workflow_name, workflow_path, started_at, status) \
             VALUES (?1, 'wf', 'wf.yaml', ?2, 'succeeded')",
                params![old_id, old_ts],
            )
            .unwrap();
        store
            .create_run(new_id, "wf", std::path::Path::new("wf.yaml"))
            .unwrap();

        let deleted = store.prune(30).unwrap();
        assert_eq!(deleted, 1, "exactly one old run should be pruned");

        let runs = store.list_runs(10).unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].id, new_id);
    }

    #[test]
    fn prune_also_deletes_orphaned_job_states() {
        let store = open_tmp();
        let old_id = "run-orphan";
        let old_ts = now_secs() - 40 * 86400;

        store
            .conn
            .execute(
                "INSERT INTO runs (id, workflow_name, workflow_path, started_at, status) \
             VALUES (?1, 'wf', 'wf.yaml', ?2, 'succeeded')",
                params![old_id, old_ts],
            )
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO job_states (run_id, job_id, status) VALUES (?1, 'fetch', 'succeeded')",
                params![old_id],
            )
            .unwrap();

        store.prune(30).unwrap();

        let count: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM job_states WHERE run_id = ?1",
                params![old_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            count, 0,
            "orphaned job_states must be deleted with their run"
        );
    }

    // ── claim_schedule — optimistic locking ──────────────────────────────────

    fn insert_schedule(store: &RunStore, id: &str, next_run_at: u64) {
        store
            .conn
            .execute(
                "INSERT INTO schedules (id, workflow_path, cron_expr, created_at, next_run_at) \
                 VALUES (?1, 'wf.yaml', '0 * * * * *', 0, ?2)",
                params![id, next_run_at],
            )
            .unwrap();
    }

    #[test]
    fn claim_schedule_succeeds_when_next_matches() {
        let store = open_tmp();
        insert_schedule(&store, "s1", 100);
        let won = store.claim_schedule("s1", 100, 200).unwrap();
        assert!(won, "claim should succeed when old_next matches");
        let row: u64 = store
            .conn
            .query_row(
                "SELECT next_run_at FROM schedules WHERE id = 's1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(row, 200, "next_run_at must be advanced to new_next");
    }

    #[test]
    fn claim_schedule_fails_when_already_claimed() {
        let store = open_tmp();
        insert_schedule(&store, "s2", 100);
        // First claim advances next_run_at to 200.
        store.claim_schedule("s2", 100, 200).unwrap();
        // Second claim with the original old_next should lose.
        let won = store.claim_schedule("s2", 100, 300).unwrap();
        assert!(!won, "second claim with stale old_next must return false");
    }

    #[test]
    fn claim_schedule_fails_for_missing_id() {
        let store = open_tmp();
        let won = store.claim_schedule("nonexistent", 0, 100).unwrap();
        assert!(!won, "claim on unknown id must return false");
    }
}

fn serialize_status(s: &JobStatus) -> (&'static str, Option<u64>, Option<String>) {
    match s {
        JobStatus::Succeeded { elapsed } => ("succeeded", Some(elapsed.as_millis() as u64), None),
        JobStatus::Failed { elapsed, reason } => (
            "failed",
            Some(elapsed.as_millis() as u64),
            Some(reason.clone()),
        ),
        JobStatus::Cancelled => ("cancelled", None, None),
        JobStatus::Running => ("running", None, None),
        JobStatus::Skipped => ("skipped", None, None),
        _ => ("pending", None, None),
    }
}

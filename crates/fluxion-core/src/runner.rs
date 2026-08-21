use std::time::Duration;

use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct RunResult {
    pub run_id: String,
    pub workflow_name: String,
    pub jobs: Vec<JobResult>,
    pub total_elapsed_ms: u64,
    pub succeeded: usize,
    pub total: usize,
    pub success: bool,
}

#[derive(Debug, Serialize)]
pub struct JobResult {
    pub job_id: String,
    pub status: String,
    pub elapsed_ms: u64,
    pub reason: Option<String>,
    pub skipped: bool,
    /// Wasm compile phase (0 = not measured or skipped job).
    pub compile_us: u64,
    /// Component instantiate phase.
    pub instantiate_us: u64,
    /// Guest execute phase.
    pub execute_us: u64,
}

impl RunResult {
    pub fn summary(&self) -> String {
        format!(
            "Run {} — {}/{} jobs succeeded in {:.2}s",
            self.run_id,
            self.succeeded,
            self.total,
            self.total_elapsed_ms as f64 / 1000.0
        )
    }
}

impl JobResult {
    pub fn from_succeeded(job_id: String, elapsed: Duration, skipped: bool) -> Self {
        Self {
            job_id,
            status: "succeeded".into(),
            elapsed_ms: elapsed.as_millis() as u64,
            reason: None,
            skipped,
            compile_us: 0,
            instantiate_us: 0,
            execute_us: 0,
        }
    }

    pub fn from_succeeded_with_metrics(
        job_id: String,
        elapsed: Duration,
        compile_us: u64,
        instantiate_us: u64,
        execute_us: u64,
    ) -> Self {
        Self {
            job_id,
            status: "succeeded".into(),
            elapsed_ms: elapsed.as_millis() as u64,
            reason: None,
            skipped: false,
            compile_us,
            instantiate_us,
            execute_us,
        }
    }

    pub fn from_skipped(job_id: String) -> Self {
        Self {
            job_id,
            status: "skipped".into(),
            elapsed_ms: 0,
            reason: None,
            skipped: true,
            compile_us: 0,
            instantiate_us: 0,
            execute_us: 0,
        }
    }

    pub fn from_failed(job_id: String, elapsed: Duration, reason: String) -> Self {
        Self {
            job_id,
            status: "failed".into(),
            elapsed_ms: elapsed.as_millis() as u64,
            reason: Some(reason),
            skipped: false,
            compile_us: 0,
            instantiate_us: 0,
            execute_us: 0,
        }
    }
}

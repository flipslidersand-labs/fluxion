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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn make_run_result(succeeded: usize, total: usize, elapsed_ms: u64) -> RunResult {
        RunResult {
            run_id: "run-001".into(),
            workflow_name: "my-wf".into(),
            jobs: vec![],
            total_elapsed_ms: elapsed_ms,
            succeeded,
            total,
            success: succeeded == total,
        }
    }

    #[test]
    fn summary_includes_run_id_and_counts() {
        let r = make_run_result(3, 4, 2500);
        let s = r.summary();
        assert!(s.contains("run-001"));
        assert!(s.contains("3/4"));
        assert!(s.contains("2.50s"));
    }

    #[test]
    fn from_succeeded_sets_status_and_elapsed() {
        let j = JobResult::from_succeeded("step-a".into(), Duration::from_millis(123), false);
        assert_eq!(j.status, "succeeded");
        assert_eq!(j.elapsed_ms, 123);
        assert!(!j.skipped);
        assert!(j.reason.is_none());
        assert_eq!(j.compile_us, 0);
    }

    #[test]
    fn from_succeeded_with_metrics_sets_timing_fields() {
        let j = JobResult::from_succeeded_with_metrics(
            "step-b".into(),
            Duration::from_millis(500),
            100,
            200,
            300,
        );
        assert_eq!(j.status, "succeeded");
        assert!(!j.skipped);
        assert_eq!(j.compile_us, 100);
        assert_eq!(j.instantiate_us, 200);
        assert_eq!(j.execute_us, 300);
    }

    #[test]
    fn from_skipped_sets_skipped_true_and_zero_elapsed() {
        let j = JobResult::from_skipped("step-c".into());
        assert_eq!(j.status, "skipped");
        assert!(j.skipped);
        assert_eq!(j.elapsed_ms, 0);
        assert!(j.reason.is_none());
    }

    #[test]
    fn from_failed_sets_reason_and_status() {
        let j = JobResult::from_failed(
            "step-d".into(),
            Duration::from_millis(50),
            "timeout".into(),
        );
        assert_eq!(j.status, "failed");
        assert!(!j.skipped);
        assert_eq!(j.reason.as_deref(), Some("timeout"));
        assert_eq!(j.elapsed_ms, 50);
    }
}

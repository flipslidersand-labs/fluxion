use std::time::Duration;

#[derive(Debug, Clone, PartialEq)]
pub enum JobStatus {
    Pending,
    Ready,
    Running,
    Succeeded { elapsed: Duration },
    Failed { elapsed: Duration, reason: String },
    Cancelled,
    Skipped, // when: condition was false, or a dependency was skipped
}

impl JobStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Succeeded { .. } | Self::Failed { .. } | Self::Cancelled | Self::Skipped
        )
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Ready => "READY",
            Self::Running => "RUNNING",
            Self::Succeeded { .. } => "SUCCESS",
            Self::Failed { .. } => "FAILED",
            Self::Cancelled => "CANCELLED",
            Self::Skipped => "SKIPPED",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_terminal_states() {
        assert!(!JobStatus::Pending.is_terminal());
        assert!(!JobStatus::Ready.is_terminal());
        assert!(!JobStatus::Running.is_terminal());
    }

    #[test]
    fn terminal_states() {
        assert!(
            JobStatus::Succeeded {
                elapsed: Duration::ZERO
            }
            .is_terminal()
        );
        assert!(
            JobStatus::Failed {
                elapsed: Duration::ZERO,
                reason: "err".into()
            }
            .is_terminal()
        );
        assert!(JobStatus::Cancelled.is_terminal());
        assert!(JobStatus::Skipped.is_terminal());
    }

    #[test]
    fn labels_match_expected_strings() {
        assert_eq!(JobStatus::Pending.label(), "PENDING");
        assert_eq!(JobStatus::Ready.label(), "READY");
        assert_eq!(JobStatus::Running.label(), "RUNNING");
        assert_eq!(
            JobStatus::Succeeded {
                elapsed: Duration::ZERO
            }
            .label(),
            "SUCCESS"
        );
        assert_eq!(
            JobStatus::Failed {
                elapsed: Duration::ZERO,
                reason: String::new()
            }
            .label(),
            "FAILED"
        );
        assert_eq!(JobStatus::Cancelled.label(), "CANCELLED");
        assert_eq!(JobStatus::Skipped.label(), "SKIPPED");
    }
}

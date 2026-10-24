//! Worker-side permission policy (#275).
//!
//! The `PermissionSet` in a request body is client-supplied, so the worker
//! checks it against an operator-configured ceiling before running anything.
//! A request that exceeds the policy is rejected with 403.

use fluxion_core::workflow::PermissionSet;
use std::path::{Component, Path, PathBuf};

/// Operator-configured ceiling on client-supplied permissions.
/// `Default` is unrestricted (backward compatible).
#[derive(Debug, Clone, Default)]
pub struct WorkerPolicy {
    /// When non-empty, every `filesystem.{read,write}` path must resolve
    /// (symlinks followed) to somewhere under one of these roots.
    pub allow_fs_roots: Vec<PathBuf>,
    /// Maximum `limits.memory_mb` a request may ask for.
    pub max_memory_mb: Option<u64>,
    /// Reject any request with a non-empty `network.allow`.
    pub deny_network: bool,
}

impl WorkerPolicy {
    /// True when no restriction is configured.
    pub fn is_unrestricted(&self) -> bool {
        self.allow_fs_roots.is_empty() && self.max_memory_mb.is_none() && !self.deny_network
    }

    /// Canonicalize the configured roots once, at startup.
    pub fn normalized(mut self) -> Self {
        self.allow_fs_roots = self
            .allow_fs_roots
            .into_iter()
            .map(|r| r.canonicalize().unwrap_or(r))
            .collect();
        self
    }

    /// Check `perms` against the policy. `Err` carries a client-safe message.
    pub fn check(&self, perms: &PermissionSet) -> Result<(), String> {
        if let Some(max) = self.max_memory_mb {
            if perms.limits.memory_mb > max {
                return Err(format!(
                    "limits.memory_mb {} exceeds worker maximum {max}",
                    perms.limits.memory_mb
                ));
            }
        }
        if self.deny_network && !perms.network.allow.is_empty() {
            return Err("network access is denied by worker policy".into());
        }
        if !self.allow_fs_roots.is_empty() {
            for p in perms.filesystem.read.iter().chain(&perms.filesystem.write) {
                if !self.fs_path_allowed(p) {
                    return Err(format!(
                        "filesystem path {} is outside the worker's allowed roots",
                        p.display()
                    ));
                }
            }
        }
        Ok(())
    }

    fn fs_path_allowed(&self, path: &Path) -> bool {
        if !path.is_absolute() || path.components().any(|c| matches!(c, Component::ParentDir)) {
            return false;
        }
        // Resolve the deepest existing ancestor (follows symlinks) and re-append
        // the not-yet-existing tail, which holds only normal components here.
        let mut existing = path;
        let mut tail = Vec::new();
        while !existing.exists() {
            match (existing.file_name(), existing.parent()) {
                (Some(name), Some(parent)) => {
                    tail.push(name);
                    existing = parent;
                }
                _ => return false,
            }
        }
        let Ok(mut resolved) = existing.canonicalize() else {
            return false;
        };
        for part in tail.iter().rev() {
            resolved.push(part);
        }
        self.allow_fs_roots.iter().any(|r| resolved.starts_with(r))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn perms(read: &[&Path], write: &[&Path]) -> PermissionSet {
        let mut p = PermissionSet::default();
        p.filesystem.read = read.iter().map(|x| x.to_path_buf()).collect();
        p.filesystem.write = write.iter().map(|x| x.to_path_buf()).collect();
        p
    }

    #[test]
    fn default_policy_allows_everything() {
        let pol = WorkerPolicy::default();
        assert!(pol.is_unrestricted());
        assert!(pol
            .check(&perms(&[Path::new("/")], &[Path::new("/etc")]))
            .is_ok());
    }

    #[test]
    fn fs_roots_confine_paths() {
        let base = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        let pol = WorkerPolicy {
            allow_fs_roots: vec![base.path().to_path_buf()],
            ..Default::default()
        }
        .normalized();
        let inside = base.path().join("new/sub");
        assert!(pol.check(&perms(&[], &[&inside])).is_ok());
        assert!(pol.check(&perms(&[other.path()], &[])).is_err());
        assert!(pol.check(&perms(&[], &[Path::new("/")])).is_err());
        let dotdot = base.path().join("../x");
        assert!(pol.check(&perms(&[&dotdot], &[])).is_err());
        assert!(pol.check(&perms(&[Path::new("rel")], &[])).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn fs_roots_reject_symlink_escape() {
        let base = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        let link = base.path().join("link");
        std::os::unix::fs::symlink(other.path(), &link).unwrap();
        let pol = WorkerPolicy {
            allow_fs_roots: vec![base.path().to_path_buf()],
            ..Default::default()
        }
        .normalized();
        assert!(pol.check(&perms(&[], &[&link])).is_err());
        assert!(pol.check(&perms(&[], &[&link.join("sub")])).is_err());
    }

    #[test]
    fn memory_and_network_limits() {
        let pol = WorkerPolicy {
            max_memory_mb: Some(128),
            deny_network: true,
            ..Default::default()
        };
        let mut p = PermissionSet::default(); // 256 MB default
        assert!(pol.check(&p).is_err());
        p.limits.memory_mb = 64;
        assert!(pol.check(&p).is_ok());
        p.network.allow = vec!["127.0.0.1:80".into()];
        assert!(pol.check(&p).is_err());
    }
}

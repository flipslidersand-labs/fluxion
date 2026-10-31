//! Where fluxion keeps its on-disk state (run history, registry, CAS, cache).
//!
//! Single place that reads `HOME`, so callers and tests can redirect state with
//! `FLUXION_HOME` instead of rewriting the process-wide `HOME` (#292). When no
//! usable location can be resolved this is an error, never a silent write into
//! the current directory.

use std::ffi::OsString;
use std::path::PathBuf;

use anyhow::{Result, bail};

fn non_empty(v: Option<OsString>) -> Option<OsString> {
    v.filter(|s| !s.is_empty())
}

/// Resolve the data directory from explicit inputs (pure, no env access):
/// `fluxion_home` if set, else `<home>/.fluxion`. With neither, an error that
/// names `FLUXION_HOME` as the way out.
pub fn data_dir_from(fluxion_home: Option<OsString>, home: Option<OsString>) -> Result<PathBuf> {
    if let Some(dir) = non_empty(fluxion_home) {
        return Ok(PathBuf::from(dir));
    }
    match non_empty(home) {
        Some(home) => Ok(PathBuf::from(home).join(".fluxion")),
        None => bail!(
            "cannot locate the fluxion data directory: HOME is not set; \
             set FLUXION_HOME to the directory to use"
        ),
    }
}

/// Resolve the on-disk component cache base from explicit inputs (pure):
/// `xdg_cache_home`, else `<home>/.cache`, else (no HOME, e.g. a container)
/// `<fluxion_home>/cache`. Otherwise an error.
pub fn cache_dir_from(
    xdg_cache_home: Option<OsString>,
    home: Option<OsString>,
    fluxion_home: Option<OsString>,
) -> Result<PathBuf> {
    if let Some(dir) = non_empty(xdg_cache_home) {
        return Ok(PathBuf::from(dir));
    }
    if let Some(home) = non_empty(home) {
        return Ok(PathBuf::from(home).join(".cache"));
    }
    match non_empty(fluxion_home) {
        Some(dir) => Ok(PathBuf::from(dir).join("cache")),
        None => bail!(
            "cannot locate the fluxion cache directory: neither XDG_CACHE_HOME nor HOME is \
             set; set FLUXION_HOME (the cache then lives in <FLUXION_HOME>/cache)"
        ),
    }
}

/// The only place `HOME` is read.
fn home() -> Option<OsString> {
    match std::env::var("HOME") {
        Ok(h) => Some(OsString::from(h)),
        Err(std::env::VarError::NotUnicode(h)) => Some(h),
        Err(std::env::VarError::NotPresent) => None,
    }
}

/// Data directory for the current process: `$FLUXION_HOME`, else `$HOME/.fluxion`;
/// an error when neither is set.
pub fn data_dir() -> Result<PathBuf> {
    data_dir_from(std::env::var_os("FLUXION_HOME"), home())
}

/// Component-cache base directory for the current process (see [`cache_dir_from`]).
pub fn cache_dir() -> Result<PathBuf> {
    cache_dir_from(
        std::env::var_os("XDG_CACHE_HOME"),
        home(),
        std::env::var_os("FLUXION_HOME"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn os(s: &str) -> Option<OsString> {
        Some(OsString::from(s))
    }

    #[test]
    fn fluxion_home_wins_over_home() {
        assert_eq!(
            data_dir_from(os("/data/fx"), os("/home/u")).unwrap(),
            PathBuf::from("/data/fx")
        );
    }

    #[test]
    fn falls_back_to_home_dot_fluxion() {
        assert_eq!(
            data_dir_from(None, os("/home/u")).unwrap(),
            PathBuf::from("/home/u/.fluxion")
        );
    }

    #[test]
    fn empty_fluxion_home_is_ignored() {
        assert_eq!(
            data_dir_from(os(""), os("/home/u")).unwrap(),
            PathBuf::from("/home/u/.fluxion")
        );
    }

    #[test]
    fn unset_home_is_an_error_that_points_at_fluxion_home() {
        for home in [None, os("")] {
            let err = data_dir_from(None, home).unwrap_err().to_string();
            assert!(err.contains("HOME is not set"), "{err}");
            assert!(err.contains("FLUXION_HOME"), "{err}");
        }
    }

    #[test]
    fn fluxion_home_is_the_escape_hatch_when_home_is_unset() {
        assert_eq!(
            data_dir_from(os("/data/fx"), None).unwrap(),
            PathBuf::from("/data/fx")
        );
    }

    #[test]
    fn cache_dir_prefers_xdg_then_home_then_fluxion_home() {
        assert_eq!(
            cache_dir_from(os("/xdg"), os("/home/u"), os("/fx")).unwrap(),
            PathBuf::from("/xdg")
        );
        assert_eq!(
            cache_dir_from(None, os("/home/u"), os("/fx")).unwrap(),
            PathBuf::from("/home/u/.cache")
        );
        assert_eq!(
            cache_dir_from(os(""), None, os("/fx")).unwrap(),
            PathBuf::from("/fx/cache")
        );
    }

    #[test]
    fn cache_dir_without_any_location_is_an_error() {
        let err = cache_dir_from(None, None, None).unwrap_err().to_string();
        assert!(err.contains("FLUXION_HOME"), "{err}");
    }
}

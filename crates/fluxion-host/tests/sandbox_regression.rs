//! Sandbox regression tests (#312).
//!
//! These pin the security-relevant behaviour of the wasmtime/wasmtime-wasi
//! integration (epoch interruption, `StoreLimits`, WASI filesystem preopens and
//! the socket address check) so a runtime upgrade that silently changes any of
//! them fails CI. Like `e2e.rs` they need pre-built components and are gated on
//! the `ci` feature.

use std::collections::HashMap;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use fluxion_core::workflow::PermissionSet;
use fluxion_host::FluxionHost;

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root")
}

fn component(name: &str, target: &str) -> PathBuf {
    workspace_root().join("components").join(name).join(format!(
        "target/{target}/debug/{}.wasm",
        name.replace('-', "_")
    ))
}

fn perms(memory_mb: u64, timeout_secs: u64) -> PermissionSet {
    let mut p = PermissionSet::default();
    p.limits.memory_mb = memory_mb;
    p.limits.timeout_secs = timeout_secs;
    p
}

// ── epoch interruption ────────────────────────────────────────────────────────

/// The host advances the epoch every 100ms (`TICKS_PER_SEC = 10`) and sets the
/// deadline to `timeout_secs * 10` ticks, so a spinning guest is interrupted
/// somewhere in `((timeout*10 - 1) * 100ms, timeout*10 * 100ms]` after the
/// deadline is set (the first tick lands 0..100ms after `set_epoch_deadline`).
///
/// The deadline is set *before* compile/instantiate, and compile time counts
/// against it (#267), so the component is warmed up first (memory cache) and the
/// timing is taken around a second, warm run. Bounds are deliberately loose on
/// the upper side for loaded CI runners: lower = timeout - 200ms (one tick of
/// phase + one of scheduling jitter), upper = timeout + 2s.
#[test]
#[cfg_attr(not(feature = "ci"), ignore = "requires pre-built Wasm components")]
fn epoch_interrupt_fires_near_timeout() {
    let host = FluxionHost::new().unwrap();
    let spin = component("spin", "wasm32-wasip1");
    let env = HashMap::new();

    // Warm-up: a short run populates mem/pre caches so the timed run has no compile.
    let (out, _) = host
        .run_component_measured(&spin, b"1000".to_vec(), &perms(64, 30), &env)
        .expect("short spin must finish");
    assert!(String::from_utf8_lossy(&out).starts_with("sum="));

    for timeout_secs in [1u64, 2] {
        let t = Instant::now();
        let err = host
            .run_component_measured(&spin, b"".to_vec(), &perms(64, timeout_secs), &env)
            .expect_err("infinite spin must be interrupted");
        let elapsed = t.elapsed();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("epoch interrupt"),
            "timeout must be classified as an epoch interrupt, got: {msg}"
        );
        let want = Duration::from_secs(timeout_secs);
        assert!(
            elapsed >= want - Duration::from_millis(200),
            "interrupted too early: {elapsed:?} for timeout {timeout_secs}s"
        );
        assert!(
            elapsed <= want + Duration::from_secs(2),
            "interrupted too late: {elapsed:?} for timeout {timeout_secs}s"
        );
    }
}

// ── memory_mb boundary ────────────────────────────────────────────────────────

fn alloc(host: &FluxionHost, memory_mb: u64, alloc_mb: u64) -> anyhow::Result<Vec<u8>> {
    host.run_component(
        component("alloc-bomb", "wasm32-wasip1"),
        alloc_mb.to_string().into_bytes(),
        &perms(memory_mb, 30),
    )
}

/// Largest whole-MB allocation that succeeds under `memory_mb`.
fn max_alloc_mb(host: &FluxionHost, memory_mb: u64) -> u64 {
    (1..=memory_mb + 4)
        .take_while(|mb| alloc(host, memory_mb, *mb).is_ok())
        .last()
        .unwrap_or(0)
}

/// `memory_mb` must be an exact ceiling on guest linear memory: an allocation
/// that fits (limit minus the guest's own baseline) succeeds, and the first
/// allocation that does not fit fails. The guest baseline (stack, data, allocator
/// metadata) is well under 2MB, so the largest passing allocation must land in
/// `[limit-2, limit-1]`, and an allocation equal to the limit must fail.
#[test]
#[cfg_attr(not(feature = "ci"), ignore = "requires pre-built Wasm components")]
fn memory_limit_boundary() {
    let host = FluxionHost::new().unwrap();
    for limit in [8u64, 16] {
        let max = max_alloc_mb(&host, limit);
        assert!(
            (limit - 2..limit).contains(&max),
            "limit {limit}MB: largest passing allocation was {max}MB (expected {}..{})",
            limit - 2,
            limit - 1
        );
        // Just under the ceiling still works.
        assert!(alloc(&host, limit, max).is_ok());
        // At and above the ceiling: refused, and every refusal is memory-related.
        for over in [max + 1, limit, limit + 4] {
            let err = alloc(&host, limit, over)
                .expect_err(&format!("{over}MB must exceed the {limit}MB limit"));
            let msg = format!("{err:#}").to_lowercase();
            assert!(
                msg.contains("oom") || msg.contains("memory"),
                "limit {limit}MB / alloc {over}MB failed for a non-memory reason: {msg}"
            );
        }
    }
}

// ── network allowlist ─────────────────────────────────────────────────────────

fn probe_connect(host: &FluxionHost, allow: &[&str], target: &str) -> anyhow::Result<String> {
    let mut p = perms(64, 30);
    p.network.allow = allow.iter().map(|s| s.to_string()).collect();
    host.run_component(
        component("network-probe", "wasm32-wasip2"),
        target.as_bytes().to_vec(),
        &p,
    )
    .map(|o| String::from_utf8_lossy(&o).into_owned())
}

/// Allowed address connects; every other destination is refused by the socket
/// address check *before* reaching the OS. Each denied target has a live
/// listener, so "denied" cannot be confused with "connection refused", and the
/// listener must never see a connection.
#[test]
#[cfg_attr(not(feature = "ci"), ignore = "requires pre-built Wasm components")]
fn network_allowlist_allows_and_denies() {
    let host = FluxionHost::new().unwrap();
    let allowed = TcpListener::bind("127.0.0.1:0").unwrap();
    let other_port = TcpListener::bind("127.0.0.1:0").unwrap();
    allowed.set_nonblocking(true).unwrap();
    other_port.set_nonblocking(true).unwrap();
    let allowed_addr = allowed.local_addr().unwrap().to_string();
    let other_addr = other_port.local_addr().unwrap().to_string();

    // Allowed: exact ip:port.
    let out = probe_connect(&host, &[&allowed_addr], &allowed_addr).expect("allowed connect");
    assert!(out.starts_with("Connected"), "got: {out}");
    assert!(
        allowed.accept().is_ok(),
        "server should see the allowed connection"
    );

    // Allowed: bare IP permits any port on that IP.
    let out = probe_connect(&host, &["127.0.0.1"], &other_addr).expect("bare-ip allow");
    assert!(out.starts_with("Connected"), "got: {out}");
    assert!(other_port.accept().is_ok());

    // Denied: same IP, different port than the exact entry.
    let r = probe_connect(&host, &[&allowed_addr], &other_addr);
    assert!(r.is_err(), "different port must be denied, got {r:?}");
    // Denied: empty allowlist.
    let r = probe_connect(&host, &[], &allowed_addr);
    assert!(r.is_err(), "empty allowlist must deny, got {r:?}");
    // Denied: a different IP than the allowed one (127.0.0.2 also loops back on Linux).
    let port = allowed.local_addr().unwrap().port();
    let r = probe_connect(&host, &["127.0.0.1"], &format!("127.0.0.2:{port}"));
    assert!(r.is_err(), "different IP must be denied, got {r:?}");
    // Denied: allowlist prefix lookalike (10.0.0.1 must not permit 10.0.0.100, #236).
    let r = probe_connect(&host, &["127.0.0.1:1"], &allowed_addr);
    assert!(
        r.is_err(),
        "port-prefix lookalike must be denied, got {r:?}"
    );

    assert!(
        allowed.accept().is_err() && other_port.accept().is_err(),
        "denied attempts must not reach the listeners"
    );
}

// ── filesystem sandbox ────────────────────────────────────────────────────────

struct Fs {
    _tmp: tempfile::TempDir,
    base: PathBuf,
    ro: PathBuf,
    rw: PathBuf,
    host: FluxionHost,
}

#[cfg(unix)]
fn fs_fixture() -> Fs {
    use std::os::unix::fs::symlink;
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().canonicalize().unwrap();
    let (ro, rw, outside) = (base.join("ro"), base.join("rw"), base.join("outside"));
    for d in [&ro, &rw, &outside, &ro.join("sub"), &rw.join("sub")] {
        std::fs::create_dir_all(d).unwrap();
    }
    std::fs::write(base.join("secret.txt"), "SECRET").unwrap();
    std::fs::write(outside.join("secret2.txt"), "SECRET2").unwrap();
    std::fs::write(ro.join("file.txt"), "ro-content").unwrap();
    std::fs::write(ro.join("sub/inner.txt"), "inner").unwrap();
    std::fs::write(rw.join("w.txt"), "rw-content").unwrap();
    for dir in [&ro, &rw] {
        symlink("file.txt", dir.join("link_in")).ok(); // in-sandbox link (valid in ro only)
        symlink("../secret.txt", dir.join("link_file_out")).unwrap();
        symlink("../outside", dir.join("link_dir_out")).unwrap();
        symlink(base.join("secret.txt"), dir.join("link_abs_out")).unwrap();
        symlink("sub/../../secret.txt", dir.join("link_dotdot_out")).unwrap();
    }
    Fs {
        _tmp: tmp,
        base,
        ro,
        rw,
        host: FluxionHost::new().unwrap(),
    }
}

impl Fs {
    /// Run one probe op with `ro` mounted read-only and `rw` read-write.
    fn run(&self, line: &str) -> String {
        let mut p = perms(64, 30);
        p.filesystem.read = vec![self.ro.clone()];
        p.filesystem.write = vec![self.rw.clone()];
        let out = self
            .host
            .run_component(
                component("sandbox-probe", "wasm32-wasip1"),
                line.as_bytes().to_vec(),
                &p,
            )
            .unwrap_or_else(|e| panic!("probe trapped on `{line}`: {e:#}"));
        String::from_utf8(out).unwrap()
    }

    fn read_host(&self, rel: &str) -> String {
        std::fs::read_to_string(self.base.join(rel)).unwrap()
    }
}

fn p(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

/// Controls: the probe really can read/write where permitted, so the denials
/// below are not vacuous (e.g. caused by a typo'd path).
#[test]
#[cfg(unix)]
#[cfg_attr(not(feature = "ci"), ignore = "requires pre-built Wasm components")]
fn fs_sandbox_controls_allow_permitted_access() {
    let f = fs_fixture();
    assert_eq!(
        f.run(&format!("read {}", p(&f.ro.join("file.txt")))),
        "ok:ro-content"
    );
    assert_eq!(
        f.run(&format!("read {}", p(&f.ro.join("sub/inner.txt")))),
        "ok:inner"
    );
    assert_eq!(
        f.run(&format!("read {}", p(&f.ro.join("link_in")))),
        "ok:ro-content"
    );
    assert_eq!(
        f.run(&format!("read {}", p(&f.rw.join("w.txt")))),
        "ok:rw-content"
    );
    assert!(
        f.run(&format!("create {}", p(&f.rw.join("new.txt"))))
            .starts_with("ok")
    );
    assert_eq!(f.read_host("rw/new.txt"), "probe");
    assert!(
        f.run(&format!(
            "rename {} {}",
            p(&f.rw.join("new.txt")),
            p(&f.rw.join("new2.txt"))
        ))
        .starts_with("ok")
    );
    assert!(
        f.run(&format!(
            "hardlink {} {}",
            p(&f.rw.join("w.txt")),
            p(&f.rw.join("w_link.txt"))
        ))
        .starts_with("ok")
    );
    assert!(
        f.run(&format!("truncate {}", p(&f.rw.join("w.txt"))))
            .starts_with("ok")
    );
    assert_eq!(f.read_host("rw/w.txt"), "");
}

fn assert_denied(out: &str, what: &str) {
    assert!(
        out.starts_with("err:"),
        "{what} must be refused, got `{out}`"
    );
}

/// Read-only preopen: no write-side operation may take effect, including
/// `path_open` with O_TRUNC (which is a write even though no bytes are written).
#[test]
#[cfg(unix)]
#[cfg_attr(not(feature = "ci"), ignore = "requires pre-built Wasm components")]
fn fs_readonly_preopen_rejects_mutation() {
    let f = fs_fixture();
    let file = p(&f.ro.join("file.txt"));

    assert_denied(
        &f.run(&format!("truncate {file}")),
        "TRUNCATE on read-only file",
    );
    assert_eq!(
        f.read_host("ro/file.txt"),
        "ro-content",
        "file was truncated"
    );
    assert_denied(
        &f.run(&format!("append {file}")),
        "append on read-only file",
    );
    assert_eq!(f.read_host("ro/file.txt"), "ro-content");
    assert_denied(
        &f.run(&format!("create {}", p(&f.ro.join("new.txt")))),
        "create in ro dir",
    );
    assert!(!f.ro.join("new.txt").exists());
    assert_denied(
        &f.run(&format!("mkdir {}", p(&f.ro.join("newdir")))),
        "mkdir in ro dir",
    );
    assert!(!f.ro.join("newdir").exists());
    assert_denied(&f.run(&format!("remove {file}")), "remove in ro dir");
    assert!(f.ro.join("file.txt").exists());

    // rename within ro, and rename ro -> rw / rw -> ro
    assert_denied(
        &f.run(&format!("rename {file} {}", p(&f.ro.join("moved.txt")))),
        "rename in ro",
    );
    assert!(f.ro.join("file.txt").exists() && !f.ro.join("moved.txt").exists());
    assert_denied(
        &f.run(&format!("rename {file} {}", p(&f.rw.join("moved.txt")))),
        "rename ro->rw",
    );
    assert!(f.ro.join("file.txt").exists() && !f.rw.join("moved.txt").exists());
    assert_denied(
        &f.run(&format!(
            "rename {} {}",
            p(&f.rw.join("w.txt")),
            p(&f.ro.join("w.txt"))
        )),
        "rename rw->ro",
    );
    assert!(f.rw.join("w.txt").exists() && !f.ro.join("w.txt").exists());

    // hard link: within ro, ro -> rw (would expose a writable alias of a ro inode),
    // rw -> ro
    assert_denied(
        &f.run(&format!("hardlink {file} {}", p(&f.ro.join("h.txt")))),
        "hardlink in ro",
    );
    assert!(!f.ro.join("h.txt").exists());
    assert_denied(
        &f.run(&format!("hardlink {file} {}", p(&f.rw.join("h.txt")))),
        "hardlink ro->rw",
    );
    assert!(!f.rw.join("h.txt").exists());
    assert_denied(
        &f.run(&format!(
            "hardlink {} {}",
            p(&f.rw.join("w.txt")),
            p(&f.ro.join("h.txt"))
        )),
        "hardlink rw->ro",
    );
    assert!(!f.ro.join("h.txt").exists());
}

/// Nothing outside the preopens is reachable: `..`, absolute paths, symlinks
/// (relative, absolute, via `..` in the middle, to a directory), and trailing
/// slashes on any of them. Checked from both the read-only and the read-write
/// preopen, for reads and for writes through the link.
#[test]
#[cfg(unix)]
#[cfg_attr(not(feature = "ci"), ignore = "requires pre-built Wasm components")]
fn fs_sandbox_escape_attempts_are_rejected() {
    let f = fs_fixture();
    for (dir, name) in [(&f.ro, "ro"), (&f.rw, "rw")] {
        let d = p(dir);
        let reads = [
            format!("{d}/../secret.txt"),
            format!("{d}/sub/../../secret.txt"),
            format!("{d}/../outside/secret2.txt"),
            format!("{d}/link_file_out"),
            format!("{d}/link_file_out/"),
            format!("{d}/link_abs_out"),
            format!("{d}/link_abs_out/"),
            format!("{d}/link_dotdot_out"),
            format!("{d}/link_dir_out/secret2.txt"),
            format!("{d}/link_dir_out/../secret.txt"),
            p(&f.base.join("secret.txt")),
            p(&f.base.join("outside/secret2.txt")),
            "/etc/passwd".to_string(),
        ];
        for path in &reads {
            let out = f.run(&format!("read {path}"));
            assert_denied(&out, &format!("[{name}] read {path}"));
            assert!(!out.contains("SECRET"), "[{name}] leaked secret via {path}");
        }
        for path in [
            format!("{d}/link_dir_out"),
            format!("{d}/link_dir_out/"),
            format!("{d}/.."),
            format!("{d}/../"),
            p(&f.base),
        ] {
            let out = f.run(&format!("list {path}"));
            assert_denied(&out, &format!("[{name}] list {path}"));
            assert!(
                !out.contains("secret"),
                "[{name}] listed outside dir via {path}"
            );
        }
    }

    // Writes through links that leave the sandbox (rw preopen only: ro is already
    // covered above and cannot write at all).
    let d = p(&f.rw);
    for path in [
        format!("{d}/link_file_out"),
        format!("{d}/link_file_out/"),
        format!("{d}/link_abs_out"),
        format!("{d}/link_dotdot_out"),
        format!("{d}/../secret.txt"),
    ] {
        assert_denied(
            &f.run(&format!("truncate {path}")),
            &format!("truncate {path}"),
        );
        assert_denied(&f.run(&format!("append {path}")), &format!("append {path}"));
        assert_eq!(
            f.read_host("secret.txt"),
            "SECRET",
            "secret modified via {path}"
        );
    }
    assert_denied(
        &f.run(&format!("create {d}/link_dir_out/new.txt")),
        "create via dir symlink",
    );
    assert!(!f.base.join("outside/new.txt").exists());
    assert_denied(
        &f.run(&format!("create {d}/../escaped.txt")),
        "create via ..",
    );
    assert!(!f.base.join("escaped.txt").exists());
    assert_denied(
        &f.run(&format!("mkdir {d}/link_dir_out/newdir")),
        "mkdir via dir symlink",
    );
    assert!(!f.base.join("outside/newdir").exists());
    assert_denied(
        &f.run(&format!("remove {d}/link_dir_out/secret2.txt")),
        "remove via dir symlink",
    );
    assert!(f.base.join("outside/secret2.txt").exists());

    // Hard link / rename that would pull an outside file in, or push one out.
    assert_denied(
        &f.run(&format!("hardlink {d}/../secret.txt {d}/stolen.txt")),
        "hardlink from outside",
    );
    assert!(!f.rw.join("stolen.txt").exists());
    // link(2) does not follow symlinks, so hard-linking an escaping symlink just
    // duplicates the (dangling-from-inside) symlink. That is allowed; what matters
    // is that the duplicate is just as unusable for reaching the outside file.
    let _ = f.run(&format!("hardlink {d}/link_file_out {d}/stolen.txt"));
    let out = f.run(&format!("read {d}/stolen.txt"));
    assert_denied(&out, "read through hard link of an escaping symlink");
    assert!(!out.contains("SECRET"));
    assert_denied(
        &f.run(&format!("hardlink {d}/w.txt {d}/../planted.txt")),
        "hardlink to outside",
    );
    assert!(!f.base.join("planted.txt").exists());
    assert_denied(
        &f.run(&format!("rename {d}/../secret.txt {d}/stolen2.txt")),
        "rename from outside",
    );
    assert!(f.base.join("secret.txt").exists() && !f.rw.join("stolen2.txt").exists());
    assert_denied(
        &f.run(&format!("rename {d}/w.txt {d}/../planted.txt")),
        "rename to outside",
    );
    assert!(f.rw.join("w.txt").exists() && !f.base.join("planted.txt").exists());
    assert_denied(
        &f.run(&format!("rename {d}/link_dir_out/secret2.txt {d}/s2.txt")),
        "rename via dir symlink",
    );
    assert!(f.base.join("outside/secret2.txt").exists());

    // Trailing slash on a regular file must not be accepted as "the file".
    assert_denied(
        &f.run(&format!("read {d}/w.txt/")),
        "read file with trailing slash",
    );
    assert_denied(
        &f.run(&format!("truncate {d}/w.txt/")),
        "truncate file with trailing slash",
    );
    assert_eq!(f.read_host("rw/w.txt"), "rw-content");
}

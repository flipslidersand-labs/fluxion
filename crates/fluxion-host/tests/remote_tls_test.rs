//! run_remote must trust ONLY the configured CA when mTLS is configured
//! (built-in public roots disabled).
use fluxion_core::workflow::{PermissionSet, TlsConfig};
use fluxion_host::remote::run_remote;
use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair, SanType};
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

struct Ca {
    cert: rcgen::Certificate,
    key: KeyPair,
}

fn make_ca(name: &str) -> Ca {
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(vec![]).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.distinguished_name.push(DnType::CommonName, name);
    let cert = params.self_signed(&key).unwrap();
    Ca { cert, key }
}

/// Issue a leaf for 127.0.0.1 signed by `ca`; returns (cert pem, key pem, cert der, key der).
fn issue_leaf(ca: &Ca) -> (String, String, Vec<u8>, Vec<u8>) {
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(vec![]).unwrap();
    params.subject_alt_names = vec![SanType::IpAddress(IpAddr::V4(Ipv4Addr::LOCALHOST))];
    let cert = params.signed_by(&key, &ca.cert, &ca.key).unwrap();
    (
        cert.pem(),
        key.serialize_pem(),
        cert.der().to_vec(),
        key.serialize_der(),
    )
}

/// TLS server whose certificate is signed by `server_ca`. Returns (port, successful handshakes).
async fn spawn_tls_server(server_ca: &Ca) -> (u16, Arc<AtomicUsize>) {
    let (_, _, cert_der, key_der) = issue_leaf(server_ca);
    // Explicit provider: both ring and aws-lc-rs are linked, so the process default is ambiguous.
    let provider = Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
    let config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![CertificateDer::from(cert_der)],
            PrivateKeyDer::from(PrivatePkcs8KeyDer::from(key_der)),
        )
        .unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(config));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let handshakes = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&handshakes);
    tokio::spawn(async move {
        loop {
            let Ok((tcp, _)) = listener.accept().await else {
                return;
            };
            let acceptor = acceptor.clone();
            let counter = Arc::clone(&counter);
            tokio::spawn(async move {
                if let Ok(mut tls) = acceptor.accept(tcp).await {
                    counter.fetch_add(1, Ordering::SeqCst);
                    let mut buf = [0u8; 1024];
                    let _ = tls.read(&mut buf).await; // then drop: no HTTP response
                }
            });
        }
    });
    (port, handshakes)
}

fn write_tls_files(dir: &std::path::Path, trusted_ca: &Ca, client_ca: &Ca) -> TlsConfig {
    let (cert_pem, key_pem, _, _) = issue_leaf(client_ca);
    let cert = dir.join("client.crt");
    let key = dir.join("client.key");
    let ca = dir.join("ca.crt");
    std::fs::write(&cert, cert_pem).unwrap();
    std::fs::write(&key, key_pem).unwrap();
    std::fs::write(&ca, trusted_ca.cert.pem()).unwrap();
    TlsConfig { cert, key, ca }
}

#[tokio::test]
async fn run_remote_trusts_only_configured_ca() {
    let ca_good = make_ca("good");
    let ca_other = make_ca("other");
    let dir = tempfile::tempdir().unwrap();
    let wasm = dir.path().join("c.wasm");
    std::fs::write(&wasm, b"x").unwrap();
    let perms = PermissionSet::default();
    let env = HashMap::new();

    // Server cert signed by a CA that is NOT the configured one -> handshake must fail.
    let (port_bad, hs_bad) = spawn_tls_server(&ca_other).await;
    let tls = write_tls_files(dir.path(), &ca_good, &ca_good);
    let res = run_remote(
        &format!("https://127.0.0.1:{port_bad}"),
        &wasm,
        vec![],
        &perms,
        &env,
        Some(&tls),
    )
    .await;
    assert!(res.is_err());
    assert_eq!(
        hs_bad.load(Ordering::SeqCst),
        0,
        "handshake with untrusted CA succeeded"
    );

    // Control: server cert signed by the configured CA -> handshake succeeds
    // (the request itself then fails because the stub server sends no HTTP reply).
    let (port_ok, hs_ok) = spawn_tls_server(&ca_good).await;
    let _ = run_remote(
        &format!("https://127.0.0.1:{port_ok}"),
        &wasm,
        vec![],
        &perms,
        &env,
        Some(&tls),
    )
    .await;
    assert!(
        hs_ok.load(Ordering::SeqCst) > 0,
        "handshake with configured CA failed"
    );
}

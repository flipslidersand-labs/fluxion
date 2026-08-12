//! OCI Distribution Spec v1.1 pull/push client for Wasm components.
//!
//! Supports anonymous and Basic-auth registries. Uses `rustls-tls` (no openssl).
//!
//! Layer media type: `application/vnd.wasm.content.layer.v0+wasm`
//! Manifest media type: `application/vnd.oci.image.manifest.v1+json`

use anyhow::{Context, Result, bail};
use base64::Engine as _;
use reqwest::{Client, StatusCode, header};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

// ── Media types ───────────────────────────────────────────────────────────────

const WASM_LAYER_TYPE: &str = "application/vnd.wasm.content.layer.v0+wasm";
const OCI_MANIFEST_TYPE: &str = "application/vnd.oci.image.manifest.v1+json";
const OCI_CONFIG_TYPE: &str = "application/vnd.oci.image.config.v1+json";

// ── OCI manifest types ────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OciManifest {
    schema_version: u32,
    media_type: String,
    config: OciDescriptor,
    layers: Vec<OciDescriptor>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct OciDescriptor {
    media_type: String,
    digest: String,
    size: u64,
}

// ── Credentials ───────────────────────────────────────────────────────────────

/// Registry authentication credentials (optional — omit for anonymous access).
#[derive(Debug, Clone, Default)]
pub struct Credentials {
    pub username: String,
    pub password: String,
}

impl Credentials {
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self { username: username.into(), password: password.into() }
    }

    fn basic_header(&self) -> String {
        let encoded = base64::engine::general_purpose::STANDARD
            .encode(format!("{}:{}", self.username, self.password));
        format!("Basic {encoded}")
    }
}

// ── OciClient ─────────────────────────────────────────────────────────────────

/// Minimal OCI Distribution Spec v1.1 client for Wasm component registries.
#[derive(Debug, Clone)]
pub struct OciClient {
    client: Client,
    /// Registry base URL, e.g. `https://ghcr.io`.
    pub base_url: String,
    /// Optional credentials for Basic auth (leave default for anonymous).
    pub credentials: Option<Credentials>,
}

impl OciClient {
    /// Construct an `OciClient` pointing at the given registry.
    ///
    /// `base_url` should include the scheme, e.g. `"https://ghcr.io"`.
    /// Pass `None` for `credentials` to use anonymous access.
    pub fn new(base_url: impl Into<String>, credentials: Option<Credentials>) -> Result<Self> {
        let client = Client::builder()
            .use_rustls_tls()
            .build()
            .context("failed to build reqwest client")?;
        Ok(Self { client, base_url: base_url.into(), credentials })
    }

    // ── Authorization ─────────────────────────────────────────────────────────

    fn auth_header(&self) -> Option<String> {
        self.credentials.as_ref().map(|c| c.basic_header())
    }

    fn add_auth(&self, rb: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(auth) = self.auth_header() {
            rb.header(header::AUTHORIZATION, auth)
        } else {
            rb
        }
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    /// `GET /v2/<repository>/manifests/<reference>`
    async fn fetch_manifest(&self, repository: &str, reference: &str) -> Result<OciManifest> {
        let url = format!("{}/v2/{}/manifests/{}", self.base_url, repository, reference);
        let rb = self
            .client
            .get(&url)
            .header(header::ACCEPT, OCI_MANIFEST_TYPE);
        let resp = self.add_auth(rb).send().await.context("GET manifest")?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            bail!("GET {url} → {status}: {body}");
        }

        resp.json::<OciManifest>().await.context("parse manifest JSON")
    }

    /// `GET /v2/<repository>/blobs/<digest>`
    async fn fetch_blob(&self, repository: &str, digest: &str) -> Result<Vec<u8>> {
        let url = format!("{}/v2/{}/blobs/{}", self.base_url, repository, digest);
        let resp = self
            .add_auth(self.client.get(&url))
            .send()
            .await
            .context("GET blob")?;

        let status = resp.status();
        if !status.is_success() {
            bail!("GET blob {url} → {status}");
        }

        Ok(resp.bytes().await.context("read blob body")?.to_vec())
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /// Pull a Wasm component from the registry.
    ///
    /// Steps:
    /// 1. `GET /v2/<repository>/manifests/<tag>` — find the Wasm layer descriptor
    /// 2. `GET /v2/<repository>/blobs/<digest>` — download the blob
    /// 3. Verify SHA-256 digest against the manifest descriptor
    ///
    /// Returns the raw Wasm bytes.
    pub async fn pull(
        &self,
        repository: &str,
        reference: &str,
    ) -> Result<Vec<u8>> {
        let manifest = self.fetch_manifest(repository, reference).await?;

        // Find the first Wasm layer.
        let layer = manifest
            .layers
            .iter()
            .find(|l| l.media_type == WASM_LAYER_TYPE)
            .with_context(|| {
                format!(
                    "manifest for {repository}:{reference} contains no Wasm layer \
                     (expected media type '{WASM_LAYER_TYPE}')"
                )
            })?;

        let bytes = self.fetch_blob(repository, &layer.digest).await?;

        // Verify SHA-256.
        let actual_digest = sha256_digest(&bytes);
        let expected = layer.digest.strip_prefix("sha256:").unwrap_or(&layer.digest);
        if actual_digest != expected {
            bail!(
                "digest mismatch for {repository} layer {}: expected sha256:{expected}, got sha256:{actual_digest}",
                layer.digest
            );
        }

        Ok(bytes)
    }

    /// Push a Wasm component to the registry.
    ///
    /// Steps:
    /// 1. `POST /v2/<repository>/blobs/uploads/` — initiate upload, get session URL
    /// 2. `PUT <session_url>?digest=<sha256>` — upload the blob
    /// 3. Build and push an OCI manifest referencing the blob
    ///
    /// Returns the pushed manifest digest (`sha256:…`).
    pub async fn push(
        &self,
        repository: &str,
        reference: &str,
        wasm_bytes: &[u8],
    ) -> Result<String> {
        let layer_digest = format!("sha256:{}", sha256_digest(wasm_bytes));
        let layer_size = wasm_bytes.len() as u64;

        // 1. Initiate blob upload.
        let init_url = format!("{}/v2/{}/blobs/uploads/", self.base_url, repository);
        let resp = self
            .add_auth(self.client.post(&init_url))
            .send()
            .await
            .context("POST blob upload initiation")?;

        let status = resp.status();
        // 202 Accepted is the expected response; some registries return 201.
        if status != StatusCode::ACCEPTED && status != StatusCode::CREATED {
            let body = resp.text().await.unwrap_or_default();
            bail!("POST {init_url} → {status}: {body}");
        }

        let location = resp
            .headers()
            .get(header::LOCATION)
            .with_context(|| format!("POST {init_url} missing Location header"))?
            .to_str()
            .context("Location header is not valid UTF-8")?
            .to_string();

        // Resolve relative Location to an absolute URL.
        let upload_url = if location.starts_with("http") {
            location.clone()
        } else {
            format!("{}{}", self.base_url, location)
        };

        // 2. Upload blob (monolithic PUT).
        let put_url = format!(
            "{}{}&digest={}",
            upload_url,
            if upload_url.contains('?') { "" } else { "?" },
            percent_encode(&layer_digest)
        );
        let put_resp = self
            .add_auth(
                self.client
                    .put(&put_url)
                    .header(header::CONTENT_TYPE, "application/octet-stream")
                    .body(wasm_bytes.to_vec()),
            )
            .send()
            .await
            .context("PUT blob")?;

        let put_status = put_resp.status();
        if !put_status.is_success() {
            let body = put_resp.text().await.unwrap_or_default();
            bail!("PUT blob {put_url} → {put_status}: {body}");
        }

        // 3. Build and push manifest.
        // Minimal empty config blob (OCI spec requires a config object).
        let config_bytes = b"{}";
        let config_digest = format!("sha256:{}", sha256_digest(config_bytes));
        // Push config blob (same two-step flow, but inline for brevity).
        self.push_blob(repository, config_bytes).await
            .context("push config blob")?;

        let manifest = OciManifest {
            schema_version: 2,
            media_type: OCI_MANIFEST_TYPE.to_string(),
            config: OciDescriptor {
                media_type: OCI_CONFIG_TYPE.to_string(),
                digest: config_digest,
                size: config_bytes.len() as u64,
            },
            layers: vec![OciDescriptor {
                media_type: WASM_LAYER_TYPE.to_string(),
                digest: layer_digest,
                size: layer_size,
            }],
        };

        let manifest_json = serde_json::to_vec(&manifest).context("serialize manifest")?;
        let manifest_digest = format!("sha256:{}", sha256_digest(&manifest_json));

        let manifest_url = format!("{}/v2/{}/manifests/{}", self.base_url, repository, reference);
        let m_resp = self
            .add_auth(
                self.client
                    .put(&manifest_url)
                    .header(header::CONTENT_TYPE, OCI_MANIFEST_TYPE)
                    .body(manifest_json),
            )
            .send()
            .await
            .context("PUT manifest")?;

        let m_status = m_resp.status();
        if !m_status.is_success() {
            let body = m_resp.text().await.unwrap_or_default();
            bail!("PUT manifest {manifest_url} → {m_status}: {body}");
        }

        Ok(manifest_digest)
    }

    /// Push a raw blob without constructing a manifest (used for the config object).
    async fn push_blob(&self, repository: &str, bytes: &[u8]) -> Result<()> {
        let digest = format!("sha256:{}", sha256_digest(bytes));

        let init_url = format!("{}/v2/{}/blobs/uploads/", self.base_url, repository);
        let resp = self
            .add_auth(self.client.post(&init_url))
            .send()
            .await
            .context("POST blob upload")?;

        let status = resp.status();
        if status != StatusCode::ACCEPTED && status != StatusCode::CREATED {
            let body = resp.text().await.unwrap_or_default();
            bail!("POST {init_url} → {status}: {body}");
        }

        let location = resp
            .headers()
            .get(header::LOCATION)
            .context("missing Location header")?
            .to_str()
            .context("Location not UTF-8")?
            .to_string();

        let upload_url = if location.starts_with("http") {
            location
        } else {
            format!("{}{}", self.base_url, location)
        };

        let put_url = format!(
            "{}{}&digest={}",
            upload_url,
            if upload_url.contains('?') { "" } else { "?" },
            percent_encode(&digest)
        );

        let put_resp = self
            .add_auth(
                self.client
                    .put(&put_url)
                    .header(header::CONTENT_TYPE, "application/octet-stream")
                    .body(bytes.to_vec()),
            )
            .send()
            .await
            .context("PUT config blob")?;

        let put_status = put_resp.status();
        if !put_status.is_success() {
            let body = put_resp.text().await.unwrap_or_default();
            bail!("PUT config blob → {put_status}: {body}");
        }

        Ok(())
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Percent-encode a string for use in URL query parameters.
/// Only encodes characters that are unsafe in query values (`:` and `+`).
fn percent_encode(s: &str) -> String {
    s.replace('+', "%2B").replace(':', "%3A")
}

fn sha256_digest(bytes: &[u8]) -> String {
    let hash = Sha256::digest(bytes);
    hash.iter().map(|b| format!("{b:02x}")).collect()
}

// ── Unit tests (no network) ───────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_digest_is_stable() {
        let d = sha256_digest(b"hello");
        assert_eq!(
            d,
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn credentials_basic_header() {
        let creds = Credentials::new("user", "pass");
        let header = creds.basic_header();
        assert!(header.starts_with("Basic "), "must start with 'Basic '");
        let encoded = header.strip_prefix("Basic ").unwrap();
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .expect("valid base64");
        assert_eq!(decoded, b"user:pass");
    }

    #[test]
    fn oci_client_constructs() {
        let client = OciClient::new("https://ghcr.io", None);
        assert!(client.is_ok());
        let c = client.unwrap();
        assert_eq!(c.base_url, "https://ghcr.io");
        assert!(c.credentials.is_none());
    }

    #[test]
    fn oci_client_with_credentials() {
        let creds = Credentials::new("alice", "secret");
        let client = OciClient::new("https://registry.example.com", Some(creds)).unwrap();
        assert!(client.credentials.is_some());
    }

    #[test]
    fn manifest_serialization_roundtrip() {
        let manifest = OciManifest {
            schema_version: 2,
            media_type: OCI_MANIFEST_TYPE.to_string(),
            config: OciDescriptor {
                media_type: OCI_CONFIG_TYPE.to_string(),
                digest: "sha256:abc".to_string(),
                size: 2,
            },
            layers: vec![OciDescriptor {
                media_type: WASM_LAYER_TYPE.to_string(),
                digest: "sha256:def".to_string(),
                size: 1024,
            }],
        };
        let json = serde_json::to_string(&manifest).expect("serialize");
        let parsed: OciManifest = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed.schema_version, 2);
        assert_eq!(parsed.layers[0].media_type, WASM_LAYER_TYPE);
    }
}

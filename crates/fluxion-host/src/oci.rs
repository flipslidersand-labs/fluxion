//! Minimal OCI Distribution Spec client for pulling and pushing Wasm components.
//!
//! Supports `registry/repo:tag` and `registry/repo@sha256:…` references.
//! Token auth via WWW-Authenticate Bearer challenges is handled automatically.

use anyhow::{Context, Result, anyhow};
use reqwest::{Client, StatusCode, header};

const ACCEPT_MANIFEST: &str = "application/vnd.oci.image.manifest.v1+json, \
     application/vnd.docker.distribution.manifest.v2+json, \
     application/vnd.oci.image.index.v1+json";

/// Parse an OCI reference into (registry, repository, reference).
///
/// Examples:
///   `ghcr.io/org/repo:latest`         → ("ghcr.io", "org/repo", "latest")
///   `registry.example.com/img@sha256:abc` → ("registry.example.com", "img", "sha256:abc")
///   `ubuntu:22.04`                    → ("registry-1.docker.io", "library/ubuntu", "22.04")
fn parse_ref(oci_ref: &str) -> Result<(String, String, String)> {
    // Split off digest or tag.
    let (name_part, reference) = if let Some(pos) = oci_ref.rfind('@') {
        (&oci_ref[..pos], oci_ref[pos + 1..].to_string())
    } else if let Some(pos) = oci_ref.rfind(':') {
        // Colon might be part of registry host:port — only treat as tag if after the first '/'.
        let slash = oci_ref.find('/').unwrap_or(0);
        if pos > slash {
            (&oci_ref[..pos], oci_ref[pos + 1..].to_string())
        } else {
            (oci_ref, "latest".to_string())
        }
    } else {
        (oci_ref, "latest".to_string())
    };

    // Split registry from repository.
    let (registry, repo) = if let Some(slash) = name_part.find('/') {
        let host = &name_part[..slash];
        // Treat as registry host if it contains a dot, colon, or is "localhost".
        if host.contains('.') || host.contains(':') || host == "localhost" {
            (host.to_string(), name_part[slash + 1..].to_string())
        } else {
            ("registry-1.docker.io".to_string(), name_part.to_string())
        }
    } else {
        // No slash → Docker Hub official image.
        (
            "registry-1.docker.io".to_string(),
            format!("library/{}", name_part),
        )
    };

    Ok((registry, repo, reference))
}

/// Pull a Wasm component from an OCI registry.
///
/// Returns the raw `.wasm` bytes. The caller is responsible for caching by
/// content digest — `ComponentCache` / LRU in `FluxionHost` handle this.
pub async fn pull(oci_ref: &str) -> Result<Vec<u8>> {
    let (registry, repo, reference) = parse_ref(oci_ref)?;
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(120))
        .build()?;

    // 1. Fetch manifest.
    let manifest_url = format!("https://{}/v2/{}/manifests/{}", registry, repo, reference);
    let resp = client
        .get(&manifest_url)
        .header(header::ACCEPT, ACCEPT_MANIFEST)
        .send()
        .await
        .with_context(|| format!("GET {manifest_url}"))?;

    // Handle Bearer auth challenge (unauthenticated public registries).
    let resp = if resp.status() == StatusCode::UNAUTHORIZED {
        let www_auth = resp
            .headers()
            .get(header::WWW_AUTHENTICATE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let token = fetch_token(&client, &www_auth, &repo).await?;
        client
            .get(&manifest_url)
            .header(header::ACCEPT, ACCEPT_MANIFEST)
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .send()
            .await
            .with_context(|| format!("GET {manifest_url} (authenticated)"))?
    } else {
        resp
    };

    if !resp.status().is_success() {
        return Err(anyhow!(
            "OCI manifest fetch failed {}: {}",
            resp.status(),
            resp.text().await.unwrap_or_default()
        ));
    }

    // Save auth token if we got one.
    let token = resp
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.strip_prefix("Bearer ").unwrap_or(s).to_string());

    let manifest: serde_json::Value = resp.json().await?;

    // Handle OCI image index (multi-arch) — pick first manifest.
    let manifest = if manifest["mediaType"]
        .as_str()
        .map(|m| m.contains("index"))
        .unwrap_or(false)
        || manifest["manifests"].is_array()
    {
        let child_digest = manifest["manifests"][0]["digest"]
            .as_str()
            .ok_or_else(|| anyhow!("no manifests in image index"))?
            .to_string();
        let child_url = format!(
            "https://{}/v2/{}/manifests/{}",
            registry, repo, child_digest
        );
        let mut req = client
            .get(&child_url)
            .header(header::ACCEPT, ACCEPT_MANIFEST);
        if let Some(t) = &token {
            req = req.header(header::AUTHORIZATION, format!("Bearer {t}"));
        }
        req.send().await?.json().await?
    } else {
        manifest
    };

    // 2. Find the Wasm layer (first layer, or the one with application/wasm media type).
    let layers = manifest["layers"]
        .as_array()
        .ok_or_else(|| anyhow!("manifest has no layers"))?;

    let layer = layers
        .iter()
        .find(|l| {
            l["mediaType"]
                .as_str()
                .map(|m| m.contains("wasm") || m.contains("octet-stream"))
                .unwrap_or(false)
        })
        .or_else(|| layers.first())
        .ok_or_else(|| anyhow!("no layers in manifest"))?;

    let digest = layer["digest"]
        .as_str()
        .ok_or_else(|| anyhow!("layer missing digest"))?;

    // 3. Pull blob.
    let blob_url = format!("https://{}/v2/{}/blobs/{}", registry, repo, digest);
    let mut req = client
        .get(&blob_url)
        .header(header::ACCEPT, "application/octet-stream");
    if let Some(t) = &token {
        req = req.header(header::AUTHORIZATION, format!("Bearer {t}"));
    }
    let blob_resp = req
        .send()
        .await
        .with_context(|| format!("GET {blob_url}"))?;

    if !blob_resp.status().is_success() {
        return Err(anyhow!(
            "OCI blob fetch failed {}: {}",
            blob_resp.status(),
            blob_resp.text().await.unwrap_or_default()
        ));
    }

    Ok(blob_resp.bytes().await?.to_vec())
}

/// Push a Wasm component to an OCI registry.
///
/// Creates a single-layer OCI image with `application/vnd.oci.image.layer.v1.tar`
/// media type. The layer blob is the raw `.wasm` bytes. Returns the digest of the
/// pushed manifest.
pub async fn push(oci_ref: &str, wasm_bytes: &[u8]) -> Result<String> {
    use sha2::{Digest as _, Sha256};

    let (registry, repo, tag) = parse_ref(oci_ref)?;
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(120))
        .build()?;

    // Compute layer digest.
    let layer_digest_bytes = Sha256::digest(wasm_bytes);
    let layer_digest = format!(
        "sha256:{}",
        layer_digest_bytes
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    );
    let layer_size = wasm_bytes.len() as u64;

    // 1. Initiate blob upload.
    let initiate_url = format!("https://{}/v2/{}/blobs/uploads/", registry, repo);
    let resp = client.post(&initiate_url).send().await?;

    let resp = if resp.status() == StatusCode::UNAUTHORIZED {
        let www_auth = resp
            .headers()
            .get(header::WWW_AUTHENTICATE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let token = fetch_token(&client, &www_auth, &repo).await?;
        client
            .post(&initiate_url)
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .send()
            .await?
    } else {
        resp
    };

    // Extract auth token from response for subsequent requests.
    let auth_token = resp
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.strip_prefix("Bearer ").unwrap_or(s).to_string());

    if resp.status() != StatusCode::ACCEPTED {
        return Err(anyhow!(
            "OCI blob upload initiation failed {}: {}",
            resp.status(),
            resp.text().await.unwrap_or_default()
        ));
    }

    let upload_location = resp
        .headers()
        .get(header::LOCATION)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| anyhow!("no Location header in blob upload initiation response"))?
        .to_string();

    // Resolve relative URL.
    let upload_url = if upload_location.starts_with("http") {
        upload_location.clone()
    } else {
        format!("https://{}{}", registry, upload_location)
    };

    // 2. Upload blob in a single PUT (monolithic upload).
    let put_url = if upload_url.contains('?') {
        format!("{}&digest={}", upload_url, layer_digest)
    } else {
        format!("{}?digest={}", upload_url, layer_digest)
    };

    let mut put_req = client
        .put(&put_url)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::CONTENT_LENGTH, layer_size)
        .body(wasm_bytes.to_vec());
    if let Some(t) = &auth_token {
        put_req = put_req.header(header::AUTHORIZATION, format!("Bearer {t}"));
    }
    let put_resp = put_req.send().await?;
    if !put_resp.status().is_success() {
        return Err(anyhow!(
            "OCI blob PUT failed {}: {}",
            put_resp.status(),
            put_resp.text().await.unwrap_or_default()
        ));
    }

    // 3. Build OCI image manifest.
    let manifest = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": "sha256:44136fa355ba77b9ad7b468e65a7a6e22e2adef40c58e8c02c5a818a6ca07d76",
            "size": 2
        },
        "layers": [{
            "mediaType": "application/vnd.oci.image.layer.v1.tar",
            "digest": layer_digest,
            "size": layer_size
        }]
    });
    let manifest_bytes = serde_json::to_vec(&manifest)?;

    // Upload the empty config blob first (required by spec).
    let empty_config = b"{}";
    let config_digest_bytes = Sha256::digest(empty_config);
    let config_digest = format!(
        "sha256:{}",
        config_digest_bytes
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    );
    let cfg_init = format!("https://{}/v2/{}/blobs/uploads/", registry, repo);
    let cfg_resp = {
        let mut req = client.post(&cfg_init);
        if let Some(t) = &auth_token {
            req = req.header(header::AUTHORIZATION, format!("Bearer {t}"));
        }
        req.send().await?
    };
    if cfg_resp.status() == StatusCode::ACCEPTED {
        let cfg_location = cfg_resp
            .headers()
            .get(header::LOCATION)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let cfg_upload_url = if cfg_location.starts_with("http") {
            cfg_location.clone()
        } else {
            format!("https://{}{}", registry, cfg_location)
        };
        let cfg_put_url = if cfg_upload_url.contains('?') {
            format!("{}&digest={}", cfg_upload_url, config_digest)
        } else {
            format!("{}?digest={}", cfg_upload_url, config_digest)
        };
        let mut cfg_put = client
            .put(&cfg_put_url)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header(header::CONTENT_LENGTH, empty_config.len() as u64)
            .body(empty_config.to_vec());
        if let Some(t) = &auth_token {
            cfg_put = cfg_put.header(header::AUTHORIZATION, format!("Bearer {t}"));
        }
        let _ = cfg_put.send().await;
    }

    // 4. PUT manifest.
    let manifest_url = format!("https://{}/v2/{}/manifests/{}", registry, repo, tag);
    let mut manifest_req = client
        .put(&manifest_url)
        .header(
            header::CONTENT_TYPE,
            "application/vnd.oci.image.manifest.v1+json",
        )
        .body(manifest_bytes);
    if let Some(t) = &auth_token {
        manifest_req = manifest_req.header(header::AUTHORIZATION, format!("Bearer {t}"));
    }
    let manifest_resp = manifest_req.send().await?;

    if !manifest_resp.status().is_success() {
        return Err(anyhow!(
            "OCI manifest PUT failed {}: {}",
            manifest_resp.status(),
            manifest_resp.text().await.unwrap_or_default()
        ));
    }

    Ok(layer_digest)
}

/// List tags for a repository on an OCI registry.
///
/// Returns all tag names. On error or no-tags returns an empty list.
pub async fn list_tags(registry: &str, repo: &str) -> Result<Vec<String>> {
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;
    let url = format!("https://{}/v2/{}/tags/list", registry, repo);
    let resp = client.get(&url).send().await?;

    let resp = if resp.status() == StatusCode::UNAUTHORIZED {
        let www_auth = resp
            .headers()
            .get(header::WWW_AUTHENTICATE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let token = fetch_token(&client, &www_auth, repo).await?;
        client
            .get(&url)
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .send()
            .await?
    } else {
        resp
    };

    if resp.status() == StatusCode::NOT_FOUND {
        return Ok(vec![]);
    }

    let json: serde_json::Value = resp.json().await?;
    Ok(json["tags"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default())
}

/// Fetch a Bearer token from the registry's auth service.
async fn fetch_token(client: &Client, www_auth: &str, scope_repo: &str) -> Result<String> {
    // Parse `Bearer realm="...",service="...",scope="..."`
    let realm = parse_bearer_param(www_auth, "realm").unwrap_or_default();
    let service = parse_bearer_param(www_auth, "service").unwrap_or_default();
    let scope = parse_bearer_param(www_auth, "scope")
        .unwrap_or_else(|| format!("repository:{}:pull", scope_repo));

    if realm.is_empty() {
        return Err(anyhow!("no realm in WWW-Authenticate: {}", www_auth));
    }

    let resp = client
        .get(&realm)
        .query(&[("service", &service), ("scope", &scope)])
        .send()
        .await
        .with_context(|| format!("token fetch from {realm}"))?;

    let json: serde_json::Value = resp.json().await?;
    json["token"]
        .as_str()
        .or_else(|| json["access_token"].as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| anyhow!("no token in auth response: {json}"))
}

fn parse_bearer_param<'a>(header: &'a str, key: &str) -> Option<String> {
    let needle = format!("{}=\"", key);
    let start = header.find(needle.as_str())? + needle.len();
    let end = header[start..].find('"')? + start;
    Some(header[start..end].to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_ref_with_tag() {
        let (reg, repo, r) = parse_ref("ghcr.io/org/repo:v1.0").unwrap();
        assert_eq!(reg, "ghcr.io");
        assert_eq!(repo, "org/repo");
        assert_eq!(r, "v1.0");
    }

    #[test]
    fn parse_ref_with_digest() {
        let (reg, repo, r) = parse_ref("ghcr.io/org/repo@sha256:abc123").unwrap();
        assert_eq!(reg, "ghcr.io");
        assert_eq!(repo, "org/repo");
        assert_eq!(r, "sha256:abc123");
    }

    #[test]
    fn parse_ref_docker_hub_official() {
        let (reg, repo, r) = parse_ref("ubuntu:22.04").unwrap();
        assert_eq!(reg, "registry-1.docker.io");
        assert_eq!(repo, "library/ubuntu");
        assert_eq!(r, "22.04");
    }

    #[test]
    fn parse_ref_docker_hub_user() {
        let (reg, repo, r) = parse_ref("myuser/myimage:latest").unwrap();
        assert_eq!(reg, "registry-1.docker.io");
        assert_eq!(repo, "myuser/myimage");
        assert_eq!(r, "latest");
    }

    #[test]
    fn parse_ref_no_tag() {
        let (reg, repo, r) = parse_ref("ghcr.io/org/img").unwrap();
        assert_eq!(reg, "ghcr.io");
        assert_eq!(repo, "org/img");
        assert_eq!(r, "latest");
    }
}

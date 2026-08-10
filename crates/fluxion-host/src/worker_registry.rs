use anyhow::Result;
use fluxion_core::store::RunStore;

/// Health-check all dynamically registered workers.
/// Updates each worker's status in the store and returns URLs of healthy workers.
pub async fn health_check_all(store: &RunStore) -> Result<Vec<String>> {
    let workers = store.list_workers()?;
    if workers.is_empty() {
        return Ok(Vec::new());
    }

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()?;

    let mut healthy = Vec::new();
    for w in workers {
        let url = format!("{}/health", w.url.trim_end_matches('/'));
        let reachable = client.get(&url).send().await.is_ok();
        store.update_worker_health(&w.url, reachable)?;
        if reachable {
            healthy.push(w.url);
        }
    }

    Ok(healthy)
}

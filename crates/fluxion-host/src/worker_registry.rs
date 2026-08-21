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

#[cfg(test)]
mod tests {
    use super::*;
    use fluxion_core::store::RunStore;

    fn in_memory_store() -> RunStore {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS workers \
             (url TEXT PRIMARY KEY, registered_at INTEGER NOT NULL, last_health TEXT);",
        )
        .unwrap();
        RunStore::from_conn(conn)
    }

    #[tokio::test]
    async fn empty_store_returns_empty_vec() {
        let store = in_memory_store();
        let result = health_check_all(&store).await.unwrap();
        assert!(result.is_empty());
    }
}

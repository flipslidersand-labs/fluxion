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

    async fn spawn_health_ok_worker() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut buf = [0u8; 1024];
                let _ = sock.read(&mut buf).await;
                let _ = sock
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                    .await;
            }
        });
        format!("http://{addr}")
    }

    async fn closed_port_url() -> String {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        drop(l);
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn healthy_worker_is_marked_reachable_and_updated() {
        let store = in_memory_store();
        let url = spawn_health_ok_worker().await;
        store.register_worker(&url).unwrap();

        let healthy = health_check_all(&store).await.unwrap();
        assert_eq!(healthy, vec![url.clone()]);

        let workers = store.list_workers().unwrap();
        assert_eq!(workers[0].last_health.as_deref(), Some("healthy"));
    }

    #[tokio::test]
    async fn unreachable_worker_is_excluded_and_marked() {
        let store = in_memory_store();
        let url = closed_port_url().await;
        store.register_worker(&url).unwrap();

        let healthy = health_check_all(&store).await.unwrap();
        assert!(healthy.is_empty());

        let workers = store.list_workers().unwrap();
        assert_eq!(workers[0].last_health.as_deref(), Some("unreachable"));
    }
}

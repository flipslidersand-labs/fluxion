/// MCP server over stdio (Content-Length framed JSON-RPC 2.0).
///
/// Exposes three tools Claude can call:
///   workflow_run    — execute a workflow YAML, returns structured run summary
///   workflow_retry  — re-run a failed job and its downstream dependents
///   runs_list       — list recent runs from the SQLite store
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use serde_json::{Value, json};
use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader,
};

use fluxion_core::{store::RunStore, workflow::Workflow};
use fluxion_host::{FluxionHost, scheduler};

// ── Transport ────────────────────────────────────────────────────────────────

async fn read_message<R: AsyncBufRead + Unpin>(reader: &mut R) -> Result<Option<String>> {
    let mut content_length: Option<usize> = None;

    loop {
        let mut line = String::new();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            return Ok(None); // EOF
        }
        let trimmed = line.trim_end_matches('\n').trim_end_matches('\r');
        if trimmed.is_empty() {
            break; // blank line = end of headers
        }
        if let Some(val) = trimmed.strip_prefix("Content-Length: ") {
            content_length = val.trim().parse().ok();
        }
    }

    let len = match content_length {
        Some(l) if l > 0 => l,
        _ => return Ok(Some(String::new())),
    };

    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await?;
    Ok(Some(String::from_utf8(buf)?))
}

async fn write_message<W: AsyncWrite + Unpin>(writer: &mut W, body: &str) -> Result<()> {
    let header = format!("Content-Length: {}\r\n\r\n", body.len());
    writer.write_all(header.as_bytes()).await?;
    writer.write_all(body.as_bytes()).await?;
    writer.flush().await?;
    Ok(())
}

// ── Server ───────────────────────────────────────────────────────────────────

pub async fn serve() -> Result<()> {
    let stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let mut reader = BufReader::new(stdin);

    loop {
        let Some(body) = read_message(&mut reader).await? else {
            break;
        };
        if body.is_empty() {
            continue;
        }

        if let Some(response) = process_message(&body).await {
            write_message(&mut stdout, &response).await?;
        }
    }

    Ok(())
}

/// Handle one decoded JSON-RPC message. Returns the serialized response, or
/// `None` for unparsable bodies and notifications (no `id`), which get no reply.
async fn process_message(body: &str) -> Option<String> {
    let req: Value = serde_json::from_str(body).ok()?;

    let id = req.get("id").cloned();
    let method = req["method"].as_str().unwrap_or("");

    // Notifications have no id and require no response
    id.as_ref()?;

    let result = handle_request(method, req.get("params")).await;

    let response = match result {
        Ok(val) => json!({ "jsonrpc": "2.0", "id": id, "result": val }),
        Err(e) => json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": { "code": -32603, "message": e.to_string() }
        }),
    };
    Some(response.to_string())
}

async fn handle_request(method: &str, params: Option<&Value>) -> Result<Value> {
    match method {
        "initialize" => Ok(json!({
            "protocolVersion": "2024-11-05",
            "serverInfo": { "name": "fluxion-mcp", "version": env!("CARGO_PKG_VERSION") },
            "capabilities": { "tools": {} }
        })),

        "ping" => Ok(json!({})),

        "tools/list" => Ok(json!({
            "tools": [
                {
                    "name": "workflow_run",
                    "description": "Execute a Fluxion workflow YAML (DAG of Wasm components). Returns a structured run summary with per-job status and elapsed time.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "path": {
                                "type": "string",
                                "description": "Absolute or relative path to the workflow YAML file"
                            }
                        },
                        "required": ["path"]
                    }
                },
                {
                    "name": "workflow_retry",
                    "description": "Retry a previous workflow run from a specific failed job. Skips already-succeeded jobs and re-executes from the given job and all its downstream dependents.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "run_id": {
                                "type": "string",
                                "description": "Run ID from the previous execution (e.g. run-1782049716-310867)"
                            },
                            "from": {
                                "type": "string",
                                "description": "Job ID to restart from (e.g. normalize)"
                            }
                        },
                        "required": ["run_id", "from"]
                    }
                },
                {
                    "name": "runs_list",
                    "description": "List recent Fluxion workflow runs with their status.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "limit": {
                                "type": "integer",
                                "description": "Maximum number of runs to return (default 10)"
                            }
                        }
                    }
                },
                {
                    "name": "workflow_status",
                    "description": "Get detailed status of a specific run: run metadata and a per-job table showing status, elapsed time, and failure reason.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "run_id": {
                                "type": "string",
                                "description": "Run ID (e.g. run-1783861257-28913)"
                            }
                        },
                        "required": ["run_id"]
                    }
                },
                {
                    "name": "workflow_logs",
                    "description": "Get the job execution timeline for a specific run, reconstructed from stored elapsed times.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "run_id": {
                                "type": "string",
                                "description": "Run ID (e.g. run-1783861257-28913)"
                            }
                        },
                        "required": ["run_id"]
                    }
                }
            ]
        })),

        "tools/call" => {
            let name = params
                .and_then(|p| p.get("name"))
                .and_then(|n| n.as_str())
                .unwrap_or("");
            let args = params
                .and_then(|p| p.get("arguments"))
                .cloned()
                .unwrap_or(json!({}));

            let text = dispatch_tool(name, &args).await?;
            Ok(json!({ "content": [{ "type": "text", "text": text }] }))
        }

        _ => Err(anyhow::anyhow!("Method not found: {}", method)),
    }
}

// ── Tool dispatch ─────────────────────────────────────────────────────────────

async fn dispatch_tool(name: &str, args: &Value) -> Result<String> {
    match name {
        "workflow_run" => {
            let path = args["path"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("missing 'path'"))?;

            let wf = Workflow::from_file(path)
                .map_err(|e| anyhow::anyhow!("Failed to load '{}': {}", path, e))?;
            let workflow_path = PathBuf::from(path)
                .canonicalize()
                .unwrap_or(PathBuf::from(path));
            let host = Arc::new(FluxionHost::new()?);

            let result = scheduler::run_silent(&wf, &workflow_path, host).await?;
            Ok(serde_json::to_string_pretty(&result)?)
        }

        "workflow_retry" => {
            let run_id = args["run_id"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("missing 'run_id'"))?;
            let from = args["from"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("missing 'from'"))?;

            let store = RunStore::open()?;
            let (workflow_path, _) = store.load_run(run_id)?;
            let wf = Workflow::from_file(&workflow_path)
                .map_err(|e| anyhow::anyhow!("Failed to load workflow: {}", e))?;
            let wp = PathBuf::from(&workflow_path);
            let host = Arc::new(FluxionHost::new()?);

            let result = scheduler::retry_silent(&wf, &wp, host, run_id, from).await?;
            Ok(serde_json::to_string_pretty(&result)?)
        }

        "runs_list" => {
            let limit = args["limit"].as_u64().unwrap_or(10) as usize;
            let store = RunStore::open()?;
            let runs = store.list_runs(limit)?;

            let items: Vec<Value> = runs
                .iter()
                .map(|r| {
                    json!({
                        "id": r.id,
                        "workflow_name": r.workflow_name,
                        "started_at": r.started_at,
                        "status": r.status
                    })
                })
                .collect();

            Ok(serde_json::to_string_pretty(&items)?)
        }

        "workflow_status" => {
            let run_id = args["run_id"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("missing 'run_id'"))?;

            let store = RunStore::open()?;
            let run = store.get_run(run_id)?;
            let jobs = store.get_run_jobs(run_id)?;

            let elapsed_s = run
                .completed_at
                .map(|end| (end - run.started_at) as f64)
                .unwrap_or(0.0);

            let result = json!({
                "run": {
                    "id": run.id,
                    "workflow_name": run.workflow_name,
                    "workflow_path": run.workflow_path,
                    "started_at": run.started_at,
                    "elapsed_s": elapsed_s,
                    "status": run.status,
                },
                "jobs": jobs.iter().map(|j| json!({
                    "job_id": j.job_id,
                    "status": j.status,
                    "elapsed_ms": j.elapsed_ms,
                    "reason": j.reason,
                })).collect::<Vec<_>>()
            });
            Ok(serde_json::to_string_pretty(&result)?)
        }

        "workflow_logs" => {
            let run_id = args["run_id"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("missing 'run_id'"))?;

            let store = RunStore::open()?;
            let run = store.get_run(run_id)?;
            let jobs = store.get_run_jobs(run_id)?;

            // Reconstruct approximate timeline from stored elapsed times.
            let mut events: Vec<Value> = Vec::new();
            let mut cursor = run.started_at;
            for j in &jobs {
                let elapsed_ms = j.elapsed_ms.unwrap_or(0);
                events.push(json!({
                    "ts": cursor,
                    "job_id": j.job_id,
                    "event": "RUNNING",
                }));
                cursor += elapsed_ms / 1000;
                let mut entry = json!({
                    "ts": cursor,
                    "job_id": j.job_id,
                    "event": j.status.to_uppercase(),
                    "elapsed_s": elapsed_ms as f64 / 1000.0,
                });
                if let Some(ref reason) = j.reason {
                    entry["reason"] = json!(reason);
                }
                events.push(entry);
            }
            Ok(serde_json::to_string_pretty(&events)?)
        }

        _ => Err(anyhow::anyhow!("Unknown tool: {}", name)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn frame(body: &str) -> Vec<u8> {
        format!("Content-Length: {}\r\n\r\n{}", body.len(), body).into_bytes()
    }

    async fn reply(req: Value) -> Value {
        let out = process_message(&req.to_string())
            .await
            .expect("request with id must get a response");
        serde_json::from_str(&out).unwrap()
    }

    // ── framing ──────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn read_message_reads_framed_bodies_then_eof() {
        let mut data = frame(r#"{"a":1}"#);
        data.extend(frame(r#"{"b":"日本語"}"#));
        let mut r = Cursor::new(data);
        assert_eq!(read_message(&mut r).await.unwrap().unwrap(), r#"{"a":1}"#);
        // Content-Length counts bytes, so multi-byte bodies must round-trip.
        assert_eq!(
            read_message(&mut r).await.unwrap().unwrap(),
            r#"{"b":"日本語"}"#
        );
        assert!(read_message(&mut r).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn read_message_empty_input_is_eof() {
        let mut r = Cursor::new(Vec::<u8>::new());
        assert!(read_message(&mut r).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn read_message_without_or_zero_content_length_yields_empty_string() {
        let mut r = Cursor::new(b"X-Other: 1\r\n\r\n".to_vec());
        assert_eq!(read_message(&mut r).await.unwrap().unwrap(), "");
        let mut r = Cursor::new(b"Content-Length: 0\r\n\r\n".to_vec());
        assert_eq!(read_message(&mut r).await.unwrap().unwrap(), "");
    }

    #[tokio::test]
    async fn read_message_header_name_is_case_sensitive() {
        // Pins current behaviour: only the exact "Content-Length: " prefix is
        // recognised, so a lowercase header is treated as missing.
        let body = r#"{"a":1}"#;
        let data = format!("content-length: {}\r\n\r\n{}", body.len(), body);
        let mut r = Cursor::new(data.into_bytes());
        assert_eq!(read_message(&mut r).await.unwrap().unwrap(), "");
    }

    #[tokio::test]
    async fn read_message_truncated_body_is_error() {
        let mut r = Cursor::new(b"Content-Length: 50\r\n\r\n{}".to_vec());
        assert!(read_message(&mut r).await.is_err());
    }

    #[tokio::test]
    async fn write_message_emits_content_length_frame() {
        let mut out: Vec<u8> = Vec::new();
        write_message(&mut out, "{\"é\":1}").await.unwrap();
        // "é" is 2 bytes in UTF-8: body is 8 bytes, not 7 chars.
        assert_eq!(out, b"Content-Length: 8\r\n\r\n{\"\xc3\xa9\":1}");
    }

    #[tokio::test]
    async fn write_then_read_roundtrip() {
        let mut buf: Vec<u8> = Vec::new();
        write_message(&mut buf, r#"{"x":true}"#).await.unwrap();
        let mut r = Cursor::new(buf);
        assert_eq!(
            read_message(&mut r).await.unwrap().unwrap(),
            r#"{"x":true}"#
        );
    }

    // ── protocol ─────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn initialize_reports_server_info_and_tools_capability() {
        let v = reply(json!({"jsonrpc":"2.0","id":1,"method":"initialize"})).await;
        assert_eq!(v["jsonrpc"], "2.0");
        assert_eq!(v["id"], 1);
        assert_eq!(v["result"]["serverInfo"]["name"], "fluxion-mcp");
        assert_eq!(v["result"]["protocolVersion"], "2024-11-05");
        assert!(v["result"]["capabilities"]["tools"].is_object());
    }

    #[tokio::test]
    async fn ping_returns_empty_object_and_echoes_string_id() {
        let v = reply(json!({"jsonrpc":"2.0","id":"abc","method":"ping"})).await;
        assert_eq!(v["id"], "abc");
        assert_eq!(v["result"], json!({}));
    }

    #[tokio::test]
    async fn tools_list_returns_the_five_tools() {
        let v = reply(json!({"jsonrpc":"2.0","id":2,"method":"tools/list"})).await;
        let mut names: Vec<&str> = v["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap())
            .collect();
        names.sort();
        assert_eq!(
            names,
            [
                "runs_list",
                "workflow_logs",
                "workflow_retry",
                "workflow_run",
                "workflow_status"
            ]
        );
        for t in v["result"]["tools"].as_array().unwrap() {
            assert_eq!(t["inputSchema"]["type"], "object", "{t}");
        }
    }

    #[tokio::test]
    async fn notification_without_id_gets_no_response() {
        let n = json!({"jsonrpc":"2.0","method":"notifications/initialized"});
        assert!(process_message(&n.to_string()).await.is_none());
        // Even a method that would error produces no reply without an id.
        let n = json!({"jsonrpc":"2.0","method":"nope"});
        assert!(process_message(&n.to_string()).await.is_none());
    }

    #[tokio::test]
    async fn invalid_json_gets_no_response() {
        assert!(process_message("not json").await.is_none());
    }

    #[tokio::test]
    async fn unknown_method_is_error_minus_32603() {
        // Pins current behaviour: JSON-RPC says -32601 (Method not found) but
        // every error is reported as -32603 (Internal error).
        let v = reply(json!({"jsonrpc":"2.0","id":3,"method":"nope"})).await;
        assert_eq!(v["error"]["code"], -32603);
        assert!(
            v["error"]["message"]
                .as_str()
                .unwrap()
                .contains("Method not found: nope")
        );
        assert!(v.get("result").is_none());
    }

    // ── tools/call error paths (no store / Wasm access) ──────────────────────

    async fn call_tool(name: &str, args: Value) -> Value {
        reply(json!({
            "jsonrpc":"2.0","id":9,"method":"tools/call",
            "params": { "name": name, "arguments": args }
        }))
        .await
    }

    fn err_message(v: &Value) -> &str {
        v["error"]["message"]
            .as_str()
            .unwrap_or_else(|| panic!("{v}"))
    }

    #[tokio::test]
    async fn unknown_tool_is_error() {
        let v = call_tool("nope", json!({})).await;
        assert!(err_message(&v).contains("Unknown tool: nope"));
    }

    #[tokio::test]
    async fn missing_name_is_unknown_tool() {
        let v = reply(json!({"jsonrpc":"2.0","id":4,"method":"tools/call"})).await;
        assert!(err_message(&v).contains("Unknown tool"));
    }

    #[tokio::test]
    async fn workflow_run_requires_path() {
        let v = call_tool("workflow_run", json!({})).await;
        assert!(err_message(&v).contains("missing 'path'"));
        // Non-string path is treated as missing.
        let v = call_tool("workflow_run", json!({"path": 5})).await;
        assert!(err_message(&v).contains("missing 'path'"));
    }

    #[tokio::test]
    async fn workflow_run_with_missing_file_reports_load_failure() {
        let v = call_tool("workflow_run", json!({"path": "/nonexistent/wf.yaml"})).await;
        assert!(err_message(&v).contains("Failed to load '/nonexistent/wf.yaml'"));
    }

    #[tokio::test]
    async fn workflow_retry_requires_run_id_and_from() {
        let v = call_tool("workflow_retry", json!({"from": "a"})).await;
        assert!(err_message(&v).contains("missing 'run_id'"));
        let v = call_tool("workflow_retry", json!({"run_id": "r"})).await;
        assert!(err_message(&v).contains("missing 'from'"));
    }

    #[tokio::test]
    async fn workflow_status_and_logs_require_run_id() {
        for tool in ["workflow_status", "workflow_logs"] {
            let v = call_tool(tool, json!({})).await;
            assert!(err_message(&v).contains("missing 'run_id'"), "{tool}");
        }
    }
}

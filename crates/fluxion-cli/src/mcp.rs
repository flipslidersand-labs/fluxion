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
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

use fluxion_core::{store::RunStore, workflow::Workflow};
use fluxion_host::{FluxionHost, scheduler};

// ── Transport ────────────────────────────────────────────────────────────────

async fn read_message(reader: &mut BufReader<tokio::io::Stdin>) -> Result<Option<String>> {
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

async fn write_message(writer: &mut tokio::io::Stdout, body: &str) -> Result<()> {
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

        let req: Value = match serde_json::from_str(&body) {
            Ok(v) => v,
            Err(_) => continue,
        };

        let id = req.get("id").cloned();
        let method = req["method"].as_str().unwrap_or("");

        // Notifications have no id and require no response
        if id.is_none() {
            continue;
        }

        let result = handle_request(method, req.get("params")).await;

        let response = match result {
            Ok(val) => json!({ "jsonrpc": "2.0", "id": id, "result": val }),
            Err(e) => json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": { "code": -32603, "message": e.to_string() }
            }),
        };

        write_message(&mut stdout, &response.to_string()).await?;
    }

    Ok(())
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

            let opts = scheduler::RunOptions {
                progress: false,
                ..Default::default()
            };
            let result = scheduler::run_with_options(&wf, &workflow_path, host, &opts).await?;
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

            let opts = scheduler::RunOptions {
                progress: false,
                ..Default::default()
            };
            let result = scheduler::retry_with_options(&wf, &wp, host, run_id, from, &opts).await?;
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

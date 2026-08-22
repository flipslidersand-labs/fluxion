# Fluxion

[![CI](https://github.com/flipslidersand/fluxion/actions/workflows/ci.yml/badge.svg)](https://github.com/flipslidersand/fluxion/actions/workflows/ci.yml)

Safe Wasm-based job execution engine with capability-controlled sandboxing and DAG scheduling.

Each job runs as a WebAssembly component — isolated, permission-scoped, and language-agnostic.

## Required Tools

- Rust 1.82+
- `wasm32-wasip1` target: `rustup target add wasm32-wasip1`
- `wasm32-wasip2` target: `rustup target add wasm32-wasip2` (network-probe only)
- `cargo-component`: `cargo install cargo-component`

## Setup & Build

```bash
git clone https://github.com/flipslidersand/fluxion
cd fluxion

# Build the CLI (use -j2 to avoid OOM — wasmtime is large)
cargo build -j2

# Build components (each is an independent workspace)
for c in hello file-reader pipeline-stage spin alloc-bomb; do
  cargo build --manifest-path components/$c/Cargo.toml \
    --target wasm32-wasip1
done
# network-probe uses wasip2
cargo build --manifest-path components/network-probe/Cargo.toml \
  --target wasm32-wasip2
```

## CLI Commands

```bash
# Run a YAML workflow
fluxion run examples/vehicle-pipeline.yaml

# Retry a previous run from a specific job
fluxion retry <run-id> --from validate

# Check run status
fluxion status <run-id>

# Show per-job logs
fluxion logs <run-id>

# List recent runs
fluxion runs list [--limit 20]

# Validate a workflow YAML (structure + optional component paths)
fluxion validate examples/vehicle-pipeline.yaml
fluxion validate examples/vehicle-pipeline.yaml --json  # machine-readable output
fluxion validate examples/vehicle-pipeline.yaml --skip-wasm-check  # skip path checks

# Watch a workflow YAML and re-run on every save
fluxion watch examples/vehicle-pipeline.yaml
fluxion watch examples/vehicle-pipeline.yaml --debounce 200  # debounce in ms (default 500)

# Inspect a component's WIT interface
fluxion inspect components/pipeline-stage/target/wasm32-wasip1/debug/pipeline_stage.wasm

# Run a single component directly
fluxion component run components/hello/target/wasm32-wasip1/debug/hello.wasm \
  --input "hello"
```

## Directory Structure

```text
fluxion/
├── crates/
│   ├── fluxion-cli/     # CLI binary (fluxion command) + MCP server
│   ├── fluxion-core/    # Workflow types, DAG scheduler, run store (SQLite)
│   └── fluxion-host/    # Wasmtime host — loads/runs Wasm components
│       └── tests/
│           └── e2e.rs   # E2E integration tests (#[ignore])
├── components/
│   ├── hello/           # Minimal hello-world (wasip1)
│   ├── file-reader/     # Reads a file path from input; demonstrates FS caps
│   ├── network-probe/   # TCP connect; demonstrates network caps (wasip2)
│   ├── pipeline-stage/  # 4-stage ETL: fetch → normalize → validate → export
│   ├── spin/            # CPU busy-loop; used in timeout / epoch-interrupt demos
│   └── alloc-bomb/      # Allocates N MB; used in OOM / StoreLimits demos
├── examples/
│   ├── vehicle-pipeline.yaml     # 4-stage DAG — validate fails on bad year, retry fixes it
│   ├── resource-limits-demo.yaml # spin-forever killed by epoch timeout after 2s
│   ├── memory-limits-demo.yaml   # oom-job rejected by StoreLimits before code runs
│   ├── sandbox-demo.yaml         # FS cap: read-allowed vs read-denied
│   ├── network-sandbox.yaml      # Network cap: connect-allowed vs connect-denied
│   └── three-stage.yaml          # Simple 3-step sequential pipeline
├── wit/
│   └── task.wit         # WIT interface: fluxion:task/processor
├── mcp.json             # MCP server config (copy to project root, adjust binary path)
└── docs/
    ├── spec.md
    ├── tech-stack.md
    ├── data-model.md
    ├── implementation-guide.md
    └── adr/
```

## YAML Workflow Format

```yaml
name: my-workflow
version: "1.0"

jobs:
  step-a:
    component: path/to/component.wasm
    input: "payload string"
    permissions:
      filesystem:
        read: [/tmp/data]
        write: [/tmp/out]
      network:
        allow: ["93.184.216.34:443"] # IP-based allowlist only (no hostnames)
      limits: # ⚠️ must be nested under permissions:
        memory_mb: 64
        timeout_secs: 10

  step-b:
    component: path/to/other.wasm
    depends_on: [step-a] # DAG edge; multiple deps allowed
    permissions:
      filesystem:
        read: [/tmp/out]
      network:
        allow: [] # empty = deny all
      limits:
        memory_mb: 32
        timeout_secs: 5
```

> **Gotcha**: `limits` must be written **under `permissions:`**, not at the job top level.
> A top-level `limits:` key is silently ignored — memory and timeout use their defaults (256 MB / 60 s).

## Examples

### vehicle-pipeline — validate failure + retry

Row 184 of the generated data has `year=1999` (invalid). `validate` rejects it and the run fails.
Fix the CSV and retry from `validate` — only `validate` and `export` re-run:

```bash
fluxion run examples/vehicle-pipeline.yaml
# → FAILED at validate (year=1999 at row 184)

# Fix the bad row
RUN_ID=<run-id from above>
sed -i 's/,1999,/,2019,/' ~/.fluxion-pipeline/normalized.csv

fluxion retry $RUN_ID --from validate
# → validate ✅  export ✅
```

### resource-limits-demo — epoch timeout

`spin-forever` iterates `u64::MAX` times. With `timeout_secs: 2`, the wasmtime epoch ticker
kills it at the next loop back-edge after ~2 s. `fast-sum` (1M iters) completes normally:

```bash
fluxion run examples/resource-limits-demo.yaml
# spin-forever  FAILED   2.01s   (Timeout after 2s)
# fast-sum      SUCCESS  0.01s
```

### memory-limits-demo — StoreLimits OOM

`oom-job` tries to allocate 10 MB within a 1 MB limit. `StoreLimits` rejects the instantiation
before any user code runs — the failure is near-instant:

```bash
fluxion run examples/memory-limits-demo.yaml
# ok-job   SUCCESS  0.02s
# oom-job  FAILED   0.00s   (OOM: component exceeded memory_mb=1 limit)
```

### map-reduce-demo — static foreach + reduce

`transform` is expanded into one child per item (static fan-out);
`aggregate` collects all outputs via `input_from` + `reduce`:

```bash
fluxion run examples/map-reduce/workflow.yaml
# transform.0  succeeded   0.01s
# transform.1  succeeded   0.01s
# transform.2  succeeded   0.01s
# aggregate    succeeded   0.01s
```

YAML structure:

```yaml
name: map-reduce-demo
jobs:
  transform:
    component: hello.wasm
    input: '["alpha", "beta", "gamma"]'
    foreach: "$[*]"             # fan-out: one child job per item

  aggregate:
    component: hello.wasm
    depends_on: [transform]
    input_from: transform       # collect all transform outputs
    reduce: json_array          # wrap outputs as a JSON array
```

**`reduce` modes:**

| Mode | Behaviour |
|------|-----------|
| `json_array` *(default)* | Wraps each child output as a JSON array element |
| `concat` | Concatenates raw bytes |
| `json_merge` | Deep-merges JSON objects |
| `{ component: path/to/reducer.wasm }` | Pipes all outputs through a custom Wasm reducer |

## MCP Integration

Fluxion exposes an MCP server for use with Claude Code or any MCP-compatible AI editor.

Update the binary path in `mcp.json` and place it in your project root:

```json
{
  "mcpServers": {
    "fluxion": {
      "command": "/path/to/fluxion/target/debug/fluxion",
      "args": ["mcp-serve"]
    }
  }
}
```

Available tools:

| Tool              | Description                              |
| ----------------- | ---------------------------------------- |
| `workflow_run`    | Run a YAML workflow by path              |
| `workflow_retry`  | Retry a previous run from a specific job |
| `runs_list`       | List recent runs (newest first)          |
| `workflow_status` | Get status of a specific run             |
| `workflow_logs`   | Get per-job logs of a specific run       |

## E2E Tests

Integration tests are marked `#[ignore]` (they require pre-built Wasm components):

```bash
# Run E2E tests (all three scenarios)
cargo test --package fluxion-host --test e2e -- --ignored

# vehicle-pipeline validate→retry flow     (~2s)
# resource-limits spin-forever timeout     (~2s)
# memory-limits OOM enforcement            (<1s)
```

## OCI Registry

Push and pull Wasm components to/from OCI-compatible registries:

```bash
# Set up a local registry (optional)
docker run -d -p 5000:5000 registry:2

# Push a component
fluxion registry push components/hello/target/wasm32-wasip1/debug/hello.wasm \
  --to localhost:5000/fluxion/hello:latest

# Pull a component (auto-pulls before execution)
fluxion run examples/oci-registry.yaml
```

In YAML workflows, reference registry components via `oci_ref`:

```yaml
jobs:
  greet:
    oci_ref: localhost:5000/fluxion/hello:latest
    input: '{"name":"World"}'
```

## Status

| Phase   | Description                                | Status  |
| ------- | ------------------------------------------ | ------- |
| Phase 1 | Wasm component runtime + CLI               | ✅ Done |
| Phase 2 | YAML workflow + DAG scheduler              | ✅ Done |
| Phase 3 | Capability sandbox (FS / network / limits) | ✅ Done |
| Phase 4 | Persistence (SQLite) + retry from any job  | ✅ Done |
| Phase 5 | OpenTelemetry tracing                      | ✅ Done |
| Phase 6 | MCP server (5 tools)                       | ✅ Done |

## License

MIT

#!/usr/bin/env bash
# Run Criterion benchmarks for fluxion-host.
#
# Usage:
#   ./scripts/bench.sh                     # run all benchmarks
#   ./scripts/bench.sh workflow_run        # run only workflow_run group
#   ./scripts/bench.sh -- --output-format bencher  # bencher format (for CI)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Ensure hello.wasm is built (required by bench_workflow_run and bench_run_component).
HELLO_WASM="$REPO_ROOT/components/hello/target/wasm32-wasip1/debug/hello.wasm"
if [[ ! -f "$HELLO_WASM" ]]; then
    echo "==> Building hello component (required for benchmarks)..."
    (cd "$REPO_ROOT/components/hello" && cargo component build)
fi

echo "==> Running fluxion-host benchmarks..."
cargo bench -p fluxion-host "$@"

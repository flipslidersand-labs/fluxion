#!/usr/bin/env bash
# Build components/python-hello and verify it runs.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPONENT_DIR="$REPO_ROOT/components/python-hello"
OUT="$COMPONENT_DIR/hello.wasm"

cd "$REPO_ROOT"

echo "==> Checking componentize-py..."
if ! command -v componentize-py &>/dev/null; then
    echo "ERROR: componentize-py not found. Install with: pip install componentize-py" >&2
    exit 1
fi

echo "==> Building $COMPONENT_DIR/task.py -> $OUT"
fluxion build python "$COMPONENT_DIR/task.py" \
    --out "$OUT" \
    --wit-path "$REPO_ROOT/wit"

echo "==> Inspecting component..."
fluxion inspect "$OUT"

echo "==> Running component..."
fluxion component run "$OUT"

echo ""
echo "SUCCESS: python-hello built and ran at $OUT"

#!/usr/bin/env bash
# Build components/python-hello and run a quick smoke test.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPONENT_DIR="$REPO_ROOT/components/python-hello"
WIT_DIR="$REPO_ROOT/wit"
OUTPUT="$COMPONENT_DIR/hello.wasm"
FLUXION="${FLUXION:-$REPO_ROOT/target/debug/fluxion}"

echo "==> Checking componentize-py..."
if ! command -v componentize-py &>/dev/null; then
    echo "ERROR: componentize-py not found. Install with: pip install componentize-py" >&2
    exit 1
fi
componentize-py --version

echo ""
echo "==> Building components/python-hello/hello.py..."
componentize-py componentize \
    --wit-path "$WIT_DIR" \
    --world task-component \
    "$COMPONENT_DIR/hello.py" \
    -o "$OUTPUT"

echo ""
echo "==> Smoke test: fluxion component run hello.wasm --input 'world'"
"$FLUXION" component run "$OUTPUT" --input 'world'

echo ""
echo "✓ python-hello build and smoke test passed"

#!/usr/bin/env bash
# Build components/python-hello and run a smoke test.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPONENT_DIR="$REPO_ROOT/components/python-hello"
OUTPUT="$COMPONENT_DIR/hello.wasm"
FLUXION="$REPO_ROOT/target/debug/fluxion"

echo "==> Building python-hello component..."
"$FLUXION" build python "$COMPONENT_DIR/task.py" \
  -o "$OUTPUT" \
  --wit-path "$REPO_ROOT/wit"

echo "==> Running smoke test..."
result=$("$FLUXION" component run "$OUTPUT" --input "fluxion")
echo "    output: $result"

if echo "$result" | grep -q "hello from python"; then
  echo "==> OK"
else
  echo "==> FAIL: expected 'hello from python' in output"
  exit 1
fi

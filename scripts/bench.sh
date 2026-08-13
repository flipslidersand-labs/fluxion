#!/usr/bin/env bash
# Run fluxion benchmarks. Passes all arguments to cargo bench.
# Usage: ./scripts/bench.sh [cargo bench args]
# Examples:
#   ./scripts/bench.sh                           # run all benches
#   ./scripts/bench.sh --bench host_bench        # run host bench only
#   ./scripts/bench.sh -- --test                 # dry-run (no measurements)
#   ./scripts/bench.sh -- workflow_run           # filter by name
set -euo pipefail
cargo bench -j2 "$@"

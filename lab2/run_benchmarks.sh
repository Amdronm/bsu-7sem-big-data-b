#!/usr/bin/env bash
# Runner for Performance Benchmarks & Chart Generation

set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_PYTHON="$DIR/../.venv/bin/python"

if [ ! -f "$VENV_PYTHON" ]; then
  VENV_PYTHON="python3"
fi

echo "============================================================"
echo "Running Latency & Throughput Benchmarks..."
echo "Output Directory: $DIR/report"
echo "============================================================"

"$VENV_PYTHON" "$DIR/benchmark/run_live_benchmark.py" --output-dir "report" "$@"


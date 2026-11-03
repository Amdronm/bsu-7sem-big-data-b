#!/usr/bin/env bash
# Runner for Kafka User Events Producer using the project virtualenv

set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_PYTHON="$DIR/../.venv/bin/python"

if [ ! -f "$VENV_PYTHON" ]; then
  VENV_PYTHON="python3"
fi

echo "============================================================"
echo "Starting Kafka User Events Stream Producer..."
echo "Bootstrap: localhost:9092 | Topic: user-events"
echo "============================================================"

"$VENV_PYTHON" "$DIR/producer/producer.py" "$@"


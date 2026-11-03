#!/usr/bin/env bash
# Runner for Spark Structured Streaming using Docker container
# Avoids host Java 25 / Python 3.14 incompatibilities

set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODE="${1:-all}"
INTERVAL="${2:-2 seconds}"

echo "============================================================"
echo "Starting Spark Structured Streaming in Docker..."
echo "Mode: $MODE | Interval: $INTERVAL"
echo "============================================================"

docker run --rm -it -u root --network lab2-network \
  -e IVY_HOME=/tmp/.ivy2 \
  -v "$DIR":/app \
  apache/spark:3.5.3 \
  /opt/spark/bin/spark-submit \
  --master "local[2]" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  /app/spark_app/spark_stream.py \
  --bootstrap-servers kafka:29092 \
  --mode "$MODE" \
  --trigger-interval "$INTERVAL"


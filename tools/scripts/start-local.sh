#!/usr/bin/env bash
# Starts the full local AIRiskOps stack in Docker.
# Use when you already have a built JAR and want to bring up Kafka, Flink,
# Prometheus, Grafana, and the checkpoint exporter for local verification.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/deployment/local/docker-compose.yml"

cd "$ROOT_DIR"
mkdir -p \
  runtime/policies \
  runtime/replay/latest \
  runtime/flink-state/checkpoints \
  runtime/flink-state/savepoints \
  runtime/flink-state/rocksdb \
  flink-job/target
chmod 0777 \
  runtime/flink-state \
  runtime/flink-state/checkpoints \
  runtime/flink-state/savepoints \
  runtime/flink-state/rocksdb
docker compose -f "$COMPOSE_FILE" up -d kafka jobmanager taskmanager checkpoint-exporter prometheus grafana

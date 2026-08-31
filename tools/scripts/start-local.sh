#!/usr/bin/env bash
# Starts the full local AIRiskOps stack in Docker.
# Use when you already have a built JAR and want to bring up Kafka, Flink,
# Prometheus, and Grafana for local development or verification.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/deployment/local/docker-compose.yml"

cd "$ROOT_DIR"
mkdir -p runtime/policies runtime/replay/latest flink-job/target
docker compose -f "$COMPOSE_FILE" up -d kafka jobmanager taskmanager prometheus grafana

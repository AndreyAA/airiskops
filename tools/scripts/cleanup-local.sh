#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/deployment/local/docker-compose.yml"

cd "$ROOT_DIR"

echo "[cleanup] stopping local docker stack"
docker compose -f "$COMPOSE_FILE" down -v || true

echo "[cleanup] removing generated replay data"
rm -rf runtime/replay/latest
mkdir -p runtime/replay/latest

echo "[cleanup] removing generated policy snapshots"
rm -rf runtime/policies
mkdir -p runtime/policies

echo "[cleanup] removing build artifacts"
rm -rf flink-job/target

echo "[cleanup] local environment reset completed"

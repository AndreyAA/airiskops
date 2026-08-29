#!/usr/bin/env bash
# Fully resets the local AISafetyOps environment.
# Use when you need a clean start: it removes the Docker stack, runtime data,
# and build artifacts, then recreates the required local directories.
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
mkdir -p flink-job/target

echo "[cleanup] local environment reset completed"

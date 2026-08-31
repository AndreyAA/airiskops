#!/usr/bin/env bash
# Stops and removes the local AIRiskOps Docker stack.
# Use when you want to shut down the local environment without keeping
# containers, volumes, or compose state.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/deployment/local/docker-compose.yml"

cd "$ROOT_DIR"
docker compose -f "$COMPOSE_FILE" down -v

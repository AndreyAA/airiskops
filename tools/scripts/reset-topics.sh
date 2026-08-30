#!/usr/bin/env bash
# Resets the main Kafka topics used by the MVP pipeline.
# Use when the Docker stack is already running and you want to clear Kafka
# history without doing a full environment cleanup.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/deployment/local/docker-compose.yml"
KAFKA_BIN="/opt/kafka/bin"
TOPICS=(
  "agent-requests"
  "agent-responses"
  "guardrail-findings"
  "normalized-events"
  "invalid-events"
  "late-events"
  "guardrail-aggregates"
  "basic-incidents"
)

cd "$ROOT_DIR"

for topic in "${TOPICS[@]}"; do
  docker compose -f "$COMPOSE_FILE" exec -T kafka "${KAFKA_BIN}/kafka-topics.sh" \
    --bootstrap-server kafka:9092 \
    --delete \
    --if-exists \
    --topic "$topic" || true
done

sleep 3

bash tools/scripts/init-topics.sh

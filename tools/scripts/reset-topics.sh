#!/usr/bin/env bash
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

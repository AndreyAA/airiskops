#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/deployment/local/docker-compose.yml"
KAFKA_BIN="/opt/kafka/bin"
TOPICS=(
  "normalized-events"
  "invalid-events"
  "late-events"
  "guardrail-aggregates"
)

cd "$ROOT_DIR"

for topic in "${TOPICS[@]}"; do
  echo "== $topic offsets =="
  docker compose -f "$COMPOSE_FILE" exec -T kafka "${KAFKA_BIN}/kafka-get-offsets.sh" \
    --bootstrap-server kafka:9092 \
    --topic "$topic"
done

echo "== normalized-events sample =="
docker compose -f "$COMPOSE_FILE" exec -T kafka "${KAFKA_BIN}/kafka-console-consumer.sh" \
  --bootstrap-server kafka:9092 \
  --topic normalized-events \
  --partition 0 \
  --offset 0 \
  --max-messages 1

echo "== guardrail-aggregates sample =="
docker compose -f "$COMPOSE_FILE" exec -T kafka "${KAFKA_BIN}/kafka-console-consumer.sh" \
  --bootstrap-server kafka:9092 \
  --topic guardrail-aggregates \
  --partition 0 \
  --offset 0 \
  --max-messages 3

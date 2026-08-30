#!/usr/bin/env bash
# Checks the main output Kafka topics and prints sample records.
# Use after replay or live generation to verify that the pipeline produced
# normalized events and guardrail aggregates without opening Kafka manually.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/deployment/local/docker-compose.yml"
KAFKA_BIN="/opt/kafka/bin"
TOPICS=(
  "normalized-events"
  "invalid-events"
  "late-events"
  "guardrail-aggregates"
  "basic-incidents"
  "guardrail-quality-metrics"
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

echo "== basic-incidents sample =="
docker compose -f "$COMPOSE_FILE" exec -T kafka "${KAFKA_BIN}/kafka-console-consumer.sh" \
  --bootstrap-server kafka:9092 \
  --topic basic-incidents \
  --partition 0 \
  --offset 0 \
  --max-messages 3

echo "== guardrail-quality-metrics sample =="
docker compose -f "$COMPOSE_FILE" exec -T kafka "${KAFKA_BIN}/kafka-console-consumer.sh" \
  --bootstrap-server kafka:9092 \
  --topic guardrail-quality-metrics \
  --partition 0 \
  --offset 0 \
  --max-messages 3

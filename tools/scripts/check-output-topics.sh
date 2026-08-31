#!/usr/bin/env bash
# Checks the main output Kafka topics and prints sample records.
# Use after replay or live generation to verify that the pipeline produced
# normalized events and guardrail aggregates without opening Kafka manually.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/deployment/local/docker-compose.yml"
KAFKA_BIN="/opt/kafka/bin"
SAMPLE_TIMEOUT_MS=5000
TOPICS=(
  "normalized-events"
  "invalid-events"
  "late-events"
  "guardrail-aggregates"
  "basic-incidents"
  "guardrail-quality-metrics"
)

cd "$ROOT_DIR"

print_topic_sample() {
  local topic="$1"
  local sample_output
  echo "== $topic sample =="
  if sample_output="$(docker compose -f "$COMPOSE_FILE" exec -T kafka "${KAFKA_BIN}/kafka-console-consumer.sh" \
    --bootstrap-server kafka:9092 \
    --topic "$topic" \
    --partition 0 \
    --offset 0 \
    --max-messages 1 \
    --timeout-ms "$SAMPLE_TIMEOUT_MS")"; then
    printf '%s\n' "$sample_output"
  else
    echo "No sample message received within ${SAMPLE_TIMEOUT_MS}ms for topic $topic"
  fi
}

for topic in "${TOPICS[@]}"; do
  echo "== $topic offsets =="
  docker compose -f "$COMPOSE_FILE" exec -T kafka "${KAFKA_BIN}/kafka-get-offsets.sh" \
    --bootstrap-server kafka:9092 \
    --topic "$topic"
done

print_topic_sample "normalized-events"
print_topic_sample "guardrail-aggregates"
print_topic_sample "basic-incidents"
print_topic_sample "guardrail-quality-metrics"

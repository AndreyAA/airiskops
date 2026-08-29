#!/usr/bin/env bash
# Creates the Kafka topics required by the local MVP.
# Use after the local stack is up, or whenever you need to bootstrap topics
# before submitting the Flink job or replaying test traffic.
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
  "guardrail-quality-metrics"
  "policy-updates"
  "debug-incidents"
)

cd "$ROOT_DIR"

for topic in "${TOPICS[@]}"; do
  docker compose -f "$COMPOSE_FILE" exec -T kafka "${KAFKA_BIN}/kafka-topics.sh" \
    --bootstrap-server localhost:9092 \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions 3 \
    --replication-factor 1
done

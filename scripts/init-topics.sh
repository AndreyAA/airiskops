#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
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
  docker compose exec -T kafka "${KAFKA_BIN}/kafka-topics.sh" \
    --bootstrap-server localhost:9092 \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions 3 \
    --replication-factor 1
done

#!/usr/bin/env bash
# Copies the selected policy YAML into the active local runtime location and can
# optionally publish it to the runtime policy-updates topic.
# Use before submitting the job for bootstrap policy, or with --publish when you
# want to update policy thresholds in the running Flink job without redeploy.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/deployment/local/docker-compose.yml"
KAFKA_BIN="/opt/kafka/bin"
SOURCE_FILE="${1:-$ROOT_DIR/config/policies/default-policy.yaml}"
TARGET_DIR="$ROOT_DIR/runtime/policies"
TARGET_FILE="$TARGET_DIR/active-policy.yaml"
PUBLISH_MODE="${2:-}"

mkdir -p "$TARGET_DIR"

if [[ ! -f "$SOURCE_FILE" ]]; then
  echo "Policy file not found: $SOURCE_FILE" >&2
  exit 1
fi

cp "$SOURCE_FILE" "$TARGET_FILE"
echo "Active policy loaded: $TARGET_FILE"

if [[ "$PUBLISH_MODE" == "--publish" ]]; then
  awk '{printf "%s\\\\n", $0}' "$TARGET_FILE" | \
    docker compose -f "$COMPOSE_FILE" exec -T kafka "${KAFKA_BIN}/kafka-console-producer.sh" \
      --bootstrap-server kafka:9092 \
      --topic policy-updates >/dev/null
  echo "Policy update published to topic: policy-updates"
fi

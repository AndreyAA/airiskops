#!/usr/bin/env bash
# Generates a deterministic replay dataset and publishes it into local Kafka topics.
# Use when you need a one-shot scenario run for demos, regression checks,
# late/invalid/error delivery tests, or incident-policy validation.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/deployment/local/docker-compose.yml"
KAFKA_BIN="/opt/kafka/bin"
BUSINESS_SCENARIO="mixed"
DELIVERY_MODE="baseline"
REQUESTS=120
SESSIONS=12
AGENT_ID="agent-risk-01"
SEED=42
OUT_DIR="$ROOT_DIR/runtime/replay/latest"
REQUEST_OFFSET_SECONDS=2
BURST_START_SECOND=60
BURST_DURATION_SECONDS=90
BURST_MULTIPLIER=1.8
LATE_SHARE=0.12
TOO_LATE_SHARE=0.05
INVALID_SHARE=0.05
ERROR_SHARE=0.08
DETECTOR_LATENCY_MULTIPLIER=6.0
OUT_OF_ORDERNESS_SECONDS=30
LATE_TOLERANCE_SECONDS=300

while [[ $# -gt 0 ]]; do
  case "$1" in
    --business-scenario|--scenario)
      BUSINESS_SCENARIO="$2"
      shift 2
      ;;
    --delivery-mode|--mode)
      DELIVERY_MODE="$2"
      shift 2
      ;;
    --requests)
      REQUESTS="$2"
      shift 2
      ;;
    --sessions)
      SESSIONS="$2"
      shift 2
      ;;
    --agent-id)
      AGENT_ID="$2"
      shift 2
      ;;
    --seed)
      SEED="$2"
      shift 2
      ;;
    --out-dir)
      OUT_DIR="$2"
      shift 2
      ;;
    --request-offset-seconds)
      REQUEST_OFFSET_SECONDS="$2"
      shift 2
      ;;
    --burst-start-second)
      BURST_START_SECOND="$2"
      shift 2
      ;;
    --burst-duration-seconds)
      BURST_DURATION_SECONDS="$2"
      shift 2
      ;;
    --burst-multiplier)
      BURST_MULTIPLIER="$2"
      shift 2
      ;;
    --late-share)
      LATE_SHARE="$2"
      shift 2
      ;;
    --too-late-share)
      TOO_LATE_SHARE="$2"
      shift 2
      ;;
    --invalid-share)
      INVALID_SHARE="$2"
      shift 2
      ;;
    --error-share)
      ERROR_SHARE="$2"
      shift 2
      ;;
    --detector-latency-multiplier)
      DETECTOR_LATENCY_MULTIPLIER="$2"
      shift 2
      ;;
    --out-of-orderness-seconds)
      OUT_OF_ORDERNESS_SECONDS="$2"
      shift 2
      ;;
    --late-tolerance-seconds)
      LATE_TOLERANCE_SECONDS="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

mkdir -p "$OUT_DIR"

PYTHON_ARGS=(
  --business-scenario "$BUSINESS_SCENARIO"
  --delivery-mode "$DELIVERY_MODE"
  --requests "$REQUESTS"
  --sessions "$SESSIONS"
  --agent-id "$AGENT_ID"
  --seed "$SEED"
  --request-offset-seconds "$REQUEST_OFFSET_SECONDS"
  --burst-start-second "$BURST_START_SECOND"
  --burst-duration-seconds "$BURST_DURATION_SECONDS"
  --burst-multiplier "$BURST_MULTIPLIER"
  --late-share "$LATE_SHARE"
  --too-late-share "$TOO_LATE_SHARE"
  --invalid-share "$INVALID_SHARE"
  --error-share "$ERROR_SHARE"
  --detector-latency-multiplier "$DETECTOR_LATENCY_MULTIPLIER"
  --out-of-orderness-seconds "$OUT_OF_ORDERNESS_SECONDS"
  --late-tolerance-seconds "$LATE_TOLERANCE_SECONDS"
  --output-dir "$OUT_DIR"
)

python3 "$ROOT_DIR/tools/generators/generate_events.py" \
  "${PYTHON_ARGS[@]}"

for mapping in \
  "agent-requests:agent-requests.jsonl" \
  "agent-responses:agent-responses.jsonl" \
  "guardrail-findings:guardrail-findings.jsonl"
do
  topic="${mapping%%:*}"
  file="${mapping##*:}"
  docker compose -f "$COMPOSE_FILE" exec -T kafka "${KAFKA_BIN}/kafka-console-producer.sh" \
    --bootstrap-server localhost:9092 \
    --topic "$topic" < "$OUT_DIR/$file"
done

echo "Replay published from $OUT_DIR"
echo "business-scenario=$BUSINESS_SCENARIO delivery-mode=$DELIVERY_MODE requests=$REQUESTS sessions=$SESSIONS"

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KAFKA_BIN="/opt/kafka/bin"
SCENARIO="mixed"
REQUESTS=120
SESSIONS=12
AGENT_ID="agent-risk-01"
SEED=42
OUT_DIR="$ROOT_DIR/runtime/replay/latest"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --scenario)
      SCENARIO="$2"
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
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

mkdir -p "$OUT_DIR"

python3 "$ROOT_DIR/scripts/generate_events.py" \
  --scenario "$SCENARIO" \
  --requests "$REQUESTS" \
  --sessions "$SESSIONS" \
  --agent-id "$AGENT_ID" \
  --seed "$SEED" \
  --output-dir "$OUT_DIR"

for mapping in \
  "agent-requests:agent-requests.jsonl" \
  "agent-responses:agent-responses.jsonl" \
  "guardrail-findings:guardrail-findings.jsonl"
do
  topic="${mapping%%:*}"
  file="${mapping##*:}"
  docker compose exec -T kafka "${KAFKA_BIN}/kafka-console-producer.sh" \
    --bootstrap-server localhost:9092 \
    --topic "$topic" < "$OUT_DIR/$file"
done

echo "Replay published from $OUT_DIR"

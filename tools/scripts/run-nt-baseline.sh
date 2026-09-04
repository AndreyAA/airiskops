#!/usr/bin/env bash
# Runs the first reproducible AIRiskOps load-test baseline on the local stack.
# Use when you want a fixed live-generator scenario for comparing latency,
# backpressure, watermark progress, and checkpoint stability between runs.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DURATION_SECONDS=600
REQUESTS_PER_SECOND=20
SESSIONS=12
SCENARIO="mixed"
DELIVERY_MODE="baseline"
AGENT_ID="agent-risk-01"
SEED=42

while [[ $# -gt 0 ]]; do
  case "$1" in
    --duration-seconds)
      DURATION_SECONDS="$2"
      shift 2
      ;;
    --requests-per-second|--rps)
      REQUESTS_PER_SECOND="$2"
      shift 2
      ;;
    --sessions)
      SESSIONS="$2"
      shift 2
      ;;
    --scenario)
      SCENARIO="$2"
      shift 2
      ;;
    --mode)
      DELIVERY_MODE="$2"
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
    --help|-h)
      cat <<'EOF'
Usage:
  bash tools/scripts/run-nt-baseline.sh [options]

Options:
  --duration-seconds <seconds>   Default: 600
  --requests-per-second <rps>    Default: 20
  --sessions <count>             Default: 12
  --scenario <name>              Default: mixed
  --mode <name>                  Default: baseline
  --agent-id <id>                Default: agent-risk-01
  --seed <value>                 Default: 42
EOF
      exit 0
      ;;
    *)
      echo "run-nt-baseline.sh: unsupported argument: $1" >&2
      exit 1
      ;;
  esac
done

cd "$ROOT_DIR"

echo "[nt-baseline] date=$(date '+%Y-%m-%dT%H:%M:%S%z')"
echo "[nt-baseline] scenario=$SCENARIO mode=$DELIVERY_MODE duration_seconds=$DURATION_SECONDS rps=$REQUESTS_PER_SECOND sessions=$SESSIONS seed=$SEED agent_id=$AGENT_ID"
echo "[nt-baseline] watch Grafana dashboard: AIRiskOps Capacity And Performance"
echo "[nt-baseline] focus metrics: watermark, checkpoints, busy/backpressured time, aggregate e2e latency, incident e2e latency"

bash tools/scripts/run-live-generator.sh \
  --scenario "$SCENARIO" \
  --mode "$DELIVERY_MODE" \
  --duration-seconds "$DURATION_SECONDS" \
  --requests-per-second "$REQUESTS_PER_SECOND" \
  --sessions "$SESSIONS" \
  --agent-id "$AGENT_ID" \
  --seed "$SEED"

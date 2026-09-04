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
REPORT_DIR="runtime/load-tests"
SETTLE_SECONDS=30
RECOVERY_SECONDS=60
JOB_ID=""

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
    --report-dir)
      REPORT_DIR="$2"
      shift 2
      ;;
    --settle-seconds)
      SETTLE_SECONDS="$2"
      shift 2
      ;;
    --recovery-seconds)
      RECOVERY_SECONDS="$2"
      shift 2
      ;;
    --job-id)
      JOB_ID="$2"
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
  --report-dir <path>            Default: runtime/load-tests
  --settle-seconds <seconds>     Default: 30
  --recovery-seconds <seconds>   Default: 60
  --job-id <id>                  Required only when multiple matching jobs run
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

if ! [[ "$SETTLE_SECONDS" =~ ^[0-9]+$ ]] || ! [[ "$RECOVERY_SECONDS" =~ ^[0-9]+$ ]]; then
  echo "run-nt-baseline.sh: settle and recovery seconds must be non-negative integers" >&2
  exit 1
fi

RUN_START_EPOCH="$(date +%s)"
RUN_ID="$(date '+%Y-%m-%dT%H%M%S%z')-${REQUESTS_PER_SECOND}rps-${SCENARIO}-${DELIVERY_MODE}"
RUN_REPORT_DIR="$ROOT_DIR/$REPORT_DIR"
GENERATOR_LOG="$RUN_REPORT_DIR/$RUN_ID.generator.log"
KAFKA_LAG_AT_GENERATOR_END="$RUN_REPORT_DIR/$RUN_ID.kafka-lag-at-generator-end.json"
KAFKA_LAG_AFTER_SETTLE="$RUN_REPORT_DIR/$RUN_ID.kafka-lag-after-settle.json"
KAFKA_LAG_AFTER_RECOVERY="$RUN_REPORT_DIR/$RUN_ID.kafka-lag-after-recovery.json"
mkdir -p "$RUN_REPORT_DIR"

echo "[nt-baseline] date=$(date '+%Y-%m-%dT%H:%M:%S%z')"
echo "[nt-baseline] scenario=$SCENARIO mode=$DELIVERY_MODE duration_seconds=$DURATION_SECONDS rps=$REQUESTS_PER_SECOND sessions=$SESSIONS seed=$SEED agent_id=$AGENT_ID"
echo "[nt-baseline] watch Grafana dashboard: AIRiskOps Capacity And Performance"
echo "[nt-baseline] focus metrics: watermark, checkpoints, busy/backpressured time, aggregate e2e latency, incident e2e latency"
echo "[nt-baseline] generator log: $GENERATOR_LOG"

set +e
bash tools/scripts/run-live-generator.sh \
  --scenario "$SCENARIO" \
  --mode "$DELIVERY_MODE" \
  --duration-seconds "$DURATION_SECONDS" \
  --requests-per-second "$REQUESTS_PER_SECOND" \
  --sessions "$SESSIONS" \
  --agent-id "$AGENT_ID" \
  --seed "$SEED" 2>&1 | tee "$GENERATOR_LOG"
GENERATOR_EXIT_CODE="${PIPESTATUS[0]}"
set -e

RUN_END_EPOCH="$(date +%s)"
python3 tools/reporters/nt_report_collector.py --write-kafka-lag "$KAFKA_LAG_AT_GENERATOR_END"

if [[ "$SETTLE_SECONDS" -gt 0 ]]; then
  echo "[nt-baseline] waiting ${SETTLE_SECONDS}s for metrics to settle"
  sleep "$SETTLE_SECONDS"
fi
python3 tools/reporters/nt_report_collector.py --write-kafka-lag "$KAFKA_LAG_AFTER_SETTLE"

if [[ "$RECOVERY_SECONDS" -gt 0 ]]; then
  echo "[nt-baseline] waiting ${RECOVERY_SECONDS}s to measure Kafka catch-up"
  sleep "$RECOVERY_SECONDS"
fi
python3 tools/reporters/nt_report_collector.py --write-kafka-lag "$KAFKA_LAG_AFTER_RECOVERY"

REPORT_ARGS=(
  --report-dir "$RUN_REPORT_DIR"
  --run-id "$RUN_ID"
  --run-start-epoch "$RUN_START_EPOCH"
  --run-end-epoch "$RUN_END_EPOCH"
  --scenario "$SCENARIO"
  --mode "$DELIVERY_MODE"
  --rps "$REQUESTS_PER_SECOND"
  --duration-seconds "$DURATION_SECONDS"
  --sessions "$SESSIONS"
  --seed "$SEED"
  --agent-id "$AGENT_ID"
  --generator-log "$GENERATOR_LOG"
  --generator-exit-code "$GENERATOR_EXIT_CODE"
  --kafka-lag-at-generator-end "$KAFKA_LAG_AT_GENERATOR_END"
  --kafka-lag-after-settle "$KAFKA_LAG_AFTER_SETTLE"
  --kafka-lag-after-recovery "$KAFKA_LAG_AFTER_RECOVERY"
)

if [[ -n "$JOB_ID" ]]; then
  REPORT_ARGS+=(--job-id "$JOB_ID")
fi

set +e
python3 tools/reporters/nt_report_collector.py "${REPORT_ARGS[@]}"
REPORT_EXIT_CODE=$?
set -e

if [[ "$GENERATOR_EXIT_CODE" -ne 0 ]]; then
  exit "$GENERATOR_EXIT_CODE"
fi

exit "$REPORT_EXIT_CODE"

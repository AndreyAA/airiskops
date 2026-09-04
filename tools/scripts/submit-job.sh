#!/usr/bin/env bash
# Submits the built AIRiskOps job to the local Flink cluster.
# Use after the stack is running, topics are initialized, policies are loaded,
# and the shaded JAR is available in flink-job/target.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/deployment/local/docker-compose.yml"
JAR_PATH="/opt/flink/usrlib/flink-airiskops-1.0.0-SNAPSHOT-all.jar"
CONFIG_FILE="config/job/local-job.yaml"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      if [[ $# -lt 2 ]]; then
        echo "submit-job.sh: missing value for --config" >&2
        exit 1
      fi
      CONFIG_FILE="$2"
      shift 2
      ;;
    *)
      echo "submit-job.sh: unsupported argument: $1" >&2
      echo "Usage: bash tools/scripts/submit-job.sh [--config <project-relative-config-path>]" >&2
      exit 1
      ;;
  esac
done

if [[ ! -f "$ROOT_DIR/$CONFIG_FILE" ]]; then
  echo "submit-job.sh: config file not found: $CONFIG_FILE" >&2
  exit 1
fi

cd "$ROOT_DIR"
docker compose -f "$COMPOSE_FILE" exec -T jobmanager ./bin/flink run -d \
  -c com.bank.airiskops.app.job.AiRiskOpsMvpJob \
  "$JAR_PATH" \
  --configFile "/opt/flink/job-config/${CONFIG_FILE#config/}" \
  --policyBootstrapFile /opt/flink/runtime/policies/active-policy.yaml

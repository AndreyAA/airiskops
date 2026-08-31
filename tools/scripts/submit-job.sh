#!/usr/bin/env bash
# Submits the built AIRiskOps job to the local Flink cluster.
# Use after the stack is running, topics are initialized, policies are loaded,
# and the shaded JAR is available in flink-job/target.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/deployment/local/docker-compose.yml"
JAR_PATH="/opt/flink/usrlib/flink-airiskops-1.0.0-SNAPSHOT-all.jar"

cd "$ROOT_DIR"
docker compose -f "$COMPOSE_FILE" exec -T jobmanager ./bin/flink run -d \
  -c com.bank.airiskops.app.job.AiRiskOpsMvpJob \
  "$JAR_PATH" \
  --configFile /opt/flink/job-config/job/local-job.yaml \
  --policyBootstrapFile /opt/flink/runtime/policies/active-policy.yaml

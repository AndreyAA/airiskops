#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/deployment/local/docker-compose.yml"
JAR_PATH="/opt/flink/usrlib/flink-aisafetyops-1.0.0-SNAPSHOT-all.jar"

cd "$ROOT_DIR"
docker compose -f "$COMPOSE_FILE" exec -T jobmanager ./bin/flink run -d \
  -c com.bank.aisafetyops.app.job.AiSafetyOpsMvpJob \
  "$JAR_PATH" \
  --configFile /opt/flink/job-config/job/local-job.yaml

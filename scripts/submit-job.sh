#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JAR_PATH="/opt/flink/usrlib/flink-aisafetyops-1.0.0-SNAPSHOT-all.jar"

cd "$ROOT_DIR"
docker compose exec -T jobmanager ./bin/flink run -d \
  -c com.bank.aisafetyops.app.job.AiSafetyOpsMvpJob \
  "$JAR_PATH" \
  --configFile /opt/flink/job-config/local-job.yaml

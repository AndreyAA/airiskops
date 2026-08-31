#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
MAVEN_REPO_DIR="/tmp/flink-m2"

cd "$ROOT_DIR"

bash -n tools/scripts/start-local.sh
bash -n tools/scripts/stop-local.sh
bash -n tools/scripts/init-topics.sh
bash -n tools/scripts/reset-topics.sh
bash -n tools/scripts/check-output-topics.sh
bash -n tools/scripts/load-policies.sh
bash -n tools/scripts/init.sh
bash -n tools/scripts/run-replay.sh
bash -n tools/scripts/run-live-generator.sh
bash -n tools/scripts/run-e2e-smoke.sh
python3 -m unittest discover -s tools/tests -p 'test_*.py'
mvn -f "$ROOT_DIR/flink-job/pom.xml" -Dmaven.repo.local="$MAVEN_REPO_DIR" test

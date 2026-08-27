#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MAVEN_REPO_DIR="/tmp/flink-m2"

cd "$ROOT_DIR"

bash -n scripts/start-local.sh
bash -n scripts/stop-local.sh
bash -n scripts/init-topics.sh
bash -n scripts/reset-topics.sh
bash -n scripts/check-output-topics.sh
bash -n scripts/load-policies.sh
bash -n scripts/run-replay.sh
python3 -m unittest discover -s tests -p 'test_*.py'
mvn -Dmaven.repo.local="$MAVEN_REPO_DIR" test

#!/usr/bin/env bash
# Runs local static/script checks, Python tests, and Flink job tests with coverage.
# Use after code changes to catch regressions and verify the minimum Java line coverage.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
MAVEN_REPO_DIR="/tmp/flink-m2"
JACOCO_CSV="$ROOT_DIR/flink-job/target/site/jacoco/jacoco.csv"

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

set +e
mvn -f "$ROOT_DIR/flink-job/pom.xml" -Dmaven.repo.local="$MAVEN_REPO_DIR" verify
maven_status=$?
set -e

if [[ ! -f "$JACOCO_CSV" ]]; then
  echo "Coverage report was not generated: $JACOCO_CSV" >&2
  exit 1
fi

coverage_summary="$(awk -F',' '
  NR > 1 {
    missed += $8
    covered += $9
  }
  END {
    total = missed + covered
    if (total == 0) {
      print "LINE_COVERAGE=0.00"
      exit 0
    }
    printf "LINE_COVERAGE=%.2f", (covered / total) * 100
  }
' "$JACOCO_CSV")"

coverage_percent="${coverage_summary#LINE_COVERAGE=}"
echo "flink-job line coverage: ${coverage_percent}%"
echo "HTML report: $ROOT_DIR/flink-job/target/site/jacoco/index.html"

if awk "BEGIN { exit !($coverage_percent < 80.0) }"; then
  coverage_gap="$(awk "BEGIN { printf \"%.2f\", 80.0 - $coverage_percent }")"
  echo "ALERT: flink-job line coverage is ${coverage_percent}% in this run, below 80.00% by ${coverage_gap} p.p." >&2
fi

if [[ $maven_status -ne 0 ]]; then
  exit "$maven_status"
fi

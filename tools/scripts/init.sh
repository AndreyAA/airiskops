#!/usr/bin/env bash
# Initializes the local AIRiskOps stack without deleting existing local state.
# Use when you need topics, active policy, built job artifact, and submitted job
# but do not want cleanup or replay data publication.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
STEP_NUMBER=0
CURRENT_STEP_TITLE=""
CURRENT_STEP_STARTED_AT=0

if [[ -t 1 ]]; then
  COLOR_RESET=$'\033[0m'
  COLOR_STEP=$'\033[1;37m'
  COLOR_PASS=$'\033[1;32m'
  COLOR_FAIL=$'\033[1;31m'
  COLOR_COMMAND=$'\033[0;37m'
  COLOR_BORDER=$'\033[1;37m'
else
  COLOR_RESET=''
  COLOR_STEP=''
  COLOR_PASS=''
  COLOR_FAIL=''
  COLOR_COMMAND=''
  COLOR_BORDER=''
fi

timestamp() {
  date '+%Y-%m-%dT%H:%M:%S%z'
}

log() {
  local level="$1"
  shift
  printf '[init][%s][%s] %s\n' "$(timestamp)" "$level" "$*"
}

status() {
  local state="$1"
  shift
  local color="$COLOR_RESET"
  case "$state" in
    pass) color="$COLOR_PASS" ;;
    fail) color="$COLOR_FAIL" ;;
  esac
  printf '%s[init][%s][status][%s] %s%s\n' "$color" "$(timestamp)" "$state" "$*" "$COLOR_RESET"
}

print_step_banner() {
  local title="$1"
  printf '\n%s============================================================%s\n' "$COLOR_BORDER" "$COLOR_RESET"
  printf '%s[init][%s][step] STEP %s started: %s%s\n' \
    "$COLOR_STEP" "$(timestamp)" "$STEP_NUMBER" "$title" "$COLOR_RESET"
  printf '%s============================================================%s\n' "$COLOR_BORDER" "$COLOR_RESET"
}

step() {
  local title="$1"
  STEP_NUMBER=$((STEP_NUMBER + 1))
  CURRENT_STEP_TITLE="$title"
  CURRENT_STEP_STARTED_AT="$(date +%s)"
  print_step_banner "$title"
}

finish_step() {
  local finished_at
  local elapsed_seconds
  finished_at="$(date +%s)"
  elapsed_seconds=$((finished_at - CURRENT_STEP_STARTED_AT))
  status pass "STEP $STEP_NUMBER completed: $CURRENT_STEP_TITLE"
  log step "STEP $STEP_NUMBER duration: ${elapsed_seconds}s"
}

format_command() {
  printf '%q ' "$@"
}

log_command() {
  printf '%s[init][%s][command] %s%s\n' \
    "$COLOR_COMMAND" "$(timestamp)" "$*" "$COLOR_RESET"
}

run_checked() {
  local description="$1"
  shift
  log info "$description"
  log_command "$(format_command "$@")"
  if "$@"; then
    status pass "$description"
  else
    status fail "$description"
    return 1
  fi
}

cd "$ROOT_DIR"

step "Start local Docker services"
run_checked "Starting Kafka, Flink, Prometheus, and Grafana" bash tools/scripts/start-local.sh
finish_step

step "Initialize Kafka topics"
run_checked "Creating local Kafka topics" bash tools/scripts/init-topics.sh
finish_step

step "Load bootstrap policy"
run_checked "Copying bootstrap policy into runtime/policies" bash tools/scripts/load-policies.sh
finish_step

step "Build shaded Flink job jar"
run_checked "Building Flink job artifact" bash tools/scripts/build-job.sh
finish_step

step "Submit Flink job"
run_checked "Submitting AIRiskOps job to local Flink cluster" bash tools/scripts/submit-job.sh
finish_step

status pass "Local initialization completed successfully"

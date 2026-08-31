#!/usr/bin/env bash
# ATTENTION: This script destroys the current local AIRiskOps state and then
# runs a full end-to-end smoke test of the local stack.
# Use when you need a clean reset, fresh job submission, replay bootstrap, and
# automated checks for Kafka outputs, Prometheus, and Grafana.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/deployment/local/docker-compose.yml"
PROMETHEUS_URL="http://localhost:9090"
GRAFANA_URL="http://localhost:3000"
GRAFANA_USER="admin"
GRAFANA_PASSWORD="admin"
SCENARIO="attack"
DELIVERY_MODE="baseline"
REQUESTS=120
SESSIONS=12
AGENT_ID="agent-risk-01"
MAX_WAIT_SECONDS=180
ASSUME_YES="false"
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
  printf '[e2e][%s][%s] %s\n' "$(timestamp)" "$level" "$*"
}

status() {
  local state="$1"
  shift
  local color="$COLOR_RESET"
  case "$state" in
    pass) color="$COLOR_PASS" ;;
    fail) color="$COLOR_FAIL" ;;
  esac
  printf '%s[e2e][%s][status][%s] %s%s\n' "$color" "$(timestamp)" "$state" "$*" "$COLOR_RESET"
}

print_step_banner() {
  local title="$1"
  printf '\n%s============================================================%s\n' "$COLOR_BORDER" "$COLOR_RESET"
  printf '%s[e2e][%s][step] STEP %s started: %s%s\n' \
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
  printf '%s[e2e][%s][command] %s%s\n' \
    "$COLOR_COMMAND" "$(timestamp)" "$*" "$COLOR_RESET"
}

usage() {
  cat <<'EOF'
ATTENTION:
  This script deletes the current local AIRiskOps Docker state, runtime data,
  and build artifacts before running a full end-to-end smoke test.

Purpose:
  Validate the local stack from clean reset to job submission, replay load,
  Kafka outputs, Prometheus metrics, and Grafana provisioning.

Usage:
  bash tools/scripts/run-e2e-smoke.sh [options]

Options:
  --yes                          Skip destructive confirmation prompt.
  --scenario <name>              Replay scenario. Default: attack
  --mode <name>                  Replay delivery mode. Default: baseline
  --requests <count>             Replay request count. Default: 120
  --sessions <count>             Replay session count. Default: 12
  --agent-id <id>                Replay agent id. Default: agent-risk-01
  --max-wait-seconds <seconds>   Readiness wait timeout. Default: 180
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --yes)
      ASSUME_YES="true"
      shift
      ;;
    --scenario|--business-scenario)
      SCENARIO="$2"
      shift 2
      ;;
    --mode|--delivery-mode)
      DELIVERY_MODE="$2"
      shift 2
      ;;
    --requests)
      REQUESTS="$2"
      shift 2
      ;;
    --sessions)
      SESSIONS="$2"
      shift 2
      ;;
    --agent-id)
      AGENT_ID="$2"
      shift 2
      ;;
    --max-wait-seconds)
      MAX_WAIT_SECONDS="$2"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

confirm_destructive_action() {
  if [[ "$ASSUME_YES" == "true" ]]; then
    status pass "Destructive confirmation skipped via --yes"
    return
  fi

  cat <<'EOF'
[e2e][warning] ATTENTION: destructive end-to-end smoke test
[e2e][warning] Purpose: validate the full local stack from cleanup to Grafana checks.
[e2e][warning] This script will:
[e2e][warning] - stop and remove local Docker containers and volumes;
[e2e][warning] - remove local replay runtime data;
[e2e][warning] - remove local runtime policy snapshots;
[e2e][warning] - remove local build artifacts in flink-job/target.
EOF
  printf '[e2e][prompt] Type "DESTROY" to continue: '
  read -r confirmation
  if [[ "$confirmation" != "DESTROY" ]]; then
    status fail "Confirmation phrase mismatch. Aborting without changes."
    exit 1
  fi
  status pass "Destructive confirmation accepted"
}

wait_until() {
  local description="$1"
  local timeout_seconds="$2"
  local command="$3"
  local started_at
  started_at="$(date +%s)"
  log check "Checking: $description"
  log_command "$command"

  while true; do
    if bash -lc "$command" >/tmp/airiskops-e2e-check.out 2>/tmp/airiskops-e2e-check.err; then
      status pass "$description"
      return 0
    fi
    local now
    now="$(date +%s)"
    if (( now - started_at >= timeout_seconds )); then
      status fail "$description"
      if [[ -s /tmp/airiskops-e2e-check.err ]]; then
        log error "$(tr '\n' ' ' < /tmp/airiskops-e2e-check.err)"
      fi
      return 1
    fi
    sleep 2
  done
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

assert_prometheus_query_nonzero() {
  local description="$1"
  local query="$2"
  local response
  log check "Checking Prometheus: $description"
  log check "Command: curl --get --data-urlencode query=$query $PROMETHEUS_URL/api/v1/query"
  response="$(curl -fsS --get --data-urlencode "query=$query" "$PROMETHEUS_URL/api/v1/query")"
  if [[ "$response" == *'"status":"success"'* ]] && [[ "$response" != *'"result":[]'* ]] && [[ "$response" != *',"0"]'* ]] && [[ "$response" != *',"0.0"]'* ]]; then
    status pass "$description"
    return 0
  fi
  status fail "$description"
  log error "Prometheus query response: $response"
  return 1
}

assert_prometheus_query_equals() {
  local description="$1"
  local query="$2"
  local expected_fragment="$3"
  local response
  log check "Checking Prometheus: $description"
  log check "Command: curl --get --data-urlencode query=$query $PROMETHEUS_URL/api/v1/query"
  log check "Expected fragment: $expected_fragment"
  response="$(curl -fsS --get --data-urlencode "query=$query" "$PROMETHEUS_URL/api/v1/query")"
  if [[ "$response" == *'"status":"success"'* ]] && [[ "$response" == *"$expected_fragment"* ]]; then
    status pass "$description"
    return 0
  fi
  status fail "$description"
  log error "Prometheus query response: $response"
  return 1
}

assert_grafana_contains() {
  local description="$1"
  local url="$2"
  local expected_fragment="$3"
  local response
  log check "Checking Grafana: $description"
  log check "Command: curl -u $GRAFANA_USER:*** $url"
  log check "Expected fragment: $expected_fragment"
  response="$(curl -fsS -u "$GRAFANA_USER:$GRAFANA_PASSWORD" "$url")"
  if [[ "$response" == *"$expected_fragment"* ]]; then
    status pass "$description"
    return 0
  fi
  status fail "$description"
  log error "Grafana response: $response"
  return 1
}

assert_kafka_offsets_nonzero() {
  local topic="$1"
  local description="$2"
  local output
  log check "Checking Kafka offsets: $description"
  log check "Command: docker compose -f $COMPOSE_FILE exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic $topic"
  output="$(docker compose -f "$COMPOSE_FILE" exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server kafka:9092 \
    --topic "$topic")"
  if printf '%s\n' "$output" | grep -Eq ':[1-9][0-9]*$'; then
    status pass "$description"
    log info "$topic offsets: $output"
    return 0
  fi
  status fail "$description"
  log error "$topic offsets: $output"
  return 1
}

cd "$ROOT_DIR"

confirm_destructive_action

step "1. Cleanup local environment"
run_checked "Running cleanup-local.sh" bash tools/scripts/cleanup-local.sh
finish_step

step "2. Start local Docker services"
run_checked "Starting Kafka, Flink, Prometheus, and Grafana" bash tools/scripts/start-local.sh
finish_step

step "3. Wait for local services to become reachable"
wait_until "Flink Web UI is reachable" "$MAX_WAIT_SECONDS" "curl -fsS http://localhost:8081 >/dev/null"
wait_until "Prometheus readiness endpoint is healthy" "$MAX_WAIT_SECONDS" "curl -fsS $PROMETHEUS_URL/-/ready >/dev/null"
wait_until "Grafana health endpoint is healthy" "$MAX_WAIT_SECONDS" "curl -fsS $GRAFANA_URL/api/health >/dev/null"
finish_step

step "4. Verify container status"
run_checked "Docker compose reports local services" docker compose -f "$COMPOSE_FILE" ps
finish_step

step "5. Initialize Kafka topics"
run_checked "Creating local Kafka topics" bash tools/scripts/init-topics.sh
finish_step

step "6. Load bootstrap policy"
run_checked "Copying bootstrap policy into runtime/policies" bash tools/scripts/load-policies.sh
finish_step

step "7. Build shaded Flink job jar"
run_checked "Building Flink job artifact" bash tools/scripts/build-job.sh
finish_step

step "8. Submit Flink job"
run_checked "Submitting AIRiskOps job to local Flink cluster" bash tools/scripts/submit-job.sh
finish_step

step "9. Wait for job and metrics to appear"
wait_until \
  "Prometheus sees one running Flink job" \
  "$MAX_WAIT_SECONDS" \
  "curl -fsS --get --data-urlencode 'query=flink_jobmanager_numRunningJobs' $PROMETHEUS_URL/api/v1/query | grep -q '\"1\"'"
wait_until \
  "Grafana Prometheus datasource is available" \
  "$MAX_WAIT_SECONDS" \
  "curl -fsS -u $GRAFANA_USER:$GRAFANA_PASSWORD $GRAFANA_URL/api/datasources/name/Prometheus | grep -q '\"type\":\"prometheus\"'"
finish_step

step "10. Replay initial dataset into Kafka"
run_checked \
  "Publishing replay scenario=$SCENARIO mode=$DELIVERY_MODE requests=$REQUESTS sessions=$SESSIONS" \
  bash tools/scripts/run-replay.sh \
    --scenario "$SCENARIO" \
    --mode "$DELIVERY_MODE" \
    --requests "$REQUESTS" \
    --sessions "$SESSIONS" \
    --agent-id "$AGENT_ID"
finish_step

step "11. Verify Kafka output topics"
wait_until \
  "Normalized events topic receives data" \
  "$MAX_WAIT_SECONDS" \
  "docker compose -f '$COMPOSE_FILE' exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic normalized-events | grep -Eq ':[1-9][0-9]*$'"
wait_until \
  "Guardrail aggregates topic receives data" \
  "$MAX_WAIT_SECONDS" \
  "docker compose -f '$COMPOSE_FILE' exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic guardrail-aggregates | grep -Eq ':[1-9][0-9]*$'"
wait_until \
  "Basic incidents topic receives data" \
  "$MAX_WAIT_SECONDS" \
  "docker compose -f '$COMPOSE_FILE' exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic basic-incidents | grep -Eq ':[1-9][0-9]*$'"
wait_until \
  "Guardrail quality metrics topic receives data" \
  "$MAX_WAIT_SECONDS" \
  "docker compose -f '$COMPOSE_FILE' exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic guardrail-quality-metrics | grep -Eq ':[1-9][0-9]*$'"
run_checked "Printing output topic samples" bash tools/scripts/check-output-topics.sh
finish_step

step "12. Verify Prometheus targets and key metrics"
assert_prometheus_query_equals \
  "Prometheus query returns one running job" \
  "flink_jobmanager_numRunningJobs" \
  "\"1\""
assert_prometheus_query_nonzero \
  "Completed checkpoints metric is present and non-zero" \
  "flink_jobmanager_job_numberOfCompletedCheckpoints{job_name=\"AIRiskOps_MVP_Increment_1\"}"
assert_prometheus_query_nonzero \
  "Guardrail aggregate emissions metric is present and non-zero" \
  "sum(flink_taskmanager_job_task_operator_guardrail_aggregate_records_total_1m{job_name=\"AIRiskOps_MVP_Increment_1\"})"
assert_prometheus_query_nonzero \
  "Runtime contract info metric is present" \
  "count(flink_taskmanager_job_task_operator_airiskops_runtime_contract_window_type_delivery_guarantee_analysis_mode_aggregate_windows_info{job_name=\"AIRiskOps_MVP_Increment_1\"})"
assert_prometheus_query_nonzero \
  "Incident open sessions metric is present" \
  "count(flink_taskmanager_job_task_operator_airiskops_incident_open_sessions{job_name=\"AIRiskOps_MVP_Increment_1\"})"
assert_prometheus_query_nonzero \
  "Detector quality emission metric is present and non-zero" \
  "sum(flink_taskmanager_job_task_operator_airiskops_quality_window_guardrail_emitted_total{job_name=\"AIRiskOps_MVP_Increment_1\",window=\"1m\"})"
finish_step

step "13. Verify Grafana health, datasource, and dashboards"
assert_grafana_contains \
  "Grafana health endpoint reports ok" \
  "$GRAFANA_URL/api/health" \
  "\"database\":\"ok\""
assert_grafana_contains \
  "Grafana Prometheus datasource exists" \
  "$GRAFANA_URL/api/datasources/name/Prometheus" \
  "\"type\":\"prometheus\""
assert_grafana_contains \
  "Grafana dashboard search includes Flink Overview" \
  "$GRAFANA_URL/api/search?query=AIRiskOps" \
  "AIRiskOps Flink Overview"
assert_grafana_contains \
  "Grafana dashboard search includes Business Metrics" \
  "$GRAFANA_URL/api/search?query=AIRiskOps" \
  "AIRiskOps Business Metrics"
assert_grafana_contains \
  "Grafana dashboard search includes Capacity And Performance" \
  "$GRAFANA_URL/api/search?query=AIRiskOps" \
  "AIRiskOps Capacity And Performance"
assert_grafana_contains \
  "Grafana dashboard search includes Detector Quality" \
  "$GRAFANA_URL/api/search?query=AIRiskOps" \
  "AIRiskOps Detector Quality"
finish_step

step "14. Final summary"
assert_kafka_offsets_nonzero "normalized-events" "Final normalized-events offset check"
assert_kafka_offsets_nonzero "guardrail-aggregates" "Final guardrail-aggregates offset check"
assert_kafka_offsets_nonzero "basic-incidents" "Final basic-incidents offset check"
assert_kafka_offsets_nonzero "guardrail-quality-metrics" "Final guardrail-quality-metrics offset check"
status pass "End-to-end smoke test completed successfully"
finish_step

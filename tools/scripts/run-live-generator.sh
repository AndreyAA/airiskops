#!/usr/bin/env bash
# Streams live AIRiskOps traffic into local Kafka with configurable scenario and chaos modes.
# Use when you want a multi-minute moving signal for Grafana/Prometheus,
# including bursts, late arrivals, invalid payloads, or detector degradation.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

cd "$ROOT_DIR"
python3 tools/generators/stream_live_events.py "$@"

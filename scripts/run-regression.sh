#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$ROOT_DIR"

bash -n scripts/start-local.sh
bash -n scripts/stop-local.sh
bash -n scripts/init-topics.sh
bash -n scripts/load-policies.sh
bash -n scripts/run-replay.sh
python3 -m unittest discover -s tests -p 'test_*.py'

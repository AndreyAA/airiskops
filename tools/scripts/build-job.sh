#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
MAVEN_REPO_DIR="/tmp/flink-m2"

cd "$ROOT_DIR"
mvn -f "$ROOT_DIR/flink-job/pom.xml" -Dmaven.repo.local="$MAVEN_REPO_DIR" package

#!/usr/bin/env bash
# Builds the Flink job JAR with tests and shading.
# Use before submitting the job, and after Java changes that must be packaged
# into the local Docker-based Flink cluster.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
MAVEN_REPO_DIR="/tmp/flink-m2"

cd "$ROOT_DIR"
mkdir -p "$ROOT_DIR/flink-job/target"
mvn -f "$ROOT_DIR/flink-job/pom.xml" -Dmaven.repo.local="$MAVEN_REPO_DIR" package

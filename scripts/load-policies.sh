#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SOURCE_FILE="${1:-$ROOT_DIR/policies/default-policy.yaml}"
TARGET_DIR="$ROOT_DIR/runtime/policies"
TARGET_FILE="$TARGET_DIR/active-policy.yaml"

mkdir -p "$TARGET_DIR"

if [[ ! -f "$SOURCE_FILE" ]]; then
  echo "Policy file not found: $SOURCE_FILE" >&2
  exit 1
fi

cp "$SOURCE_FILE" "$TARGET_FILE"
echo "Active policy loaded: $TARGET_FILE"

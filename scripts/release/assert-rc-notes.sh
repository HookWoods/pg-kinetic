#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd -P)"
NOTES="$REPO_ROOT/docs/release-notes/v1.0.0-rc.1.md"

for required_heading in \
  'Rollback trigger' \
  'Known exclusions' \
  'Compatibility evidence'; do
  if ! rg -q "$required_heading" "$NOTES"; then
    printf 'missing RC notes requirement: %s\n' "$required_heading" >&2
    exit 1
  fi
done

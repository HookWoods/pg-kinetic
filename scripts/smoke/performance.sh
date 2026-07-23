#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$BASH_SOURCE")" && pwd -P)"
source "$SCRIPT_DIR/../lib/common.sh"

SCENARIO="${SCENARIO:-bench/scenarios/benchmark-simple-query.toml}"
BASELINE="${BASELINE:-}"
CURRENT="${CURRENT:-}"
dry_run=false

for argument in "$@"; do
  case "$argument" in
    --dry-run) dry_run=true ;;
    *)
      echo "unknown argument: $argument" >&2
      exit 2
      ;;
  esac
done

if "$dry_run"; then
  success "performance smoke dry-run"
  exit 0
fi

report_path="bench/results/performance-smoke-$$.json"
report_file="$REPO_ROOT/$report_path"
cleanup() {
  rm -f "$report_file"
}
trap cleanup EXIT

run_from_repo_root bash scripts/bench/run-performance.sh \
  --scenario "$SCENARIO" \
  --output "$report_path" \
  --dry-run

if ! grep -q '"ok":true' "$report_file" || ! grep -q '"dry_run":true' "$report_file"; then
  echo "benchmark smoke did not produce a valid dry-run report" >&2
  exit 1
fi
if grep -q 'benchmark-secret' "$report_file"; then
  echo "benchmark smoke report contains an unredacted credential" >&2
  exit 1
fi

run_from_repo_root bash scripts/bench/profile-performance.sh --validate

if [[ -n "$BASELINE" || -n "$CURRENT" ]]; then
  if [[ -z "$BASELINE" || -z "$CURRENT" ]]; then
    echo "BASELINE and CURRENT must be supplied together" >&2
    exit 2
  fi
  run_from_repo_root cargo run -p pg-kinetic -- benchmark score \
    --baseline "$BASELINE" \
    --current "$CURRENT" \
    --release
fi

success "performance smoke passed"

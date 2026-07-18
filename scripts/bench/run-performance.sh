#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$BASH_SOURCE")" && pwd -P)"
source "$SCRIPT_DIR/../lib/common.sh"

SCENARIO="bench/scenarios/benchmark-simple-query.toml"
OUTPUT="bench/results/benchmark-simple-query.json"
dry_run=false

while (($# > 0)); do
  case "$1" in
    --scenario)
      SCENARIO="$2"
      shift 2
      ;;
    --output)
      OUTPUT="$2"
      shift 2
      ;;
    --dry-run)
      dry_run=true
      shift
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

arguments=(
  run -p pg-kinetic --
  benchmark run
  --scenario "$SCENARIO"
  --format json
  --output "$OUTPUT"
)

if "$dry_run"; then
  arguments+=(--dry-run)
fi

run_from_repo_root cargo "${arguments[@]}"
success "performance run completed"

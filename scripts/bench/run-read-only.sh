#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$BASH_SOURCE")" && pwd -P)"
source "$SCRIPT_DIR/../lib/common.sh"

SCENARIO="bench/scenarios/benchmark-simple-query.toml"
SAMPLE_RATE="${PG_KINETIC_PHASE_TIMING_SAMPLE_RATE:-1.0}"
OUTPUT="bench/results/read-only-phase-sampling-${SAMPLE_RATE//[^0-9A-Za-z_.-]/_}.json"
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

run_from_repo_root env PG_KINETIC_PHASE_TIMING_SAMPLE_RATE="$SAMPLE_RATE" cargo "${arguments[@]}"
success "read-only performance run completed"

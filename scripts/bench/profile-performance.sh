#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$BASH_SOURCE")" && pwd -P)"
source "$SCRIPT_DIR/../lib/common.sh"

KIND="flamegraph"
SCENARIO="bench/scenarios/benchmark-simple-query.toml"
TARGET="pg-kinetic"
OUTPUT=""
validate=false
dry_run=false

while (($# > 0)); do
  case "$1" in
    --kind)
      KIND="$2"
      shift 2
      ;;
    --scenario)
      SCENARIO="$2"
      shift 2
      ;;
    --target)
      TARGET="$2"
      shift 2
      ;;
    --output)
      OUTPUT="$2"
      shift 2
      ;;
    --validate)
      validate=true
      shift
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

if [[ "$KIND" != "flamegraph" && "$KIND" != "perf" && "$KIND" != "ebpf" ]]; then
  echo "--kind must be flamegraph, perf, or ebpf" >&2
  exit 2
fi

if "$dry_run"; then
  success "performance profile dry-run"
  exit 0
fi

if "$validate"; then
  run_from_repo_root cargo run -p pg-kinetic -- profile validate
  success "performance profile validation completed"
  exit 0
fi

arguments=(
  run -p pg-kinetic --
  profile run
  --scenario "$SCENARIO"
  --kind "$KIND"
  --target "$TARGET"
)

if [[ -n "$OUTPUT" ]]; then
  arguments+=(--output "$OUTPUT")
fi

run_from_repo_root cargo "${arguments[@]}"
success "performance profile completed"

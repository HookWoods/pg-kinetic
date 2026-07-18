#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$BASH_SOURCE")" && pwd -P)"
source "$SCRIPT_DIR/../lib/common.sh"

BASELINE=""
CURRENT=""
dry_run=false

while (($# > 0)); do
  case "$1" in
    --baseline)
      BASELINE="$2"
      shift 2
      ;;
    --current)
      CURRENT="$2"
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

if [[ -z "$BASELINE" || -z "$CURRENT" ]]; then
  echo "--baseline and --current are required" >&2
  exit 2
fi

if "$dry_run"; then
  success "performance comparison dry-run"
  exit 0
fi

run_from_repo_root cargo run -p pg-kinetic -- benchmark compare --baseline "$BASELINE" --current "$CURRENT"
success "performance comparison completed"

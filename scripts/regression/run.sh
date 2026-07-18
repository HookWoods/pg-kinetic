#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$BASH_SOURCE")" && pwd -P)"
source "$SCRIPT_DIR/../lib/common.sh"

manifest="regression/manifest.toml"
mode="run"
arguments=()

while (($# > 0)); do
  case "$1" in
    --manifest)
      manifest="$2"
      shift 2
      ;;
    --list)
      mode="list"
      shift
      ;;
    *)
      arguments+=("$1")
      shift
      ;;
  esac
done

run_from_repo_root cargo run -p pg-kinetic -- regression "$mode" --manifest "$manifest" "${arguments[@]}"

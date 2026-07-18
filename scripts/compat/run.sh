#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$BASH_SOURCE")" && pwd -P)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd -P)"

args=()
for argument in "$@"; do
  args+=("$argument")
done

cd "$REPO_ROOT"
cargo run -p pg-kinetic -- compat run --manifest regression/manifest.toml "${args[@]}"

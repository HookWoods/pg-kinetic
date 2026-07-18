#!/usr/bin/env sh
set -eu

manifest=${1:-regression/manifest.toml}
if [ "$#" -gt 0 ]; then
  shift
fi

cargo run -p pg-kinetic -- regression run --manifest "$manifest" "$@"

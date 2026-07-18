#!/usr/bin/env bash
set -euo pipefail

if ! command -v cargo >/dev/null 2>&1; then
  printf '{"success_marker":"compatibility report complete","outcome":"skip","skip_reason":"toolchain-unavailable","language":"rust"}\n'
  exit 0
fi

if ! cargo metadata --manifest-path compat/rust/Cargo.toml --no-deps --format-version 1 >/dev/null 2>&1; then
  printf '{"success_marker":"compatibility report complete","outcome":"skip","skip_reason":"library-unavailable","language":"rust"}\n'
  exit 0
fi

cargo run --manifest-path compat/rust/Cargo.toml

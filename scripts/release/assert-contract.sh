#!/usr/bin/env bash
set -euo pipefail

contains() {
  local pattern="$1"
  local path="$2"

  if command -v rg >/dev/null 2>&1; then
    rg -q --fixed-strings "$pattern" "$path"
  else
    grep -Fq "$pattern" "$path"
  fi
}

contains 'single-primary' docs/release-contract.md
contains 'not supported for live traffic' docs/release-contract.md
contains 'cargo run -p xtask -- compat --language rust --target pg-kinetic --smoke' docs/release-contract.md

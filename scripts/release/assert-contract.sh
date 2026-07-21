#!/usr/bin/env bash
set -euo pipefail

rg -q 'single-primary' docs/release-contract.md
rg -q 'not supported for live traffic' docs/release-contract.md
rg -q 'cargo run -p xtask -- compat --target pg-kinetic --smoke' docs/release-contract.md

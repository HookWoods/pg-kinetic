#!/usr/bin/env bash
set -euo pipefail

if ! command -v python >/dev/null 2>&1; then
  printf '{"success_marker":"compatibility report complete","outcome":"skip","skip_reason":"toolchain-unavailable","language":"python"}\n'
  exit 0
fi

python compat/python/smoke.py

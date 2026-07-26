#!/usr/bin/env bash
set -euo pipefail

python_bin="${PYTHON:-}"
if [ -z "$python_bin" ]; then
  if command -v python >/dev/null 2>&1; then
    python_bin="python"
  elif command -v python3 >/dev/null 2>&1; then
    python_bin="python3"
  fi
fi

if [ -z "$python_bin" ]; then
  printf '{"success_marker":"compatibility report complete","outcome":"skip","skip_reason":"toolchain-unavailable","language":"python"}\n'
  exit 0
fi

"$python_bin" compat/python/smoke.py

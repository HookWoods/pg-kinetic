#!/usr/bin/env bash
set -euo pipefail

cd "$(cd "$(dirname "$0")/../.." && pwd -P)"

if ! command -v cc >/dev/null 2>&1; then
  printf '{"ok":true,"success_marker":"compatibility report complete","outcome":"skip","skip_reason":"toolchain-unavailable","language":"c","error_summary":"C compiler is unavailable"}\n'
  exit 0
fi

if ! pkg-config --exists libpq 2>/dev/null; then
  printf '{"ok":true,"success_marker":"compatibility report complete","outcome":"skip","skip_reason":"library-unavailable","language":"c","error_summary":"libpq development files are unavailable"}\n'
  exit 0
fi

make -C compat/c compat

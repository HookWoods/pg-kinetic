#!/usr/bin/env bash
set -euo pipefail

cd "$(cd "$(dirname "$0")/../.." && pwd -P)"

if ! command -v npm >/dev/null 2>&1; then
  printf '{"ok":true,"success_marker":"compatibility report complete","outcome":"skip","skip_reason":"toolchain-unavailable","language":"javascript","error_summary":"npm is unavailable"}\n'
  exit 0
fi

if [ ! -d compat/javascript/node_modules/pg ] || [ ! -d compat/javascript/node_modules/kysely ]; then
  printf '{"ok":true,"success_marker":"compatibility report complete","outcome":"skip","skip_reason":"library-unavailable","language":"javascript","error_summary":"install compat/javascript dependencies before running"}\n'
  exit 0
fi

npm --prefix compat/javascript run compat --silent

#!/usr/bin/env bash
set -euo pipefail

cd "$(cd "$(dirname "$0")/../.." && pwd -P)"

if ! command -v java >/dev/null 2>&1; then
  printf '{"ok":true,"success_marker":"compatibility report complete","language":"java","outcome":"skip","skip_reason":"toolchain-unavailable","error_summary":"java is unavailable"}\n'
elif command -v gradle >/dev/null 2>&1; then
  gradle --no-daemon -p compat/java compatSmoke --quiet
elif [ -x compat/java/gradlew ]; then
  compat/java/gradlew --no-daemon -p compat/java compatSmoke --quiet
else
  printf '{"ok":true,"success_marker":"compatibility report complete","language":"java","outcome":"skip","skip_reason":"toolchain-unavailable","error_summary":"Gradle is unavailable"}\n'
fi

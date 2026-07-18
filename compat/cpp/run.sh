#!/usr/bin/env bash
set -euo pipefail

cd "$(cd "$(dirname "$0")/../.." && pwd -P)"

if ! command -v cmake >/dev/null 2>&1; then
  printf '{"ok":true,"success_marker":"compatibility report complete","outcome":"skip","skip_reason":"toolchain-unavailable","language":"cpp","error_summary":"CMake is unavailable"}\n'
  exit 0
fi

if ! command -v c++ >/dev/null 2>&1 && ! command -v g++ >/dev/null 2>&1; then
  printf '{"ok":true,"success_marker":"compatibility report complete","outcome":"skip","skip_reason":"toolchain-unavailable","language":"cpp","error_summary":"C++ compiler is unavailable"}\n'
  exit 0
fi

if ! pkg-config --exists libpqxx 2>/dev/null; then
  printf '{"ok":true,"success_marker":"compatibility report complete","outcome":"skip","skip_reason":"library-unavailable","language":"cpp","error_summary":"libpqxx development files are unavailable"}\n'
  exit 0
fi

cmake -S compat/cpp -B target/compat/cpp
cmake --build target/compat/cpp
target/compat/cpp/compat_libpqxx

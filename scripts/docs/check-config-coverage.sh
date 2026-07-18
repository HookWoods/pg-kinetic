#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
script_path="$repo_root/scripts/docs/check-config-docs.py"

run_python() {
  local python_bin="$1"
  local path="$script_path"
  if [[ "$python_bin" == *.exe ]] && command -v wslpath >/dev/null 2>&1; then
    path="$(wslpath -w "$script_path")"
  fi
  "$python_bin" "$path"
}

if [[ -n "${PYTHON:-}" ]]; then
  run_python "$PYTHON"
  exit
fi

for python_bin in python3 python python.exe py.exe; do
  if command -v "$python_bin" >/dev/null 2>&1 &&
    "$python_bin" -c 'import tomllib' >/dev/null 2>&1; then
    run_python "$python_bin"
    exit
  fi
done

printf 'Python 3.11+ with tomllib is required for config docs validation.\n' >&2
exit 1

#!/usr/bin/env bash
set -euo pipefail

COMMON_DIR="$(cd "$(dirname "$BASH_SOURCE")" && pwd -P)"
readonly REPO_ROOT="$(cd "$COMMON_DIR/../.." && pwd -P)"

success() {
  printf 'PASS: %s\n' "$*"
}

skip() {
  printf 'SKIP: %s\n' "$*"
}

resolve_command() {
  local command_name="$1"

  if command -v "$command_name" >/dev/null 2>&1; then
    printf '%s\n' "$command_name"
    return 0
  fi

  if command -v "$command_name.exe" >/dev/null 2>&1; then
    printf '%s.exe\n' "$command_name"
    return 0
  fi

  return 1
}

require_command() {
  local command_name="$1"

  if resolve_command "$command_name" >/dev/null; then
    return 0
  fi

  skip "$command_name is not available"
  return 1
}

temporary_output_path() {
  local prefix="${1:-pg-kinetic}"
  local suffix="${2:-tmp}"
  local temp_root="${TMPDIR:-/tmp}"

  printf '%s/%s-%s.%s\n' "$temp_root" "$prefix" "$$" "$suffix"
}

run_from_repo_root() {
  local executable
  executable="$(resolve_command "$1")" || {
    echo "$1 is not available" >&2
    return 127
  }
  shift

  (
    cd "$REPO_ROOT"
    "$executable" "$@"
  )
}

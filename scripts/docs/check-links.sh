#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
failed=0

check_file() {
  local file="$1"
  local match target path

  while IFS= read -r match; do
    target="${match#*](}"
    target="${target%)}"
    target="${target%% *}"
    target="${target%%#*}"

    case "$target" in
      ''|http://*|https://*|mailto:*|tel:*|data:*|//*)
        continue
        ;;
    esac

    if [[ "$target" = /* ]]; then
      path="$repo_root/${target#/}"
    else
      path="$(dirname "$file")/$target"
    fi

    if [[ ! -e "$path" ]]; then
      printf 'broken link: %s -> %s\n' "${file#$repo_root/}" "$target" >&2
      failed=1
    fi
  done < <(grep -oE '\[[^]]+\]\([^)]+\)' "$file" || true)
}

check_file "$repo_root/README.md"
check_file "$repo_root/docs-site/README.md"

while IFS= read -r -d '' file; do
  check_file "$file"
done < <(find "$repo_root/docs" -type f -name '*.md' -print0)

if (( failed )); then
  exit 1
fi

printf 'Markdown links are valid.\n'

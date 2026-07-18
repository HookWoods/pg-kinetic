#!/usr/bin/env python3
from __future__ import annotations

import re
import sys
import tomllib
from pathlib import Path


FIELD_CATALOG = [
    {
        "path": "runtime.production.adaptive_enabled",
        "type": "bool",
        "default": "false",
        "reload": "restart",
    },
    {
        "path": "runtime.production.adaptive_mode",
        "type": "enum",
        "default": "recommend",
        "reload": "restart",
    },
    {
        "path": "runtime.production.adaptive_window_ms",
        "type": "milliseconds",
        "default": "60000",
        "reload": "restart",
    },
    {
        "path": "runtime.production.adaptive_min_confidence",
        "type": "float",
        "default": "0.8",
        "reload": "restart",
    },
    {
        "path": "runtime.production.adaptive_apply_enabled",
        "type": "bool",
        "default": "false",
        "reload": "restart",
    },
    {
        "path": "runtime.production.adaptive_apply_allowlist",
        "type": "list",
        "default": "[]",
        "reload": "restart",
    },
    {
        "path": "runtime.production.adaptive_max_change_percent",
        "type": "integer",
        "default": "10",
        "reload": "restart",
    },
]

FORBIDDEN_PATH_PREFIXES = (
    "runtime.production.apply.",
    "runtime.production.guardrail.",
    "runtime.production.adaptive.",
)


def strip_inline_code(value: str) -> str:
    value = value.strip()
    if value.startswith("`") and value.endswith("`"):
        return value[1:-1]
    return value


def table_rows(markdown: str) -> dict[str, dict[str, str]]:
    rows: dict[str, dict[str, str]] = {}
    for line_number, line in enumerate(markdown.splitlines(), 1):
        if not line.startswith("| `"):
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) < 6:
            continue
        path = strip_inline_code(cells[0])
        rows[path] = {
            "type": cells[1],
            "default": strip_inline_code(cells[2]),
            "reload": cells[5],
            "line": str(line_number),
        }
    return rows


def toml_blocks(path: Path) -> list[tuple[int, str]]:
    blocks: list[tuple[int, str]] = []
    in_block = False
    start_line = 0
    current: list[str] = []

    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not in_block and line.strip() == "```toml":
            in_block = True
            start_line = line_number + 1
            current = []
            continue
        if in_block and line.strip() == "```":
            blocks.append((start_line, "\n".join(current)))
            in_block = False
            continue
        if in_block:
            current.append(line)

    return blocks


def validate_toml_block(path: Path, line_number: int, block: str) -> list[str]:
    errors: list[str] = []
    try:
        parsed = tomllib.loads(block)
    except tomllib.TOMLDecodeError as error:
        return [f"{path}:{line_number}: invalid TOML example: {error}"]

    production = (
        parsed.get("runtime", {})
        .get("production", {})
    )
    if isinstance(production, dict):
        for nested in ("adaptive", "apply", "guardrail"):
            if nested in production:
                errors.append(
                    f"{path}:{line_number}: adaptive runtime fields are flattened; do not use [runtime.production.{nested}]"
                )

    return errors


def source_field_names(source: str) -> set[str]:
    return set(re.findall(r"(?m)^\s*pub\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*:", source))


def main() -> int:
    repo_root = Path(__file__).resolve().parents[2]
    config_docs_path = repo_root / "docs" / "configuration.md"
    config_source_path = repo_root / "crates" / "pg-kinetic-proxy" / "src" / "config.rs"

    config_docs = config_docs_path.read_text(encoding="utf-8")
    rows = table_rows(config_docs)
    errors: list[str] = []

    for path in rows:
        if path.startswith(FORBIDDEN_PATH_PREFIXES):
            errors.append(f"{config_docs_path}:{rows[path]['line']}: forbidden config path: {path}")

    for expected in FIELD_CATALOG:
        path = expected["path"]
        row = rows.get(path)
        if row is None:
            errors.append(f"{config_docs_path}: missing documented config path: {path}")
            continue
        for key in ("type", "default", "reload"):
            if row[key] != expected[key]:
                errors.append(
                    f"{config_docs_path}:{row['line']}: {path} has {key}={row[key]!r}; expected {expected[key]!r}"
                )

    source_fields = source_field_names(config_source_path.read_text(encoding="utf-8"))
    missing_fields = [
        field
        for field in sorted(source_fields)
        if not re.search(rf"(?m)(^|[^a-zA-Z0-9_]){re.escape(field)}([^a-zA-Z0-9_]|$)", config_docs)
    ]
    for field in missing_fields:
        errors.append(f"{config_docs_path}: configuration field is missing from docs/configuration.md: {field}")

    for path in sorted((repo_root / "docs").rglob("*")):
        if path.suffix.lower() not in {".md", ".mdx"}:
            continue
        for line_number, block in toml_blocks(path):
            errors.extend(validate_toml_block(path, line_number, block))

    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        return 1

    print("Configuration docs coverage is valid.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

---
title: "CLI Reference"
description: "Command reference for running pg-kinetic, validating config, previewing routes and policies, profiling, benchmarking, and regression workflows."
keywords:
  - pg-kinetic CLI
  - PostgreSQL proxy commands
  - preflight command
  - route preview
---

# CLI Reference

The `pg-kinetic` binary can run the proxy or execute operator commands such as preflight, route preview, policy preview, compatibility checks, regression runs, profiling, and benchmark validation.

Examples assume the installed binary or container entrypoint is named `pg-kinetic`. Container examples use the local image from `docker build -t pg-kinetic:local .`.

## Command Model

All commands share the global runtime flags documented in [Configuration](./configuration.md). Command-specific flags are listed here.

| Command | Purpose | Output | Success exit | Failure exit |
| --- | --- | --- | --- | --- |
| no subcommand | Run the proxy process. | logs and network listeners | process keeps running | nonzero on config, bind, TLS/auth asset, startup, or runtime failure |
| `preflight` | Validate deployable config and runtime assets. | JSON report | `0` when `error_count = 0` | nonzero when config cannot load or report has errors |
| `route-preview` | Evaluate offline sharding route selection. | JSON summary | `0` when preview succeeds | nonzero when preview file/input cannot parse or selection fails |
| `policy-preview` | Evaluate offline policy model behavior. | JSON summary | `0` when preview succeeds | nonzero when preview file/input cannot parse or policy validation fails |
| `benchmark validate` | Validate benchmark scenario structure. | JSON summary | `0` when scenario is valid | nonzero on validation error |
| `benchmark run` | Prepare benchmark report; live execution currently requires `--dry-run`. | JSON report and optional file | `0` for valid dry run | nonzero on invalid scenario or live execution request |
| `benchmark compare` | Compare baseline/current benchmark reports. | JSON comparison | `0` unless comparison outcome is failed | nonzero on invalid reports or failed comparison |
| `benchmark score` | Score benchmark report regression budgets. | JSON score | `0`, or `0` with `--release` when release gate passes | nonzero with `--release` when release gate fails |
| `compat list` | List compatibility suites from a manifest. | JSON list | `0` | nonzero on manifest or filter error |
| `compat run` | Run selected compatibility suites. | JSON report and optional file | `0` when no selected suite fails | nonzero on runner error or failed suite |
| `regression list` | List regression cases from a manifest. | JSON list | `0` | nonzero on manifest or filter error |
| `regression run` | Run selected regression cases. | JSON report and optional file | `0` when no selected case fails | nonzero on runner error or failed case |
| `profile validate` | Report local profiling tool availability. | JSON status map | `0` | nonzero on unexpected validation error |
| `profile run` | Run a selected profiler for a benchmark scenario. | JSON report | `0` when profiler run succeeds | nonzero on invalid scenario, unavailable tool, or profiler failure |

## Run The Proxy

```bash
pg-kinetic --config-file /etc/pg-kinetic/pg-kinetic.toml
```

Container:

```bash
docker run --rm \
  -v "$PWD/pg-kinetic.toml:/etc/pg-kinetic/pg-kinetic.toml:ro" \
  pg-kinetic:local \
  --config-file /etc/pg-kinetic/pg-kinetic.toml
```

## Preflight

Validate a config before rollout:

```bash
pg-kinetic preflight --config /etc/pg-kinetic/pg-kinetic.toml
```

Container:

```bash
docker run --rm \
  -v "$PWD/pg-kinetic.toml:/etc/pg-kinetic/pg-kinetic.toml:ro" \
  pg-kinetic:local \
  preflight --config /etc/pg-kinetic/pg-kinetic.toml
```

Treat preflight errors as deployment blockers.

Flags:

| Flag | Required | Default | Meaning |
| --- | --- | --- | --- |
| `--config path` | yes | none | Config file to parse and validate. |
| `--format json` | no | `json` | Output format; JSON is the only supported value. |

JSON shape:

```json
{
  "ok": true,
  "config": "deploy/pg-kinetic.toml",
  "warning_count": 0,
  "error_count": 0,
  "warnings": [],
  "errors": []
}
```

## Route Preview

Preview routing and sharding models without live traffic:

```bash
pg-kinetic route-preview \
  --config /etc/pg-kinetic/pg-kinetic.toml \
  --database billing \
  --user reporter \
  --application-name dashboard \
  --sql "select * from orders where tenant_id = 'tenant-a'"
```

The output is JSON with the selected route, shard id when known, backend role when known, and the decision reason. This is a structural preview command, not a live operation.

Flags:

| Flag | Required | Default | Meaning |
| --- | --- | --- | --- |
| `--config path` | yes | none | Offline preview TOML containing `[sharding]`. |
| `--database name` | yes | none | Synthetic startup database. |
| `--user name` | yes | none | Synthetic startup user. |
| `--sql sql` | yes | none | SQL text used for shard hint/key extraction. |
| `--application-name name` | no | unset | Synthetic application name for scope matching. |

JSON shape:

```json
{
  "ok": true,
  "route": "billing/reporter/<none>/default",
  "shard_id": "orders-b",
  "backend_role": "replica",
  "reason": "hash_match",
  "shard_reason": "hash_match"
}
```

## Policy Preview

Evaluate policy rules against a synthetic request context:

```bash
pg-kinetic policy-preview \
  --config /etc/pg-kinetic/pg-kinetic.toml \
  --database billing \
  --user reporter \
  --route primary \
  --shard tenant-a \
  --query-class read_candidate
```

The preview context contains redacted secret-bearing fields so policy model behavior can be inspected safely. This is a structural preview command, not a live operation.

Flags:

| Flag | Required | Default | Meaning |
| --- | --- | --- | --- |
| `--config path` | yes | none | Offline preview TOML containing `[policy]` and optional `[sharding]`. |
| `--database name` | yes | none | Synthetic database. |
| `--user name` | yes | none | Synthetic user. |
| `--route id` | yes | none | Synthetic route id before policy adjustment. |
| `--shard id` | yes | none | Synthetic shard id before policy adjustment. |
| `--query-class class` | yes | none | One of `write`, `read_only`, `read_candidate`, `transaction_control`, `session_mutation`, `copy`, `unknown`. |
| `--application-name name` | no | unset | Synthetic application name. |
| `--format json` | no | `json` | Output format; JSON is the only supported value. |

JSON shape:

```json
{
  "ok": true,
  "policy_mode": "dry_run",
  "original_route": "primary",
  "policy_adjusted_route": "primary",
  "original_shard": "orders-a",
  "policy_adjusted_shard": "orders-a",
  "action": "require_replica",
  "dry_run_outcome": "dry_run",
  "dry_run_reason": "would_require_replica",
  "deny_reason": null,
  "sqlstate": null,
  "context": "database=billing, user=reporter, sensitive_inputs=<redacted>"
}
```

## Exit Codes

| Command class | Success | Failure |
| --- | --- | --- |
| Proxy run | process stays running | startup, config, bind, TLS/auth asset, or runtime error exits nonzero |
| `preflight` | zero when validation succeeds | nonzero when errors are found or config cannot load |
| `route-preview` | zero with JSON output | nonzero when config or input cannot be parsed |
| `policy-preview` | zero with JSON output | nonzero when config or input cannot be parsed |
| `benchmark`, `compat`, `regression`, `profile` | zero when the selected check succeeds | nonzero on validation failure, failed budget, failed smoke, or invalid arguments |

## Compatibility Commands

```bash
pg-kinetic compat list
pg-kinetic compat run --target pg-kinetic --smoke
```

Use filters such as `--language`, `--library`, `--target`, and `--category` to narrow the matrix.

Flags:

| Command | Flags |
| --- | --- |
| `compat list` | `--manifest path` default `regression/manifest.toml`; optional `--language`, `--library`, `--target`, `--category`, `--smoke`, `--format json`. |
| `compat run` | Same filters as `compat list`, plus optional `--output path`. |

List output contains `ok` and `suites[]`. Run output contains the compatibility report and exits nonzero when selected suites fail.

## Regression Commands

```bash
pg-kinetic regression list --manifest regression/manifest.toml
pg-kinetic regression run --manifest regression/manifest.toml --category smoke
```

Regression outputs are JSON and redact sensitive text before printing errors.

Flags:

| Command | Flags |
| --- | --- |
| `regression list` | Required `--manifest path`; optional `--category`, `--platform`, `--format json`. |
| `regression run` | Required `--manifest path`; optional `--category`, `--platform`, `--output path`, `--format json`. |

List output contains `ok` and `cases[]`. Run output contains selected case results and exits nonzero when selected cases fail.

## Benchmark Commands

Benchmark commands are for controlled performance validation, not initial installation:

```bash
pg-kinetic benchmark validate --scenario bench/scenarios/benchmark-simple-query.toml
pg-kinetic benchmark run --scenario bench/scenarios/benchmark-simple-query.toml --dry-run
pg-kinetic benchmark compare --baseline bench/baselines/simple.json --current bench/results/simple.json
pg-kinetic benchmark score --baseline bench/baselines/simple.json --current bench/results/simple.json
```

Live load execution and local-noise interpretation are covered in [Benchmarking](./benchmarking.md).

Flags:

| Command | Flags |
| --- | --- |
| `benchmark validate` | Required `--scenario path`. |
| `benchmark run` | Required `--scenario path`; optional `--format json`, `--output path`, `--dry-run`. Live execution is not implemented without `--dry-run`. |
| `benchmark compare` | Required `--baseline path` and `--current path`. |
| `benchmark score` | Required `--baseline path` and `--current path`; optional `--format json`, `--release`. |

## Profiling Commands

```bash
pg-kinetic profile validate
pg-kinetic profile run --scenario bench/scenarios/benchmark-simple-query.toml --kind flamegraph
```

Supported profile tools are validated locally because platform support differs between Windows, Linux, and developer machines.

Flags:

| Command | Flags |
| --- | --- |
| `profile validate` | No command-specific flags. |
| `profile run` | Required `--scenario path` and `--kind flamegraph|perf`; optional `--target name` default `pg-kinetic`; optional `--output path`. |

Common failures: profiler executable missing, OS support missing, invalid benchmark scenario, or output path not writable.

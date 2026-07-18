---
title: "Regression Workflow"
description: "Regression workflow for pg-kinetic manifests, runner inputs, performance scoring, compatibility checks, and CI-oriented validation."
keywords:
  - pg-kinetic regression
  - PostgreSQL proxy testing
  - performance regression
  - compatibility runner
---

# Regression Platform

Regression cases live in `regression/manifest.toml`. The checked-in schema at
`regression/manifest.schema.json` defines the supported category, platform,
timeout, service, success-marker, artifact-policy, and compatibility metadata
fields.

List matching cases without running them:

```sh
cargo run -p pg-kinetic -- regression list --manifest regression/manifest.toml --category benchmark
```

Run a filtered set and emit a redacted JSON report:

```sh
cargo run -p pg-kinetic -- regression run --manifest regression/manifest.toml --platform windows
```

The runner records `pass`, `fail`, `skip`, `timeout`, and `blocked`. A case is
blocked when it declares a service that is not present in the comma-separated
`PG_KINETIC_REGRESSION_SERVICES` environment variable. Shell output is used only
to check the success marker and is not included in the report. `large` artifacts
and explicit `--output` reports are accepted only when the destination is ignored
by Git; manifest artifact paths must be relative paths under `target/`.

Use `scripts/regression/run.sh` or `scripts/regression/run.ps1` for the same
command on Unix-like shells or PowerShell.

## Compatibility reports

Compatibility cases use the same regression manifest with an additional
`case.compatibility` table. The table records suite id, language, library,
library version, target, command, required services, artifact policy, smoke,
category, and required-suite metadata. Optional suites include a stable skip
reason.

List compatibility regression cases:

```sh
cargo run -p xtask -- regression --category compatibility --list
cargo run -p xtask -- compat --list
```

Run smoke reports for both comparison targets:

```sh
cargo run -p xtask -- compat --target direct-postgres --smoke
cargo run -p xtask -- compat --target pg-kinetic --smoke
```

Normalized reports include direct PostgreSQL and pg-kinetic target labels,
library versions, stable skip reasons, durations, and redacted error summaries.
Large artifacts must stay under `target/compat/`.

## Performance score

`benchmark score` compares benchmark report JSON. It evaluates p50, p95, p99,
p999, throughput, CPU/query, memory/client, error rate, checkout latency, and
prepared-cache hit rate. Missing values produce `missing-baseline`; lower is
better for latency, CPU, memory, and error rate, while higher is better for
throughput and prepared-cache hit rate.

```sh
cargo run -p pg-kinetic -- benchmark score \
  --baseline regression/baselines/performance-score.sample.json \
  --current regression/baselines/performance-score.sample.json \
  --format json
```

Use `--release` to return a nonzero exit code for `fail` or `missing-baseline`.
Score JSON does not include target DSNs and redacts credential-shaped paths or
error text before rendering.

Compatibility cases share the manifest and report contracts with smoke,
protocol, docs, and benchmark regressions.

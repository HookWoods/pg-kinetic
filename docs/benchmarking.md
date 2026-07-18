---
title: "Benchmarking"
description: "Benchmarking workflow for pg-kinetic, including local noise, target baselines, scenario matrices, reports, profiling hooks, and update policy."
keywords:
  - pg-kinetic benchmarks
  - PostgreSQL proxy benchmark
  - connection pooler performance
  - benchmark regression
---

# Benchmarking

pg-kinetic uses versioned scenarios and JSON reports to compare the proxy with direct PostgreSQL, PgBouncer, and PgDog. The same scenario, driver, target matrix, host, and load settings must be used for a baseline and its candidate report.

## Local Targets

Start the local target matrix before collecting measurements:

```powershell
docker compose -f bench/compose.yml up -d --build postgres pgbouncer pgdog pg-kinetic
```

The compose stack exposes direct PostgreSQL on `55432`, PgBouncer on `56432`, PgDog on `57432`, and pg-kinetic on `58432`. Verify that the stack is ready with a `psql` query before collecting a baseline or candidate. Stop it after the run with `docker compose -f bench/compose.yml down`.

The benchmark scenarios use their own target DSNs. For a host-side workload driver, set them to the compose ports (`55432`, `56432`, `57432`, and `58432`) before collecting measurements; the checked-in scenario ports model an internal target matrix. Do not commit credentials in reports. The runner redacts DSN credentials in JSON output.

## Scenario Format

Scenarios live under `bench/scenarios/`. Each scenario defines:

- `name`, `driver`, `workload`, and duration
- warmup duration and connection concurrency
- feature flags for routing, sharding, and policy overhead
- expected latency, throughput, CPU, memory, and error-rate measurements
- a target matrix with `label`, `comparison`, and `dsn` entries

Supported comparison labels are `direct_postgresql`, `pgbouncer`, `pgdog`, and `pg_kinetic`. Supported drivers are `pgbench`, `psql`, `tokio_postgres`, `pgx`, `node_pg`, and `psycopg`.

Validate a scenario before collecting or comparing results:

```powershell
cargo run -p pg-kinetic -- benchmark validate --scenario bench/scenarios/benchmark-simple-query.toml
```

## Reports And Budgets

Create a structural dry-run JSON report with the product runner:

```powershell
powershell.exe -ExecutionPolicy Bypass -File scripts/bench/run-performance.ps1 `
  -Scenario bench/scenarios/benchmark-simple-query.toml `
  -Output bench/results/simple-query.json
```

On Linux, use the Bash wrapper with the same scenario and output contract:

```bash
bash scripts/bench/run-performance.sh \
  --scenario bench/scenarios/benchmark-simple-query.toml \
  --output bench/results/simple-query.json
```

Store reviewed live-measurement baselines under `bench/baselines/` and keep candidate reports under the ignored `bench/results/` directory. A report records scenario metadata, target comparison labels, latency percentiles, throughput, error rate, CPU/query, memory/client, process-metric collection status, environment data, and the current Git commit when available.

Compare reports only when they use the same scenario and target set:

```powershell
powershell.exe -ExecutionPolicy Bypass -File scripts/bench/compare-performance.ps1 `
  -Baseline bench/baselines/simple-query.json `
  -Current bench/results/simple-query.json
```

The Linux equivalent is:

```bash
bash scripts/bench/compare-performance.sh \
  --baseline bench/baselines/simple-query.json \
  --current bench/results/simple-query.json
```

The comparison returns a warning for more than 5% latency, throughput, CPU/query, or memory/client regression and fails above 10%. Error rate warns above `0.001` and fails above `0.01`. A warning leaves the command successful for review; a failed budget returns a nonzero exit code. Missing or unknown baseline values remain warnings rather than passes, while missing current values fail the gate.

`benchmark run` currently validates the scenario and emits a structured report; it does not itself drive traffic against the target DSNs. Use the local stack and workload driver to collect measurements, and do not treat a dry-run report as performance evidence.

## Local Noise

Treat local benchmark output as noisy unless the machine is pinned and quiet. Close background build, browser, antivirus scan, and indexing work; keep the PostgreSQL, PgBouncer, PgDog, and pg-kinetic targets on the same host or container placement between baseline and candidate runs; keep CPU power mode, thermal state, and container resource limits unchanged; and avoid mixing Windows and Linux reports in one comparison.

Collect at least three live runs for a baseline update and review the median with the p95 and p99 spread before accepting a new file. Do not update a baseline from a single run, a dry-run report, or a run that skipped process metrics unexpectedly. If one target is optional for local development, mark the absence as a local blocker in notes and keep required comparison targets present in committed baselines.

## Baseline Updates

Use this workflow for checked-in performance baselines:

1. Validate the scenario and target matrix with `benchmark validate`.
2. Start the local target matrix and verify direct PostgreSQL, PgBouncer, PgDog, and pg-kinetic are reachable.
3. Collect repeated live reports under `bench/results/` with the PowerShell or Bash wrapper for the current platform.
4. Compare the reviewed candidate against the existing baseline with `compare-performance`.
5. Commit only the reviewed baseline file under `bench/baselines/` or `regression/baselines/`; leave raw run output in ignored result directories.

Keep the same target labels and target set when updating a baseline. Adding or removing `direct_postgresql`, `pgbouncer`, `pgdog`, or `pg_kinetic` is a scenario change and needs review before the performance score gate can be trusted.

## Profiles And Process Hooks

Check whether optional profile tools are available:

```powershell
powershell.exe -ExecutionPolicy Bypass -File scripts/bench/profile-performance.ps1 -Validate
```

On Linux:

```bash
bash scripts/bench/profile-performance.sh --validate
```

Validate profile capture plumbing after the scenario and target are fixed:

```powershell
powershell.exe -ExecutionPolicy Bypass -File scripts/bench/profile-performance.ps1 `
  -Kind flamegraph `
  -Scenario bench/scenarios/benchmark-simple-query.toml `
  -Target pg-kinetic `
  -Output bench/profiles/simple-query-flamegraph.svg
```

The Bash wrapper accepts the same values with kebab-case flags:

```bash
bash scripts/bench/profile-performance.sh \
  --kind flamegraph \
  --scenario bench/scenarios/benchmark-simple-query.toml \
  --target pg-kinetic \
  --output bench/profiles/simple-query-flamegraph.svg
```

`flamegraph` requires `cargo-flamegraph`; `perf` is available only on Linux. Tool absence is reported as skipped, so it is visible without making the local smoke gate platform-dependent. Benchmark reports also include process CPU time, resident memory, and open-file-descriptor collection status where the host supports them. Windows reports these process measurements as unavailable.

Inspect the current process and budget snapshot through the admin listener with `SHOW PERFORMANCE;`. Use `SHOW BENCHMARKS;` to inspect the recorded scenario and target measurements. See [the admin reference](admin.md) and [the metrics catalog](metrics.md) for the available fields and monitoring series.

## Local Gates

Run the portable smoke gate before publishing a benchmark change:

```powershell
powershell.exe -ExecutionPolicy Bypass -File scripts/smoke/performance.ps1
```

Linux:

```bash
bash scripts/smoke/performance.sh
```

It validates the default scenario through the existing benchmark runner, confirms the temporary JSON report is redacted and well-formed, and checks optional profile-tool availability. It does not require containers or make network connections.

After collecting real reports, pass both paths to the same gate to enforce regression budgets:

```powershell
powershell.exe -ExecutionPolicy Bypass -File scripts/smoke/performance.ps1 `
  -Baseline bench/baselines/simple-query.json `
  -Current bench/results/simple-query.json
```

Linux:

```bash
BASELINE=bench/baselines/simple-query.json \
CURRENT=bench/results/simple-query.json \
bash scripts/smoke/performance.sh
```

Use the exact command in local release checks or any existing automation that needs a nonzero exit on a failed budget.

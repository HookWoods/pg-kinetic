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

Create a JSON report with the product runner:

```powershell
powershell.exe -ExecutionPolicy Bypass -File scripts/bench/run-performance.ps1 `
  -Scenario bench/scenarios/benchmark-simple-query.toml `
  -Output bench/results/simple-query.json
```

Store reviewed baselines under `bench/baselines/` and keep candidate reports under the ignored `bench/results/` directory. A report records scenario metadata, target comparison labels, latency percentiles, throughput, error rate, process-metric collection status, environment data, and the current Git commit when available.

Compare reports only when they use the same scenario and target set:

```powershell
powershell.exe -ExecutionPolicy Bypass -File scripts/bench/compare-performance.ps1 `
  -Baseline bench/baselines/simple-query.json `
  -Current bench/results/simple-query.json
```

The comparison returns a warning for more than 5% latency or throughput regression and fails above 10%. Error rate warns above `0.001` and fails above `0.01`. A warning leaves the command successful for review; a failed budget returns a nonzero exit code. Missing or unknown baseline values remain warnings rather than passes.

`benchmark run` currently validates the scenario and emits a structured report; it does not itself drive traffic against the target DSNs. Use the local stack and workload driver to collect measurements, and do not treat a dry-run report as performance evidence.

## Profiles And Process Hooks

Check whether optional profile tools are available:

```powershell
powershell.exe -ExecutionPolicy Bypass -File scripts/bench/profile-performance.ps1 -Validate
```

Capture a pg-kinetic profile after the scenario and target are fixed:

```powershell
powershell.exe -ExecutionPolicy Bypass -File scripts/bench/profile-performance.ps1 `
  -Kind flamegraph `
  -Scenario bench/scenarios/benchmark-simple-query.toml `
  -Target pg-kinetic `
  -Output bench/profiles/simple-query-flamegraph.svg
```

`flamegraph` requires `cargo-flamegraph`; `perf` is available only on Linux. Tool absence is reported as skipped, so it is visible without making the local smoke gate platform-dependent. Benchmark reports also include process CPU time, resident memory, and open-file-descriptor collection status where the host supports them. Windows reports these process measurements as unavailable.

Inspect the current process and budget snapshot through the admin listener with `SHOW PERFORMANCE;`. Use `SHOW BENCHMARKS;` to inspect the recorded scenario and target measurements. See [the admin reference](admin.md) and [the metrics catalog](metrics.md) for the available fields and monitoring series.

## Local Gates

Run the portable smoke gate before publishing a benchmark change:

```powershell
powershell.exe -ExecutionPolicy Bypass -File scripts/smoke/performance.ps1
```

It validates the default scenario through the existing benchmark runner, confirms the temporary JSON report is redacted and well-formed, and checks optional profile-tool availability. It does not require containers or make network connections.

After collecting real reports, pass both paths to the same gate to enforce regression budgets:

```powershell
powershell.exe -ExecutionPolicy Bypass -File scripts/smoke/performance.ps1 `
  -Baseline bench/baselines/simple-query.json `
  -Current bench/results/simple-query.json
```

Use the exact command in local release checks or any existing automation that needs a nonzero exit on a failed budget.

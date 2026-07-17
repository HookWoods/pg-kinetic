# Benchmarking

pg-kinetic ships a small benchmark format for comparing the proxy against direct PostgreSQL and other pooling layers.

## Scenario Format

Benchmark scenarios live in TOML files such as `bench/scenarios/benchmark-basic.toml`.

Required fields:

- `name`
- `driver`
- `duration_ms`
- `warmup_ms`
- `[[targets]]`

Each target includes:

- `label`
- `comparison`
- `dsn`

Supported comparison labels are `direct_postgresql`, `pgbouncer`, `pgdog`, and `pg_kinetic`.

Supported drivers include `pgbench`, `psql`, `tokio_postgres`, `pgx`, `node_pg`, and `psycopg`.

## Validate And Run

Validate a scenario before you run it:

```powershell
pg-kinetic benchmark validate --scenario bench\scenarios\benchmark-basic.toml
```

Run a scenario to produce the result payload:

```powershell
pg-kinetic benchmark run --scenario bench\scenarios\benchmark-basic.toml --format json
```

The JSON output reports the scenario metadata plus per-target metrics for latency, throughput, error rate, CPU label, and memory label.

## Reading The Output

Use the output to compare the proxy against the baselines in the same scenario:

- `p50_ms` shows the typical request path
- `p95_ms` and `p99_ms` show the tail
- `throughput_qps` shows the achieved rate
- `error_rate` should stay at or near zero for a healthy scenario

The `comparison` field keeps the results aligned to the intended baseline.

## How Later Work Should Use It

- Keep the scenario file in version control.
- Reuse the same driver and comparison labels when you want a before-and-after read.
- Feed the benchmark output into adaptive-ops decisions and performance investigations.
- Record the scenario name in the follow-up work so the measurements are easy to trace.

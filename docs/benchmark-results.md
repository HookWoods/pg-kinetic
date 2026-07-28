---
title: "Benchmark Results"
description: "Capacity-matched Linux benchmark charts for pg-kinetic compared with direct PostgreSQL, PgBouncer, and PgDog."
keywords:
  - pg-kinetic benchmark results
  - PostgreSQL proxy benchmark graph
  - PgBouncer benchmark
  - PgDog benchmark
  - Postgres pooler performance
---

# Benchmark Results

For evaluators who want a quick performance picture before reading the full benchmarking workflow.

These charts use reviewed live Linux VM runs. They are not universal
performance claims. They are reproducible snapshots from isolated environments,
with five 30-second rounds per target and one PostgreSQL instance per target.

The latest result on this page is the July 24, 2026 `io_uring` simple-query
run.

The comparison uses capacity-matched benchmark settings: each PostgreSQL
backend accepts up to 512 connections, pg-kinetic uses
`PG_KINETIC_MAX_BACKENDS=512` and `PG_KINETIC_POOL_MAX_SIZE=512`, and PgBouncer
and PgDog use `default_pool_size=512`. Detailed phase timing and debug trace
sampling were disabled for the run.

## Latest io_uring Simple Query

Higher throughput is better. Lower latency is better.

```mermaid
xychart-beta
  title "Simple query throughput at c=256, qps"
  x-axis ["direct", "PgBouncer", "PgDog", "thread/core", "io_uring"]
  y-axis "queries per second" 0 --> 120000
  bar [111780, 28098, 24788, 57807, 65327]
```

| Target | c=64 median TPS | c=64 median ms | c=256 median TPS | c=256 median ms |
| --- | ---: | ---: | ---: | ---: |
| direct PostgreSQL | 105,358.8 | 0.607 | 111,780.2 | 2.290 |
| PgBouncer | 31,614.1 | 2.024 | 28,097.9 | 9.111 |
| PgDog | 25,787.2 | 2.482 | 24,787.7 | 10.328 |
| pg-kinetic `thread_per_core` | 54,259.8 | 1.180 | 57,807.4 | 4.428 |
| pg-kinetic `io_uring` | 59,969.5 | 1.067 | 65,327.0 | 3.919 |

In this run, `io_uring` is about 10.5% faster than `thread_per_core` at c=64
and about 13.0% faster at c=256. It is about 132.5% faster than PgBouncer and
about 163.6% faster than PgDog at c=256. Direct PostgreSQL remains the
throughput ceiling.

## Prior thread_per_core Simple Query

Higher is better.

```mermaid
xychart-beta
  title "Simple query throughput at c=256, qps"
  x-axis ["direct", "PgBouncer", "PgDog", "pg-kinetic"]
  y-axis "queries per second" 0 --> 50000
  bar [49748, 23475, 19886, 38099]
```

| Target | c=64 TPS | c=64 avg ms | c=256 TPS | c=256 avg ms |
| --- | ---: | ---: | ---: | ---: |
| direct PostgreSQL | 54,523.9 | 1.174 | 49,748.0 | 5.146 |
| PgBouncer | 23,698.4 | 2.701 | 23,475.2 | 10.905 |
| PgDog | 21,111.7 | 3.031 | 19,886.1 | 12.873 |
| pg-kinetic | 32,808.2 | 1.951 | 38,099.2 | 6.719 |

## Prior thread_per_core Prepared Statement

Higher is better.

```mermaid
xychart-beta
  title "Prepared statement reuse throughput at c=256, qps"
  x-axis ["direct", "PgBouncer", "PgDog", "pg-kinetic"]
  y-axis "queries per second" 0 --> 70000
  bar [65226, 22059, 20354, 50573]
```

| Target | c=64 TPS | c=64 avg ms | c=256 TPS | c=256 avg ms |
| --- | ---: | ---: | ---: | ---: |
| direct PostgreSQL | 68,834.3 | 0.930 | 65,225.8 | 3.925 |
| PgBouncer | 22,328.8 | 2.866 | 22,058.8 | 11.605 |
| PgDog | 20,957.4 | 3.054 | 20,353.6 | 12.578 |
| pg-kinetic | 40,984.7 | 1.562 | 50,572.9 | 5.062 |

## Prior thread_per_core Tail Latency

Lower is better.

| Workload | Target | c=256 p95 ms | c=256 p99 ms |
| --- | --- | ---: | ---: |
| Simple query | direct PostgreSQL | 10.206 | 13.597 |
| Simple query | PgBouncer | 14.843 | 18.072 |
| Simple query | PgDog | 17.846 | 20.687 |
| Simple query | pg-kinetic | 12.613 | 17.308 |
| Prepared statement reuse | direct PostgreSQL | 7.203 | 9.321 |
| Prepared statement reuse | PgBouncer | 14.565 | 16.728 |
| Prepared statement reuse | PgDog | 17.249 | 19.827 |
| Prepared statement reuse | pg-kinetic | 10.007 | 13.833 |

## How To Read This

Direct PostgreSQL is the ceiling for proxy overhead, not a drop-in comparison for connection-storm behavior. It does not provide the proxy boundary, route-aware backpressure, admin views, or pooling behavior being evaluated.

PgBouncer and PgDog are included as directional comparison targets because the benchmark stack starts one isolated PostgreSQL backend per target. These numbers do not claim broad feature parity or global superiority.

In the July 24 `io_uring` snapshot, pg-kinetic is the fastest pooler target for
the simple-query workload at both c=64 and c=256. In the July 23
`thread_per_core` snapshot, pg-kinetic is the fastest pooler target for both
simple and prepared read-only workloads. Direct PostgreSQL remains the
throughput ceiling in both snapshots.

Transaction-pool write-heavy results are intentionally excluded from the
headline table. The current TPC-B style write workload is dominated by
PostgreSQL commit and fsync behavior, so it is not a clean proxy-overhead
comparison.

## Prior thread_per_core Commands

The July 23 `thread_per_core` run used the compose benchmark stack with the
comparison profile:

```bash
export PGPASSWORD=postgres
export PG_KINETIC_RUNTIME_ENGINE=thread_per_core
export PG_KINETIC_PHASE_TIMING_SAMPLE_RATE=0.0
export PG_KINETIC_DEBUG_TRACE_SAMPLING_RATE=0.0
export PG_KINETIC_MAX_BACKENDS=512
export PG_KINETIC_POOL_MAX_SIZE=512

sudo -E docker compose -f bench/compose.yml --profile comparison up -d --wait --build
```

For the Linux `io_uring` benchmark target, build the benchmark image with the
`io-uring` cargo feature and opt in to the runtime explicitly:

```bash
export PG_KINETIC_BENCH_FEATURES=io-uring
export PG_KINETIC_RUNTIME_ENGINE=io_uring
export PG_KINETIC_EXPERIMENTAL_RUNTIME_ENABLED=true
export PG_KINETIC_PHASE_TIMING_SAMPLE_RATE=0.0

sudo -E docker compose -f bench/compose.yml up -d --wait --build pg-kinetic driver
```

The benchmark compose service runs pg-kinetic with `seccomp=unconfined` because
Docker's default seccomp profile blocks the `io_uring_setup` syscall used by
monoio.

Each isolated PostgreSQL backend was initialized before measurement:

```bash
sudo docker compose -f bench/compose.yml exec -T -e PGPASSWORD=postgres driver \
  pgbench -i -s 10 -h pg-direct -p 5432 -U postgres pgkinetic

sudo docker compose -f bench/compose.yml exec -T -e PGPASSWORD=postgres driver \
  pgbench -i -s 10 -h pg-bouncer-db -p 5432 -U postgres pgkinetic

sudo docker compose -f bench/compose.yml exec -T -e PGPASSWORD=postgres driver \
  pgbench -i -s 10 -h pg-dog-db -p 5432 -U postgres pgkinetic

sudo docker compose -f bench/compose.yml exec -T -e PGPASSWORD=postgres driver \
  pgbench -i -s 10 -h pg-kinetic-db -p 5432 -U postgres pgkinetic
```

The simple-query workload was run for each target, each concurrency, and each
of five interleaved rounds:

```bash
pgbench -h 127.0.0.1 -p <port> -U postgres \
  -c <64-or-256> -j 8 -T 30 --log --log-prefix <outside-git-path> \
  -n -S pgkinetic
```

The prepared-statement workload used the same matrix with prepared query mode:

```bash
pgbench -h 127.0.0.1 -p <port> -U postgres \
  -c <64-or-256> -j 8 -T 30 --log --log-prefix <outside-git-path> \
  -n -M prepared -S pgkinetic
```

The target ports were:

| Target | Port |
| --- | ---: |
| direct PostgreSQL | 55432 |
| PgBouncer | 56432 |
| PgDog | 57432 |
| pg-kinetic | 58432 |

The stack was stopped after collection:

```bash
sudo docker compose -f bench/compose.yml --profile comparison down --volumes --remove-orphans
```

## Reproduce Or Update

The checked-in baseline reports used by the regression score gate are:

- `bench/baselines/simple-query.json`
- `bench/baselines/transaction-pool.json`
- `bench/baselines/prepared.json`

The July 23, 2026 capacity-matched VM run and the July 24, 2026 `io_uring` run
were collected as raw benchmark output outside Git. Read
[Benchmarking](./benchmarking.md) before updating these numbers. Do not replace
checked-in baselines with dry-run output or a single local measurement.

No reviewed `io_uring` flamegraph is currently published with these results.
When profiling a new `io_uring` run, retain the raw `perf`, folded-stack, and
flamegraph artifacts outside Git and summarize the top costs beside the
throughput and tail-latency tables.

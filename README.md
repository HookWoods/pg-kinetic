# pg-kinetic

pg-kinetic is a low-overhead PostgreSQL wire proxy for high-concurrency applications.

The first milestone focuses on:

- PostgreSQL startup and message forwarding
- a typed wire protocol parser
- transaction state tracking
- reproducible benchmarks against direct PostgreSQL, PgBouncer, and PgDog

## Local Benchmark Stack

Start the local stack:

```bash
docker compose -f bench/compose.yml up -d --build postgres pgbouncer pgdog pg-kinetic
```

Run smoke checks:

```bash
PGPASSWORD=postgres psql -h 127.0.0.1 -p 55432 -U postgres -d pgkinetic -c "select count(*) from accounts;"
PGPASSWORD=postgres psql -h 127.0.0.1 -p 58432 -U postgres -d pgkinetic -c "select count(*) from accounts;"
```

Run simple pgbench baselines:

```bash
PGPASSWORD=postgres pgbench -h 127.0.0.1 -p 55432 -U postgres -d pgkinetic -n -f bench/pgbench/basic.sql -c 32 -j 4 -T 30
PGPASSWORD=postgres pgbench -h 127.0.0.1 -p 56432 -U postgres -d pgkinetic -n -f bench/pgbench/basic.sql -c 32 -j 4 -T 30
PGPASSWORD=postgres pgbench -h 127.0.0.1 -p 57432 -U postgres -d pgkinetic -n -f bench/pgbench/basic.sql -c 32 -j 4 -T 30
PGPASSWORD=postgres pgbench -h 127.0.0.1 -p 58432 -U postgres -d pgkinetic -n -f bench/pgbench/basic.sql -c 32 -j 4 -T 30
```

Ports:

- `55432`: direct PostgreSQL
- `56432`: PgBouncer
- `57432`: PgDog
- `58432`: pg-kinetic

## Driver Compatibility Smoke Tests

Start the local stack before running compatibility checks:

```bash
docker compose -f bench/compose.yml up -d --build postgres pg-kinetic
```

Windows:

```powershell
powershell.exe -ExecutionPolicy Bypass -File scripts\smoke\compat.ps1
```

Unix:

```bash
chmod +x scripts/smoke/compat.sh
scripts/smoke/compat.sh
```

The smoke clients exercise prepared queries through:

- Rust `tokio-postgres`
- Go `pgx`
- Node.js `pg`
- Python `psycopg`

## Virtual Sessions

pg-kinetic tracks lightweight PostgreSQL session state so transaction pooling remains safe.

The proxy returns a backend to the pool when the session is idle and replayable. It keeps, recovers, drains, resets, or discards a backend when the client uses stateful PostgreSQL features or disconnects before a backend is ready for reuse.

Pinned backend reasons:

- open transaction
- failed transaction
- unsafe session state
- temporary table
- advisory lock
- `COPY`
- `LISTEN/NOTIFY`
- unknown protocol state

Replayable settings:

- `application_name`
- `timezone`
- `datestyle`
- `search_path`
- `extra_float_digits`

`DISCARD ALL` clears tracked virtual session state. `DISCARD TEMP` clears temporary table pinning. Unknown or unsafe session state is handled conservatively.

Recovery modes:

- `recover`: roll back abandoned transactions and drain abandoned responses when possible.
- `rollback_only`: roll back abandoned transactions but discard backends abandoned mid-response.
- `drop`: discard backends on recovery triggers.

Recovery is bounded by `recovery_timeout_ms`. If recovery times out or the protocol state remains uncertain, pg-kinetic discards the backend rather than returning it to the pool.

## Backpressure And QoS

pg-kinetic applies queueing and timeout limits per route key. A route key groups traffic by database, user, application name, client address, and query class so one noisy path does not starve the rest of the pool.

Backpressure is enforced with both route and global limits:

- `max_route_in_flight` caps concurrent checkouts for a single route key.
- `max_route_waiters` caps queued waiters for a single route key.
- the global gate prevents the whole proxy from overcommitting even when several routes are active at once.

When a route is saturated, pg-kinetic returns a PostgreSQL overload error instead of hanging indefinitely. The SQLSTATE is configured by `overload_error_code`, and the default is `53300` (`too many connections`).

Timeouts and budgets:

- `query_timeout_ms` bounds the time spent on a query cycle once a backend is assigned.
- `idle_client_timeout_ms` bounds how long an idle client connection may sit without activity.
- `idle_transaction_timeout_ms` bounds pinned sessions that remain in a transaction.
- `max_client_buffer_bytes` caps client-side buffering.
- `max_backend_buffer_bytes` caps backend response buffering.

If a timeout or buffer limit is hit, pg-kinetic tries to recover the backend when the protocol state is still safe. If recovery cannot prove the session is reusable, the backend is discarded instead of being returned to the pool.

The relevant QoS metrics are:

- `pg_kinetic_backpressure_events_total`
- `pg_kinetic_route_checkout_wait_ms`
- `pg_kinetic_route_in_flight`
- `pg_kinetic_route_waiting`
- `pg_kinetic_timeout_total`
- `pg_kinetic_buffer_limit_total`

Metrics:

- `pg_kinetic_backend_pin_total`
- `pg_kinetic_backend_cleanup_total`
- `pg_kinetic_backend_recovery_total`
- `pg_kinetic_backend_sqlstate_total`

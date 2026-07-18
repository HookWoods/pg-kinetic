# pg-kinetic

pg-kinetic is a low-overhead PostgreSQL wire proxy for high-concurrency applications.

The first milestone focuses on:

- PostgreSQL startup and message forwarding
- a typed wire protocol parser
- transaction state tracking
- reproducible benchmarks against direct PostgreSQL, PgBouncer, and PgDog

## Public Docs

- [Documentation site](docs-site/README.md)
- [Testing guide](docs/testing.md)
- [Regression workflow](docs/regression.md)
- [Admin reference](docs/admin.md)
- [Production runtime guide](docs/production-runtime.md)
- [Mirroring guide](docs/mirroring.md)
- [Adaptive operations guide](docs/adaptive-ops.md)
- [Benchmarking guide](docs/benchmarking.md)
- [Metrics catalog](docs/metrics.md)
- [Policy guide](docs/policy.md)
- [Read routing guide](docs/read-routing.md)
- [Sharding guide](docs/sharding.md)

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

Validate the local performance gate without running load:

```powershell
powershell.exe -ExecutionPolicy Bypass -File scripts\smoke\performance.ps1
```

For target setup, report comparison budgets, profile hooks, and admin/metrics inspection, see the [Benchmarking guide](docs/benchmarking.md).

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
powershell.exe -ExecutionPolicy Bypass -File scripts\smoke\runtime.ps1
powershell.exe -ExecutionPolicy Bypass -File scripts\smoke\mirroring.ps1
powershell.exe -ExecutionPolicy Bypass -File scripts\smoke\read-routing.ps1
powershell.exe -ExecutionPolicy Bypass -File scripts\smoke\sharding.ps1
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

## Sharding

Sharding adds route maps, shard lifecycle tracking, and conservative shard-key extraction on top of the existing routing and backpressure model.

Useful checks:

```powershell
powershell.exe -ExecutionPolicy Bypass -File scripts\smoke\sharding.ps1
```

For manual inspection:

```bash
cargo run -p pg-kinetic -- route-preview --config path/to/sharding.toml --database billing --user reporter --sql "select * from public.orders where tenant_id = 'tenant-a'"
```

## Read Routing

pg-kinetic can route safe reads to replicas while keeping conservative fallbacks for anything ambiguous or stale.

- Read-only statements and read-only transactions are the best fit for replica routing.
- `BEGIN READ ONLY` and `SET TRANSACTION READ ONLY` strengthen the read signal inside a transaction.
- `/* pg-kinetic: replica */`, `/* pg-kinetic: primary */`, `/* pg-kinetic: stale-ok */`, and `/* pg-kinetic: strict-fresh */` provide explicit per-statement hints.
- Read-after-write protection uses session write LSN tracking, replica lag checks, and the configured timeout before falling back.
- `SHOW CLIENTS`, `SHOW SERVERS`, and `SHOW ROUTES` expose the live routing picture for operators.

See [docs/read-routing.md](docs/read-routing.md) for the full rollout and policy guide.

## Production Security And Operations

### TLS

Client TLS modes:

- `disable`: accept plaintext startup only.
- `allow`: accept either plaintext startup or PostgreSQL `SSLRequest`.
- `require`: reject plaintext startup and require TLS.
- `verify_client`: require TLS and verify the client certificate chain.

Backend TLS modes:

- `disable`: connect to PostgreSQL without TLS.
- `prefer`: try TLS first and fall back to plaintext if the backend refuses TLS.
- `require`: fail closed if the backend refuses TLS.
- `verify_ca`: require TLS and verify the backend certificate chain.
- `verify_full`: require TLS, verify the backend CA, and match `backend_server_name`.

`verify_client` needs `client_cert_path`, `client_key_path`, and `client_ca_path`. Backend verification modes need `backend_ca_path`, and `verify_full` also needs `backend_server_name`.

### Authentication

`auth_mode` controls the client auth path:

- `pass_through`: preserve the backend's normal authentication flow.
- `trust`: authenticate locally with the configured user store.
- `scram_sha_256`: run local SCRAM-SHA-256 authentication before backend checkout.

`auth_failure_message_mode` controls whether failures stay generic or include the user and reason. `backend_user` and `backend_password_env_var_name` are used when the proxy needs its own backend credentials.

The local user store file accepts one entry per line:

```text
# comments and blank lines are ignored
alice=trust
bob=SCRAM-SHA-256$4096:base64salt:base64storedkey:base64serverkey
```

Usernames are case-sensitive by default.

### Reload And Drain

Set `config_file` to load a TOML config, and enable `reload_enabled` to keep checking it every `config_reload_interval_ms`.

Safe reloads apply QoS, timeout, socket, TLS certificate material, and user-store updates. Listener addresses, backend addresses, and auth mode changes are rejected and leave the active config in place.

Routing policy and shard override behavior are documented in [docs/policy.md](docs/policy.md), alongside the reload and audit expectations for rule changes.

Shutdown uses graceful drain: the proxy stops accepting new clients, lets active clients finish within `drain_timeout_ms`, and then closes out any remaining work.

### Health

Bind `health_addr` to expose:

- `GET /healthz`
- `GET /readyz`
- `GET /state`

`/readyz` returns `503` while draining or when backend readiness is failing. `/state` only exposes non-secret process and backend state.

### Socket Tuning

`tcp_nodelay` is on by default. Optional socket tuning includes keepalive, keepalive idle/interval/retries, `TCP_USER_TIMEOUT`, send and receive buffer sizes, and `strict_socket_option_mode` for fail-closed startup on unsupported options.

### Production Metrics

The production metric set now includes:

- `pg_kinetic_tls_handshakes_total`
- `pg_kinetic_tls_failures_total`
- `pg_kinetic_auth_attempts_total`
- `pg_kinetic_auth_failures_total`
- `pg_kinetic_config_reload_total`
- `pg_kinetic_drain_state`
- `pg_kinetic_health_status`
- `pg_kinetic_socket_option_total`

The existing QoS and backend metrics remain available alongside them.

### Smoke Commands

Useful production checks:

```bash
cargo test --workspace
cargo test -p pg-kinetic --test tls_smoke
cargo test -p pg-kinetic --test auth_smoke
cargo test -p pg-kinetic --test reload_config
cargo test -p pg-kinetic --test graceful_drain
cargo test -p pg-kinetic --test health_endpoints
cargo test -p pg-kinetic --test socket_options
```

Runtime and mirroring smoke checks:

```powershell
powershell.exe -ExecutionPolicy Bypass -File scripts\smoke\runtime.ps1
powershell.exe -ExecutionPolicy Bypass -File scripts\smoke\mirroring.ps1
```

For a local stack:

```bash
docker compose -f bench/compose.yml up -d --build postgres pgbouncer pgdog pg-kinetic
PGPASSWORD=postgres psql -h 127.0.0.1 -p 58432 -U postgres -d pgkinetic -c "select count(*) from accounts;"
powershell.exe -ExecutionPolicy Bypass -File scripts\smoke\psql.ps1 -Port 58432
powershell.exe -ExecutionPolicy Bypass -File scripts\smoke\compat.ps1
powershell.exe -ExecutionPolicy Bypass -File scripts\smoke\read-routing.ps1
```

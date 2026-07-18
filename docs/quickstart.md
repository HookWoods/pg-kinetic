---
title: "Quickstart"
description: "Run pg-kinetic locally with Docker Compose, PostgreSQL, health checks, admin views, metrics, and a verified psql query path."
keywords:
  - pg-kinetic quickstart
  - Docker Compose PostgreSQL proxy
  - Postgres proxy quickstart
  - psql
---

# Quickstart

This quickstart uses a local Docker image built from this checkout. It does not require a published release image.

## Prerequisites

- Docker
- `psql`

The example starts PostgreSQL and pg-kinetic with Docker Compose. It uses a numeric backend address because `backend_addr` is parsed as a socket address, not a hostname.

## Review The Config

The repository includes `deploy/pg-kinetic.toml`:

```toml
[connection]
listen_addr = "0.0.0.0:6432"
backend_addr = "172.30.0.10:5432"

[capacity]
max_clients = 1000
max_backends = 64
max_checkout_waiters = 512

[performance]
checkout_timeout_ms = 500
recovery_mode = "recover"
recovery_timeout_ms = 5000
backend_reset_query = "DISCARD ALL"

[qos]
max_route_in_flight = 256
max_route_waiters = 512
query_timeout_ms = 30000
idle_client_timeout_ms = 300000
idle_transaction_timeout_ms = 60000
max_client_buffer_bytes = 1048576
max_backend_buffer_bytes = 4194304
overload_error_code = "53300"

[admin]
admin_addr = "0.0.0.0:7000"
admin_require_tls = false
admin_query_timeout_ms = 2000
admin_max_clients = 16

[observability]
metrics_addr = "0.0.0.0:9090"
debug_trace_sampling_rate = 0.0
otel_enabled = false
otel_service_name = "pg-kinetic"

[health]
health_addr = "0.0.0.0:9091"
readiness_backend_check_interval_ms = 1000
readiness_timeout_ms = 5000

[drain]
drain_timeout_ms = 45000
reject_new_clients_during_drain = true
```

The sample `backend_addr` points at the PostgreSQL service IP in `deploy/docker-compose.yml`.

## Start The Stack

```bash
docker compose -f deploy/docker-compose.yml up -d --build
```

Compose publishes:

| Local port | Service |
| --- | --- |
| `6432` | pg-kinetic PostgreSQL listener |
| `7000` | pg-kinetic admin listener |
| `9090` | metrics endpoint |
| `9091` | health endpoint |
| `55432` | direct PostgreSQL backend |

## Verify Health

In another terminal:

```bash
curl -fsS http://127.0.0.1:9091/healthz
curl -fsS http://127.0.0.1:9091/readyz
```

Expected outputs:

```text
live
ready
```

If `/readyz` returns `not_ready`, pg-kinetic could not connect to `backend_addr`.

## Query Through pg-kinetic

Use the PostgreSQL credentials from `deploy/docker-compose.yml`:

```bash
PGPASSWORD=pgkinetic PGSSLMODE=disable psql "postgres://pgkinetic@127.0.0.1:6432/pgkinetic" -c "select 1;"
```

Expected output includes:

```text
 ?column?
----------
        1
```

## Inspect Admin And Metrics

Admin views use the PostgreSQL protocol:

```bash
PGSSLMODE=disable psql "postgres://admin@127.0.0.1:7000/postgres" -c "SHOW POOLS;"
```

Metrics are exposed when `metrics_addr` is set:

```bash
curl -fsS http://127.0.0.1:9090/metrics
```

## Cleanup

```bash
docker compose -f deploy/docker-compose.yml down
```

## Troubleshooting

| Symptom | Check |
| --- | --- |
| `/readyz` returns `not_ready` | Verify `backend_addr` from inside the container network. |
| `psql` reports SSL negotiation problems | Set `PGSSLMODE=disable` unless client TLS is configured. |
| `backend_addr` fails config parsing | Use `IP:port`; hostnames are not accepted in this field. |
| Admin connection fails | Verify `admin_addr` is set and port `7000` is published. |
| Metrics request fails | Verify `metrics_addr` is set and port `9090` is published. |

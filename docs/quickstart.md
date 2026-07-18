# Quickstart

This quickstart runs pg-kinetic as a production-style container in front of an existing PostgreSQL server. It uses the same image and config-file flow as a real deployment.

## Create A Config File

Create `pg-kinetic.toml`:

```toml
[connection]
listen_addr = "0.0.0.0:6432"
backend_addr = "postgres.internal:5432"

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
max_backend_buffer_bytes = 1048576
overload_error_code = "53300"

[admin]
admin_addr = "0.0.0.0:7000"
admin_require_tls = false
admin_query_timeout_ms = 2000
admin_max_clients = 16

[health]
health_addr = "0.0.0.0:9091"
readiness_backend_check_interval_ms = 1000
readiness_timeout_ms = 5000

[drain]
drain_timeout_ms = 45000
reject_new_clients_during_drain = true
```

Replace `postgres.internal:5432` with the address pg-kinetic should use to reach PostgreSQL from inside the container or cluster.

## Run The Container

```bash
docker run --rm \
  --name pg-kinetic \
  -p 6432:6432 \
  -p 7000:7000 \
  -p 9090:9090 \
  -p 9091:9091 \
  -v "$PWD/pg-kinetic.toml:/etc/pg-kinetic/pg-kinetic.toml:ro" \
  hookwoods/pg-kinetic:latest \
  --config-file /etc/pg-kinetic/pg-kinetic.toml
```

Applications connect to `localhost:6432` using the PostgreSQL protocol. The admin listener is available on `localhost:7000` when configured, metrics are available on `localhost:9090` when configured, and HTTP health endpoints are available on `localhost:9091`.

## Verify The Proxy

```bash
PGSSLMODE=disable psql "postgres://app_user@127.0.0.1:6432/app_db" -c "select 1;"
```

Use the same username, database, TLS mode, and password flow your application uses. If client TLS is required, configure the client `sslmode` and certificate material to match [TLS And Authentication](./tls-and-auth.md).

## Inspect Operations

Admin views use the PostgreSQL protocol:

```bash
psql "postgres://admin@127.0.0.1:7000/postgres" -c "SHOW POOLS;"
psql "postgres://admin@127.0.0.1:7000/postgres" -c "SHOW CLIENTS;"
```

Health endpoints use HTTP:

```bash
curl -fsS http://127.0.0.1:9091/healthz
curl -fsS http://127.0.0.1:9091/readyz
```

## Next Steps

- Install with [Docker, Compose, or Kubernetes](./installation.md).
- Review every supported setting in [Configuration](./configuration.md).
- Configure [TLS and authentication](./tls-and-auth.md) before exposing pg-kinetic beyond a trusted network.
- Add readiness and drain behavior from [Health, Readiness, And Drain](./health-and-drain.md).

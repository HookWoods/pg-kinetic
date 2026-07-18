# Configuration

pg-kinetic loads configuration from CLI flags, environment variables, or a TOML file. Production deployments should use a mounted `pg-kinetic.toml` plus secret references for credentials.

## Loading Configuration

Run the proxy with:

```bash
pg-kinetic --config-file /etc/pg-kinetic/pg-kinetic.toml
```

The container entrypoint is the same binary:

```bash
docker run --rm \
  -v "$PWD/pg-kinetic.toml:/etc/pg-kinetic/pg-kinetic.toml:ro" \
  hookwoods/pg-kinetic:0.1.0 \
  --config-file /etc/pg-kinetic/pg-kinetic.toml
```

Environment variables use the `PG_KINETIC_` prefix. Secrets should be passed by reference, for example with `backend_password_env_var_name = "PG_KINETIC_BACKEND_PASSWORD"`.

## Complete Production Template

This template shows every production config section accepted by the main proxy config. Remove sections you do not use, and replace addresses, TLS paths, limits, and credentials for your environment.

```toml
[connection]
listen_addr = "0.0.0.0:6432"
backend_addr = "postgres.internal:5432"

[[routes]]
[routes.primary]
address = "postgres-primary.internal:5432"
connect_timeout_ms = 1000
tls_mode = "require"

[[routes.replicas]]
address = "postgres-replica-a.internal:5432"
connect_timeout_ms = 1000
tls_mode = "require"
weight = 1

[routes.read_routing]
read_routing_mode = "prefer_replica"
fallback_policy = "primary"

[routes.freshness]
freshness_policy = "session_write_lsn_and_max_lag"
max_replica_lag_ms = 1000
read_after_write_timeout_ms = 500

[routes.ha]
replica_health_interval_ms = 1000
replica_health_timeout_ms = 500

[runtime.lifecycle]
startup_grace_ms = 30000
shutdown_grace_ms = 30000
readiness_fail_during_drain = true
pre_stop_drain_enabled = true
pre_stop_drain_endpoint = "/drain"
startup_backend_checks_enabled = true
termination_grace_period_seconds = 65

[runtime.node]
node_id = "pg-kinetic-a"

[runtime.engine]
runtime_engine = "tokio_default"
experimental_runtime_enabled = false

[runtime.production]
control_plane_enabled = false
mirroring_enabled = false
adaptive_enabled = false

[runtime.production.adaptive]
adaptive_mode = "recommend"
adaptive_window_ms = 60000
adaptive_min_confidence = 0.8
adaptive_apply_enabled = false
adaptive_apply_allowlist = []
adaptive_max_change_percent = 10

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
admin_require_tls = true
admin_allowed_user = "pg_kinetic_admin"
admin_query_timeout_ms = 2000
admin_max_clients = 16

[observability]
metrics_addr = "0.0.0.0:9090"
debug_trace_sampling_rate = 0.0
otel_enabled = false
otel_endpoint = "http://otel-collector.observability.svc.cluster.local:4318"
otel_service_name = "pg-kinetic"

[tls]
client_tls_mode = "require"
client_cert_path = "/etc/pg-kinetic/certs/client.crt"
client_key_path = "/etc/pg-kinetic/certs/client.key"
client_ca_path = "/etc/pg-kinetic/certs/client-ca.crt"
backend_tls_mode = "verify_full"
backend_ca_path = "/etc/pg-kinetic/certs/backend-ca.crt"
backend_server_name = "postgres.internal"

[auth]
auth_mode = "pass_through"
auth_users_file = "/etc/pg-kinetic/auth/users.txt"
backend_user = "pg_kinetic_proxy"
backend_password_env_var_name = "PG_KINETIC_BACKEND_PASSWORD"
auth_failure_message_mode = "generic"

[reload]
config_file = "/etc/pg-kinetic/pg-kinetic.toml"
config_reload_interval_ms = 5000
reload_enabled = true

[drain]
drain_timeout_ms = 45000
reject_new_clients_during_drain = true

[health]
health_addr = "0.0.0.0:9091"
readiness_backend_check_interval_ms = 1000
readiness_timeout_ms = 5000

[socket]
tcp_nodelay = true
tcp_keepalive = true
tcp_keepalive_idle_ms = 30000
tcp_keepalive_interval_ms = 10000
tcp_keepalive_retries = 3
tcp_user_timeout_ms = 30000
tcp_send_buffer_bytes = 1048576
tcp_recv_buffer_bytes = 1048576
strict_socket_option_mode = false
```

## Section Reference

| Section | Purpose |
| --- | --- |
| `connection` | Client listener and fallback backend address. |
| `routes` | Explicit primary, replicas, read-routing policy, freshness policy, and replica health checks. |
| `runtime.lifecycle` | Startup, shutdown, readiness, pre-stop drain, and termination timing. |
| `runtime.node` | Stable node identity for runtime snapshots and operations. |
| `runtime.engine` | Runtime engine selection. Experimental engines require an explicit gate. |
| `runtime.production` | Feature gates for production control-plane, mirroring, and adaptive behavior. |
| `runtime.production.adaptive` | Recommendation/apply mode and guardrails for adaptive tuning. |
| `capacity` | Client count, backend pool size, and checkout waiter budget. |
| `performance` | Checkout timeout, backend recovery mode, recovery timeout, and reset query. |
| `qos` | Route concurrency, waiters, query/idle timeouts, buffer caps, and overload SQLSTATE. |
| `admin` | PostgreSQL-compatible admin listener. |
| `observability` | Metrics listener, trace sampling, and OpenTelemetry export settings. |
| `tls` | Client and backend TLS mode and certificate paths. |
| `auth` | Client authentication mode, local user file, and backend credential source. |
| `reload` | Config file path and reload loop. |
| `drain` | Graceful drain timeout and new-client behavior. |
| `health` | HTTP health/readiness address and backend readiness polling. |
| `socket` | TCP_NODELAY, keepalive, user timeout, buffer sizes, and strict socket option behavior. |

## Important Defaults

| Setting | Default |
| --- | --- |
| `connection.listen_addr` | `127.0.0.1:6543` |
| `connection.backend_addr` | `127.0.0.1:5432` |
| `capacity.max_clients` | `10000` |
| `capacity.max_backends` | `100` |
| `performance.checkout_timeout_ms` | `1000` |
| `performance.recovery_mode` | `recover` |
| `qos.overload_error_code` | `53300` |
| `tls.client_tls_mode` | `disable` |
| `tls.backend_tls_mode` | `disable` |
| `auth.auth_mode` | `pass_through` |
| `socket.tcp_nodelay` | `true` |

## Mode Values

| Setting | Values |
| --- | --- |
| `tls.client_tls_mode` | `disable`, `allow`, `require`, `verify_client` |
| `tls.backend_tls_mode` | `disable`, `prefer`, `require`, `verify_ca`, `verify_full` |
| `auth.auth_mode` | `pass_through`, `trust`, `scram_sha_256` |
| `auth.auth_failure_message_mode` | `generic`, `detailed` |
| `runtime.engine.runtime_engine` | `tokio_default`, `tokio_current_thread`, `experimental_thread_per_core`, `experimental_io_uring` |
| `performance.recovery_mode` | `recover`, `rollback_only`, `drop` |
| `routes.read_routing.read_routing_mode` | `off`, `prefer_replica`, `require_replica`, `primary_only` |
| `routes.read_routing.fallback_policy` | `primary`, `reject`, `wait` |
| `routes.freshness.freshness_policy` | `none`, `session_write_lsn`, `max_replica_lag`, `session_write_lsn_and_max_lag` |
| `runtime.production.adaptive.adaptive_mode` | `recommend`, `apply` |

## Validate Before Rollout

Run preflight against the final mounted config:

```bash
pg-kinetic preflight --config /etc/pg-kinetic/pg-kinetic.toml
```

Container:

```bash
docker run --rm \
  -v "$PWD/pg-kinetic.toml:/etc/pg-kinetic/pg-kinetic.toml:ro" \
  hookwoods/pg-kinetic:0.1.0 \
  preflight --config /etc/pg-kinetic/pg-kinetic.toml
```

Preflight should run in CI before updating Kubernetes ConfigMaps or Docker Compose mounts.

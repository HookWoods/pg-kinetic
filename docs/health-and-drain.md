# Health, Readiness, And Drain

Health endpoints let schedulers decide when pg-kinetic is live, ready, or draining. Drain behavior lets rolling restarts stop new work without abruptly cutting active clients.

## Endpoints

Bind `health_addr` to enable the HTTP health listener:

```toml
[health]
health_addr = "0.0.0.0:9091"
readiness_backend_check_interval_ms = 1000
readiness_timeout_ms = 5000
```

Supported endpoints:

| Endpoint | Purpose |
| --- | --- |
| `GET /healthz` | Process liveness. |
| `GET /readyz` | Readiness for new traffic. |
| `GET /state` | Non-secret runtime and backend state. |
| `POST /drain` | Enter drain mode when pre-stop drain is enabled. |

`/readyz` returns unavailable while draining or when backend readiness checks fail.

## Drain Settings

```toml
[runtime.lifecycle]
readiness_fail_during_drain = true
pre_stop_drain_enabled = true
pre_stop_drain_endpoint = "/drain"
termination_grace_period_seconds = 65

[drain]
drain_timeout_ms = 45000
reject_new_clients_during_drain = true
```

During drain, pg-kinetic stops accepting new clients when configured to do so, waits for active work to finish until the drain timeout, then closes remaining connections.

## Kubernetes Shape

Use readiness probes against `/readyz`, liveness probes against `/healthz`, and a pre-stop hook that calls `/drain`. The pod termination grace period should be longer than `drain_timeout_ms` so the process has time to finish active sessions.

See [Kubernetes Deployment](./kubernetes.md) for deployment details.

## Operational Signals

Watch:

- `pg_kinetic_drain_state`
- `pg_kinetic_health_status`
- `pg_kinetic_client_connections`
- `pg_kinetic_pool_checkout_wait_ms`
- `pg_kinetic_timeout_total`

If readiness fails outside planned drain windows, inspect backend reachability and route health before restarting all pods.


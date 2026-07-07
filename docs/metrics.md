# Metrics

pg-kinetic exports a low-cardinality Prometheus catalog. Metric families are named once and labeled with bounded enums or route identity only.

## Connection And Prepared Cache

| Metric | Type | Labels | Unit | Cardinality notes | Example interpretation |
| --- | --- | --- | --- | --- | --- |
| `pg_kinetic_client_connections_total` | counter | none | `1` | Single series. | Baseline connection rate; spikes usually mean reconnect storms or probe traffic. |
| `pg_kinetic_prepared_events_total` | counter | `event` | `1` | Bounded by prepared lifecycle events (`parse`, `bind`, `materialize`, `close`, `invalidate`). | Compare `materialize` and `invalidate` to spot prepared cache churn. |

## Queueing And Backpressure

| Metric | Type | Labels | Unit | Cardinality notes | Example interpretation |
| --- | --- | --- | --- | --- | --- |
| `pg_kinetic_pool_checkout_wait_ms` | histogram | `outcome` | `ms` | Bounded by checkout outcome. | p95 should stay below the checkout timeout; `timeout` and `canceled` shares reveal a starved pool. |
| `pg_kinetic_backpressure_events_total` | counter | `route`, `outcome` | `1` | Route labels come from database, user, application name, and query class only. | Rising `rejected` or `timeout` shares point to overload on a specific route. |
| `pg_kinetic_route_checkout_wait_ms` | histogram | `route`, `outcome` | `ms` | Same route bound as the backpressure counter. | A hot-route p95 isolates noisy neighbors before they spread. |
| `pg_kinetic_route_in_flight` | gauge | `route`, `scope` | `1` | Route bound plus a fixed `route` scope. | High in-flight with long waits means sustained saturation. |
| `pg_kinetic_route_waiting` | gauge | `route`, `scope` | `1` | Route bound plus a fixed `route` scope. | Sustained waiters mean queue pressure or too-small caps. |
| `pg_kinetic_timeout_total` | counter | `kind` | `1` | Bounded by timeout kind. | Split query, idle client, and idle transaction timeouts to tell workload pressure from hygiene issues. |
| `pg_kinetic_buffer_limit_total` | counter | `kind` | `1` | Bounded by client/backend buffer kind. | Client limits point to request size; backend limits point to response bursts. |

## Pinning And Recovery

| Metric | Type | Labels | Unit | Cardinality notes | Example interpretation |
| --- | --- | --- | --- | --- | --- |
| `pg_kinetic_backend_pin_total` | counter | `reason` | `1` | Bounded by pin reasons. | The top reason shows what keeps backends unreusable. |
| `pg_kinetic_backend_cleanup_total` | counter | `action` | `1` | Bounded by cleanup actions. | `discard` should stay rare; spikes usually mean unsafe session state. |
| `pg_kinetic_backend_recovery_total` | counter | `trigger`, `action`, `outcome` | `1` | Bounded by recovery enums. | Watch `timeout` and `discard` outcomes when recovery stops converging. |
| `pg_kinetic_backend_sqlstate_total` | counter | `sqlstate` | `1` | Bounded by normalized SQLSTATEs, not SQL text. | Top SQLSTATEs show failure classes without exposing query bodies. |

## Security And Operations

| Metric | Type | Labels | Unit | Cardinality notes | Example interpretation |
| --- | --- | --- | --- | --- | --- |
| `pg_kinetic_tls_handshakes_total` | counter | `scope`, `mode` | `1` | Bounded by TLS scope and configured mode. | Use as a rollout sanity check; client and backend scopes should both move when TLS is enabled. |
| `pg_kinetic_tls_failures_total` | counter | `scope`, `mode`, `reason` | `1` | Bounded by TLS scope, mode, and failure reason. | `verification_failed` or `io_error` usually points to trust-chain or transport problems. |
| `pg_kinetic_auth_attempts_total` | counter | `mode` | `1` | Bounded by auth mode. | Compare attempts to failures to see whether auth is healthy or unused. |
| `pg_kinetic_auth_failures_total` | counter | `mode`, `reason` | `1` | Bounded by auth mode and failure reason. | Spikes in `unknown_user` or `invalid_password` usually mean client drift or secret mismatch. |
| `pg_kinetic_config_reload_total` | counter | `outcome` | `1` | Bounded by reload outcome. | Rejected or error reloads show bad live config before users do. |
| `pg_kinetic_drain_state` | gauge | `state` | `1` | One active state is `1.0`; the others are `0.0`. | Keep `draining` and `drained` short-lived during deploys or shutdown. |
| `pg_kinetic_health_status` | gauge | `kind`, `status` | `1` | Bounded by health kind and status enum. | `ready=0` or `backend=0` means the proxy should stop taking traffic. |
| `pg_kinetic_socket_option_total` | counter | `socket`, `option`, `outcome` | `1` | Bounded by socket kind, option, and outcome. | `unsupported` and `failed` show when the OS or container runtime rejects tuning. |

## Protocol Timing

| Metric | Type | Labels | Unit | Cardinality notes | Example interpretation |
| --- | --- | --- | --- | --- | --- |
| `pg_kinetic_protocol_phase_duration_ms` | histogram | `phase`, `outcome` | `ms` | Bounded by protocol phase and outcome enums. | Rising `auth` or `backend_checkout` p95 points to startup pressure; rising `execute` or `rows` points to query-side latency. |

## Recommended Panels

- Pool checkout wait: graph p95 of `pg_kinetic_pool_checkout_wait_ms` and overlay `timeout` and `canceled` outcomes.
- Route waiting clients: plot `pg_kinetic_route_waiting` by route and add `pg_kinetic_route_in_flight` for queue depth context.
- Overload rejection rate: chart `rate(pg_kinetic_backpressure_events_total{outcome="rejected"}[5m]) / rate(pg_kinetic_backpressure_events_total[5m])`.
- Pinning reason counts: bar chart `pg_kinetic_backend_pin_total` by `reason`.
- Recovery outcomes: stacked bars for `pg_kinetic_backend_recovery_total` by `trigger`, `action`, and `outcome`.
- Prepared cache materialization/invalidation: compare `pg_kinetic_prepared_events_total{event="materialize"}` with `event="invalidate"`.
- Auth failures: break down `pg_kinetic_auth_failures_total` by `mode` and `reason`.
- TLS failures: break down `pg_kinetic_tls_failures_total` by `scope`, `mode`, and `reason`.
- Drain state: use `pg_kinetic_drain_state` as a single-value status panel.
- Protocol phase latency: graph p95 of `pg_kinetic_protocol_phase_duration_ms` by `phase` and `outcome`, especially `startup`, `auth`, `backend_checkout`, `execute`, `rows`, `reset`, and `cancel`.

## Reading The Charts

- A rising checkout wait with flat in-flight usually means the pool is full and work is waiting.
- A rising route wait with one hot route usually means noisy-neighbor pressure rather than a global capacity issue.
- A sharp rise in prepared invalidations usually means client-side churn or a workload that does not benefit from caching.
- A sustained spike in TLS or auth failures usually means a rollout or secret mismatch rather than normal traffic variation.
- A long tail in `backend_checkout` or `auth` usually points to startup-path regressions before user queries are affected.

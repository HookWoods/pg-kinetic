---
title: "Metrics"
description: "Metrics reference for pg-kinetic runtime paths, Prometheus scraping, operational interpretation, preview-only metrics, and dashboard inputs."
keywords:
  - pg-kinetic metrics
  - PostgreSQL proxy Prometheus
  - database proxy observability
  - OpenMetrics
---

# Metrics

pg-kinetic exports Prometheus metrics when `metrics_addr` is configured.

```toml
[observability]
metrics_addr = "0.0.0.0:9090"
```

Scrape URL:

```text
http://<host>:9090/metrics
```

Metric availability follows the active runtime paths. Metrics for preview-only or disabled paths can remain absent or zero.

## Current Runtime Metrics

| Metric | Type | Labels | Availability | Notes |
| --- | --- | --- | --- | --- |
| `pg_kinetic_client_connections_total` | counter | none | live proxy | Increments for accepted client connections. |
| `pg_kinetic_pool_checkout_wait_ms` | histogram | `stage`, `outcome` | live proxy | Measures checkout stages and outcomes. |
| `pg_kinetic_backpressure_events_total` | counter | `route`, `outcome` | live proxy | Records route overload, timeout, and cancellation outcomes. |
| `pg_kinetic_route_checkout_wait_ms` | histogram | `route`, `outcome` | live proxy | Measures per-route wait time. |
| `pg_kinetic_route_in_flight` | gauge | `route`, `scope` | live proxy | Shows route in-flight work. |
| `pg_kinetic_route_waiting` | gauge | `route`, `scope` | live proxy | Shows route waiters. |
| `pg_kinetic_timeout_total` | counter | `kind` | live proxy | Records query, idle-client, and idle-transaction timeouts. |
| `pg_kinetic_buffer_limit_total` | counter | `kind` | live proxy | Records client/backend buffer cap hits. |
| `pg_kinetic_route_decisions_total` | counter | `route`, `target_role`, `query_class` | live read routing | Records primary/replica routing decisions. |
| `pg_kinetic_route_fallbacks_total` | counter | `route`, `reason`, `fallback_policy` | live read routing | Records primary fallback, waits, and rejections. |
| `pg_kinetic_read_after_write_wait_ms` | histogram | `route`, `outcome` | live read routing | Measures read-after-write waits. |
| `pg_kinetic_read_after_write_rejections_total` | counter | `route`, `outcome` | live read routing | Records strict freshness rejections. |
| `pg_kinetic_replica_health` | gauge | `endpoint`, `health` | live read routing with replicas | Shows replica probe health. |
| `pg_kinetic_replica_lag_ms` | gauge | `endpoint`, `lag_state` | live read routing with replicas | Shows replica lag state. |
| `pg_kinetic_split_brain_warnings_total` | counter | `endpoint`, `target_role`, `reason` | live read routing with role probes | Nonzero values require operator review. |
| `pg_kinetic_backend_pin_total` | counter | `reason` | live proxy | Counts backend pin reasons. |
| `pg_kinetic_backend_cleanup_total` | counter | `action` | live proxy | Counts cleanup decisions. |
| `pg_kinetic_backend_recovery_total` | counter | `trigger`, `action`, `outcome` | live proxy | Counts backend recovery paths. |
| `pg_kinetic_backend_sqlstate_total` | counter | `sqlstate` | live proxy | Counts normalized SQLSTATEs. |
| `pg_kinetic_tls_handshakes_total` | counter | `scope`, `mode` | TLS enabled | Counts client/backend TLS handshakes. |
| `pg_kinetic_tls_failures_total` | counter | `scope`, `mode`, `reason` | TLS enabled | Counts TLS failures. |
| `pg_kinetic_auth_attempts_total` | counter | `mode` | auth path enabled | Counts auth attempts. |
| `pg_kinetic_auth_failures_total` | counter | `mode`, `reason` | auth path enabled | Counts auth failures. |
| `pg_kinetic_config_reload_total` | counter | `outcome` | reload enabled | `applied`, `rejected`, `unchanged`, or `error`. |
| `pg_kinetic_drain_state` | gauge | `state` | live proxy | Current drain state. |
| `pg_kinetic_health_status` | gauge | `kind`, `status` | health enabled | Health and readiness status. |
| `pg_kinetic_socket_option_total` | counter | `socket`, `option`, `outcome` | socket options configured | Socket tuning results. |
| `pg_kinetic_runtime_lifecycle_state` | gauge | `state` | live proxy | Runtime lifecycle state. |
| `pg_kinetic_runtime_readiness_state` | gauge | `state` | live proxy | Runtime readiness state. |
| `pg_kinetic_runtime_shutdown_total` | counter | `reason` | shutdown path | Shutdown reasons. |
| `pg_kinetic_protocol_phase_duration_ms` | histogram | `phase`, `outcome` | live proxy | Protocol phase timings. |

## Preview Or Tooling Metrics

| Metric | Status |
| --- | --- |
| `pg_kinetic_shard_*` | Preview-only until sharding config is wired into live traffic. |
| `pg_kinetic_route_map_*` | Preview/tooling until route-map reload is wired into live traffic. |
| `pg_kinetic_mirror_*` | Can remain zero because live proxy constructs a disabled mirror dispatcher. |
| `pg_kinetic_adaptive_*` | Recommendation/simulation only; apply counters do not mean active config was mutated. |
| `pg_kinetic_benchmark_*` | Tooling metrics from benchmark commands, not proxy traffic. |
| `pg_kinetic_performance_budget_status` | Tooling metric from report comparison, not proxy traffic. |
| `pg_kinetic_preflight_findings_total` | Tooling metric from preflight checks. |

## Alert Starters

- `/readyz` returns `503` or `pg_kinetic_health_status{kind="ready"}` is not ready.
- `pg_kinetic_backpressure_events_total{outcome="rejected"}` increases.
- `pg_kinetic_pool_checkout_wait_ms` p95 rises for `stage="checkout"`.
- `pg_kinetic_tls_failures_total` or `pg_kinetic_auth_failures_total` increases after a rollout.
- `pg_kinetic_config_reload_total{outcome="rejected"}` increases.
- `pg_kinetic_split_brain_warnings_total` is nonzero.

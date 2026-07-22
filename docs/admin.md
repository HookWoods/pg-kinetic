---
title: "Admin Endpoint"
description: "PostgreSQL-compatible admin views for pg-kinetic clients, pools, routing state, prepared statements, metrics snapshots, and runtime inspection."
keywords:
  - pg-kinetic admin
  - PostgreSQL admin endpoint
  - SHOW POOLS
  - database proxy operations
---

# Admin Endpoint

pg-kinetic exposes a separate PostgreSQL-compatible admin listener for operational reads.
Enable it by setting `admin_addr`. When that address is unset, the admin plane stays off.

For runtime behavior, see [docs/production-runtime.md](production-runtime.md). Policy, sharding, mirroring, and adaptive pages are status-aware references; some views can be empty or zero until those runtime paths record data.

## Connection Behavior

- `admin_require_tls` forces TLS on the admin socket.
- If `admin_require_tls` is enabled but client TLS support is not available, startup fails fast.
- `admin_allowed_user`, when set, requires the startup packet `user` field to match before the connection is accepted.
- The admin listener answers from in-process snapshots; `SHOW` queries do not checkout a backend.
- `admin_query_timeout_ms` bounds admin startup and query handling.
- `admin_max_clients` caps concurrent admin sessions.

## Query Support

- The admin endpoint only accepts the simple query protocol.
- Extended-protocol messages such as `Parse`, `Bind`, `Describe`, and `Execute` are rejected.
- Supported SQL is limited to `SHOW <view>`.
- One trailing semicolon is ignored.
- Matching is case-insensitive.
- Unsupported SQL returns SQLSTATE `0A000`.

## Supported Views

`route_key` values are rendered as `database/user/application_name/client_addr/query_class`.
Unset optional fields render as `<none>`.

| Command | What it shows |
| --- | --- |
| `SHOW CLIENTS` | Connected clients with id, user, database, application name, route key, state, connected duration, current target role, and required session write LSN when available. |
| `SHOW POOLS` | The global pool shape: configured backends, active backends, idle backends, and waiting clients. |
| `SHOW SERVERS` | Backend slots with backend id, route key, state, last checkout age, transaction state, endpoint role, detected role, health, lag, replay LSN, and last probe age. |
| `SHOW PREPARED` | Prepared-statement counts only: statement count and materialization count. |
| `SHOW PINNING` | Current pinning reasons, backend id, route key, and how long each pin has lasted. |
| `SHOW RECOVERY` | Recovery trigger/action/outcome counts plus the latest error text for each combination. |
| `SHOW BACKPRESSURE` | Per-route waiting, in-flight, rejected, timed-out, and canceled counts. |
| `SHOW ROUTES` | Per-route client and backend counts plus primary and replica counts, routing mode, fallback policy, freshness policy, and read-after-write timeout. |
| `SHOW ROUTE MAPS` | Route-map snapshot fields when route-map data exists. Preview-only for live traffic today. |
| `SHOW SHARDS` | Shard lifecycle snapshot fields when shard data exists. Preview-only for live traffic today. |
| `SHOW MIGRATIONS` | Migration snapshot fields when migration data exists. Preview-only for live traffic today. |
| `SHOW RUNTIME` | Runtime lifecycle state, readiness state, selected runtime engine, and process uptime. |
| `SHOW MIRRORING` | Mirror snapshot fields. Live proxy constructs a disabled mirror dispatcher today. |
| `SHOW ADAPTIVE` | Recommendation and simulated outcome fields when adaptive controller is enabled. It does not prove live config mutation. |
| `SHOW BENCHMARKS` | Scenario name, target, comparison, driver, duration, latency percentiles, throughput, error rate, CPU label, memory label, target-matrix labels, and comparison outcome. |
| `SHOW PERFORMANCE` | Regression-budget thresholds and outcomes, profile and process-metric status, process CPU and resident-memory samples, and proxy performance counters. |
| `SHOW SETTINGS` | Current runtime settings, sanitized for public display. |
| `SHOW LIMITS` | Effective capacity, timeout, and admin limits. |
| `PAUSE` | Queues new backend checkouts without killing connected clients or in-flight queries. |
| `RESUME` | Releases clients waiting behind `PAUSE`. |
| `RELOAD` | Applies the configured config file immediately when the change is reload-compatible; incompatible changes are rejected with the reason surfaced as an error. |

## Availability Rules

| View family | Availability |
| --- | --- |
| `SHOW CLIENTS`, `SHOW POOLS`, `SHOW SERVERS`, `SHOW RUNTIME`, `SHOW SETTINGS`, `SHOW LIMITS` | Available when the admin listener is enabled. Empty rows mean no matching in-process snapshot has been recorded yet. |
| `SHOW PREPARED`, `SHOW PINNING`, `SHOW RECOVERY`, `SHOW BACKPRESSURE`, `SHOW ROUTES` | Available when runtime paths have recorded their snapshots; rows can be empty on idle systems. |
| `SHOW POLICIES`, `SHOW POLICY DECISIONS`, `SHOW POLICY AUDIT` | Snapshot surfaces for policy models and audit events. They do not prove live policy enforcement unless the runtime path records such events. |
| `SHOW ROUTE MAPS`, `SHOW SHARDS`, `SHOW MIGRATIONS` | Preview/model snapshot surfaces. They are not evidence that live traffic is sharded. |
| `SHOW MIRRORING` | Returns disabled/default mirror state unless a runtime path records mirror summaries; live traffic mirroring is not active today. |
| `SHOW ADAPTIVE` | Shows recommendation/simulation state when adaptive controller records snapshots; it does not prove live config mutation. |
| `SHOW BENCHMARKS`, `SHOW PERFORMANCE` | Shows benchmark, profile, process-metric, and performance-budget snapshots when those tools have recorded data. |

## Field Dictionary

| View | Columns |
| --- | --- |
| `SHOW CLIENTS` | `client_id`, `user`, `database`, `application_name`, `route_key`, `state`, `connected_duration_ms`, `current_target_role`, `required_session_write_lsn` |
| `SHOW POOLS` | `route_key`, `max_backends`, `active_backends`, `idle_backends`, `waiting_clients`, `checkout_lock_wait_ms` |
| `SHOW SERVERS` | `backend_id`, `route_key`, `state`, `last_checkout_age_ms`, `in_transaction`, `endpoint_role`, `detected_role`, `health`, `lag_ms`, `replay_lsn`, `last_probe_age_ms` |
| `SHOW RUNTIME` | `node_id`, `lifecycle_state`, `readiness_state`, `runtime_engine`, `uptime_ms` |
| `SHOW NODES` | `role`, `node_id`, `lifecycle_state`, `readiness_state`, `health`, `route_map_generation_id`, `policy_generation_id`, `heartbeat_age_ms`, `overloaded` |
| `SHOW MIRRORING` | `mode`, `sample_rate`, `in_flight`, `dropped`, `timeout_total`, `decisions_total`, `mirrored_total`, `skipped_total`, `rejected_total` |
| `SHOW ADAPTIVE` | `mode`, `latest_recommendation`, `apply_status`, `guardrails` |
| `SHOW BENCHMARKS` | `scenario`, `target`, `comparison`, `driver`, `duration_ms`, `p50_ms`, `p95_ms`, `p99_ms`, `throughput_qps`, `error_rate`, `cpu_label`, `memory_label`, `workload`, `matrix_targets`, `comparison_outcome` |
| `SHOW PERFORMANCE` | `metric`, `warning_threshold`, `failure_threshold`, `observed_value`, `baseline_value`, `regression_outcome`, `profile_status`, `process_status`, `process_cpu_seconds`, `process_resident_memory_bytes`, `cpu_per_query`, `memory_per_client_bytes`, `protocol_buffer_copies`, `pool_checkout_lock_wait_ms`, `prepared_cache_hits`, `prepared_cache_misses`, `observability_hot_path_allocations`, `idle_clients` |
| `SHOW PREPARED` | `session_id`, `client_statement_name`, `backend_statement_name`, `materialized_backend_count`, `invalidation_count`, `prepared_cache_hits`, `prepared_cache_misses` |
| `SHOW PINNING` | `client_id`, `backend_id`, `route_key`, `reason`, `duration_ms` |
| `SHOW RECOVERY` | `trigger`, `action`, `outcome`, `count`, `last_error` |
| `SHOW BACKPRESSURE` | `route_key`, `waiting`, `in_flight`, `rejected`, `timed_out`, `canceled` |
| `SHOW POLICIES` | `policy_id`, `policy_version`, `policy_mode`, `source`, `enabled`, `last_reload_outcome`, `error_code` |
| `SHOW POLICY DECISIONS` | `policy_id`, `policy_version`, `hook_point`, `action`, `outcome`, `reason`, `route`, `shard`, `target_role`, `context` |
| `SHOW POLICY AUDIT` | `kind`, `policy_id`, `policy_version`, `hook_point`, `action`, `outcome`, `reason`, `route`, `shard`, `target_role`, `context` |
| `SHOW ROUTES` | `database`, `user`, `application_name`, `query_class`, `client_count`, `backend_count`, `primary_count`, `replica_count`, `read_routing_mode`, `fallback_policy`, `freshness_policy`, `read_after_write_timeout_ms`, `route_map_generation_id`, `sharding_enabled` |
| `SHOW ROUTE MAPS` | `scope`, `strategy`, `priority`, `multi_shard_policy` |
| `SHOW SHARDS` | `shard_id`, `route_key`, `lifecycle_state`, `primary_backend_count`, `replica_backend_count`, `health_summary` |
| `SHOW MIGRATIONS` | `migration_state`, `migration_override_explicit`, `source_shard_ids`, `target_shard_ids`, `active_client_count`, `prepared_statement_count`, `open_transaction_count`, `last_required_lsn` |
| `SHOW SETTINGS` | `listen_addr`, `backend_addr`, `client_tls_mode`, `backend_tls_mode`, `auth_mode`, `auth_failure_message_mode`, `backend_user`, `backend_reset_query`, `recovery_mode`, `reload_enabled`, `config_reload_interval_ms`, `drain_timeout_ms`, `reject_new_clients_during_drain`, `health_addr`, `readiness_backend_check_interval_ms`, `readiness_timeout_ms`, `metrics_addr`, TCP socket options |
| `SHOW LIMITS` | `max_clients`, `max_backends`, `max_checkout_waiters`, `max_route_in_flight`, `max_route_waiters`, timeout limits, buffer limits, admin limits, `overload_error_code` |

All values are point-in-time in-process snapshots. They are not transactional reads from PostgreSQL.

## Redaction Rules

- `SHOW SETTINGS` omits secret-bearing config fields such as certificate and key file paths, CA file paths, the backend password env var name, and the backend server name.
- `SHOW PREPARED` exposes counts only; it never prints statement text.
- Optional fields that are not configured are shown as `<none>` rather than an empty string.
- The admin listener does not forward startup credentials or query text to the backend in order to render `SHOW` output.

## Practical Use

- Use `SHOW POOLS` and `SHOW BACKPRESSURE` together to see whether a queue is forming because the pool is full or because a specific route is overloaded.
- Use `SHOW CLIENTS`, `SHOW SERVERS`, and `SHOW ROUTES` together to understand how read traffic is flowing and whether replicas are healthy.
- Treat `SHOW ROUTE MAPS`, `SHOW SHARDS`, `SHOW MIGRATIONS`, and `SHOW MIRRORING` as snapshot inspection surfaces, not proof that those preview features are active in live traffic.
- Use `SHOW PINNING` and `SHOW RECOVERY` together to distinguish long-lived state from recovery churn.
- Use `SHOW RUNTIME`, `SHOW ADAPTIVE`, and `SHOW BENCHMARKS` with the runtime, adaptive, and benchmarking guides to track available runtime and tooling state.
- Use `SHOW PERFORMANCE` after a report comparison to review the warning and failure thresholds, observed and baseline values, profile status, and process-metric availability.
- Use `SHOW SETTINGS` and `SHOW LIMITS` to verify the live configuration after a reload or before a support investigation.
- Use `PAUSE` before backend maintenance when new query checkouts should wait, then `RESUME` after the backend is ready. Existing in-flight queries are not canceled.
- Use `RELOAD` for compatible runtime config changes such as timeout and reloadable asset updates; listener, capacity, routing topology, TLS mode, and auth mode changes still require restart.

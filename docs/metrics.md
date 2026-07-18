# Metrics

pg-kinetic exports a low-cardinality Prometheus catalog. Metric families are named once and labeled with bounded enums or route identity only.

For policy behavior, pair the routing and sharding metrics below with [docs/policy.md](policy.md) and the admin views in [docs/admin.md](admin.md). For runtime, mirroring, adaptive control, and benchmarking, use [docs/production-runtime.md](production-runtime.md), [docs/mirroring.md](mirroring.md), [docs/adaptive-ops.md](adaptive-ops.md), and [docs/benchmarking.md](benchmarking.md).

## Connection And Prepared Cache

| Metric | Type | Labels | Unit | Cardinality notes | Example interpretation |
| --- | --- | --- | --- | --- | --- |
| `pg_kinetic_client_connections_total` | counter | none | `1` | Single series. | Baseline connection rate; spikes usually mean reconnect storms or probe traffic. |
| `pg_kinetic_prepared_events_total` | counter | `event` | `1` | Bounded by prepared lifecycle events (`parse`, `bind`, `materialize`, `close`, `invalidate`). | Compare `materialize` and `invalidate` to spot prepared cache churn. |

## Queueing And Backpressure

| Metric | Type | Labels | Unit | Cardinality notes | Example interpretation |
| --- | --- | --- | --- | --- | --- |
| `pg_kinetic_pool_checkout_wait_ms` | histogram | `stage`, `outcome` | `ms` | `stage` is fixed to `request`, `route_gate_registry` (including lock lookup), or `checkout`; `outcome` is bounded. | Compare p95 by `stage`; rising registry/lock or checkout time identifies the contended portion, while `timeout` and `canceled` shares reveal a starved pool. |
| `pg_kinetic_backpressure_events_total` | counter | `route`, `outcome` | `1` | Route labels come from database, user, application name, and query class only. | Rising `rejected` or `timeout` shares point to overload on a specific route. |
| `pg_kinetic_route_checkout_wait_ms` | histogram | `route`, `outcome` | `ms` | Same route bound as the backpressure counter. | A hot-route p95 isolates noisy neighbors before they spread. |
| `pg_kinetic_route_in_flight` | gauge | `route`, `scope` | `1` | Route bound plus a fixed `route` scope. | High in-flight with long waits means sustained saturation. |
| `pg_kinetic_route_waiting` | gauge | `route`, `scope` | `1` | Route bound plus a fixed `route` scope. | Sustained waiters mean queue pressure or too-small caps. |
| `pg_kinetic_timeout_total` | counter | `kind` | `1` | Bounded by timeout kind. | Split query, idle client, and idle transaction timeouts to tell workload pressure from hygiene issues. |
| `pg_kinetic_buffer_limit_total` | counter | `kind` | `1` | Bounded by client/backend buffer kind. | Client limits point to request size; backend limits point to response bursts. |

## Read Routing And Replica Safety

| Metric | Type | Labels | Unit | Cardinality notes | Example interpretation |
| --- | --- | --- | --- | --- | --- |
| `pg_kinetic_route_decisions_total` | counter | `route`, `target_role`, `query_class` | `1` | Route labels stay bounded by database, user, application name, and query class. | A rising replica share means read routing is working; a rising primary share means the classifier or safety checks are falling back. |
| `pg_kinetic_route_fallbacks_total` | counter | `route`, `reason`, `fallback_policy` | `1` | Bounded by route, routing reason, and configured fallback policy. | `fallback_wait` or `fallback_reject` spikes point to replica freshness or availability pressure. |
| `pg_kinetic_read_after_write_wait_ms` | histogram | `route`, `outcome` | `ms` | Outcome is a small freshness enum. | Long waits show read-after-write protection is holding traffic until a replica catches up. |
| `pg_kinetic_read_after_write_rejections_total` | counter | `route`, `outcome` | `1` | Bounded by freshness outcome. | `stale` or `unavailable` rejections usually mean a strict policy or an unhealthy replica. |
| `pg_kinetic_replica_health` | gauge | `endpoint`, `health` | `1` | Endpoint labels use numeric ids. | A healthy replica should keep one `healthy` series at `1.0`; anything else should stay `0.0`. |
| `pg_kinetic_replica_lag_ms` | gauge | `endpoint`, `lag_state` | `1` | Bounded by replica lag state. | Rising lag in `lagging` state means the replica is drifting behind the primary. |
| `pg_kinetic_replica_replay_lsn` | gauge | `endpoint`, `target_role` | `1` | Bounded by endpoint id and expected role. | Use it with `SHOW SERVERS` to confirm the detected role and replay position line up. |
| `pg_kinetic_split_brain_warnings_total` | counter | `endpoint`, `target_role`, `reason` | `1` | Bounded by endpoint id, expected role, and warning reason. | Any nonzero value means role autodetection disagrees with the configured target role. |

## Sharding

| Metric | Type | Labels | Unit | Cardinality notes | Example interpretation |
| --- | --- | --- | --- | --- | --- |
| `pg_kinetic_shard_route_decisions_total` | counter | `route`, `shard`, `strategy`, `reason`, `outcome` | `1` | Shard labels are bucketed and strategy/reason enums are bounded. | Rising `hash_match`, `range_match`, or `list_match` shares show sharded routing is active on that path. |
| `pg_kinetic_shard_multi_shard_rejections_total` | counter | `route`, `shard`, `policy`, `reason`, `outcome` | `1` | Bucketed shard labels and bounded policy labels. | Spikes mean fan-out was attempted but the policy rejected it. |
| `pg_kinetic_shard_primary_fallbacks_total` | counter | `route`, `shard`, `policy`, `outcome` | `1` | Bucketed shard labels and bounded policy labels. | Fallbacks usually mean the shard key was missing, ambiguous, or unusable. |
| `pg_kinetic_route_map_reload_total` | counter | `outcome`, `error_code` | `1` | Bounded by reload outcome and reload error code. | Rejected reloads point to scope conflicts, empty route maps, or migration safety blockers. |
| `pg_kinetic_route_map_generation` | gauge | none | `1` | Single series. | A higher generation means a newer route-map snapshot is live. |
| `pg_kinetic_shard_lifecycle_state` | gauge | `shard`, `lifecycle_state` | `1` | Bucketed shard labels and a bounded lifecycle enum. | A nonzero `draining` or `readonly` series should line up with a planned migration. |
| `pg_kinetic_shard_active_transactions` | gauge | `shard` | `1` | Bucketed shard labels. | Nonzero values on a shard being removed mean migration safety is not settled yet. |
| `pg_kinetic_shard_prepared_statements` | gauge | `shard` | `1` | Bucketed shard labels. | Nonzero values tell you prepared work still needs to be cleaned up before a move. |

## Policy And Reloads

| Metric | Type | Labels | Unit | Cardinality notes | Example interpretation |
| --- | --- | --- | --- | --- | --- |
| `pg_kinetic_route_map_reload_total` | counter | `outcome`, `error_code` | `1` | Bounded by reload outcome and reload error code. | Use rejected reloads to spot scope conflicts, missing targets, or safety blockers before policy changes reach traffic. |
| `pg_kinetic_route_map_generation` | gauge | none | `1` | Single series. | A higher generation means a newer route-map snapshot is live. |
| `pg_kinetic_config_reload_total` | counter | `outcome` | `1` | Bounded by reload outcome. | Use this to see whether live policy or routing config changes were accepted. |

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

## Runtime And Shutdown

| Metric | Type | Labels | Unit | Cardinality notes | Example interpretation |
| --- | --- | --- | --- | --- | --- |
| `pg_kinetic_runtime_lifecycle_state` | gauge | `state` | `1` | Bounded by the lifecycle enum. | Exactly one state should be `1.0`; `starting`, `draining`, or `stopping` should be brief. |
| `pg_kinetic_runtime_readiness_state` | gauge | `state` | `1` | Bounded by the readiness enum. | `ready` should be the steady-state value once listeners are live. |
| `pg_kinetic_runtime_shutdown_total` | counter | `reason` | `1` | Bounded by shutdown reason. | Use the reason mix to tell signal-driven exits from admin or pre-stop drains. |

## Mirroring

| Metric | Type | Labels | Unit | Cardinality notes | Example interpretation |
| --- | --- | --- | --- | --- | --- |
| `pg_kinetic_mirror_decisions_total` | counter | `mode`, `target`, `outcome` | `1` | Bounded by mirror mode and outcome. | Rising `mirrored` counts mean the shadow path is active; rising `rejected` or `timeout` counts mean the mirror target is struggling. |
| `pg_kinetic_mirror_in_flight` | gauge | `mode`, `target` | `1` | Bounded by mirror mode and a fixed target label. | Keep the gauge near zero; sustained growth means the mirror path is lagging. |
| `pg_kinetic_mirror_duration_ms` | histogram | `mode`, `target`, `outcome` | `ms` | Bounded by mirror mode, target, and outcome. | Tail growth points to a slow or overloaded shadow target. |
| `pg_kinetic_mirror_dropped_total` | counter | `mode`, `reason` | `1` | Bounded by mirror mode and drop reason. | Drops should stay rare; spikes usually mean the sample rate or in-flight cap is too aggressive. |

## Adaptive Control

| Metric | Type | Labels | Unit | Cardinality notes | Example interpretation |
| --- | --- | --- | --- | --- | --- |
| `pg_kinetic_adaptive_recommendations_total` | counter | `mode`, `target`, `outcome` | `1` | Bounded by mode, knob, and recommendation outcome. | Use this to see which recommendations are showing up most often. |
| `pg_kinetic_adaptive_apply_total` | counter | `mode`, `target`, `outcome` | `1` | Bounded by mode, knob, and apply outcome. | Any non-`applied` outcome means the guardrails or allowlist blocked the change. |

## Benchmarking And Preflight

| Metric | Type | Labels | Unit | Cardinality notes | Example interpretation |
| --- | --- | --- | --- | --- | --- |
| `pg_kinetic_benchmark_runs_total` | counter | `engine`, `target`, `outcome` | `1` | Bounded by benchmark engine, comparison target, and run outcome. | Use it to confirm benchmark coverage and report outcome before comparing results. |
| `pg_kinetic_performance_budget_status` | gauge | `metric`, `outcome` | `1` | Bounded by budget metric and outcome. | A `warning` or `failed` series shows that the latest report exceeded its configured budget. |
| `pg_kinetic_preflight_findings_total` | counter | `check`, `severity` | `1` | Bounded by preflight check and severity. | A nonzero error series means the configuration did not pass operational validation. |

## Protocol Timing

| Metric | Type | Labels | Unit | Cardinality notes | Example interpretation |
| --- | --- | --- | --- | --- | --- |
| `pg_kinetic_protocol_phase_duration_ms` | histogram | `phase`, `outcome` | `ms` | Bounded by protocol phase and outcome enums. | Rising `auth` or `backend_checkout` p95 points to startup pressure; rising `execute` or `rows` points to query-side latency. |

## Recommended Panels

- Pool checkout wait: graph p95 of `pg_kinetic_pool_checkout_wait_ms` by `stage` and overlay `timeout` and `canceled` outcomes.
- Route waiting clients: plot `pg_kinetic_route_waiting` by route and add `pg_kinetic_route_in_flight` for queue depth context.
- Overload rejection rate: chart `rate(pg_kinetic_backpressure_events_total{outcome="rejected"}[5m]) / rate(pg_kinetic_backpressure_events_total[5m])`.
- Read routing mix: compare `pg_kinetic_route_decisions_total{target_role="replica"}` with `target_role="primary"`.
- Replica freshness: graph `pg_kinetic_read_after_write_wait_ms` and `pg_kinetic_read_after_write_rejections_total` together.
- Replica safety: show `pg_kinetic_replica_health`, `pg_kinetic_replica_lag_ms`, and `pg_kinetic_split_brain_warnings_total` side by side.
- Shard routing mix: compare `pg_kinetic_shard_route_decisions_total{strategy="hash"}` with `strategy="range"` and `strategy="list"`.
- Route-map reload health: graph `pg_kinetic_route_map_reload_total` by `outcome` and `error_code`.
- Shard lifecycle: show `pg_kinetic_shard_lifecycle_state` with `pg_kinetic_shard_active_transactions` and `pg_kinetic_shard_prepared_statements`.
- Pinning reason counts: bar chart `pg_kinetic_backend_pin_total` by `reason`.
- Recovery outcomes: stacked bars for `pg_kinetic_backend_recovery_total` by `trigger`, `action`, and `outcome`.
- Prepared cache materialization/invalidation: compare `pg_kinetic_prepared_events_total{event="materialize"}` with `event="invalidate"`.
- Auth failures: break down `pg_kinetic_auth_failures_total` by `mode` and `reason`.
- TLS failures: break down `pg_kinetic_tls_failures_total` by `scope`, `mode`, and `reason`.
- Drain state: use `pg_kinetic_drain_state` as a single-value status panel.
- Runtime lifecycle: show `pg_kinetic_runtime_lifecycle_state` and `pg_kinetic_runtime_readiness_state` together.
- Shutdown reason mix: chart `pg_kinetic_runtime_shutdown_total` by `reason`.
- Mirror health: graph `pg_kinetic_mirror_decisions_total`, `pg_kinetic_mirror_in_flight`, and `pg_kinetic_mirror_dropped_total` together.
- Adaptive apply rate: compare `pg_kinetic_adaptive_recommendations_total` with `pg_kinetic_adaptive_apply_total`.
- Benchmark coverage: show `pg_kinetic_benchmark_runs_total` by `engine` and `target`.
- Benchmark budget outcome: show `pg_kinetic_performance_budget_status` by `metric` and `outcome`; investigate non-passing outcomes with `SHOW PERFORMANCE`.
- Preflight health: chart `pg_kinetic_preflight_findings_total` by `check` and `severity`.
- Protocol phase latency: graph p95 of `pg_kinetic_protocol_phase_duration_ms` by `phase` and `outcome`, especially `startup`, `auth`, `backend_checkout`, `execute`, `rows`, `reset`, and `cancel`.

## Reading The Charts

- A rising checkout wait with flat in-flight usually means the pool is full and work is waiting.
- A rising route wait with one hot route usually means noisy-neighbor pressure rather than a global capacity issue.
- A rising primary share in route decisions usually means replicas are unhealthy, too stale, or blocked by a strict freshness policy.
- A spike in split-brain warnings means role autodetection and the configured endpoint role need attention before trusting replica reads.
- A spike in shard primary fallbacks usually means shard-key extraction failed or the route map did not cover the statement.
- A rejected route-map reload usually means scope overlap, missing shards, or a migration safety check stopped the change.
- A sharp rise in prepared invalidations usually means client-side churn or a workload that does not benefit from caching.
- A sustained spike in TLS or auth failures usually means a rollout or secret mismatch rather than normal traffic variation.
- A rising mirror drop rate usually means the shadow target is too slow or the sample rate is too high.
- A nonzero adaptive apply rejection rate usually means the allowlist or guardrail needs review before widening rollout.
- A nonzero preflight error series means the config should not be promoted without another review pass.
- A long tail in `backend_checkout` or `auth` usually points to startup-path regressions before user queries are affected.

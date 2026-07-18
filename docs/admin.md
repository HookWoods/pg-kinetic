---
slug: /
---

# Admin Endpoint

pg-kinetic exposes a separate PostgreSQL-compatible admin listener for operational reads.
Enable it by setting `admin_addr`. When that address is unset, the admin plane stays off.

For routing and policy guidance, see [docs/policy.md](policy.md). For runtime, mirroring, adaptive control, and benchmark operations, see [docs/production-runtime.md](production-runtime.md), [docs/mirroring.md](mirroring.md), [docs/adaptive-ops.md](adaptive-ops.md), and [docs/benchmarking.md](benchmarking.md).

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
| `SHOW ROUTE MAPS` | Sharding scopes, strategies, priorities, and the active multi-shard policy. |
| `SHOW SHARDS` | Shard lifecycle state, route scope, primary and replica target counts, and a health summary. |
| `SHOW MIGRATIONS` | Migration state, override status, source and target shard ids, and the current safety report. |
| `SHOW RUNTIME` | Runtime lifecycle state, readiness state, selected runtime engine, and process uptime. |
| `SHOW MIRRORING` | Mirror mode, sample rate, in-flight work, dropped work, and mirrored/skipped/rejected totals. |
| `SHOW ADAPTIVE` | Adaptive mode, latest recommendation, apply status, and guardrails. |
| `SHOW BENCHMARKS` | Scenario name, target, comparison, driver, duration, latency percentiles, throughput, error rate, CPU label, memory label, target-matrix labels, and comparison outcome. |
| `SHOW PERFORMANCE` | Regression-budget thresholds and outcomes, profile and process-metric status, process CPU and resident-memory samples, and proxy performance counters. |
| `SHOW SETTINGS` | Current runtime settings, sanitized for public display. |
| `SHOW LIMITS` | Effective capacity, timeout, and admin limits. |

## Redaction Rules

- `SHOW SETTINGS` omits secret-bearing config fields such as certificate and key file paths, CA file paths, the backend password env var name, and the backend server name.
- `SHOW PREPARED` exposes counts only; it never prints statement text.
- Optional fields that are not configured are shown as `<none>` rather than an empty string.
- The admin listener does not forward startup credentials or query text to the backend in order to render `SHOW` output.

## Practical Use

- Use `SHOW POOLS` and `SHOW BACKPRESSURE` together to see whether a queue is forming because the pool is full or because a specific route is overloaded.
- Use `SHOW CLIENTS`, `SHOW SERVERS`, and `SHOW ROUTES` together to understand how read traffic is flowing, whether replicas are healthy, and which policy is active.
- Use `SHOW ROUTE MAPS`, `SHOW SHARDS`, and `SHOW MIGRATIONS` together to see how shard scopes, lifecycle state, and migration safety line up.
- Use `SHOW ROUTE MAPS` and `SHOW MIGRATIONS` with the policy guide to confirm that any route or shard override is still within the intended safety envelope.
- Use `SHOW PINNING` and `SHOW RECOVERY` together to distinguish long-lived state from recovery churn.
- Use `SHOW RUNTIME`, `SHOW MIRRORING`, `SHOW ADAPTIVE`, and `SHOW BENCHMARKS` with the runtime, mirroring, adaptive, and benchmarking guides to track rollout health.
- Use `SHOW PERFORMANCE` after a report comparison to review the warning and failure thresholds, observed and baseline values, profile status, and process-metric availability.
- Use `SHOW SETTINGS` and `SHOW LIMITS` to verify the live configuration after a reload or before a support investigation.

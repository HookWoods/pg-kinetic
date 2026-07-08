# Admin Endpoint

pg-kinetic exposes a separate PostgreSQL-compatible admin listener for operational reads.
Enable it by setting `admin_addr`. When that address is unset, the admin plane stays off.

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
- Use `SHOW PINNING` and `SHOW RECOVERY` together to distinguish long-lived state from recovery churn.
- Use `SHOW SETTINGS` and `SHOW LIMITS` to verify the live configuration after a reload or before a support investigation.

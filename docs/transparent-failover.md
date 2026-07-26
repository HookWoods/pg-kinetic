# Transparent failover

Transparent failover is opt-in with `resilience.failover_enabled = true`.
Reconnect work is bounded by both `resilience.failover_max_reconnect_ms` and
the current query timeout. A replacement backend must pass the normal route,
breaker, backpressure, authentication, and pool checkout path.

## Session matrix

| Session state | v1 behavior |
| --- | --- |
| Idle, between transactions, replay-safe read | Reconnect once before any response, replay tracked safe settings, and retry the read. |
| Idle with tracked `application_name`, `search_path`, `timezone`, `datestyle`, or `extra_float_digits` | Safe when `failover_replay_session_state` is enabled; only those tracked settings are replayed. |
| Mid-transaction or failed transaction | No replay. Return PostgreSQL `57P01` with a failed-transaction ready status and finish the client session. |
| Pinned by temp tables, advisory locks, `LISTEN`, `COPY`, or other session state | No replay. Return PostgreSQL `57P01` and finish the client session. |
| Unknown protocol state or partial response already sent | No replay and no fabricated success; discard the backend and finish the client session. |
| Writes, multi-statement requests, prepared state, or unknown requests | Not transparent in v1; use the normal backend failure error path. |

`pg_kinetic_failover_survived_total` counts successful bounded retries.
`pg_kinetic_failover_failed_total` counts enabled failover attempts that cannot
safely complete. Both metrics are unlabeled counters.

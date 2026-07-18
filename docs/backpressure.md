# Backpressure And Overload

pg-kinetic uses route-aware backpressure so one noisy path does not consume all backend capacity. Overload should fail predictably instead of letting clients wait forever.

## Route Keys

A route key groups traffic by:

- database
- user
- application name
- client address
- query class

This makes queueing more useful than a single global counter. A bulk worker can saturate its own route without starving unrelated interactive traffic.

## Limits

| Setting | Purpose |
| --- | --- |
| `max_route_in_flight` | Maximum concurrent checkouts for one route key. |
| `max_route_waiters` | Maximum queued waiters for one route key. |
| `max_checkout_waiters` | Global checkout waiter cap. |
| `checkout_timeout_ms` | Maximum backend checkout wait. |
| `query_timeout_ms` | Maximum time for an assigned query cycle. |
| `idle_client_timeout_ms` | Maximum idle client lifetime. |
| `idle_transaction_timeout_ms` | Maximum idle time while pinned in a transaction. |
| `max_client_buffer_bytes` | Client-side buffer cap. |
| `max_backend_buffer_bytes` | Backend response buffer cap. |
| `overload_error_code` | SQLSTATE returned when overload is rejected. |

The default overload SQLSTATE is `53300`, PostgreSQL's `too many connections` class.

## Failure Behavior

When a route is saturated, pg-kinetic returns an overload error. When a timeout or buffer limit fires, the proxy recovers the backend if the state is safe. If recovery cannot prove the backend is reusable, the backend is discarded.

This behavior protects the pool from returning contaminated backend state to a different client.

## Observability

Watch these signals together:

- `pg_kinetic_backpressure_events_total`
- `pg_kinetic_route_checkout_wait_ms`
- `pg_kinetic_route_in_flight`
- `pg_kinetic_route_waiting`
- `pg_kinetic_timeout_total`
- `pg_kinetic_buffer_limit_total`

Admin views:

```sql
SHOW BACKPRESSURE;
SHOW LIMITS;
SHOW PERFORMANCE;
```

Sustained route waiters usually means capacity, query latency, or traffic isolation needs attention. A short spike during deploy or failover can be normal if readiness and drain behavior recover quickly.


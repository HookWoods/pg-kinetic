---
title: "Transaction Pooling And Virtual Sessions"
description: "Transaction pooling and virtual session behavior in pg-kinetic, including pinning, recovery, backend reuse, and unsafe state handling."
keywords:
  - PostgreSQL transaction pooling
  - pg-kinetic virtual sessions
  - connection pooling
  - backend reuse
---

# Transaction Pooling And Virtual Sessions

Transaction pooling lets many client sessions share fewer backend PostgreSQL connections. pg-kinetic makes this safe by tracking a virtual session for each client and by returning a backend to the pool only when the backend is known to be reusable.

## Pooling Model

The proxy checks out a backend when a query cycle needs one. When the cycle completes and the session is idle, pg-kinetic decides whether the backend can be reset and reused, must stay pinned, should be recovered, or must be discarded.

This model keeps PostgreSQL session semantics visible to applications while reducing the number of server connections needed under high client counts.

## Pinning Reasons

Backends are pinned when the client uses state that cannot be safely moved to a different backend:

| Reason | Why it pins |
| --- | --- |
| Open transaction | The transaction is bound to the backend connection. |
| Failed transaction | The backend must be rolled back before it can be reused. |
| Temporary table | The table exists only in that backend session. |
| Advisory lock | The lock is held by that backend session. |
| `COPY` | Streaming state is protocol-sensitive. |
| `LISTEN/NOTIFY` | Notifications are tied to the backend session. |
| Unknown protocol state | The proxy cannot prove reuse is safe. |

## Replayable Settings

Some settings can be tracked and replayed:

- `application_name`
- `timezone`
- `datestyle`
- `search_path`
- `extra_float_digits`

If a backend is reused for a client with replayable settings, pg-kinetic applies the necessary session state before the client continues.

## Cleanup And Recovery

`backend_reset_query` controls the cleanup query used before reuse. The default production-safe option is usually `DISCARD ALL`, while narrower reset choices such as `DISCARD TEMP` can be used only when the deployment understands the tradeoff.

`recovery_mode` controls abandoned backend handling:

| Mode | Behavior |
| --- | --- |
| `recover` | Try to roll back abandoned transactions and drain responses when possible. |
| `rollback_only` | Roll back abandoned transactions but discard backends abandoned mid-response. |
| `drop` | Discard backends on recovery triggers. |

Recovery is bounded by `recovery_timeout_ms`. If the timeout is reached, pg-kinetic discards the backend.

## Operator Checks

Use admin views to inspect pool safety:

```sql
SHOW CLIENTS;
SHOW POOLS;
SHOW SERVERS;
SHOW PINNING;
SHOW RECOVERY;
```

Useful metrics include:

- `pg_kinetic_pool_checkout_wait_ms`
- `pg_kinetic_backend_pin_total`
- `pg_kinetic_backend_cleanup_total`
- `pg_kinetic_backend_recovery_total`
- `pg_kinetic_backend_sqlstate_total`


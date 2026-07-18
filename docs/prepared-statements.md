---
title: "Prepared Statements"
description: "Prepared statement behavior in pg-kinetic, including cache safety, invalidation, transaction pooling implications, and protocol correctness."
keywords:
  - PostgreSQL prepared statements
  - pg-kinetic prepared cache
  - wire protocol correctness
  - connection pooling
---

# Prepared Statements

Prepared statements improve client performance, but they are also connection-scoped in PostgreSQL. pg-kinetic tracks prepared-statement behavior so transaction pooling does not accidentally reuse a backend with the wrong statement state.

## Protocol Surface

The extended query protocol uses messages such as:

- `Parse`
- `Bind`
- `Describe`
- `Execute`
- `Sync`
- `Close`

Named prepared statements are more stateful than unnamed statements because the name persists in the backend session until closed or reset. pg-kinetic treats that state as part of the virtual session.

## Cache Safety

The prepared statement hot path is optimized around reuse, but invalidation remains conservative:

| Event | Expected behavior |
| --- | --- |
| `Parse` for a named statement | Track statement name and query shape. |
| `Close` for a named statement | Remove tracked statement state. |
| Backend reset | Drop backend-local prepared state. |
| Protocol error or unknown state | Avoid unsafe reuse; pin or discard as needed. |
| Session cleanup | Keep only replayable state. |

The cache should reduce repeated parsing and allocation work without allowing statement state to leak between logical client sessions.

## Operator Checks

Use:

```sql
SHOW PREPARED;
SHOW PERFORMANCE;
```

Relevant metrics:

- `pg_kinetic_prepared_events_total`
- `pg_kinetic_protocol_parse_ms`
- `pg_kinetic_pool_checkout_wait_ms`

If prepared statement behavior changes, validate with driver compatibility tests because each driver has different extended-query patterns.

## Driver Notes

Most PostgreSQL drivers use prepared statements differently:

- some use unnamed statements by default
- some promote repeated statements to named prepared statements
- some expose explicit prepare APIs
- some wrap prepared statements in transactions

The compatibility matrix exists to keep those differences visible across Rust, Go, Java, JavaScript and TypeScript, Python, .NET, C, and C++ clients.


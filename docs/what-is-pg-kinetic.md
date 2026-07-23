---
title: "What Is pg-kinetic?"
description: "Plain-language explanation of pg-kinetic, the PostgreSQL connection-storm problem it solves, who should use it, and what it is not."
keywords:
  - what is pg-kinetic
  - PostgreSQL connection storm
  - Postgres connection pooler
  - PostgreSQL wire proxy
  - database proxy for Postgres
---

# What Is pg-kinetic?

For evaluators deciding whether pg-kinetic belongs in front of a PostgreSQL workload.

pg-kinetic is a PostgreSQL wire proxy for applications that can create more database work than PostgreSQL should accept directly. It sits between application clients and PostgreSQL, keeps the application protocol unchanged, and gives operators one place to control pooling, overload behavior, routing, readiness, metrics, and admin visibility.

The problem it targets is the connection storm: deployments, autoscaling events, worker bursts, and retry loops can open many PostgreSQL sessions at once. PostgreSQL can handle a lot of work, but every backend connection has cost. If the database spends its budget accepting sessions, queueing work, or recovering from overload, the application sees latency spikes and failures at the worst time.

pg-kinetic adds a controlled boundary. Applications keep using PostgreSQL drivers and SQL. Operators point connection strings at pg-kinetic, set route and pool limits, and decide whether overload should wait, fail fast, or fall back to the primary. The stable surface is aimed at teams that need predictable database access without rewriting application data access code.

## Who It Is For

| Use case | Why pg-kinetic fits |
| --- | --- |
| Many app instances share one PostgreSQL primary | Transaction pooling reduces backend connection pressure. |
| Bursty workers or deploy waves overload the database | Route-aware backpressure gives each traffic class explicit limits. |
| Operators need a proxy-side readiness and admin plane | Health endpoints, metrics, and `SHOW` views expose in-process state. |
| Read replicas exist but stale reads are risky | Conservative read routing checks health, lag, and session write state. |
| Prepared statements matter | pg-kinetic tracks virtual session state before reusing backend connections. |

## Adoption Cost

The first production adoption should be a connection-string change plus a config file, not an application rewrite:

- no PostgreSQL driver change
- no SQL rewrite
- no ORM replacement
- no PostgreSQL extension installed in the database
- no required schema migration

The real cost is operational: choose pool sizes, route limits, timeout budgets, TLS/authentication modes, and rollout gates. Start with the [migration guide](./migration.md), then use [preflight and commands](./commands.md) before moving traffic.

## What It Is Not

pg-kinetic is not a sharding engine in the stable runtime. The repository has sharding models and route-preview tooling, but live sharded traffic is outside the stable contract.

pg-kinetic is not an ORM, query builder, SQL rewrite layer, database extension, multi-primary coordinator, or failover consensus system. It does not promote PostgreSQL servers. It does not make unsafe replica reads safe by guessing.

## Decision Shortcut

Use pg-kinetic when you want a PostgreSQL-compatible proxy boundary that makes pooling, overload, routing, health, and visibility explicit.

Do not choose pg-kinetic as your primary answer to cross-shard SQL, automatic failover, application authorization, or broad query rewriting. Those are different systems with different failure modes.

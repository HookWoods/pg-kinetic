---
title: "Read Routing"
description: "Read-routing behavior and safety model for pg-kinetic, including primary fallback, replica eligibility, freshness, and route snapshots."
keywords:
  - PostgreSQL read routing
  - pg-kinetic routing
  - replica reads
  - database proxy routing
---

# Read Routing

pg-kinetic can route eligible reads to replicas, but it does so conservatively. The goal is to avoid stale or unsafe reads rather than chase replica utilization at any cost.

## When Routing Is Safe

Read routing is safest when all of these are true:

- the statement is read-only or a read candidate
- the session is not inside a write transaction
- the session does not carry unsafe state that would pin the backend
- the selected replica is healthy and within the configured freshness budget

If any of those checks fail, pg-kinetic falls back to the primary or rejects the read, depending on policy.

If the primary connection fails before any response byte is forwarded, a safe
read may be retried once on a newly checked-out primary connection. This does not
apply to writes, ambiguous statements, stateful sessions, authentication failures,
or partially forwarded responses.

## Conservative Classification

The classifier prefers the primary for anything ambiguous:

- writes, DDL, and transaction control stay on the primary
- `COPY`, temporary tables, advisory locks, listen/notify, and other stateful features stay on the primary
- unknown or mixed statements stay on the primary
- read-only transactions such as `BEGIN READ ONLY` and `SET TRANSACTION READ ONLY` are treated as strong read signals

This keeps the routing rules predictable even when a client mixes simple reads with session state.

## Explicit Hints

You can override the default classification with SQL comments at the start of a statement:

- `/* pg-kinetic: primary */`
- `/* pg-kinetic: replica */`
- `/* pg-kinetic: stale-ok */`
- `/* pg-kinetic: strict-fresh */`

Examples:

```sql
/* pg-kinetic: replica */ select id, name from accounts where id = $1;
/* pg-kinetic: primary */ update accounts set last_seen = now() where id = $1;
```

Hints are still subject to safety checks. A replica hint does not override a dead or stale replica.

## Read-After-Write Protection

pg-kinetic tracks read-after-write state inside a session. When a read depends on a recent write, the proxy can wait for a replica to catch up before routing the read away from the primary.

The relevant freshness controls are:

- `freshness_policy`
- `max_replica_lag_ms`
- `read_after_write_timeout_ms`

When the timeout expires, the configured fallback policy decides whether the proxy uses the primary, waits longer, or rejects the request.

## Replica Health And Lag

Replica routing is only considered when the health signals look good:

- the replica must be reachable
- the detected role must match the configured endpoint role
- the lag must be within the configured maximum when lag checks are enabled
- the replay LSN must satisfy the required session write LSN when that protection is enabled

`SHOW SERVERS` exposes the live health, detected role, lag, replay LSN, and last probe age so operators can confirm why a replica was or was not chosen.

## Fallback Policies

The fallback policy controls what happens when a replica is not acceptable:

- `primary` routes the read to the primary
- `reject` fails the read immediately
- `wait` waits for a replica to become fresh enough, up to the configured timeout

`require_replica` mode is the strictest setting. If a replica is not available, the proxy rejects the read rather than using the primary.

## Role Autodetection And Split-Brain Warnings

pg-kinetic probes backend role separately from simple reachability. That helps it spot cases where a server is alive but reporting the wrong role.

When the observed role does not match the configured endpoint role, the proxy raises a split-brain warning and keeps routing decisions conservative. Those warnings are a sign to check failover, DNS, service discovery, and backend promotion state before trusting replica reads.

## Known Limitations

- routing is intentionally conservative and may send some eligible reads to the primary
- SQL comments are hints, not a security boundary
- the classifier does not try to understand every SQL dialect extension
- health and lag signals are only as fresh as the last successful probe
- replica reads are still subject to pool limits and backpressure

## Example Config

```toml
[[routes]]
[routes.primary]
address = "127.0.0.1:5432"

[[routes.replicas]]
address = "127.0.0.1:5433"
weight = 1

[routes.read_routing]
read_routing_mode = "prefer_replica"
fallback_policy = "wait"

[routes.freshness]
freshness_policy = "session_write_lsn_and_max_lag"
max_replica_lag_ms = 2500
read_after_write_timeout_ms = 750

[routes.ha]
replica_health_interval_ms = 2000
replica_health_timeout_ms = 750
```

## Rollout Path

1. Start with `read_routing_mode = "off"` and verify replica health reporting.
2. Move one route to `prefer_replica` with `fallback_policy = "primary"`.
3. Add `session_write_lsn_and_max_lag` freshness checks on a low-risk route.
4. Watch `SHOW CLIENTS`, `SHOW SERVERS`, `SHOW ROUTES`, and the read-routing metrics.
5. Tighten to `wait` or `require_replica` only after the fallback and lag behavior is predictable.

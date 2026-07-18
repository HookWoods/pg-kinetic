---
title: "Architecture"
description: "Architecture of the pg-kinetic PostgreSQL wire proxy, including request flow, crate layout, safety model, control planes, and observability."
keywords:
  - pg-kinetic architecture
  - PostgreSQL wire protocol
  - Rust PostgreSQL proxy
  - database proxy design
---

# Architecture

pg-kinetic is built around PostgreSQL wire correctness, conservative backend reuse, and operator-visible decisions. It does not require application driver changes because clients continue speaking the PostgreSQL protocol.

## Request Path

```mermaid
flowchart LR
  Client["PostgreSQL client"] --> Listener["pg-kinetic listener"]
  Listener --> Parser["Wire parser"]
  Parser --> Session["Virtual session tracker"]
  Session --> Router["Route and policy decision"]
  Router --> Pool["Backend pool checkout"]
  Pool --> Backend["PostgreSQL primary or replica"]
  Router --> Admin["Admin snapshots and metrics"]
```

The proxy parses enough frontend and backend messages to understand transaction state, prepared-statement state, backend readiness, and unsafe session features. When the state is uncertain, pg-kinetic prefers pinning, recovery, or backend discard over unsafe reuse.

## Crate Layout

| Crate | Responsibility |
| --- | --- |
| `pg-kinetic-wire` | PostgreSQL wire protocol parsing and frame helpers. |
| `pg-kinetic-core` | Shared domain models for sessions, routing, preview policy/sharding models, metrics, benchmark, compatibility, and regression. |
| `pg-kinetic-proxy` | Runtime proxy behavior, config loading, admin rendering, benchmarks, profiling, compatibility, regression, and preflight execution. |
| `pg-kinetic` | CLI entry point and command dispatch. |
| `xtask` | Repository automation for CI-style local orchestration. |

## Control Planes

pg-kinetic separates the traffic path from operational control surfaces:

- the client listener accepts PostgreSQL application traffic
- the admin listener accepts PostgreSQL-compatible `SHOW` queries for snapshots
- the health listener exposes HTTP readiness, liveness, and state endpoints
- metrics and traces are emitted from bounded runtime snapshots

This keeps operational reads out of the application traffic pool and avoids requiring a separate SQL extension in PostgreSQL.

## Safety Model

The reuse decision is based on the current client and backend state:

| Condition | Reuse behavior |
| --- | --- |
| Idle and replayable | Backend can return to the pool after reset/replay handling. |
| Open or failed transaction | Backend remains pinned or is recovered before reuse. |
| Temporary table, advisory lock, COPY, LISTEN/NOTIFY | Backend remains pinned until the session is safe or discarded. |
| Unknown protocol state | Backend is discarded instead of being returned to the pool. |
| Recovery timeout | Backend is discarded. |

The same conservative model is used by live routing and prepared-statement behavior. Sharding, policy, and mirroring have domain models and preview/tooling surfaces, but they are not documented as live traffic features today. Speedups must not weaken PostgreSQL wire correctness.

## Observability Model

Runtime decisions become visible through:

- Prometheus/OpenMetrics style metric names
- admin views such as `SHOW CLIENTS`, `SHOW POOLS`, `SHOW ROUTES`, and `SHOW PREPARED`
- compatibility and regression reports
- benchmark report and score outputs
- preflight reports for deployment readiness

Operational outputs redact secret-bearing fields and should be safe to attach to CI logs.

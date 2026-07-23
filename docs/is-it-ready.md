---
title: "Is pg-kinetic Ready?"
description: "Current maturity, stable release boundary, version story, supported PostgreSQL targets, and preview exclusions for pg-kinetic."
keywords:
  - is pg-kinetic ready
  - pg-kinetic 1.0
  - pg-kinetic production
  - PostgreSQL proxy stable release
  - pg-kinetic release contract
---

# Is pg-kinetic Ready?

For evaluators deciding whether pg-kinetic is ready for a real PostgreSQL rollout.

pg-kinetic is ready to evaluate against the stable 1.0 runtime contract. It should not be evaluated as a general-purpose sharding engine, policy engine, mirroring platform, or Kubernetes operator.

## Stable Runtime Scope

The stable contract covers:

- PostgreSQL wire proxying for ordinary client traffic
- transaction pooling with virtual session tracking
- prepared-statement handling inside the proxy safety model
- route-aware backpressure
- conservative read routing
- TLS and authentication modes documented in the runtime guides
- health checks, readiness, metrics, and PostgreSQL-protocol admin views
- single-primary recovery behavior

The exact support boundary lives in the [Stable 1.0 Release Contract](./release-contract.md). Treat that page as the source of truth for release claims.

## Version Story

The docs describe the current release line and the stable 1.0 target. Release candidates can be used for validation and rollout rehearsal, but production claims should be tied to an immutable image tag and the matching release contract.

Use `latest` for quick local trials only. Pin an explicit image tag for controlled environments, and keep the previous direct PostgreSQL path available until the proxy passes workload-specific checks.

## What Is Not Ready As Live Traffic

These surfaces are present as preview, offline tooling, or implementation groundwork, but are not part of the stable live-traffic contract:

| Surface | Current status | Production guidance |
| --- | --- | --- |
| Sharding | Preview/offline route models and `route-preview` | Use another production sharding layer today. |
| Policy | Preview/offline policy model and `policy-preview` | Keep authorization in PostgreSQL, network policy, or the application. |
| Mirroring | Disabled live dispatcher and model surfaces | Do not treat mirror counters as proof of live traffic shadowing. |
| Adaptive operations | Recommendation and simulation | Review output manually; it does not mutate live settings. |
| Kubernetes operator | Not supported | Use the Helm chart; no CRDs or controller exist. |

## Readiness Checklist

Before sending real traffic through pg-kinetic:

1. Confirm your use case fits the [stable runtime scope](./release-contract.md).
2. Run the local [quickstart](./quickstart.md) once to verify the proxy path.
3. Define capacity, pool, route, timeout, TLS, and authentication settings in [configuration](./configuration.md).
4. Run `pg-kinetic preflight --config ...` and clear all errors.
5. Validate the application through the [migration guide](./migration.md) and [compatibility matrix](./compatibility.md).
6. Monitor [health](./health-and-drain.md), [metrics](./metrics.md), and [admin views](./admin.md) during a narrow rollout.

## Short Answer

Ready for: a controlled PostgreSQL proxy rollout with pooling, backpressure, conservative read routing, health checks, metrics, and admin inspection.

Not ready for: live sharding, policy enforcement, live mirroring, automatic tuning, or operator-managed Kubernetes reconciliation.

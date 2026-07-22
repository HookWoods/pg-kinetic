---
title: "Production Runtime"
description: "Production runtime guidance for pg-kinetic process lifecycle, startup checks, graceful shutdown, preflight validation, and operational signals."
keywords:
  - pg-kinetic production
  - PostgreSQL proxy runtime
  - graceful shutdown
  - preflight validation
---

# Production Runtime

pg-kinetic runtime behavior is centered on lifecycle state, readiness state, shutdown coordination, health endpoints, and admin snapshots.

For the exact stable 1.0 support boundary, PostgreSQL targets, authentication,
TLS, pooling, recovery, compatibility, and preview exclusions, see the [Stable
1.0 Release Contract](./release-contract.md).

## Lifecycle States

`SHOW RUNTIME` can report:

- `starting`
- `ready`
- `draining`
- `stopping`
- `stopped`

Readiness is separate:

- `ready` means the proxy is accepting new work
- `not_ready` means startup is incomplete or the proxy is not accepting work
- `draining` means the runtime is winding down

## Readiness

The proxy becomes ready after listeners initialize and backend checks pass when those checks are enabled.

`GET /readyz` returns `503` when the drain controller is not accepting clients or the backend probe is not ready.

## Shutdown Sequence

Signal-driven shutdown follows this sequence:

1. begin drain
2. stop accepting new clients
3. wait for active sessions within `drain_timeout_ms`
4. wait through `shutdown_grace_ms` when sessions overrun the drain window
5. force-close remaining sessions
6. transition toward stop

The HTTP health server does not implement `/drain`, so Kubernetes pre-stop HTTP hooks must not call it.

## Runtime Engine Selection

```toml
[runtime.engine]
runtime_engine = "tokio_default"
experimental_runtime_enabled = false
```

| Engine | Status |
| --- | --- |
| `tokio_default` | default and stable |
| `tokio_current_thread` | stable option |
| `experimental_thread_per_core` | requires `experimental_runtime_enabled = true` |
| `experimental_io_uring` | requires `experimental_runtime_enabled = true` |

`experimental_io_uring` is a Linux-only plaintext pass-through experiment. It
requires the `io-uring` cargo feature and currently rejects client TLS, backend
TLS, and pg-kinetic-managed authentication modes. Use it only for isolated
benchmarking against a trusted backend that performs its own PostgreSQL
authentication. `tokio_default` and `experimental_thread_per_core` remain the
runtime engines for the full pooled/authenticated proxy surface.

## Preflight

```bash
pg-kinetic preflight --config path/to/pg-kinetic.toml --format json
```

Preflight checks runtime assets and guardrails such as TLS files, auth user store loading, mirror isolation models, lifecycle timing, and adaptive config validity. Sharding, policy, and mirroring checks do not mean those features are live traffic features.

The JSON output includes `ok`, `warning_count`, `error_count`, `warnings`, and `errors`.

## Release Checklist

- Confirm `SHOW RUNTIME` reports the expected node id, lifecycle state, readiness state, and runtime engine.
- Confirm `SHOW SETTINGS` matches the intended backend address, drain budget, and health configuration.
- Run `pg-kinetic preflight --config ...` and clear all errors before rollout.
- Verify the shutdown window is shorter than the supervisor termination grace period.
- Keep experimental runtime engines out of default deployments unless they have workload-specific benchmark evidence.

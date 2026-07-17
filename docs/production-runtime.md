# Production Runtime

pg-kinetic runtime behavior is centered on a small lifecycle model that is visible through `SHOW RUNTIME`, the health endpoints, and the runtime metrics family.

## Lifecycle States

The runtime state machine uses these states:

- `starting`
- `ready`
- `draining`
- `stopping`
- `stopped`

The readiness state reported by `SHOW RUNTIME` is separate from the lifecycle state:

- `ready` means the proxy is accepting new work
- `not_ready` means startup is incomplete or the proxy is no longer accepting work
- `draining` means the runtime is winding down and drain is still in progress

`SHOW RUNTIME` also reports the selected runtime engine and process uptime.

## Readiness And Drain

The proxy becomes ready after its listeners are initialized and, when enabled, the startup backend checks have completed.

Drain changes the readiness posture immediately:

- new client sessions are rejected once drain starts
- existing sessions continue until the drain window ends
- `GET /readyz` returns `503` during drain
- `SHOW RUNTIME` reflects the lifecycle and readiness transition

The default runtime behavior keeps readiness failing during drain. If that setting is relaxed, the runtime snapshot can show `draining` while the process is still winding down.

## Shutdown Sequence

Shutdown follows the same order for signals, pre-stop hooks, and admin-initiated drain:

1. begin drain
2. stop accepting new clients
3. wait for active sessions to finish within `drain_timeout_ms`
4. wait through `shutdown_grace_ms` if sessions overrun the drain window
5. force-close any remaining sessions
6. transition to `stopping`
7. finish in `stopped`

Keep `shutdown_grace_ms` within the Kubernetes termination grace period. The preflight check rejects values that overrun the configured termination budget.

## Runtime Engine Selection

The runtime engine is selected in `[runtime.engine]`:

- `tokio_default` is the default and stable choice
- `tokio_current_thread` is stable and useful for lightweight local runs
- `experimental_thread_per_core` is gated as experimental
- `experimental_io_uring` is gated as experimental

Experimental engines require `experimental_runtime_enabled = true`. Keep the default engine in production unless you have benchmarked a specific alternative on the same workload and platform.

## Preflight Command

Use preflight before a rollout:

```powershell
pg-kinetic preflight --config path\to\pg-kinetic.toml --format json
```

Preflight checks:

- TLS file access and TLS mode compatibility
- authentication and user-store loading
- route-map and sharding configuration
- mirror isolation settings
- lifecycle guardrails
- adaptive control guardrails

The JSON output includes `ok`, `warning_count`, `error_count`, `warnings`, and `errors`.

## Release Checklist

- Confirm `SHOW RUNTIME` reports the expected node id, lifecycle state, readiness state, and runtime engine.
- Confirm `SHOW SETTINGS` matches the intended backend address, drain budget, and health configuration.
- Run `pg-kinetic preflight --config ...` and clear all errors before shipping.
- Verify the shutdown window is shorter than the termination grace period.
- Keep experimental runtime engines out of the default release profile unless they are explicitly approved.

## Operational Limits

- `SHOW RUNTIME` is a snapshot; it does not replace end-to-end readiness checks.
- The health listener is separate from the admin listener and may be disabled independently.
- Startup backend checks can be disabled for controlled environments, but that reduces the protection offered by readiness.
- Experimental runtime engines are not the right choice for a default production rollout.

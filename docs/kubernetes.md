# Kubernetes deployment

pg-kinetic can participate in Kubernetes lifecycle management without a controller or custom
resource. The same probe and drain semantics are also suitable for systemd, Nomad, and other
process supervisors.

## Deployment shape

Run one pg-kinetic process per pod and expose separate ports for PostgreSQL traffic, health
probes, metrics, and the optional admin listener. Keep the health and admin listeners private to
the pod or cluster network.

```yaml
spec:
  terminationGracePeriodSeconds: 65
  containers:
    - name: pg-kinetic
      ports:
        - { name: postgres, containerPort: 6543 }
        - { name: health, containerPort: 8080 }
      startupProbe:
        httpGet: { path: /readyz, port: health }
        periodSeconds: 2
        failureThreshold: 15
      readinessProbe:
        httpGet: { path: /readyz, port: health }
        periodSeconds: 2
      livenessProbe:
        httpGet: { path: /healthz, port: health }
        periodSeconds: 10
      lifecycle:
        preStop:
          httpGet: { path: /drain, port: health }
```

## Lifecycle settings

The lifecycle section keeps Kubernetes policy explicit:

```toml
[runtime.lifecycle]
startup_grace_ms = 30000
shutdown_grace_ms = 30000
readiness_fail_during_drain = true
pre_stop_drain_enabled = true
pre_stop_drain_endpoint = "/drain"
startup_backend_checks_enabled = true
termination_grace_period_seconds = 65

[drain]
drain_grace_ms = 30000
```

`termination_grace_period_seconds` documents the value that should be copied to the pod spec. It
should be greater than the drain grace plus the shutdown grace; the default leaves five seconds
of scheduling margin.

## Probes and drain semantics

- The startup probe remains unsuccessful until listeners are initialized and, when enabled,
  backend pool warmup checks complete.
- Readiness returns unsuccessful during drain by default, so Services stop sending new sessions
  before the process exits. Set `readiness_fail_during_drain = false` only when the surrounding
  supervisor deliberately treats draining as ready.
- Liveness remains successful throughout normal drain and shutdown coordination. A rollout must
  not restart a pod merely because it is draining.
- Calling the configured preStop drain endpoint starts drain with the `pre_stop_hook` reason.
  Repeated calls are safe and do not restart the grace period.
- SIGTERM follows the same sequence: stop accepting sessions, wait for drain grace, apply shutdown
  grace for remaining sessions, then stop.
- The lifecycle admin snapshot reports the lifecycle state, readiness state, initialized
  listeners and pools, active sessions, shutdown reason, transition count, and force-close state.

## Rolling restarts

Use the preStop hook and keep `terminationGracePeriodSeconds` longer than both runtime grace
windows. During a rolling restart, pg-kinetic becomes not-ready before waiting for existing
sessions. Configure a PodDisruptionBudget and Deployment surge/unavailable values according to
the connection capacity required during that drain window.

## Configuration reload

Use the existing file reload mechanism for reloadable routing and policy changes. Lifecycle
listener addresses, probe wiring, and pod termination values are rollout-time settings; change
them through a new Deployment revision. A reload does not emulate pod replacement and must not be
used as a substitute for a drain.

## Controller-free limitations

The built-in behavior manages only the local process. It does not create Services, coordinate
PodDisruptionBudgets, sequence drains across replicas, mutate Deployment settings, or reconcile
configuration. Use standard Kubernetes resources or an external deployment system for those
cluster-level responsibilities.

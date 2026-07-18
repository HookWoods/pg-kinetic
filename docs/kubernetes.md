# Kubernetes Deployment

pg-kinetic is stateless from the Kubernetes control-plane perspective. Each pod owns only its active client and backend sockets; route maps, policy files, TLS material, and user stores are supplied as configuration.

The production Kubernetes path is the published Helm repository at `https://helm.pgkinetic.dev`.

## Helm Install

Add the chart repository:

```bash
helm repo add pgkinetic https://helm.pgkinetic.dev
helm repo update
```

Create a backend-password secret when the config references `PG_KINETIC_BACKEND_PASSWORD`:

```bash
kubectl create secret generic pg-kinetic-backend \
  --from-literal=backend-password='replace-me'
```

Install:

```bash
helm install pg-kinetic pgkinetic/pg-kinetic \
  --set image.tag=0.1.0 \
  --set backendPassword.existingSecret=pg-kinetic-backend
```

For local chart development, use `helm install pg-kinetic ./charts/pg-kinetic`. Override `values.yaml` for your backend address, TLS paths, admin settings, resource requests, and replica count.

## Deployment Shape

The chart creates:

- a Deployment running one pg-kinetic process per pod
- a ConfigMap containing `pg-kinetic.toml`
- a Service exposing PostgreSQL, admin, and health ports
- readiness and liveness probes
- a pre-stop drain hook
- non-root container security defaults

Default service ports:

| Port | Purpose |
| --- | --- |
| `6432` | PostgreSQL client traffic |
| `7000` | PostgreSQL-compatible admin listener |
| `9090` | Prometheus metrics |
| `9091` | HTTP health and readiness |

Keep health and admin listeners private to the cluster network unless there is a deliberate operational reason to expose them.

## Helm Values

Important values:

| Value | Purpose |
| --- | --- |
| `image.repository` | Container repository. |
| `image.tag` | Immutable release tag to deploy. |
| `replicaCount` | Number of proxy pods. |
| `service.proxyPort` | PostgreSQL client-facing service port. |
| `service.adminPort` | Admin listener service port. |
| `service.healthPort` | HTTP health service port. |
| `backendPassword.existingSecret` | Secret containing the backend password, when required. |
| `config` | Full `pg-kinetic.toml` rendered into a ConfigMap. |

## Lifecycle Settings

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
drain_timeout_ms = 45000
reject_new_clients_during_drain = true
```

`termination_grace_period_seconds` documents the value that should be copied to the pod spec. It should be greater than the drain window so the process can reject new traffic, wait for active sessions, and shut down cleanly.

## Probes And Drain Semantics

- The startup path remains unavailable until listeners are initialized and, when enabled, backend pool warmup checks complete.
- Readiness returns unavailable during drain by default, so Services stop sending new sessions before the process exits.
- Liveness remains available throughout normal drain and shutdown coordination.
- Calling the configured pre-stop drain endpoint starts drain with the `pre_stop_hook` reason.
- Repeated pre-stop calls are safe and do not restart the grace period.
- SIGTERM follows the same sequence: stop accepting sessions, wait for drain, apply shutdown grace, then stop.

## Rolling Restarts

Use the pre-stop hook and keep `terminationGracePeriodSeconds` longer than the configured drain timeout. During a rolling restart, pg-kinetic becomes not-ready before waiting for existing sessions.

Configure a PodDisruptionBudget and Deployment surge/unavailable values according to the connection capacity required during that drain window.

## Configuration Reload

Use the file reload mechanism for reloadable routing and policy changes. Listener addresses, probe wiring, pod termination values, and secret mounts are rollout-time settings; change them through a new Deployment revision.

Reload does not emulate pod replacement and must not be used as a substitute for drain.

## Operator Status

The repository ships a Helm chart, not a Kubernetes operator. Do not rely on CRDs, controller-managed failover, automatic resharding, or operator-managed config reconciliation. Use ordinary Deployment, Service, ConfigMap, Secret, readiness, and pre-stop primitives.

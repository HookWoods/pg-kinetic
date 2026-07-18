---
title: "Kubernetes Deployment"
description: "Deploy pg-kinetic with the local Helm chart, configure probes and values, understand reload limits, and plan Kubernetes rollback."
keywords:
  - pg-kinetic Kubernetes
  - PostgreSQL proxy Helm
  - database proxy Kubernetes
  - Helm chart
---

# Kubernetes Deployment

pg-kinetic currently ships Kubernetes manifests as a local Helm chart in `charts/pg-kinetic`.

The chart repository workflow is present, but `https://helm.pgkinetic.dev` is usable only after a version tag publishes the first chart index. Until that release exists, install from the local chart path.

## Install From The Local Chart

```bash
helm lint ./charts/pg-kinetic
helm template pg-kinetic ./charts/pg-kinetic
helm install pg-kinetic ./charts/pg-kinetic \
  --set image.repository=pg-kinetic \
  --set image.tag=local \
  --set image.pullPolicy=Never
```

Use that local-image form only for a development cluster where `pg-kinetic:local` has been loaded into the node image store. For production clusters, set `image.repository` and `image.tag` to an immutable image that already exists in a registry reachable by the cluster.

After the first chart release exists:

```bash
helm repo add pgkinetic https://helm.pgkinetic.dev
helm repo update
helm install pg-kinetic pgkinetic/pg-kinetic \
  --set image.tag=0.1.0
```

## Deployment Shape

The chart renders:

- a Deployment running one pg-kinetic process per pod
- a ConfigMap containing `pg-kinetic.toml`
- a ClusterIP Service
- readiness and liveness probes
- non-root container security defaults

Default service ports:

| Port | Purpose |
| --- | --- |
| `6432` | PostgreSQL client traffic |
| `7000` | PostgreSQL-compatible admin listener |
| `9090` | Prometheus metrics when `metrics_addr` is configured |
| `9091` | HTTP health and readiness when `health_addr` is configured |

Keep admin, metrics, and health listeners private to the cluster network.

## Helm Values

Important values:

| Value | Default | Purpose |
| --- | --- | --- |
| `image.repository` | `hookwoods/pg-kinetic` | Container image repository. Override until the first public image exists. |
| `image.tag` | `0.1.0` | Image tag to deploy. Use only a tag that already exists. |
| `replicaCount` | `2` | Number of proxy pods. |
| `service.proxyPort` | `6432` | PostgreSQL client-facing service port. |
| `service.adminPort` | `7000` | Admin listener service port. |
| `service.metricsPort` | `9090` | Metrics service port. |
| `service.healthPort` | `9091` | Health service port. |
| `backendPassword.existingSecret` | empty | Optional secret for backend password injection. |
| `config` | embedded TOML string | Full `pg-kinetic.toml` rendered into a ConfigMap. |

The chart does not validate the TOML beyond YAML rendering. Run `pg-kinetic preflight --config` against the final rendered config before rollout.

## Probes

The chart uses:

- readiness: `GET /readyz`
- liveness: `GET /healthz`

The chart does not configure an HTTP pre-stop drain hook because the proxy does not implement `/drain` today.

## Reload Behavior

The file reload loop reloads `config_file` when `reload_enabled = true`. A reload is accepted only when every field checked by `Config::is_reload_compatible_with` stays equal between the active config and the next config.

Accepted reloads affect new client connections and newly loaded assets. They do not rewrite existing client sessions, already checked-out backends, listener sockets, route lists, runtime settings, capacity limits, TLS modes, auth mode, health listener configuration, or socket options.

| Change | Applied By Reload | Requires Restart |
| --- | --- | --- |
| Runtime adaptive scalar values | no | yes |
| TLS certificate file contents at the same configured paths | yes, after asset validation |
| Auth user file contents at the same configured path | yes, after asset validation |
| Listener address | no | yes |
| Backend address or route list | no | yes |
| Capacity limits | no | yes |
| Admin listener config | no | yes |
| Metrics listener config | no | yes |
| TLS mode or backend TLS mode | no | yes |
| Auth mode or backend credential variable name | no | yes |
| Health listener config | no | yes |
| Socket options | no | yes |
| Policy/sharding runtime config | not part of the main runtime config | not supported as live traffic config |

Rejected reloads leave the active config running and increment the config reload metric with `outcome="rejected"`.

## Rollout And Rollback

Use ordinary Kubernetes Deployment rollout controls:

```bash
kubectl rollout status deployment/pg-kinetic
kubectl rollout undo deployment/pg-kinetic
```

Rollback triggers:

- `/readyz` stays `503`
- application connection errors increase after the Service points to pg-kinetic
- admin or metrics endpoints expose unexpected capacity, timeout, or backend health state
- client drivers hit unsupported protocol or session-state behavior

## Operator Status

The repository ships a Helm chart, not a Kubernetes operator. There are no CRDs, controller-managed failover, automatic resharding, or operator-managed config reconciliation.

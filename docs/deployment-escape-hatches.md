---
title: "Deployment Escape Hatches"
description: "Local image loading, Kubernetes rollout and rollback notes, network-collision checks, and support-report contents for pg-kinetic deployments."
keywords:
  - pg-kinetic deployment
  - Kubernetes local image
  - support report
  - network troubleshooting
---

# Deployment Escape Hatches

Use this page when the happy path in [Quickstart](./quickstart.md) or [Kubernetes Deployment](./kubernetes.md) does not match your local cluster or rollout environment.

## Local Image Loading

The local Helm example assumes `pg-kinetic:local` already exists in the Kubernetes node image store.

| Cluster | Command |
| --- | --- |
| kind | `kind load docker-image pg-kinetic:local --name <cluster>` |
| minikube | `minikube image load pg-kinetic:local` |
| Docker Desktop Kubernetes | Build with Docker Desktop's Docker engine, then use `image.pullPolicy=Never`. |

Install with:

```bash
helm install pg-kinetic ./charts/pg-kinetic \
  --set image.repository=pg-kinetic \
  --set image.tag=local \
  --set image.pullPolicy=Never
```

If the pod reports `ImagePullBackOff`, the node cannot see the local image. Load it into the cluster or push an immutable image tag to a registry reachable by the cluster.

## Network Collision Checks

The Compose quickstart uses subnet `172.30.0.0/24` and static PostgreSQL address `172.30.0.10`.

If Docker reports a subnet collision, change both places together:

1. `deploy/docker-compose.yml` network subnet and service `ipv4_address` values.
2. `deploy/pg-kinetic.toml` `connection.backend_addr`.

`backend_addr` must stay `IP:port`; hostnames are not accepted by the runtime config parser.

## Upgrade And Rollback Expectations

- Treat listener addresses, backend addresses, route lists, capacity limits, runtime settings, TLS modes, auth mode, health listener config, and socket options as restart-required.
- Use a Deployment rollout for image or restart-required config changes.
- Keep `terminationGracePeriodSeconds` longer than the configured shutdown and drain windows.
- Roll back with `kubectl rollout undo deployment/pg-kinetic` when readiness stays down or application connection errors rise.
- Accepted reloads affect new client connections and reloadable assets only; they do not rewrite existing sessions.

## Support Report

Include this information when asking for help:

- pg-kinetic version or commit SHA and deployment method.
- Redacted `pg-kinetic.toml`.
- Exact command used to start the process or render Helm.
- `pg-kinetic preflight --config <path> --format json` output.
- `/healthz`, `/readyz`, and `/state` responses.
- Admin outputs for `SHOW RUNTIME`, `SHOW POOLS`, `SHOW CLIENTS`, `SHOW SERVERS`, `SHOW SETTINGS`, and `SHOW LIMITS`.
- Relevant logs around startup, reload, drain, TLS/auth failures, or backend checkout failures.
- Client library, version, connection string options excluding secrets, and `PGSSLMODE`/TLS mode.
- PostgreSQL server version and whether the connection works directly without pg-kinetic.

Do not include private keys, passwords, bearer tokens, raw customer query text, or unredacted environment dumps.

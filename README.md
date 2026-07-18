<p align="center">
  <img src="docs-site/static/img/pg-kinetic-og.png" width="760" alt="pg-kinetic PostgreSQL wire proxy" />
</p>

<p align="center">
  <a href="https://pgkinetic.dev">Website</a> |
  <a href="https://docs.pgkinetic.dev">Documentation</a> |
  <a href="https://docs.pgkinetic.dev/docs/quickstart">Quickstart</a> |
  <a href="https://github.com/HookWoods/pg-kinetic/issues">Issues</a>
</p>

# pg-kinetic

**Keep PostgreSQL responsive under connection spikes.** pg-kinetic is a drop-in Rust PostgreSQL wire proxy: keep your driver and SQL, then add transaction pooling, route-level backpressure, read routing, and operator-visible health.

## Start Here

Run a complete local stack: PostgreSQL plus pg-kinetic, built from this checkout.

```bash
git clone https://github.com/HookWoods/pg-kinetic.git
cd pg-kinetic
docker compose -f deploy/docker-compose.yml up -d --build
```

This builds the local image, starts PostgreSQL, and exposes pg-kinetic on `localhost:6432`. It does not require a published container image.

Verify that the proxy is live and ready before pointing an application at it:

```bash
curl -fsS http://127.0.0.1:9091/healthz
curl -fsS http://127.0.0.1:9091/readyz

PGPASSWORD=pgkinetic PGSSLMODE=disable \
  psql "postgres://pgkinetic@127.0.0.1:6432/pgkinetic" \
  -c "select 1;"
```

Expected health responses are `live` and `ready`. The query uses the proxy, not the PostgreSQL container directly.

| Local port | Purpose |
| --- | --- |
| `6432` | PostgreSQL listener through pg-kinetic |
| `7000` | Operator admin listener |
| `9090` | Prometheus metrics endpoint |
| `9091` | Liveness and readiness endpoints |
| `55432` | Direct PostgreSQL access for local comparison |

Stop the local stack when you are finished:

```bash
docker compose -f deploy/docker-compose.yml down
```

## Why pg-kinetic

- **Control the connection boundary.** Accept PostgreSQL wire connections before they consume backend capacity.
- **Keep sessions conservative.** Use transaction pooling with explicit handling for session state and prepared statements.
- **Route and protect traffic.** Apply routing, bounded queues, timeouts, and overload behavior where a database operator can reason about them.
- **Operate from signals.** Inspect health, readiness, metrics, and admin views without adding application-side instrumentation first.
- **Test the behavior.** Compatibility, regression, and benchmarking workflows keep protocol changes and performance claims reviewable.

## From Local Stack To Production

| Stage | Use it for | Start here |
| --- | --- | --- |
| Local Compose | Development, integration checks, and operator familiarization | [Quickstart](docs/quickstart.md) |
| Configured container | A controlled environment with your own PostgreSQL backend | [Installation](docs/installation.md) and [Configuration](docs/configuration.md) |
| Kubernetes | Rendering and validating the local Helm chart | [Kubernetes deployment](docs/kubernetes.md) |
| Production rollout | Readiness, drain, migration, and rollback planning | [Production runtime](docs/production-runtime.md) and [Migration](docs/migration.md) |

Public Docker Hub, GHCR, and Helm repository artifacts are published after the first version tag. Until then, build from this repository or use the local chart as described in the installation guide.

## Operator Workflow

After the quickstart, these are the usual next steps:

1. Set your PostgreSQL backend and connection limits in `pg-kinetic.toml`.
2. Verify readiness and run a representative query through port `6432`.
3. Inspect pool state through the admin listener and scrape metrics from port `9090`.
4. Exercise timeout, backpressure, and rollback behavior before changing production traffic.

The [configuration guide](docs/configuration.md), [admin reference](docs/admin.md), [metrics catalog](docs/metrics.md), and [health and drain guide](docs/health-and-drain.md) cover those steps in detail.

## Capabilities

| Area | What pg-kinetic provides |
| --- | --- |
| Connections | PostgreSQL wire protocol proxying, transaction pooling, and virtual session handling |
| Routing | Read routing and route-aware capacity controls |
| Reliability | Health, readiness, bounded waiting, timeouts, and explicit overload responses |
| Observability | PostgreSQL-protocol admin views, Prometheus metrics, and trace configuration |
| Verification | Compatibility tests, regression workflows, benchmark tooling, and deployment assets |

Sharding, policy, mirroring, compatibility, benchmarking, and packaging also have dedicated documentation. Treat preview tooling separately from the live traffic path when evaluating a deployment.

## Documentation

**Get running**

- [Installation](docs/installation.md)
- [Quickstart](docs/quickstart.md)
- [Configuration](docs/configuration.md)
- [CLI reference](docs/commands.md)

**Operate safely**

- [Transaction pooling and virtual sessions](docs/transaction-pooling.md)
- [Backpressure](docs/backpressure.md)
- [Prepared statements](docs/prepared-statements.md)
- [TLS and authentication](docs/tls-and-auth.md)
- [Health, readiness, and drain](docs/health-and-drain.md)
- [Troubleshooting](docs/troubleshooting.md)

**Validate changes**

- [Architecture](docs/architecture.md)
- [Compatibility matrix](docs/compatibility.md)
- [Regression workflow](docs/regression.md)
- [Benchmarking](docs/benchmarking.md)
- [Testing](docs/testing.md)

The published documentation is available at [docs.pgkinetic.dev](https://docs.pgkinetic.dev).

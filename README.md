# pg-kinetic

pg-kinetic is a low-overhead PostgreSQL wire proxy for high-concurrency applications.

It provides PostgreSQL wire proxying, transaction pooling, routing, sharding, policy checks, mirroring, adaptive operations, runtime lifecycle controls, TLS/authentication, health endpoints, admin views, and metrics.

## Public Docs

- [Documentation overview](docs/index.mdx)
- [Installation](docs/installation.md)
- [Quickstart](docs/quickstart.md)
- [Configuration guide](docs/configuration.md)
- [CLI reference](docs/commands.md)
- [Architecture guide](docs/architecture.md)
- [Transaction pooling and virtual sessions](docs/transaction-pooling.md)
- [Backpressure guide](docs/backpressure.md)
- [Prepared statements guide](docs/prepared-statements.md)
- [Admin reference](docs/admin.md)
- [Read routing guide](docs/read-routing.md)
- [Sharding guide](docs/sharding.md)
- [Policy guide](docs/policy.md)
- [Mirroring guide](docs/mirroring.md)
- [Adaptive operations guide](docs/adaptive-ops.md)
- [Metrics catalog](docs/metrics.md)
- [Production runtime guide](docs/production-runtime.md)
- [TLS and authentication](docs/tls-and-auth.md)
- [Health, readiness, and drain](docs/health-and-drain.md)
- [Kubernetes deployment](docs/kubernetes.md)
- [Troubleshooting](docs/troubleshooting.md)
- [Benchmarking guide](docs/benchmarking.md)
- [Regression workflow](docs/regression.md)
- [Compatibility matrix](docs/compatibility.md)
- [Testing guide](docs/testing.md)
- [Documentation site workflow](docs-site/README.md)

## Install

Use the published container image for production deployments:

```bash
docker run --rm \
  --name pg-kinetic \
  -p 6432:6432 \
  -p 7000:7000 \
  -p 9090:9090 \
  -p 9091:9091 \
  -v "$PWD/pg-kinetic.toml:/etc/pg-kinetic/pg-kinetic.toml:ro" \
  hookwoods/pg-kinetic:latest \
  --config-file /etc/pg-kinetic/pg-kinetic.toml
```

For Compose:

```bash
docker compose -f deploy/docker-compose.yml up -d
```

For Kubernetes:

```bash
helm repo add pgkinetic https://helm.pgkinetic.dev
helm repo update
helm install pg-kinetic pgkinetic/pg-kinetic \
  --set image.tag=0.1.0 \
  --values pg-kinetic-values.yaml
```

See [Installation](docs/installation.md) for Docker, Docker Compose, and Kubernetes options.

## Configure

pg-kinetic reads a TOML configuration file through `--config-file` or `PG_KINETIC_CONFIG_FILE`.

Start from [Configuration](docs/configuration.md), which includes a complete production template and explains every top-level section.

## Operate

Applications connect to the proxy over the PostgreSQL protocol:

```bash
PGSSLMODE=disable psql "postgres://app_user@127.0.0.1:6432/app_db" -c "select 1;"
```

Admin views use the PostgreSQL protocol:

```bash
psql "postgres://admin@127.0.0.1:7000/postgres" -c "SHOW POOLS;"
psql "postgres://admin@127.0.0.1:7000/postgres" -c "SHOW CLIENTS;"
```

Health endpoints use HTTP:

```bash
curl -fsS http://127.0.0.1:9091/healthz
curl -fsS http://127.0.0.1:9091/readyz
```

## Release Images

The container workflow publishes multi-platform images to Docker Hub and GitHub Container Registry when a version tag matching `v*.*.*` is pushed.

Tags follow the release version, the major/minor series, and `latest` on version tags.

The Helm workflow publishes chart releases for the same version tags and updates the chart repository index for `https://helm.pgkinetic.dev`.

## Validate

Use the docs, compatibility, regression, and benchmark guides for validation workflows:

- [Testing guide](docs/testing.md)
- [Compatibility matrix](docs/compatibility.md)
- [Regression workflow](docs/regression.md)
- [Benchmarking guide](docs/benchmarking.md)

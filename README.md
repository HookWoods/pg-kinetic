# pg-kinetic

pg-kinetic is a PostgreSQL wire proxy for connection pooling, conservative session handling, read routing, admin inspection, health checks, metrics, and regression tooling.

The repository also contains preview tools for sharding, policy, mirroring, compatibility, benchmarking, and deployment packaging. Preview tooling is documented separately from live traffic behavior.

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
- [Migration and rollback](docs/migration.md)
- [Kubernetes deployment](docs/kubernetes.md)
- [Troubleshooting](docs/troubleshooting.md)
- [Benchmarking guide](docs/benchmarking.md)
- [Regression workflow](docs/regression.md)
- [Compatibility matrix](docs/compatibility.md)
- [Testing guide](docs/testing.md)
- [Documentation site workflow](docs-site/README.md)

## Current Install Path

Build the local container image from this checkout:

```bash
docker build -t pg-kinetic:local .
```

Run it with a mounted config:

```bash
docker run --rm \
  --name pg-kinetic \
  -p 6432:6432 \
  -p 7000:7000 \
  -p 9090:9090 \
  -p 9091:9091 \
  -v "$PWD/pg-kinetic.toml:/etc/pg-kinetic/pg-kinetic.toml:ro" \
  pg-kinetic:local \
  --config-file /etc/pg-kinetic/pg-kinetic.toml
```

Use [Quickstart](docs/quickstart.md) for an end-to-end local check.

## Future Release Images

When a version tag matching `v*.*.*` is pushed, the container workflow publishes Docker Hub and GitHub Container Registry images.

When the first chart release is published, `https://helm.pgkinetic.dev/index.yaml` becomes the Helm repository index.

Do not treat release image or Helm repository commands as available until the corresponding tag workflow has completed.

## Validate

Use the docs, compatibility, regression, and benchmark guides for validation workflows:

- [Testing guide](docs/testing.md)
- [Compatibility matrix](docs/compatibility.md)
- [Regression workflow](docs/regression.md)
- [Benchmarking guide](docs/benchmarking.md)

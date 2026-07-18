# CLI Reference

The `pg-kinetic` binary can run the proxy or execute operator commands such as preflight, route preview, policy preview, compatibility checks, regression runs, profiling, and benchmark validation.

Production examples assume the installed binary or container entrypoint is named `pg-kinetic`.

## Run The Proxy

```bash
pg-kinetic --config-file /etc/pg-kinetic/pg-kinetic.toml
```

Container:

```bash
docker run --rm \
  -v "$PWD/pg-kinetic.toml:/etc/pg-kinetic/pg-kinetic.toml:ro" \
  hookwoods/pg-kinetic:0.1.0 \
  --config-file /etc/pg-kinetic/pg-kinetic.toml
```

## Preflight

Validate a config before rollout:

```bash
pg-kinetic preflight --config /etc/pg-kinetic/pg-kinetic.toml
```

Container:

```bash
docker run --rm \
  -v "$PWD/pg-kinetic.toml:/etc/pg-kinetic/pg-kinetic.toml:ro" \
  hookwoods/pg-kinetic:0.1.0 \
  preflight --config /etc/pg-kinetic/pg-kinetic.toml
```

Treat preflight errors as deployment blockers.

## Route Preview

Preview read-routing and sharding decisions without live traffic:

```bash
pg-kinetic route-preview \
  --config /etc/pg-kinetic/pg-kinetic.toml \
  --database billing \
  --user reporter \
  --application-name dashboard \
  --sql "select * from orders where tenant_id = 'tenant-a'"
```

The output is JSON with the selected route, shard id when known, backend role when known, and the decision reason.

## Policy Preview

Evaluate policy rules against a synthetic request context:

```bash
pg-kinetic policy-preview \
  --config /etc/pg-kinetic/pg-kinetic.toml \
  --database billing \
  --user reporter \
  --route primary \
  --shard tenant-a \
  --query-class read_candidate
```

The preview context contains redacted secret-bearing fields so policy and audit formatting can be tested safely.

## Compatibility Commands

```bash
pg-kinetic compat list
pg-kinetic compat run --target pg-kinetic --smoke
```

Use filters such as `--language`, `--library`, `--target`, and `--category` to narrow the matrix.

## Regression Commands

```bash
pg-kinetic regression list --manifest regression/manifest.toml
pg-kinetic regression run --manifest regression/manifest.toml --category smoke
```

Regression outputs are JSON and redact sensitive text before printing errors.

## Benchmark Commands

Benchmark commands are for controlled performance validation, not initial installation:

```bash
pg-kinetic benchmark validate --scenario bench/scenarios/benchmark-simple-query.toml
pg-kinetic benchmark run --scenario bench/scenarios/benchmark-simple-query.toml --dry-run
pg-kinetic benchmark compare --baseline bench/baselines/simple.json --current bench/results/simple.json
pg-kinetic benchmark score --baseline bench/baselines/simple.json --current bench/results/simple.json
```

Live load execution and local-noise interpretation are covered in [Benchmarking](./benchmarking.md).

## Profiling Commands

```bash
pg-kinetic profile validate
pg-kinetic profile run --scenario bench/scenarios/benchmark-simple-query.toml --kind flamegraph
```

Supported profile tools are validated locally because platform support differs between Windows, Linux, and developer machines.

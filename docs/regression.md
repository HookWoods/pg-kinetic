# Regression Platform

Regression cases live in `regression/manifest.toml`. The checked-in schema at
`regression/manifest.schema.json` defines the supported category, platform,
timeout, service, success-marker, and artifact-policy fields.

List matching cases without running them:

```sh
cargo run -p pg-kinetic -- regression list --manifest regression/manifest.toml --category benchmark
```

Run a filtered set and emit a redacted JSON report:

```sh
cargo run -p pg-kinetic -- regression run --manifest regression/manifest.toml --platform windows
```

The runner records `pass`, `fail`, `skip`, `timeout`, and `blocked`. A case is
blocked when it declares a service that is not present in the comma-separated
`PG_KINETIC_REGRESSION_SERVICES` environment variable. Shell output is used only
to check the success marker and is not included in the report. `large` artifacts
and explicit `--output` reports are accepted only when the destination is ignored
by Git; manifest artifact paths must be relative paths under `target/`.

Use `scripts/regression/run.sh` or `scripts/regression/run.ps1` for the same
command on Unix-like shells or PowerShell.

## Performance score

`benchmark score` compares benchmark report JSON. It evaluates p50, p95, p99,
p999, throughput, CPU/query, memory/client, error rate, checkout latency, and
prepared-cache hit rate. Missing values produce `missing-baseline`; lower is
better for latency, CPU, memory, and error rate, while higher is better for
throughput and prepared-cache hit rate.

```sh
cargo run -p pg-kinetic -- benchmark score \
  --baseline regression/baselines/performance-score.sample.json \
  --current regression/baselines/performance-score.sample.json \
  --format json
```

Use `--release` to return a nonzero exit code for `fail` or `missing-baseline`.
Score JSON does not include target DSNs and redacts credential-shaped paths or
error text before rendering.

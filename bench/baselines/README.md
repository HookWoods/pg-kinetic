# Performance Baselines

Store reviewed JSON reports from real workload measurements in this directory.

Validate the report schema with:

```bash
cargo run -p pg-kinetic -- benchmark validate --scenario bench/scenarios/benchmark-simple-query.toml
```

Do not commit or bless dry-run output as a performance baseline. Baselines in this directory must come from at least three live, interleaved Linux runs with one PostgreSQL instance per target.

Compare a current report against a baseline with:

```bash
cargo run -p pg-kinetic -- benchmark score \
  --baseline bench/baselines/simple-query.json \
  --current bench/results/simple-query.json --release
```

The score gate warns after a 5% regression and fails after a 10% regression for scored latency, throughput, CPU/query, memory/client, checkout latency, and prepared-cache-hit metrics. Error rate warns above `0.001` and fails above `0.01`.

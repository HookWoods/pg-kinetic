# Performance Baselines

Store reviewed JSON reports from real workload measurements in this directory.

Validate the report schema with:

```powershell
scripts/bench/run-performance.ps1 -Scenario bench/scenarios/benchmark-simple-query.toml -Output bench/baselines/simple-query.json
```

The helper emits a dry-run structural report only. Do not commit or bless dry-run output as a performance baseline.

Compare a current report against a baseline with:

```powershell
scripts/bench/compare-performance.ps1 -Baseline bench/baselines/simple-query.json -Current bench/results/simple-query.json
```

Comparison warns after a 5% regression and fails after a 10% regression for latency and throughput. Error rate warns above `0.001` and fails above `0.01`.

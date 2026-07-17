# Performance Baselines

Store reviewed JSON reports from `pg-kinetic benchmark run` in this directory.

Create a report with:

```powershell
scripts/bench/run-performance.ps1 -Scenario bench/scenarios/benchmark-simple-query.toml -Output bench/baselines/simple-query.json
```

Compare a current report against a baseline with:

```powershell
scripts/bench/compare-performance.ps1 -Baseline bench/baselines/simple-query.json -Current bench/results/simple-query.json
```

Comparison warns after a 5% regression and fails after a 10% regression for latency and throughput. Error rate warns above `0.001` and fails above `0.01`.

# Adaptive Operations

Adaptive control helps pg-kinetic turn runtime observations into recommendations and, when explicitly allowed, bounded applies.

## Recommendation Mode

Recommendation mode is the default.

- `adaptive_mode = "recommend"` records recommendations without applying them
- `SHOW ADAPTIVE` surfaces the latest recommendation and the current guardrails
- `pg_kinetic_adaptive_recommendations_total` tracks the recommendation stream

Use recommendation mode until the workload has enough history to justify a change.

## Guarded Apply Mode

Apply mode is opt-in and intentionally constrained.

- set `adaptive_mode = "apply"`
- set `adaptive_apply_enabled = true`
- provide a non-empty `adaptive_apply_allowlist`
- keep `adaptive_max_change_percent` within the accepted change envelope
- keep `adaptive_min_confidence` high enough to filter weak signals

Apply mode refuses unbounded changes, disallowed knobs, duplicate allowlist entries, and changes that exceed the configured percentage cap.

## Using Benchmark Outputs

Benchmark results should be the input to adaptive decisions, not the other way around.

- run a benchmark scenario before changing the adaptive mode
- compare the `pg_kinetic` target against direct PostgreSQL and the other baselines in the same scenario
- review the JSON output for `p50_ms`, `p95_ms`, `p99_ms`, `throughput_qps`, and `error_rate`
- keep the benchmark scenario file alongside the decision so later performance work can reproduce it

Later tuning work should use the same scenario name, driver, and comparison labels so results stay easy to compare.

## Operational Checklist

- Start in recommendation mode.
- Review `SHOW ADAPTIVE` and the adaptive metrics family before enabling apply.
- Move to guarded apply only after the benchmark outputs justify the change.
- Keep the allowlist small and specific.
- Re-run benchmarks after each applied change so the next decision has a fresh baseline.

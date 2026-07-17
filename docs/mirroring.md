# Mirroring

Mirroring lets pg-kinetic observe or shadow selected traffic without changing the primary request path. The feature is intentionally conservative and defaults to off.

## Safe Shadow Model

Mirroring is designed as an observation path:

- primary traffic keeps flowing through the production backend path
- mirrored work is best-effort and bounded by a timeout and an in-flight cap
- mirror failures do not rewrite the primary routing decision
- the mirror target should be isolated from the production target

The admin view `SHOW MIRRORING` summarizes the live mirror mode, sample rate, in-flight work, and outcome counters.

## Default Behavior

The default mirror mode is off.

- if mirroring is not enabled, the proxy keeps the mirror path disabled
- the default sample rate is `0.0`
- the default safety posture rejects unsafe target reuse
- the default target isolation check is on

The preflight command reports a warning when a mirror document is present but mirroring is still disabled.

## Target Isolation

Mirror targets should point at a system that can absorb shadow traffic without affecting production.

Recommended checks:

- use a distinct address for the mirror target
- set `mirror_target_isolated = true`
- keep `mirror_require_isolated_target = true`
- avoid pointing mirror traffic at the same backend address used for production

If the mirror target matches the production target and isolation is not declared, preflight fails.

## Sampling

Sampling controls how much eligible traffic is sent to the mirror path.

- `mirror_sample_rate = 0.0` disables sampled mirroring
- higher rates increase coverage but also increase mirror load
- the mirror timeout and in-flight limits should stay small enough to protect the production proxy

Use low sample rates first, then raise them only after the mirror target has proven stable.

## Unsupported Traffic

Mirroring is intentionally conservative around stateful or side-effect-heavy traffic.

Treat these as unsupported unless you have explicitly enabled and reviewed them:

- writes
- explicit transaction control
- `COPY`
- `LISTEN/NOTIFY`
- temporary table mutation
- session mutation

The mirror safety section exists to keep these classes from slipping into a broad rollout by accident.

## Telemetry And Redaction

Mirror telemetry is summarized, not replayed in full.

- `SHOW MIRRORING` exposes counters and rates, not query text
- mirror metrics are bounded by mode and outcome
- preflight only reports the presence of configuration problems
- no raw SQL, credentials, or backend secrets should be exposed in mirror diagnostics

## Rollout Checklist

- Start with mirroring disabled and confirm the admin view reports `off`.
- Run preflight on the config that includes the mirror section.
- Use an isolated target before raising the sample rate.
- Keep the sample rate low until the mirror target proves stable.
- Review telemetry for dropped, skipped, rejected, and timed-out mirror work before expanding the rollout.

---
title: "Adaptive Operations"
description: "How pg-kinetic records adaptive runtime recommendations, what apply mode does today, and why active settings are not mutated automatically."
keywords:
  - pg-kinetic adaptive operations
  - PostgreSQL proxy tuning
  - adaptive control
  - runtime recommendations
---

# Adaptive Operations

Adaptive operations are recommendation and simulation tooling today. The controller records recommendations and proposed before/after values; it does not mutate the active proxy settings.

## Current Behavior

When `adaptive_enabled = true`, the adaptive controller:

- collects runtime signals
- builds recommendations
- records recommendation snapshots
- evaluates configured guardrails
- records an outcome snapshot

`adaptive_mode = "apply"` does not change live settings. It only changes whether the recorded outcome is accepted, rejected, skipped, or recommended according to the allowlist and guardrails.

## Config

```toml
[runtime.production]
adaptive_enabled = true
adaptive_mode = "recommend"
adaptive_window_ms = 60000
adaptive_min_confidence = 0.8
adaptive_apply_enabled = false
adaptive_apply_allowlist = []
adaptive_max_change_percent = 10
```

Adaptive fields are flattened into `[runtime.production]`. Do not put them under `[runtime.production.adaptive]`; that is not the runtime parser contract.

| Field | Type | Default | Effect |
| --- | --- | --- | --- |
| `adaptive_enabled` | bool | `false` | Starts the adaptive controller when true. |
| `adaptive_mode` | enum | `recommend` | `recommend` records recommendations; `apply` records accepted/rejected simulated apply outcomes. |
| `adaptive_window_ms` | integer | `60000` | Controller tick interval. Must be greater than zero. |
| `adaptive_min_confidence` | float | `0.8` | Rejects recommendations below this confidence. Valid range is `0.0` to `1.0`. |
| `adaptive_apply_enabled` | bool | `false` | Required for `adaptive_mode = "apply"`. Does not mutate live settings. |
| `adaptive_apply_allowlist` | list | `[]` | Required and duplicate-free for `adaptive_mode = "apply"`. |
| `adaptive_max_change_percent` | integer | `10` | Valid range is `1` to `100`. Bounds simulated change size. |

## Failure Modes

| Condition | Result |
| --- | --- |
| `adaptive_window_ms = 0` | Config parse fails. |
| `adaptive_min_confidence` outside `0.0..=1.0` | Config parse fails. |
| `adaptive_mode = "apply"` with `adaptive_apply_enabled = false` | Config parse fails. |
| `adaptive_mode = "apply"` with an empty allowlist | Config parse fails. |
| Duplicate allowlist entries | Config parse fails. |
| Recommendation confidence below threshold | Outcome is rejected. |
| Recommendation knob not in allowlist | Outcome is rejected. |
| Recommendation exceeds max change percent | Outcome is rejected. |

## Operator Guidance

Use adaptive output as a review signal. Change live settings through configuration and a controlled rollout after benchmark or production evidence supports the change.

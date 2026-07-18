---
title: "Mirroring"
description: "Current mirroring status in pg-kinetic, including disabled live traffic behavior, model surfaces, validation limits, and production alternatives."
keywords:
  - pg-kinetic mirroring
  - PostgreSQL traffic mirroring
  - shadow traffic
  - preview tooling
---

# Mirroring Status

Mirroring is not active in the live proxy path today. The proxy constructs a disabled mirror dispatcher even when production flags are present.

Do not deploy pg-kinetic expecting traffic shadowing until the runtime wires mirror config into the request path.

## What Exists

- mirror domain models
- mirror safety and sampling config structs
- mirror outcome recording types
- admin and metrics surfaces that can display mirror data when a runtime path records it

## What Does Not Work For Live Traffic

- no live traffic is sent to a mirror target
- mirror target config is not part of the main runtime `Config`
- `SHOW MIRRORING` can only reflect available in-process snapshots
- mirror metrics can stay zero because no mirror dispatcher is active

## Offline-Only Config Shape

Mirroring has parser and preflight fields, but the live proxy still constructs a disabled dispatcher. This file is useful only for parser/preflight checks:

```toml
[runtime.production]
mirroring_enabled = false

[mirror]
mirroring_enabled = false
mirror_mode = "off"
mirror_timeout_ms = 100
mirror_max_in_flight = 128
mirror_sample_rate = 0.0
mirror_writes_enabled = false
mirror_transactions_enabled = false
mirror_copy_enabled = false
mirror_listen_notify_enabled = false
mirror_temp_table_enabled = false
mirror_session_mutation_enabled = false
mirror_require_isolated_target = true
```

Preflight accepts this as disabled mirroring and reports a warning rather than enabling traffic shadowing.

## Future Runtime Contract

A production mirroring feature needs:

- explicit runtime config in the main `Config`
- target isolation validation
- sample-rate enforcement
- bounded timeout and in-flight limits
- clear unsupported traffic classes
- metrics that distinguish skipped, mirrored, timed out, and rejected work

Until then, treat mirroring docs and counters as implementation groundwork, not an operator feature.

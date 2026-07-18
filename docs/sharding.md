---
title: "Sharding"
description: "Current sharding status in pg-kinetic, including route-preview tooling, model constraints, live traffic limitations, and safe production alternatives."
keywords:
  - pg-kinetic sharding
  - PostgreSQL sharding preview
  - route preview
  - database proxy sharding
---

# Sharding Preview

Sharding is preview tooling today. The core route-map models and `route-preview` command exist, but the main proxy runtime does not expose live sharding traffic configuration.

Do not publish sharding as a production traffic feature until the proxy accepts sharding config in `Config` and applies it in the request path.

## What Works Now

- `pg-kinetic route-preview` can evaluate sharding decisions offline.
- Core models exist for route maps, shard ids, multi-shard policy, shard hints, and conservative shard-key extraction.
- Admin and metrics code can represent sharding snapshots when data is recorded.

## Not Supported For Live Traffic

- `[sharding]` is not part of the main runtime `Config`.
- Config reload does not hot-swap live route maps for proxy traffic.
- The proxy runtime uses the first effective route and does not dispatch production traffic across independent shard targets.
- There is no operator, CRD, or controller-managed shard migration.

## Route Preview

Use the preview command without starting the proxy listener:

```bash
pg-kinetic route-preview \
  --config preview-sharding.toml \
  --database billing \
  --user reporter \
  --sql "select * from public.orders where tenant_id = 'tenant-a'"
```

The preview config must match the parser contract used by the command. Treat preview output as an explanation tool, not a production routing guarantee.

Minimal offline-only preview file:

```toml
[sharding]
sharding_enabled = true
multi_shard_policy = "first_match"
route_preview_enabled = true

[[sharding.route_maps]]
scope = { kind = "schema_table", schema = "public", table = "orders" }
strategy = { kind = "hash" }
targets = [
  { kind = "primary", shard_id = "orders-a" },
  { kind = "replicas", shard_id = "orders-b" },
]
```

Tested preview invocation:

```bash
pg-kinetic route-preview \
  --config preview-sharding.toml \
  --database billing \
  --user reporter \
  --sql "select * from public.orders where tenant_id = 'tenant-a'"
```

Expected JSON shape:

```json
{
  "ok": true,
  "route": "billing/reporter/<none>/default",
  "shard_id": "orders-b",
  "backend_role": "replica",
  "reason": "hash_match",
  "shard_reason": "hash_match"
}
```

## Conservative Extraction Model

The shard-key extraction code is intentionally narrow:

- simple statement shapes are easier to classify than complex SQL
- ambiguous expressions are rejected
- hints are advisory and still need validation
- multi-shard behavior must fail closed until a live runtime contract exists

## Production Guidance

Use application-side routing, direct PostgreSQL routing, or another proven sharding layer for production sharding today. Keep pg-kinetic sharding docs and demos labeled as preview until live runtime wiring exists and compatibility tests cover it.

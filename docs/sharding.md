# Sharding Guide

pg-kinetic sharding is designed to stay conservative first and fast second. The proxy only routes to a shard when it can explain why that shard is safe for the current statement, session, and route map.

## Mental Model

- A route key groups traffic by database, user, application name, client address, and query class.
- A route map binds one scope to one sharding strategy and a set of shard targets.
- The proxy prefers a single shard over fan-out, and it falls back when the input is ambiguous.
- The admin plane shows the live route-map and shard snapshot without checking out a backend.

## Route-Map Scopes

Route-map scopes are the first filter. A route map only applies when the incoming request matches its scope.

- `database/user` matches the authenticated database and user pair.
- `application_name` matches the startup packet application name.
- `schema.table` matches a concrete table reference and enables shard-key extraction.
- `tenant_key` matches an explicit shard hint and is useful when the tenant id is already in the statement text.

Route maps with overlapping scopes need an explicit priority so the match order stays predictable.

## Sharding Strategies

pg-kinetic supports three route-map strategies:

- `hash` spreads keys across the configured targets with a deterministic hash.
- `range` matches a key against the first inclusive or exclusive boundary pair that contains it.
- `list` matches an explicit value list and is the clearest fit for direct tenant-to-shard mappings.

Strategy selection is per route map, not global. That keeps one tenant family on range routing while another family uses exact lists.

## Explicit Hints

Operators can override the classifier with SQL comments at the start of a statement:

- `/* pg-kinetic: shard=tenant-a */`
- `/* pg-kinetic: tenant=tenant-a */`
- `/* pg-kinetic: route=billing_eu */`

Hints are advisory, not a trust boundary. They still go through route-map validation and multi-shard policy checks.

## Conservative Shard-Key Extraction

Automatic shard-key extraction is intentionally narrow:

- it understands simple `SELECT`, `INSERT`, `UPDATE`, and `DELETE` statements
- it resolves a single matching table definition
- it requires a direct equality or value list on the configured shard key column
- it rejects ambiguous cases such as `OR`, complex expressions, or parameter-only keys

When extraction cannot prove a shard key, the proxy keeps routing conservative and falls back to the non-sharded path.

## Transaction Affinity

Once a transaction picks a shard, pg-kinetic keeps that transaction on the same shard until the transaction ends.

- The first routed statement establishes the affinity.
- Later statements that stay on the same shard are accepted.
- A statement that tries to move the transaction to another shard is rejected by default.
- Read-only transactions can still use replicas inside the selected shard.

This is what keeps a transaction from drifting across shards in the middle of a unit of work.

## Prepared Statements

Prepared statements follow the same shard rules as regular statements.

- The proxy still applies shard extraction and transaction affinity checks.
- A prepared statement does not bypass the conservative classifier.
- If the proxy cannot explain a shard choice for the prepared SQL, it falls back instead of guessing.

## Multi-Shard Policy

The multi-shard policy controls what happens when a route map would touch more than one target.

- `reject` is the safest default and fails closed.
- `first_match` uses the first matching target in configuration order.
- `fan_out` allows the proxy to keep more than one target in play when the route map is explicitly designed for it.

Use `reject` until you have a good reason to broaden the fan-out surface.

## Route Preview

The route preview command evaluates sharding offline without starting the proxy listener:

```powershell
cargo run -p pg-kinetic -- route-preview `
  --config path\to\sharding.toml `
  --database billing `
  --user reporter `
  --sql "select * from public.orders where tenant_id = 'tenant-a'"
```

The config file only needs the `[sharding]` section for previewing route choice.

## Route-Map Hot Reload

When config reload is enabled and a config file is present, pg-kinetic reloads sharding state from disk and swaps the active route maps atomically.

- Valid reloads replace the active sharding snapshot in place.
- Invalid reloads are rejected and leave the current config running.
- Removing a shard that still has active transactions requires an explicit migration override.
- `SHOW ROUTES`, `SHOW ROUTE MAPS`, `SHOW SHARDS`, and `SHOW MIGRATIONS` reflect the live snapshot after reload.

## Shard Lifecycle States

`SHOW SHARDS` reports the current lifecycle state for each shard.

- `active` means the shard can accept normal traffic.
- `draining` means the shard is being wound down.
- `readonly` means writes are blocked but reads may still be allowed.
- `disabled` means the shard is out of service.

The lifecycle state is a live operational signal, not just a config label.

## Migration Safety Checks

Shard migrations are gated on a safety report before the route-map reload can move traffic.

- active client ids must be accounted for
- prepared statements must be accounted for
- open transactions must be accounted for
- the last required LSN must be tracked when freshness matters

If the safety report still shows active work on a shard that is being removed, pg-kinetic keeps the old route map until the override is explicit.

## Rollout Checklist

1. Start with `sharding_enabled = false` and verify the route preview output.
2. Add route maps with a single clear scope and a single known target first.
3. Check `SHOW ROUTE MAPS` and `SHOW SHARDS` after startup.
4. Watch `pg_kinetic_route_map_reload_total` and `pg_kinetic_route_map_generation` during config changes.
5. Turn on transaction-affinity-sensitive workloads only after the shard key extraction looks stable.
6. Keep the multi-shard policy on `reject` until the fallback path has been exercised.

## Known Limitations

- shard-key extraction is conservative by design and misses many valid SQL shapes
- explicit hints are useful for operators but they do not replace validation
- hash routing is deterministic, but it still depends on the configured route order
- hot reload only updates the route-map snapshot; it does not rewrite application SQL
- multi-shard read/write behavior remains intentionally narrow until the route map says otherwise

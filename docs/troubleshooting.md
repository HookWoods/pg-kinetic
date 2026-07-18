# Troubleshooting

This guide maps common local and production symptoms to the first checks that usually matter.

## Client Cannot Connect

Check the listener and TLS/auth mode first:

```bash
cargo run -p pg-kinetic -- --config-file pg-kinetic.toml
```

Then verify:

- `listen_addr` is bound to the expected interface and port
- `client_tls_mode` matches the client's `sslmode` or `PGSSLMODE`
- `auth_mode` matches the expected authentication path
- `auth_users_file` exists when local auth is enabled
- firewall, container port, or Kubernetes service rules expose the listener

For local plaintext `psql` checks, set `PGSSLMODE=disable`.

## Backend Checkout Times Out

Inspect pool and route pressure:

```sql
SHOW POOLS;
SHOW BACKPRESSURE;
SHOW LIMITS;
```

Then check:

- `max_backends` is high enough for the workload
- `max_route_in_flight` is not too low for one route
- backend PostgreSQL accepts new connections
- slow queries are not occupying every backend
- route health is not forcing all reads back to the primary

## Read Traffic Does Not Reach Replicas

Confirm the query is safe to route:

- read routing is enabled for the route
- the SQL is classified as read-only or has an explicit replica hint
- the session did not recently write data that requires primary freshness
- replica lag is within `max_replica_lag_ms`
- fallback policy is not forcing primary

Use:

```bash
cargo run -p pg-kinetic -- route-preview --config pg-kinetic.toml --database app --user app --sql "select * from accounts"
```

## Shard Is Not Selected

Shard extraction is conservative. A query may route to the primary/default shard when:

- the shard key is missing
- the predicate is too complex
- bind values cannot be matched safely
- multi-shard behavior is not allowed
- a policy override changes the target

Use [Sharding Guide](./sharding.md) and `route-preview` to inspect the decision.

## Prepared Statement Errors

Prepared statement behavior depends heavily on the client driver. Check:

- whether the driver uses named or unnamed statements
- whether the statement was closed or invalidated
- whether a backend reset dropped backend-local statement state
- whether a failed transaction left the backend pinned

Run the compatibility smoke for the affected language:

```bash
cargo run -p pg-kinetic -- compat run --target pg-kinetic --smoke
```

## Docs Build Fails

Run the docs build from the repo root:

```bash
npm.cmd --prefix docs-site run build
```

Broken links fail the build. Prefer relative markdown links for docs in `docs/`, and keep generated/private workflow material out of the public tree.

## Windows Build Runs Out Of Resources

If a broad Rust workspace test fails with MSVC or linker resource errors, retry serially:

```powershell
$env:CARGO_BUILD_JOBS='1'
cargo test --workspace -j 1
```

This is slower but avoids parallel linker pressure on constrained Windows machines.


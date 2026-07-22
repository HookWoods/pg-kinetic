---
title: "Migration And Rollback"
description: "Migration and rollback guide for introducing pg-kinetic in front of PostgreSQL applications with validation, cutover, and recovery steps."
keywords:
  - pg-kinetic migration
  - PostgreSQL proxy rollout
  - database proxy rollback
  - connection pooler migration
---

# Migration And Rollback

Use pg-kinetic as a connection-string change first. Keep the original PostgreSQL path available until the proxy has passed health, query, and driver checks for the workload.

## Direct PostgreSQL To pg-kinetic

1. Keep the existing PostgreSQL endpoint running.
2. Start pg-kinetic with `backend_addr` pointing at that endpoint.
3. Verify `GET /readyz` returns `ready`.
4. Run the application smoke query through pg-kinetic.
5. Move one low-risk client or one canary deployment to the pg-kinetic host and port.
6. Watch connection errors, SQLSTATEs, checkout wait, backend pins, and readiness.
7. Increase traffic only when the canary has no new driver or session-state errors.

For the 1.0.0-rc.1 rehearsal, deploy the immutable image tag
`ghcr.io/hookwoods/pg-kinetic:1.0.0-rc.1` with Helm chart version `1.0.0-rc.1`
and record the resolved image digest. Start at 1% traffic. The health probes
are `/healthz` for liveness and `/readyz` for readiness on port `9091`.
Allow the configured 45-second drain timeout to complete and use a 65-second
or longer termination grace period.

Abort after two consecutive five-minute windows when session or checkout
errors exceed 1%, PostgreSQL connection errors or unexpected SQLSTATEs exceed
0.1%, p95 latency is more than 25% above baseline and over 250 ms, checkout
wait p95 exceeds 100 ms, or `/readyz` remains `503` for more than two probes.

During a single-primary outage, readiness returns `503` and existing backend
sessions are discarded. A stateless, classified read can be retried once when no
response byte was sent; writes and uncertain or partially forwarded requests are
not replayed and surface PostgreSQL connection failure SQLSTATE `08006`. Keep the
original endpoint available for rollback during this test.

Connection-string change:

```text
postgres://app_user@postgres.example.internal:5432/app_db
postgres://app_user@pg-kinetic.example.internal:6432/app_db
```

Consolidating two database/user pairs through one proxy:

```toml
[[pools]]
database = "app_a"
user = "app_a"
backend_addr = "postgres.example.internal:5432"

[[pools]]
database = "app_b"
user = "app_b"
backend_addr = "postgres.example.internal:5432"
```

Both application pairs connect to the same pg-kinetic listener, and pg-kinetic routes each startup database/user pair to the shared PostgreSQL backend service explicitly. Duplicate `(database,user)` entries are rejected, and the global `capacity.max_backends` limit remains aggregate across the configured pools.

v1 has one shared backend service identity. Pool entries do not select or infer separate backend credentials; pass-through authentication retains its existing credential-forwarding behavior.

## PgBouncer To pg-kinetic

1. Match the application-visible host, user, database, TLS mode, and password behavior.
2. Check prepared statement behavior in staging. Different poolers have different prepared-statement semantics.
3. Check transaction-pooling-sensitive features: temp tables, advisory locks, `LISTEN/NOTIFY`, `COPY`, and long transactions.
4. Move one canary client from PgBouncer to pg-kinetic.
5. Keep PgBouncer available as the rollback target until the canary and a full driver smoke pass succeed.

## Canary Signals

Watch these before increasing traffic:

- `/readyz` response
- application connection failures
- `pg_kinetic_pool_checkout_wait_ms`
- `pg_kinetic_backpressure_events_total`
- `pg_kinetic_backend_pin_total`
- `pg_kinetic_backend_recovery_total`
- `pg_kinetic_backend_sqlstate_total`
- driver-specific errors in application logs

## Rollback Triggers

Rollback to the previous endpoint when any of these occur:

- `/readyz` stays `503`
- connection errors increase after moving traffic
- checkout wait grows without a known capacity reason
- backend recovery or discard outcomes spike
- a required client driver fails its smoke contract
- session-state behavior differs from the previous pooler
- TLS or authentication failures appear after the cutover

Rollback is a connection-string or Service selector change. Do not rely on config reload for backend address, listener, capacity, auth mode, TLS mode, or route changes.

## Known Incompatibility Checks

Check these before broad rollout:

- prepared statements with transaction pooling
- temp tables
- advisory locks
- `COPY`
- `LISTEN/NOTIFY`
- long idle transactions
- client TLS mode and `sslmode`
- backend TLS verification and server name
- SCRAM user-store format when local auth is enabled
- read-routing freshness behavior when replicas are configured

## After Rollback

Keep pg-kinetic running but remove user traffic. Capture:

- config file used for the rollout
- exact client driver and version
- SQLSTATE and application error text
- admin snapshots from `SHOW CLIENTS`, `SHOW SERVERS`, `SHOW POOLS`, and `SHOW PINNING`
- relevant metrics around the failure window

Use that evidence to decide whether the issue is configuration, unsupported client/session behavior, capacity, or a proxy bug.

Restore direct PostgreSQL by reverting the application connection string to the
original endpoint, for example:

```text
postgres://app_user@postgres.example.internal:5432/app_db
```

Alternatively, restore the previous validated pg-kinetic image tag and Helm
chart version with `helm upgrade --install ... --version <previous-chart-version>
--set image.tag=<previous-image-tag>`. Verify `/readyz` and the application
smoke query before returning traffic.

---
title: "Compatibility Matrix"
description: "Compatibility status for PostgreSQL client behavior through pg-kinetic, including supported protocol paths, preview cases, and validation commands."
keywords:
  - PostgreSQL client compatibility
  - pg-kinetic compatibility
  - PostgreSQL wire compatibility
  - driver testing
  - PHP PostgreSQL driver
---

# Compatibility Matrix

For engineers checking whether PostgreSQL clients and protocol behaviors are covered before a rollout.

pg-kinetic keeps a cross-language compatibility matrix for PostgreSQL clients. The matrix can run the same behavior contract against direct PostgreSQL and pg-kinetic so reports can compare proxy behavior with the baseline server path.

The stable 1.0 support boundary is defined in the [Stable 1.0 Release Contract](./release-contract.md). It covers PostgreSQL 16 and 18 as tested release targets; other server versions require a matching compatibility run before a support claim is made.

## Current Compatibility Status

| Area | Status |
| --- | --- |
| PostgreSQL server versions | Tested only where the local compatibility or CI stack starts PostgreSQL. Do not infer broad version coverage from docs alone. |
| Simple query protocol | Covered by smoke checks. |
| Extended query protocol | Covered by prepared-query smoke checks where each driver suite supports it. |
| Cancellation requests | Supported for bound backends; unknown cancel keys are dropped silently. |
| Backend parameter status | Captured from backend startup and replayed to clients on pooled connection reuse. |
| COPY, LISTEN/NOTIFY, temp tables, advisory locks | Treated as stateful or pinning-sensitive behavior; not broad compatibility guarantees. |
| Sharding and policy-denial cases | Preview/opt-in only; not default live proxy compatibility. |

## Protocol Details

### Cancellation

PostgreSQL cancellation uses a separate startup packet containing a process id and secret key. pg-kinetic issues client-facing cancel keys during startup, binds them to the currently checked-out backend key while a backend is assigned, and forwards cancellation by opening a backend cancel connection with the backend's own process id and secret key.

If the cancel key is unknown or no backend target is currently bound, pg-kinetic drops the cancel request without surfacing an error to the canceling connection. This matches the PostgreSQL cancellation shape where the cancel connection is not the query connection.

### `server_version` And Parameter Status

pg-kinetic forwards backend `ParameterStatus` fields such as `server_version`, `client_encoding`, and `standard_conforming_strings` to the client. When a pooled backend is reused, the proxy replays the captured parameter status in the synthetic startup-ready response so clients still see the backend parameters they expect.

Do not use `server_version` as a broad compatibility guarantee by itself. The release contract names the PostgreSQL versions covered by the current validation matrix; clients can observe a backend-reported version even when that exact version has not been promoted to a documented support target.

## Libraries

| Language | Libraries |
| --- | --- |
| Rust | `tokio-postgres`, `sqlx`, optional `diesel` |
| Go | `pgx`, `database/sql` through the pgx stdlib adapter |
| Java | JDBC `DriverManager`, PostgreSQL `DataSource`, HikariCP, Spring JDBC, optional Spring Boot DataSource, optional jOOQ |
| JavaScript and TypeScript | `pg`, Prisma where available, Kysely |
| Python | `psycopg` 3, `asyncpg`, SQLAlchemy |
| PHP | PDO PostgreSQL, `pgsql`, framework database layers that use the PostgreSQL protocol |
| .NET | Npgsql, optional EF Core provider |
| C | `libpq` |
| C++ | optional `libpqxx` |

Optional suites keep their build files in the tree and report a structured skip
when the local toolchain or system library is not available.

## Local Commands

List configured suites:

```bash
cargo run -p xtask -- compat --list
```

Run smoke suites for each target:

```bash
cargo run -p xtask -- compat --target direct-postgres --smoke
cargo run -p xtask -- compat --target pg-kinetic --smoke
```

Run a language-specific check:

```bash
cargo run -p xtask -- compat --language java --target pg-kinetic
```

PowerShell parity:

```powershell
powershell.exe -ExecutionPolicy Bypass -File scripts\compat\run.ps1 -Language java -Target pg-kinetic
```

## Live Runs

Structural report generation does not require a database. Live suite execution
requires both target services and explicit opt-in:

```bash
docker compose -f bench/compose.yml up -d --build postgres pg-kinetic
export PG_KINETIC_COMPAT_LIVE=1
export PG_KINETIC_COMPAT_SERVICES=direct-postgres,pg-kinetic
export DATABASE_URL_DIRECT=postgres://postgres:postgres@127.0.0.1:55432/pgkinetic
export DATABASE_URL_PROXY=postgres://postgres:postgres@127.0.0.1:58432/pgkinetic
psql "$DATABASE_URL_DIRECT" -f compat/common/schema.sql
psql "$DATABASE_URL_DIRECT" -f compat/common/seed.sql
cargo run -p xtask -- compat --language rust --target pg-kinetic
```

Use `DATABASE_URL_DIRECT` and `DATABASE_URL_PROXY` for suite-level connection
strings when a language runner needs a non-default address.

## Reports

Every suite emits normalized JSON with the stable fields `language`, `library`,
`version`, `target`, `outcome`, `skip_reason`, and `error_summary`. Reports may
also include `duration_ms` and suite-specific case details. Missing
toolchains, unavailable services, and unsupported library features are `skip`
outcomes with stable reasons; they are never converted to synthetic passes.

Large per-suite artifacts are written only under `target/compat/`, which is
ignored by Git.

## CI

Pull requests start the local PostgreSQL and pg-kinetic stack, load the shared
fixtures, and run each language smoke matrix against both direct PostgreSQL and
pg-kinetic. Manual and scheduled compatibility jobs also select the framework
category for heavier Spring-style coverage.

## Behavior Contract

`compat/common/contract.toml` defines the shared cases:

- connection startup
- simple and parameterized queries
- prepared statements and invalidation
- commit and rollback
- pool reuse
- server error propagation
- TLS and authentication paths
- read routing and primary writes
- preview-only sharding and policy-denial cases when explicitly selected

Advanced cases require matching local pg-kinetic configuration and remain opt-in
so the default smoke matrix stays bounded.

## Adding A Library

Add the language project files under `compat/<language>/`, register a
compatibility entry in `regression/manifest.toml`, and keep reports in the
normalized shape. Required libraries must have direct PostgreSQL and pg-kinetic
coverage unless the library only exposes behavior through a framework wrapper.

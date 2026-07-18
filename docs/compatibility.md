---
title: "Compatibility Matrix"
description: "Compatibility status for PostgreSQL client behavior through pg-kinetic, including supported protocol paths, preview cases, and validation commands."
keywords:
  - PostgreSQL client compatibility
  - pg-kinetic compatibility
  - PostgreSQL wire compatibility
  - driver testing
---

# Compatibility Matrix

pg-kinetic keeps a cross-language compatibility matrix for PostgreSQL clients. The matrix can run the same behavior contract against direct PostgreSQL and pg-kinetic so reports can compare proxy behavior with the baseline server path.

## Current Compatibility Status

| Area | Status |
| --- | --- |
| PostgreSQL server versions | Tested only where the local compatibility or CI stack starts PostgreSQL. Do not infer broad version coverage from docs alone. |
| Simple query protocol | Covered by smoke checks. |
| Extended query protocol | Covered by prepared-query smoke checks where each driver suite supports it. |
| COPY, LISTEN/NOTIFY, temp tables, advisory locks | Treated as stateful or pinning-sensitive behavior; not broad compatibility guarantees. |
| Sharding and policy-denial cases | Preview/opt-in only; not default live proxy compatibility. |

## Libraries

| Language | Libraries |
| --- | --- |
| Rust | `tokio-postgres`, `sqlx`, optional `diesel` |
| Go | `pgx`, `database/sql` through the pgx stdlib adapter |
| Java | JDBC `DriverManager`, PostgreSQL `DataSource`, HikariCP, Spring JDBC, optional Spring Boot DataSource, optional jOOQ |
| JavaScript and TypeScript | `pg`, Prisma where available, Kysely |
| Python | `psycopg` 3, `asyncpg`, SQLAlchemy |
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

Every suite emits normalized JSON with `language`, `library`, `version`,
`target`, `outcome`, `duration_ms`, `skip_reason`, and `error_summary`. Missing
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

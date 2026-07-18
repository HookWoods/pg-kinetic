# Compatibility Matrix

The compatibility matrix checks pg-kinetic with production PostgreSQL client
libraries. Every suite can run against direct PostgreSQL and pg-kinetic, then
emits the same JSON report shape so regressions can compare target behavior.

## Environment

Use these variables for live runs:

- `PG_KINETIC_COMPAT_LIVE=1` enables command execution.
- `PG_KINETIC_COMPAT_SERVICES=direct-postgres,pg-kinetic` marks reachable services.
- `DATABASE_URL_DIRECT` points at direct PostgreSQL.
- `DATABASE_URL_PROXY` points at pg-kinetic.
- `PG_KINETIC_COMPAT_TARGET` is set by the runner for each suite.

When live mode is not enabled or a toolchain is missing, suites return a
structured skip with a stable reason. Structural listing and report validation
do not require language runtimes.

Load the shared schema and seed data before live runs:

~~~bash
psql "$DATABASE_URL_DIRECT" -f compat/common/schema.sql
psql "$DATABASE_URL_DIRECT" -f compat/common/seed.sql
~~~

When pg-kinetic points at the same backing database, the proxy target observes
the same fixture rows through `DATABASE_URL_PROXY`.

## Commands

~~~bash
cargo run -p xtask -- compat --list
cargo run -p xtask -- compat --language rust --target direct-postgres
cargo run -p xtask -- compat --language rust --target pg-kinetic
bash scripts/compat/run.sh --language python --target pg-kinetic
~~~

~~~powershell
powershell.exe -ExecutionPolicy Bypass -File scripts\compat\run.ps1 -Language python -Target pg-kinetic
~~~

## Report Format

Each run emits JSON with:

- `ok`
- `success_marker`
- `summary` with `pass`, `fail`, `skip`, and `blocked` counts
- `results[]` suite records with `suite_id`, `language`, `library`, `version`,
  `target`, `outcome`, `duration_ms`, `skip_reason`, and `error_summary`
- `cases[]` optional behavior records with `case_id` and the contract outcome
  when a library executes multiple contract cases

The success marker is `compatibility report complete`. A toolchain, library, or
optional capability that is unavailable is represented as `outcome = "skip"`
with a stable `skip_reason`; it is never reported as a synthetic pass.

## Contract

`compat/common/contract.toml` is the machine-readable source of truth for the
shared cases. The contract covers connection startup, simple and parameterized
queries, prepared statements, transactions, pool reuse, errors, TLS, auth,
routing, sharding, and policy denial. Each case declares its category,
operation, protocol mode, capability requirement, assertion type, and
`expected_result` key. That key must resolve in
`compat/common/expected-results.json` under `cases`; language suites report the
same case ID and contract outcome, and the runner rejects unknown or mismatched
results.

The JSON fixture is versioned and uses the same `cases` result key. SQL fixtures
are deterministic and idempotent: `schema.sql` defines the shared relations and
`seed.sql` restores their contents. Credentials and TLS material are never
stored in the repository; the contract names the environment variables needed
for those profiles.

The `[[skip]]` records are structured, case-scoped skip policies. A suite must
emit `outcome = "skip"` with one of the contract's stable `reason` values when
the stated condition applies. Missing optional capabilities, toolchains, live
services, or TLS/auth profiles are skips, not synthetic passes.

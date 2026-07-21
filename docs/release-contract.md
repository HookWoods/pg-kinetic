---
title: "Stable 1.0 Release Contract"
description: "Machine-checkable support and compatibility claims for the pg-kinetic 1.0 stable release."
keywords:
  - pg-kinetic 1.0
  - PostgreSQL proxy support
  - stable release contract
  - compatibility gate
---

# Stable 1.0 Release Contract

This page is the support boundary for the stable 1.0 release. A release claim
means that the behavior below is covered by the repository tests and by the
Linux Docker validation described in [Release Validation](#release-validation).
Local macOS runs are useful for development, but their timings and capacity
observations are directional only.

## Supported PostgreSQL Targets

The 1.0 release claim covers PostgreSQL **16 and 18**, the versions exercised by
the deployment and Linux compatibility stacks. pg-kinetic is a single-primary
PostgreSQL proxy for this release. The proxy speaks the PostgreSQL wire
protocol, but no support claim is made for another server version until it has a
matching compatibility run.

The contract covers ordinary PostgreSQL databases reachable through a single
primary endpoint, with optional healthy replicas used only for eligible reads.
Automatic promotion, multi-primary operation, and shard placement are outside
the stable contract.

## Stable Runtime Scope

The following behavior is stable for production use:

- PostgreSQL wire-compatible startup, simple queries, parameterized queries,
  prepared statements, transactions, errors, and backend recovery covered by
  the required compatibility smoke suites.
- Transaction pooling with virtual client sessions. Open or failed
  transactions, temporary tables, advisory locks, `COPY`, `LISTEN/NOTIFY`,
  session mutations, and unknown protocol state remain pinned, are recovered,
  or cause the backend to be discarded rather than reused unsafely.
- Route-aware backpressure with bounded checkout, queue, timeout, and buffer
  behavior. Overload is expected to fail according to the configured limits.
- Conservative read routing. Writes, ambiguous statements, unsafe session
  state, and reads that fail health or freshness checks stay on the primary or
  follow the configured fallback policy.
- Single-primary recovery. Backend health and role checks remove an unhealthy
  endpoint from routing, recover or discard affected pooled sessions, and use
  the configured primary fallback. pg-kinetic does not promote a PostgreSQL
  server or provide a distributed consensus mechanism.
- Readiness, graceful drain, metrics, and PostgreSQL-protocol admin views for
  operating the proxy and inspecting pool, route, health, and recovery state.

The default runtime engine is `tokio_default`. `experimental_thread_per_core`
and `experimental_io_uring` remain opt-in and are not part of the default
release path.

## Authentication Contract

The stable client authentication modes are:

| Mode | Contract |
| --- | --- |
| `pass_through` | Preserve PostgreSQL's backend authentication exchange. |
| `trust` | Authenticate against the configured local user store. |
| `scram_sha_256` | Perform local SCRAM-SHA-256 authentication using configured verifiers before backend checkout. |

Backend service credentials may be supplied separately through the configured
environment variable. Secrets are not embedded in configuration examples or
compatibility reports.

## TLS Contract

Client TLS supports `disable`, `allow`, `require`, and `verify_client`.
Backend TLS supports `disable`, `prefer`, `require`, `verify_ca`, and
`verify_full`. Verification modes fail closed when their CA, certificate, key,
or server-name requirements are not satisfied. The selected client and backend
modes are independent and must be tested against the deployment's trust
boundary.

## Compatibility Contract

Each compatibility report consumes and produces the stable fields below:

```json
{
  "language": "rust",
  "library": "tokio-postgres",
  "version": "...",
  "target": "direct-postgres",
  "outcome": "pass",
  "skip_reason": "",
  "error_summary": ""
}
```

The target is either `direct-postgres` or `pg-kinetic`. A `pass` must be
observed for every required smoke suite on both targets. A missing toolchain or
optional library is reported as `skip` with a stable `skip_reason`; it is not
converted into a synthetic pass. A failed suite must include `error_summary`
and blocks the release gate when the suite is required.

The release suite includes the configured Rust, Go, Java, JavaScript, Python,
.NET, C, and C++ smoke paths where their required toolchains and libraries are
available. The direct PostgreSQL run is the baseline; the paired pg-kinetic run
must exercise the same cases.

## Preview-Only Exclusions

The following are preview or offline tooling and are **not supported for live traffic** in the 1.0 contract:

- sharding and shard migrations
- policy enforcement and policy-driven traffic decisions
- live traffic mirroring
- adaptive automation and automatic tuning

Do not describe these surfaces as production capabilities until their own
runtime, safety, recovery, observability, and compatibility contracts exist.
The 1.0 contract also makes no PgBouncer, PgDog, RDS Proxy, or Hyperdrive
feature-parity claim.

## Release Validation

Run the release gate on Linux. The reproducible compatibility environment is
the repository's Docker Compose stack; it starts PostgreSQL and pg-kinetic with
the pinned service configuration before running the smoke suites.

```bash
docker compose -f bench/compose.yml up --detach --wait --build postgres pg-kinetic
cargo fmt --check
cargo test --workspace --locked
cargo run -p xtask -- compat --target direct-postgres --smoke
cargo run -p xtask -- compat --target pg-kinetic --smoke
docker compose -f bench/compose.yml down --volumes
```

The CI workflow must run the focused contract assertion and the documentation
site build as well:

```bash
bash scripts/release/assert-contract.sh
npm --prefix docs-site run build
```

The contract assertion intentionally checks the stable-primary wording, the
live-traffic exclusion, and the proxy compatibility command. Keep those checks
machine-readable when revising this page.

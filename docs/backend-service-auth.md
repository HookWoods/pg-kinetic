---
title: "Backend Service Authentication"
description: "Pool PostgreSQL backend sessions behind a dedicated service identity while authenticating clients locally."
keywords:
  - PostgreSQL connection pool service account
  - pg-kinetic backend authentication
  - PostgreSQL MD5 SCRAM proxy
---

# Backend Service Authentication

Use backend service authentication when pg-kinetic should authenticate clients locally and reuse a smaller pool of PostgreSQL sessions owned by one dedicated backend role. It removes the pass-through limitation where every newly opened backend connection must wait for the original client password exchange.

## Configuration

Configure local client authentication and both backend service credential settings:

```toml
[auth]
auth_mode = "scram_sha_256"
auth_users_file = "/etc/pg-kinetic/auth-users.txt"
backend_user = "kinetic_pool"
backend_password_env_var_name = "PG_KINETIC_BACKEND_PASSWORD"

[tls]
backend_tls_mode = "verify_full"
backend_ca_path = "/etc/pg-kinetic/postgres-ca.pem"
backend_server_name = "postgres.internal"
```

Inject the password through the process environment or your deployment secret mechanism; do not place it in TOML:

```bash
export PG_KINETIC_BACKEND_PASSWORD='replace-with-your-secret'
```

`backend_user` and `backend_password_env_var_name` are an atomic pair. Supplying only one is rejected before the listener starts. They are also rejected with `auth_mode = "pass_through"`: pass-through deliberately preserves PostgreSQL's client-owned authentication exchange and cannot safely impersonate a service role.

The service-role boundary is implemented by `BackendCredentialProvider`. The default `EnvironmentCredentialProvider` reads the configured environment variable when a new backend authentication exchange begins. This keeps provider-specific credential lookup out of pool and wire-protocol code; future providers can implement the same interface without changing those callers.

## Service-Pool Warm-Up

After the first successful locally authenticated client startup, pg-kinetic asynchronously prepares up to two idle backend sessions for the primary pool, bounded by `capacity.max_backends`. This removes most connection and backend-auth work from the next client checkout without creating more sessions than the configured pool allows.

Warm-up is available only with backend service credentials. Pass-through clients still reconnect on demand because their PostgreSQL authentication exchange belongs to the client connection.

## Supported PostgreSQL Backend Methods

| Backend request | Support | Requirement |
| --- | --- | --- |
| Cleartext password | Supported | The negotiated backend connection must use TLS. Use `require`, `verify_ca`, or preferably `verify_full`. |
| MD5 password | Supported | A service password is required. Backend TLS is still recommended. |
| SCRAM-SHA-256 | Supported | A service password is required; pg-kinetic verifies PostgreSQL's server signature. |

pg-kinetic never forwards the service password to clients and does not expose it—or the configured environment-variable name—in admin or debug snapshots.

## Client Authentication

The client identity and backend service identity are separate:

| Client mode | Client verification | Backend session identity |
| --- | --- | --- |
| `trust` | Matching `username=trust` in the local user store | `backend_user` |
| `scram_sha_256` | Matching local SCRAM verifier | `backend_user` |
| `pass_through` | PostgreSQL verifies the client directly | Client startup user; service credentials are invalid |

Choose a minimally privileged PostgreSQL role for `backend_user`. If PostgreSQL authorization must remain distinct per client, keep `pass_through` instead; a single service role intentionally centralizes backend privileges.

## Rotation And Troubleshooting

Rotate the injected secret, then discard or recycle idle pooled backends. New backend connections read the current environment value through `EnvironmentCredentialProvider`; existing checked-out sessions retain their established PostgreSQL authentication until they are released or discarded. The secret value is never logged.

- `auth.backend_user requires auth.backend_password_env_var_name`: configure the password source too.
- `auth.backend_user and auth.backend_password_env_var_name are incompatible with auth_mode=pass_through`: select local `trust` or `scram_sha_256`, or remove service credentials.
- `backend requested a cleartext password without TLS`: require backend TLS before allowing PostgreSQL cleartext authentication.
- PostgreSQL `28P01` or a backend authentication failure: verify the service role, injected password, and PostgreSQL `pg_hba.conf` method.

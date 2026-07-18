---
title: "TLS And Authentication"
description: "TLS and authentication guide for pg-kinetic, including client TLS modes, backend TLS, SCRAM verifier format, secrets, and rotation."
keywords:
  - pg-kinetic TLS
  - PostgreSQL SCRAM
  - PostgreSQL proxy authentication
  - backend TLS
---

# TLS And Authentication

pg-kinetic can terminate client TLS, connect to PostgreSQL with backend TLS, and authenticate clients locally or through PostgreSQL's normal authentication flow.

## Client TLS

| Mode | Behavior | Failure cases |
| --- | --- | --- |
| `disable` | Accept plaintext startup only. | Clients that require TLS fail to connect. |
| `allow` | Accept plaintext startup or PostgreSQL `SSLRequest`. | TLS setup fails when certificate/key files are invalid. |
| `require` | Reject plaintext startup and require TLS. | Plaintext clients are rejected. |
| `verify_client` | Require TLS and verify the client certificate chain. | Missing CA, invalid cert, or unverifiable client cert fails the connection. |

`verify_client` requires:

- `client_cert_path`
- `client_key_path`
- `client_ca_path`

Mount private keys read-only and readable only by the pg-kinetic process user.

## Backend TLS

| Mode | Behavior | Failure cases |
| --- | --- | --- |
| `disable` | Connect to PostgreSQL without TLS. | TLS-required backends reject the connection. |
| `prefer` | Try TLS first and fall back to plaintext if the backend refuses TLS. | Verification is not enforced. |
| `require` | Fail closed if the backend refuses TLS. | Backend TLS refusal fails checkout/readiness. |
| `verify_ca` | Require TLS and verify the backend certificate chain. | Missing or invalid CA fails checkout/readiness. |
| `verify_full` | Require TLS, verify the backend CA, and match `backend_server_name`. | CA failure or hostname mismatch fails checkout/readiness. |

Backend verification modes require `backend_ca_path`. `verify_full` also requires `backend_server_name`.

## Client Auth Modes

| Mode | Behavior |
| --- | --- |
| `pass_through` | Preserve the backend's normal authentication flow. |
| `trust` | Authenticate locally with the configured user store. |
| `scram_sha_256` | Run local SCRAM-SHA-256 authentication before backend checkout. |

Use `auth_failure_message_mode = "generic"` for public-facing deployments. `detailed` can expose user/reason context to clients.

## User Store Format

The local user store accepts one `username=secret` entry per line. Blank lines and `#` comments are ignored.

```text
alice=trust
bob=SCRAM-SHA-256$4096:c2FsdA==$base64storedkey32bytes:base64serverkey32bytes
```

SCRAM verifier format:

```text
SCRAM-SHA-256$<iterations>:<base64-salt>$<base64-stored-key>:<base64-server-key>
```

Rules:

- `iterations` must be a positive integer.
- `salt`, `stored_key`, and `server_key` must be valid standard Base64.
- `stored_key` must decode to 32 bytes.
- `server_key` must decode to 32 bytes.
- usernames are matched case-sensitively. `alice=trust` does not authenticate a startup user named `Alice`.

## Backend Credentials

When pg-kinetic needs its own backend identity, set:

```toml
[auth]
backend_user = "proxy_user"
backend_password_env_var_name = "PG_KINETIC_BACKEND_PASSWORD"
```

The proxy reads the backend password from the named environment variable. If the variable is absent when backend credentials are needed, backend authentication fails.

Rotate the secret by updating the orchestration secret/environment and restarting pg-kinetic. Changing the env var name itself is restart-required.

## SSL Fallback For Clients

PostgreSQL clients often use `sslmode` or `PGSSLMODE` to decide whether they send an `SSLRequest`.

For local plaintext smoke tests:

```bash
PGSSLMODE=disable psql "postgres://app_user@127.0.0.1:6432/app_db" -c "select 1;"
```

For TLS-required deployments, configure certificates and use a client `sslmode` that matches the intended trust boundary.

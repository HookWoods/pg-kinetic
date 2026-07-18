# TLS And Authentication

pg-kinetic can terminate client TLS, connect to PostgreSQL with backend TLS, and authenticate clients locally or through PostgreSQL's normal authentication flow.

## Client TLS

| Mode | Behavior |
| --- | --- |
| `disable` | Accept plaintext startup only. |
| `allow` | Accept plaintext startup or PostgreSQL `SSLRequest`. |
| `require` | Reject plaintext startup and require TLS. |
| `verify_client` | Require TLS and verify the client certificate chain. |

`verify_client` requires:

- `client_cert_path`
- `client_key_path`
- `client_ca_path`

## Backend TLS

| Mode | Behavior |
| --- | --- |
| `disable` | Connect to PostgreSQL without TLS. |
| `prefer` | Try TLS first and fall back to plaintext if the backend refuses TLS. |
| `require` | Fail closed if the backend refuses TLS. |
| `verify_ca` | Require TLS and verify the backend certificate chain. |
| `verify_full` | Require TLS, verify the backend CA, and match `backend_server_name`. |

Backend verification modes require `backend_ca_path`. `verify_full` also requires `backend_server_name`.

## Client Auth Modes

| Mode | Behavior |
| --- | --- |
| `pass_through` | Preserve the backend's normal authentication flow. |
| `trust` | Authenticate locally with the configured user store. |
| `scram_sha_256` | Run local SCRAM-SHA-256 authentication before backend checkout. |

The local user store accepts one entry per line:

```text
# comments and blank lines are ignored
alice=trust
bob=SCRAM-SHA-256$4096:base64salt:base64storedkey:base64serverkey
```

Use `auth_failure_message_mode = "generic"` for public-facing deployments unless detailed failures are required for a private environment.

## Backend Credentials

When pg-kinetic needs its own backend identity, set:

```toml
[auth]
backend_user = "proxy_user"
backend_password_env_var_name = "PG_KINETIC_BACKEND_PASSWORD"
```

Keep the password in the environment, secret manager, or orchestration platform. Do not commit it to the config file.

## SSL Fallback For Clients

PostgreSQL clients often use `sslmode` or `PGSSLMODE` to decide whether they send an `SSLRequest`. If the proxy is configured with client TLS `allow`, both plaintext and SSL-capable clients can connect. If client TLS is `require`, plaintext startup is rejected.

For local plaintext smoke tests, use:

```bash
PGSSLMODE=disable PGPASSWORD=postgres psql -h 127.0.0.1 -p 58432 -U postgres -d pgkinetic -c "select 1;"
```

For TLS-required deployments, configure certificates and use a client `sslmode` that validates the intended trust boundary.


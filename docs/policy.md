---
title: "Policy"
description: "Policy preview documentation for pg-kinetic, explaining offline policy model evaluation, validation limits, and current live-traffic status."
keywords:
  - pg-kinetic policy
  - PostgreSQL proxy policy
  - policy preview
  - database access control
---

# Policy Preview

Policy is preview tooling today. The repository contains policy models and a `policy-preview` command, but the main proxy runtime does not expose live policy configuration for traffic.

Do not document policy rules as production traffic controls until the proxy accepts policy config in `Config` and applies the resulting decisions in the live request path.

## What Works Now

- `pg-kinetic policy-preview` evaluates a synthetic request context.
- Core policy models exist for allow, deny, require-primary, require-replica, route override, shard override, and WASM action variants.
- Policy validation code can reject invalid action combinations in preview contexts.

## Not Supported For Live Traffic

- `[policy]` is not part of the main runtime `Config`.
- There is no documented executable `match` / `when` / `then` policy file format for live deployments.
- Policy reload is not a live runtime configuration path for proxy traffic.
- Native plugins are not supported.

## Preview Command

Minimal offline-only preview file:

```toml
[policy]
policy_mode = "dry_run"
policy_eval_timeout_ms = 5
policy_max_context_bytes = 8192

[[policy.inline_rules]]
policy_id = "reporter-reads"
hook_point = "before_routing"
kind = "require_replica"
```

```bash
pg-kinetic policy-preview \
  --config preview-policy.toml \
  --database billing \
  --user reporter \
  --route primary \
  --shard tenant-a \
  --query-class read_candidate
```

Use preview output to inspect model behavior only. Do not use it as evidence that live proxy traffic will be allowed, denied, or rerouted.

Expected JSON fields:

| Field | Meaning |
| --- | --- |
| `ok` | `true` when the preview config and synthetic request evaluate successfully. |
| `policy_mode` | Parsed preview mode, such as `dry_run` or `enforce`. |
| `original_route`, `original_shard` | Route and shard supplied on the CLI. |
| `policy_adjusted_route`, `policy_adjusted_shard` | Synthetic target after the preview action. |
| `action` | Selected policy action. |
| `dry_run_outcome`, `dry_run_reason` | Dry-run result and explanation. |
| `deny_reason`, `sqlstate` | Present for deny actions. |
| `context` | Redacted synthetic context; secrets are rendered as `<redacted>`. |

## Security Boundary

Policy preview is not a trust boundary. It must not be used to authorize database access, expose raw SQL, expose credentials, or bypass backend health and PostgreSQL authentication.

## Production Guidance

Keep production access control in PostgreSQL, network policy, application authorization, or another deployed policy layer until pg-kinetic policy is wired into the runtime config and traffic path.

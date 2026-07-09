# Policy Guide

pg-kinetic policy controls how routing decisions are shaped, explained, and overridden. The goal is to keep policy expressive enough for operators while staying conservative, auditable, and easy to roll back.

## Policy Mental Model

- routing starts with the built-in classifier for read safety, shard safety, and session state
- policy then decides whether to accept that choice, redirect it, wait for a safer target, or reject it
- rules are evaluated against bounded request facts such as route key, statement class, shard scope, freshness state, and session state
- policy never depends on unbounded user text as a control surface

Think of policy as a narrow decision layer above the router, not as a replacement for parser safety or backend health checks.

## Hook Points

Policy can participate at the points where the proxy already has enough context to make a safe decision:

- route classification
- read-versus-primary selection
- shard target selection
- multi-shard handling
- read-after-write freshness handling
- route-map and shard migration reloads
- dry-run evaluation and audit logging

The hook points are intentionally small. That keeps routing behavior predictable and makes it easier to explain why a decision changed.

## Declarative Rule Format

Policy should be expressed as declarative rules, not embedded application code.

Typical rule fields:

- `match` selects the route, shard scope, statement class, or freshness state the rule applies to
- `when` records optional boolean guards
- `then` declares the action to take
- `priority` resolves overlaps when more than one rule matches
- `reason` stores a short operator-facing explanation

Example shape:

```toml
[[policy.rules]]
match.route = "billing/*"
match.statement_class = "read"
when.freshness = "strict"
then.action = "wait_for_fresh_replica"
then.timeout_ms = 750
reason = "Prefer replica reads when freshness can still be proven."

[[policy.rules]]
match.shard_scope = "tenant/*"
when.migration_state = "moving"
then.action = "reject"
reason = "Block routing changes while shard ownership is in flux."
```

The exact storage format may be TOML, YAML, or another declarative file, but the semantics should stay the same: small matches, bounded actions, predictable precedence.

## Policy Actions

Supported actions should stay intentionally small:

- `accept` keeps the router's decision
- `route_primary` forces primary routing
- `route_replica` allows replica routing when safety checks still pass
- `wait` waits for a freshness or capacity condition to become safe
- `reject` fails the request with an explicit policy outcome
- `override_route` pins an approved route choice for a bounded scope
- `override_shard` pins an approved shard choice for a bounded scope
- `audit_only` records the decision without changing traffic

Policy actions should always be explainable in operator language.

## Dry-Run Mode

Dry-run mode evaluates policy without changing the live routing result.

- the router still uses the normal safety checks
- the policy engine records what it would have changed
- dry-run results are surfaced through audit events and operator inspection
- dry-run is the preferred first step for new rules

Use dry-run to prove that a rule matches the expected traffic before enabling a live override.

## Audit Events

Policy decisions should emit audit events that are easy to search and safe to expose.

Useful fields:

- timestamp
- route key
- shard scope when relevant
- matched rule id
- selected action
- final outcome
- dry-run flag
- reason
- safety blockers that prevented the override

Audit output should avoid raw SQL text, passwords, certificate material, and any other secret-bearing payload.

## Failure Modes

Policy should fail closed when it cannot prove a safe answer.

Common failure modes:

- rule parse error
- unsupported action
- overlapping rules with no stable priority
- missing route or shard context
- stale policy snapshot after reload failure
- sandbox compilation or validation error

In each case, the proxy should keep the last known safe policy or fall back to the conservative built-in router behavior.

## Policy Hot Reload

Policy reload should be atomic from the operator's point of view.

- a validated policy snapshot replaces the current one all at once
- invalid reloads leave the existing policy in place
- reloads should be visible in admin output and metrics
- hot reload should never require a process restart for rule changes

When reloads fail, operators should be able to see whether the issue was parsing, validation, or a safety conflict.

## Route And Shard Override Safety

Overrides are powerful, so they need guardrails.

- route overrides should stay bounded to a narrow scope such as a route key, a statement class, or an explicit operator label
- shard overrides should only apply when the shard identity is known and the migration state is safe
- overrides should not bypass freshness checks, backend health checks, or migration safety checks unless that exception is explicitly modeled
- override rules should be easy to revoke and easy to audit

The main safety rule is simple: policy may narrow routing choices, but it should not silently widen them past the built-in safety checks.

## Optional WASM Policy Sandbox

An optional WASM sandbox can host policy logic that needs more expression than declarative rules alone.

- WASM modules should run in a constrained runtime
- the module should receive a compact, typed decision input
- host calls should stay minimal and deterministic
- the sandbox should have time and memory limits
- policy output should still be reduced to the same bounded actions used by declarative rules

The sandbox is optional. Declarative policy remains the default and preferred path for most installations.

## Unsupported Native Plugins

Native plugins are not part of the public policy model.

- loading arbitrary native code is not a supported extension path
- out-of-process policy helpers are preferred over in-process native hooks
- anything that can crash the proxy or reach private process memory is outside the supported boundary

This keeps policy portable and easier to reason about across deployments.

## Rollout Checklist

1. Start in `dry-run` mode and confirm the rule matches the expected traffic.
2. Check the audit stream for the exact route, shard, and reason fields you expect.
3. Keep overrides narrow until the safety checks are boring.
4. Verify hot reload on a staging environment before changing production traffic.
5. Watch admin output and policy-related metrics during the rollout.
6. Remove temporary overrides once the stable rule is in place.

## Security Limitations

- policy is a control surface, not a trust boundary
- any text-based hint from a client remains subject to the proxy's safety checks
- policy should not expose secrets, raw credentials, or sensitive SQL text in audit output
- a sandboxed policy module still needs host-side validation before its output is trusted
- unsupported native extensions are intentionally excluded because they weaken isolation

Policy is strongest when it is narrow, observable, and easy to reverse.

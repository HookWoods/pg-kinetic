---
title: "Statement Audit"
description: "Configure pg-kinetic's bounded metadata-only statement audit stream."
---

# Statement Audit

Statement audit is disabled by default. Enable it only when an operator has
approved the destination and retention policy:

```toml
[audit]
enabled = true
sink = "/var/log/pg-kinetic/audit.jsonl"
sample_rate = 0.25
include_reads = false
```

Records are JSON lines containing opaque route and identity identifiers, the
existing fingerprint/template, query class, outcome, elapsed milliseconds, and
row count. Raw SQL, literal values, credentials, connection strings, tokens,
and client identity values are never written. Fingerprints still describe
statement shape and should be treated as sensitive operational metadata.

The queue is bounded and uses a nonblocking enqueue. When it is full, records
are dropped and `pg_kinetic_audit_dropped_total` increases; query sessions are
never held for audit I/O. Sink open, write, and flush failures stop the audit
worker without stopping the proxy; records attempted after worker exit are also
counted as dropped and are not included in `pg_kinetic_audit_records_total`.

Operators must restrict sink permissions, protect collected files and transport
endpoints, and set retention according to their privacy and incident-response
requirements. Audit data is not a complete query history: sampling, disabled
read capture, queue overflow, sink failures, and process shutdown can leave
gaps. Do not use it as the sole source for compliance evidence.

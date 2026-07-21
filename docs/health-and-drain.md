---
title: "Health, Readiness, And Drain"
description: "Current pg-kinetic HTTP health endpoints, readiness behavior, drain state reporting, Kubernetes probe guidance, and unsupported drain routes."
keywords:
  - pg-kinetic health checks
  - PostgreSQL proxy readiness
  - Kubernetes probes
  - drain endpoint
---

# Health, Readiness, And Drain

This page describes the HTTP health listener that exists in the proxy today.

Readiness is primary-oriented: it becomes unavailable when the configured primary
cannot be reached. A failed pooled backend is discarded immediately, so it is not
returned to service while the probe reports the primary as unavailable.

## Current HTTP Endpoints

Enable the listener with `health_addr`.

```toml
[health]
health_addr = "0.0.0.0:9091"
readiness_backend_check_interval_ms = 1000
readiness_timeout_ms = 5000
```

| Endpoint | Method | Status | Body | Meaning |
| --- | --- | --- | --- | --- |
| `/healthz` | `GET` | `200` | `live` | The process accepted the health request. |
| `/readyz` | `GET` | `200` | `ready` | The proxy is accepting clients and the backend probe is ready. |
| `/readyz` | `GET` | `503` | `not_ready` | The proxy is draining, not accepting clients, or backend probing failed. |
| `/state` | `GET` | `200` | JSON | Non-secret health snapshot. |

All other paths return `404 not_found`. Non-`GET` requests are treated as unknown paths.

`POST /drain` and `GET /drain` are not implemented in the HTTP health server. Do not configure Kubernetes pre-stop hooks against `/drain` until the endpoint exists in the proxy.

Backend failures during a request follow the same conservative boundary as
readiness. A read may be replayed once only when no backend response byte has
reached the client. Writes, authentication failures, and responses that have
started are never replayed. The latter cases discard the backend and return
SQLSTATE `08006` when the client protocol is still safe to continue.

## State Payload

`GET /state` returns:

```json
{
  "process": "live",
  "ready": "ready",
  "drain_state": "accepting",
  "active_clients": 0,
  "backend_health": "ready"
}
```

Fields:

| Field | Values | Notes |
| --- | --- | --- |
| `process` | `live` | The current server always reports process liveness as `live` while it can answer. |
| `ready` | `ready`, `not_ready` | Combines drain acceptance and backend probe status. |
| `drain_state` | `accepting`, `draining`, `drained` | Comes from the in-process drain controller. |
| `active_clients` | integer | Number of active client sessions known to the drain controller. |
| `backend_health` | `ready`, `not_ready`, `degraded`, `live` | Result of the background backend connection probe. |

## Kubernetes Probes

Use readiness and liveness probes only:

```yaml
readinessProbe:
  httpGet:
    path: /readyz
    port: health
livenessProbe:
  httpGet:
    path: /healthz
    port: health
```

Do not add a pre-stop HTTP hook yet. Current graceful shutdown starts from process signal handling, not an HTTP drain request.

## Failure Modes

| Condition | Result |
| --- | --- |
| Backend connection fails or times out | `/readyz` returns `503 not_ready`. |
| Health listener address is unset | No HTTP health server is started. |
| Health listener bind fails | Proxy startup fails. |
| Request path is unknown | HTTP `404 not_found`. |
| Request method is not `GET` | HTTP `404 not_found`. |

## Operational Checks

```bash
curl -fsS http://127.0.0.1:9091/healthz
curl -fsS http://127.0.0.1:9091/readyz
curl -fsS http://127.0.0.1:9091/state
```

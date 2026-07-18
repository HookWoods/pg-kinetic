---
title: "Installation"
description: "Install pg-kinetic from local Docker assets today and prepare for future GHCR and Helm chart release workflows."
keywords:
  - install pg-kinetic
  - Docker PostgreSQL proxy
  - Helm PostgreSQL proxy
  - Postgres connection pooler install
---

# Installation

This repository contains local deployment assets for Docker, Docker Compose, and Kubernetes. Public GHCR and Helm chart releases are created when a GitHub Release with a `vMAJOR.MINOR.PATCH` tag is published.

## Current Installation Source

Until the first release tag exists, build the container image from this checkout:

```bash
docker build -t pg-kinetic:local .
```

Run the local image with a mounted config:

```bash
docker run --detach \
  --name pg-kinetic \
  --restart unless-stopped \
  --publish 6432:6432 \
  --publish 7000:7000 \
  --publish 9090:9090 \
  --publish 9091:9091 \
  --volume "$PWD/pg-kinetic.toml:/etc/pg-kinetic/pg-kinetic.toml:ro" \
  pg-kinetic:local \
  --config-file /etc/pg-kinetic/pg-kinetic.toml
```

## Docker Compose

The repository includes `deploy/docker-compose.yml`. It starts PostgreSQL, builds `pg-kinetic:local` from the repository root, and mounts `deploy/pg-kinetic.toml`.

```bash
docker compose -f deploy/docker-compose.yml up -d --build
```

The sample config points at PostgreSQL on `172.30.0.10:5432` inside the Compose network. Use `IP:port` for `backend_addr`; hostnames are not accepted in that field.

Clean up:

```bash
docker compose -f deploy/docker-compose.yml down
```

## Kubernetes With Local Helm Chart

The repository includes a Helm chart under `charts/pg-kinetic`.

```bash
helm lint ./charts/pg-kinetic
helm template pg-kinetic ./charts/pg-kinetic
helm install pg-kinetic ./charts/pg-kinetic \
  --set image.repository=pg-kinetic \
  --set image.tag=local \
  --set image.pullPolicy=Never
```

The chart renders:

- a Deployment with non-root container security defaults
- a ConfigMap containing `pg-kinetic.toml`
- a ClusterIP Service exposing PostgreSQL, admin, metrics, and health ports
- readiness and liveness probes

The chart does not configure a pre-stop drain hook because the HTTP health server does not implement `/drain`.

## Future Release Images

After a GitHub Release with a tag matching `v*.*.*` is published, the container workflow publishes multi-platform images:

| Registry | Image |
| --- | --- |
| GitHub Container Registry | `ghcr.io/hookwoods/pg-kinetic:<version>` |

Use immutable version tags in production once they exist:

```bash
docker pull ghcr.io/hookwoods/pg-kinetic:0.1.0
```

## Future Helm Repository

The Helm workflow publishes a chart repository for `https://helm.pgkinetic.dev` after a GitHub Release creates the first chart archive and chart index.

After that release exists:

```bash
helm repo add pgkinetic https://helm.pgkinetic.dev
helm repo update
helm install pg-kinetic pgkinetic/pg-kinetic \
  --set image.tag=0.1.0
```

Until `https://helm.pgkinetic.dev/index.yaml` exists, use the local chart path.

## Release Publishing

The container workflow runs when a GitHub Release is published. It builds the Dockerfile and publishes `ghcr.io/hookwoods/pg-kinetic`.

The Helm workflow runs on the same GitHub Release event. It derives the chart version from the release tag, uploads the chart archive to that release, and updates the chart repository index.

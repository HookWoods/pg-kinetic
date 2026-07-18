---
title: "Installation"
description: "Install pg-kinetic from the GHCR container image, Docker Compose, or the pg-kinetic Helm repository."
keywords:
  - install pg-kinetic
  - Docker PostgreSQL proxy
  - Helm PostgreSQL proxy
  - Postgres connection pooler install
---

# Installation

Install pg-kinetic from the GitHub Container Registry image or from the Helm chart repository. The local Docker Compose stack remains available for development and smoke testing.

## Docker

Pull the release image:

```bash
docker pull ghcr.io/hookwoods/pg-kinetic:latest
```

Run the image with a mounted config:

```bash
docker run --detach \
  --name pg-kinetic \
  --restart unless-stopped \
  --publish 6432:6432 \
  --publish 7000:7000 \
  --publish 9090:9090 \
  --publish 9091:9091 \
  --volume "$PWD/pg-kinetic.toml:/etc/pg-kinetic/pg-kinetic.toml:ro" \
  ghcr.io/hookwoods/pg-kinetic:latest \
  --config-file /etc/pg-kinetic/pg-kinetic.toml
```

Use immutable version tags for production rollouts when you want reproducible deploys:

```bash
docker pull ghcr.io/hookwoods/pg-kinetic:0.1.0
```

## Docker Compose

The repository includes `deploy/docker-compose.yml`. It starts PostgreSQL, builds a local pg-kinetic image from the repository root, and mounts `deploy/pg-kinetic.toml`.

```bash
docker compose -f deploy/docker-compose.yml up -d --build
```

The sample config points at PostgreSQL on `172.30.0.10:5432` inside the Compose network. Use `IP:port` for `backend_addr`; hostnames are not accepted in that field.

Clean up:

```bash
docker compose -f deploy/docker-compose.yml down
```

## Helm

Add the chart repository and install pg-kinetic:

```bash
helm repo add pgkinetic https://helm.pgkinetic.dev
helm repo update
helm install pg-kinetic pgkinetic/pg-kinetic \
  --set image.repository=ghcr.io/hookwoods/pg-kinetic \
  --set image.tag=latest
```

The chart renders:

- a Deployment with non-root container security defaults
- a ConfigMap containing `pg-kinetic.toml`
- a ClusterIP Service exposing PostgreSQL, admin, metrics, and health ports
- readiness and liveness probes

The chart does not configure a pre-stop drain hook because the HTTP health server does not implement `/drain`.

## Local Helm Chart

Use the local chart when you are changing chart templates or testing a locally built image:

```bash
helm lint ./charts/pg-kinetic
helm template pg-kinetic ./charts/pg-kinetic
helm install pg-kinetic ./charts/pg-kinetic \
  --set image.repository=pg-kinetic \
  --set image.tag=local \
  --set image.pullPolicy=Never
```

## Release Publishing

The container workflow runs when a GitHub Release is published. It builds the Dockerfile and publishes `ghcr.io/hookwoods/pg-kinetic`.

The Helm workflow runs on the same GitHub Release event. It derives the chart version from the release tag, uploads the chart archive to that release, and updates the chart repository index.

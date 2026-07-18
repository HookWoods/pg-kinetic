# Installation

pg-kinetic is distributed as a container image and can be deployed with Docker, Docker Compose, or Kubernetes. The production path is image-first; building from source is a contributor workflow, not the normal installation path.

## Container Images

Tagged releases publish multi-architecture Linux images for `linux/amd64` and `linux/arm64`.

| Registry | Image |
| --- | --- |
| Docker Hub | `hookwoods/pg-kinetic:<version>` |
| GitHub Container Registry | `ghcr.io/hookwoods/pg-kinetic:<version>` |

Use an immutable version tag in production:

```bash
docker pull hookwoods/pg-kinetic:0.1.0
```

`latest` is updated on version tags for convenience, but production manifests should pin an explicit release tag.

## Docker

Run pg-kinetic with a mounted config file:

```bash
docker run --detach \
  --name pg-kinetic \
  --restart unless-stopped \
  --publish 6432:6432 \
  --publish 7000:7000 \
  --publish 9090:9090 \
  --publish 9091:9091 \
  --volume "$PWD/pg-kinetic.toml:/etc/pg-kinetic/pg-kinetic.toml:ro" \
  hookwoods/pg-kinetic:0.1.0 \
  --config-file /etc/pg-kinetic/pg-kinetic.toml
```

## Docker Compose

The repository includes `deploy/docker-compose.yml` for a production-shaped compose deployment:

```bash
docker compose -f deploy/docker-compose.yml up -d
```

The compose file expects:

- `deploy/pg-kinetic.toml` or an adjusted config mount
- optional certificate material under `deploy/certs`

## Kubernetes With Helm

The repository includes a Helm chart under `charts/pg-kinetic`. Published chart releases are served from `https://helm.pgkinetic.dev`.

Add the chart repository:

```bash
helm repo add pgkinetic https://helm.pgkinetic.dev
helm repo update
```

Create a backend-password secret when the config references `PG_KINETIC_BACKEND_PASSWORD`:

```bash
kubectl create secret generic pg-kinetic-backend \
  --from-literal=backend-password='replace-me'
```

Install the chart:

```bash
helm install pg-kinetic pgkinetic/pg-kinetic \
  --set image.tag=0.1.0 \
  --set backendPassword.existingSecret=pg-kinetic-backend
```

For local chart development, use `helm install pg-kinetic ./charts/pg-kinetic`.

The chart creates:

- a Deployment with non-root container security defaults
- a ConfigMap containing `pg-kinetic.toml`
- a ClusterIP Service exposing PostgreSQL, admin, and health ports
- readiness and liveness probes
- a pre-stop drain hook

For production, override `values.yaml` with your real backend address, TLS settings, auth mode, and resource requests.

## Kubernetes Without Helm

If Helm is not available, render the chart once and apply the generated manifests:

```bash
helm template pg-kinetic ./charts/pg-kinetic \
  --set image.tag=0.1.0 \
  --set backendPassword.existingSecret=pg-kinetic-backend \
  > pg-kinetic.yaml

kubectl apply -f pg-kinetic.yaml
```

## Release Publishing

The container workflow runs on tags matching `v*.*.*`. It builds the Dockerfile and publishes `hookwoods/pg-kinetic` and `ghcr.io/hookwoods/pg-kinetic`.

The workflow uses Docker Buildx, metadata tags, OCI labels, and the GitHub Actions cache.

The Helm workflow runs on the same version tags. It packages chart versions, uploads chart archives to GitHub Releases, and updates the chart repository index for `https://helm.pgkinetic.dev`.

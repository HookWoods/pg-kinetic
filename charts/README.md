# pg-kinetic Helm Chart

The chart source lives in `charts/pg-kinetic`.

Use the local chart before the first release:

```bash
helm lint ./charts/pg-kinetic
helm template pg-kinetic ./charts/pg-kinetic
helm install pg-kinetic ./charts/pg-kinetic
```

Publishing a GitHub Release with a `vMAJOR.MINOR.PATCH` tag packages the chart at the same version, uploads the chart archive to that release, and updates the chart repository index on the `gh-pages` branch.

GitHub Pages should be configured for this repository with:

- source: deploy from branch
- branch: `gh-pages`
- folder: `/`
- custom domain: `helm.pgkinetic.dev`

Cloudflare DNS should point `helm.pgkinetic.dev` to `hookwoods.github.io` with a DNS-only `CNAME` record. The chart workflow writes a `CNAME` file to the `gh-pages` branch so GitHub Pages keeps the Helm custom domain attached.

After the first GitHub Release publishes the chart and `https://helm.pgkinetic.dev/index.yaml` is reachable:

```bash
helm repo add pgkinetic https://helm.pgkinetic.dev
helm repo update
helm install pg-kinetic pgkinetic/pg-kinetic
```

# pg-kinetic Helm Chart

Published chart releases are served from:

```bash
helm repo add pgkinetic https://helm.pgkinetic.dev
helm repo update
helm install pg-kinetic pgkinetic/pg-kinetic
```

The chart source lives in `charts/pg-kinetic`. The release workflow packages changed chart versions, uploads the chart archive to GitHub Releases, and updates the chart repository index on the `gh-pages` branch.

# pg-kinetic Helm Chart

The chart source lives in `charts/pg-kinetic`.

Use the local chart before the first release:

```bash
helm lint ./charts/pg-kinetic
helm template pg-kinetic ./charts/pg-kinetic
helm install pg-kinetic ./charts/pg-kinetic
```

The release workflow packages changed chart versions, uploads chart archives to GitHub Releases, and updates the chart repository index on the `gh-pages` branch.

After the first chart release exists and `https://helm.pgkinetic.dev/index.yaml` is reachable:

```bash
helm repo add pgkinetic https://helm.pgkinetic.dev
helm repo update
helm install pg-kinetic pgkinetic/pg-kinetic
```

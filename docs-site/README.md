# pg-kinetic documentation site

This directory contains the Docusaurus site. The canonical Markdown content remains in [`../docs`](../docs); do not move or duplicate those guides into this project.

The sidebar is manually grouped by audience path and uses Docusaurus generated index pages for major categories. The root page is `../docs/index.mdx`, which uses `DocCardList` to expose the main documentation sections.

Installation docs must distinguish current local deployment assets from future release images and future Helm repository commands.

## Local development

```bash
npm install
npm run start
```

Build the production site and validate Docusaurus links:

```bash
npm run build
npm run check
```

From the repository root, validate Markdown links on either platform:

```bash
bash scripts/docs/check-links.sh
powershell.exe -ExecutionPolicy Bypass -File scripts/docs/check-links.ps1
bash scripts/docs/check-config-coverage.sh
powershell.exe -ExecutionPolicy Bypass -File scripts/docs/check-config-coverage.ps1
```

## Version policy

The implicit Docusaurus `current` version tracks the `main` branch. Released versions are cut manually only when a release is published. Do not create placeholder or synthetic versioned documentation.

## Cloudflare Pages

Production documentation is deployed by Cloudflare Pages, not GitHub Pages. Keep the project settings aligned with:

- production branch: `main`
- build command: `npm --prefix docs-site ci && npm --prefix docs-site run build`
- build output directory: `docs-site/build`
- root directory: repository root
- environment variable: `NODE_VERSION=22`
- custom domain: `docs.pgkinetic.dev`

The GitHub `Documentation` workflow only validates the documentation build and link/config checks. GitHub Pages is reserved for the Helm chart repository on `helm.pgkinetic.dev`.

## Publication gate

Do not treat the site as a final operator reference until:

- search is configured
- released Docusaurus versions exist
- command examples are covered by fixture checks
- config, CLI, and metric catalogs prevent drift from code

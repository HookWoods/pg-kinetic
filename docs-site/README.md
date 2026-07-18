# pg-kinetic documentation site

This directory contains the Docusaurus site. The canonical Markdown content remains in [`../docs`](../docs); do not move or duplicate those guides into this project.

The sidebar is manually grouped by audience path and uses Docusaurus generated index pages for major categories. The root page is `../docs/index.mdx`, which uses `DocCardList` to expose the main documentation sections. Installation docs should describe the released container image, Docker Compose, and Helm chart before contributor workflows.

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
```

## Version policy

The implicit Docusaurus `current` version tracks the `main` branch. Released versions are cut manually only when a release is published. Do not create placeholder or synthetic versioned documentation.

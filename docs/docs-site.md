# Documentation Site

The documentation site is a Docusaurus classic site in `docs-site/`. It renders the canonical Markdown files in this `docs/` directory directly, so guides have one source of truth for repository readers and the published site.

The published root route is a docs page, not a marketing landing page. `docs/index.mdx` owns `/` and uses Docusaurus `DocCardList`; `docs-site/sidebars.js` owns the category structure and generated index pages for the main sections.

## Local Workflow

Install the site dependencies and start a local server from `docs-site/`:

```bash
npm install
npm run start
```

Build and check the site before publishing documentation changes:

```bash
npm run build
npm run check
```

Validate Markdown links from the repository root with the platform-appropriate script:

```bash
bash scripts/docs/check-links.sh
powershell.exe -ExecutionPolicy Bypass -File scripts/docs/check-links.ps1
```

## Content And Versions

Add product guides to `docs/` and add them to `docs-site/sidebars.js` when they are ready to publish. Prefer category links and generated index pages for high-level sections, then use focused docs for installation, configuration, commands, operations, and reference material. Keep operational guidance public and reproducible; do not publish generated reports, credentials, local outputs, or unpublished project material.

The Docusaurus `current` documentation follows `main`. Cut released versions manually as part of a real release; the site does not maintain placeholder versioned documentation.

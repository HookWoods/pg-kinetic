// @ts-check

const siteUrl = 'https://docs.pgkinetic.dev';
const repositoryUrl = 'https://github.com/hookwoods/pg-kinetic';
const socialImage = `${siteUrl}/img/pg-kinetic-og.png`;

const structuredData = {
  '@context': 'https://schema.org',
  '@graph': [
    {
      '@type': 'Organization',
      '@id': `${siteUrl}/#organization`,
      name: 'pg-kinetic contributors',
      url: siteUrl,
      logo: `${siteUrl}/img/favicon.svg`,
      sameAs: [repositoryUrl],
    },
    {
      '@type': 'WebSite',
      '@id': `${siteUrl}/#website`,
      name: 'pg-kinetic Documentation',
      url: siteUrl,
      description:
        'Evaluate, benchmark, install, and operate pg-kinetic, a PostgreSQL wire proxy for connection pooling, backpressure, conservative read routing, health checks, metrics, and deployment validation.',
      inLanguage: 'en',
      publisher: {
        '@id': `${siteUrl}/#organization`,
      },
    },
    {
      '@type': 'SoftwareApplication',
      '@id': `${siteUrl}/#software`,
      name: 'pg-kinetic',
      applicationCategory: 'DeveloperApplication',
      operatingSystem: 'Linux',
      programmingLanguage: 'Rust',
      url: siteUrl,
      codeRepository: repositoryUrl,
      description:
        'pg-kinetic is a PostgreSQL wire proxy for connection pooling, route-aware backpressure, conservative session handling, read routing, admin inspection, health checks, metrics, benchmark baselines, and regression tooling.',
      softwareRequirements: 'PostgreSQL-compatible client and backend',
      softwareVersion: 'unreleased source build',
      downloadUrl: repositoryUrl,
      installUrl: `${siteUrl}/installation`,
      author: {
        '@id': `${siteUrl}/#organization`,
      },
      isAccessibleForFree: true,
    },
  ],
};

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'pg-kinetic',
  tagline: 'PostgreSQL wire proxy documentation for evaluators and operators',
  url: siteUrl,
  baseUrl: '/',
  organizationName: 'hookwoods',
  projectName: 'pg-kinetic',
  trailingSlash: false,
  favicon: 'img/favicon.svg',

  onBrokenLinks: 'throw',
  markdown: {
    mermaid: true,
    hooks: {
      onBrokenMarkdownLinks: 'throw',
    },
  },

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          path: '../docs',
          routeBasePath: '/',
          sidebarPath: require.resolve('./sidebars.js'),
          editUrl: 'https://github.com/hookwoods/pg-kinetic/edit/main/docs/',
          showLastUpdateTime: true,
          showLastUpdateAuthor: true,
          lastVersion: 'current',
          versions: {
            current: {
              label: 'Current',
              path: '/',
              banner: 'unreleased',
            },
          },
        },
        sitemap: {
          lastmod: 'date',
          changefreq: 'weekly',
          priority: 0.7,
          ignorePatterns: ['/search'],
          filename: 'sitemap.xml',
        },
        blog: false,
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
      }),
    ],
  ],

  plugins: [
    [
      '@easyops-cn/docusaurus-search-local',
      {
        hashed: true,
        indexDocs: true,
        indexBlog: false,
        indexPages: true,
        docsRouteBasePath: '/',
        docsDir: '../docs',
        language: ['en'],
        highlightSearchTermsOnTargetPage: true,
      },
    ],
  ],

  themes: ['@docusaurus/theme-mermaid'],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      colorMode: {
        defaultMode: 'dark',
        disableSwitch: false,
        respectPrefersColorScheme: true,
      },
      mermaid: {
        theme: { light: 'neutral', dark: 'dark' },
      },
      metadata: [
        {
          name: 'description',
          content:
            'Evaluate, benchmark, install, and operate pg-kinetic, a Rust PostgreSQL wire proxy for connection pooling, backpressure, read routing, health checks, metrics, and admin visibility.',
        },
        {
          name: 'keywords',
          content:
            'pg-kinetic, PostgreSQL proxy, Postgres connection pooler, PostgreSQL wire protocol, Rust Postgres proxy, PostgreSQL connection storm, PgBouncer alternative, PgBouncer comparison, PgDog comparison, PgCat comparison, Odyssey comparison, PostgreSQL proxy benchmark, PostgreSQL read routing, PostgreSQL backpressure, PostgreSQL metrics, database proxy',
        },
        { name: 'robots', content: 'index,follow' },
        { name: 'googlebot', content: 'index,follow,max-snippet:-1,max-image-preview:large,max-video-preview:-1' },
        { name: 'bingbot', content: 'index,follow,max-snippet:-1,max-image-preview:large,max-video-preview:-1' },
        { name: 'twitter:card', content: 'summary_large_image' },
        { name: 'twitter:title', content: 'pg-kinetic PostgreSQL Wire Proxy Documentation' },
        {
          name: 'twitter:description',
          content:
            'Evaluate and operate pg-kinetic for PostgreSQL connection pooling, backpressure, read routing, health checks, metrics, benchmarks, and regression validation.',
        },
        { name: 'twitter:image', content: socialImage },
      ],
      navbar: {
        title: 'pg-kinetic',
        logo: {
          alt: 'pg-kinetic',
          src: 'img/favicon.svg',
        },
        items: [
          {
            type: 'docSidebar',
            sidebarId: 'docsSidebar',
            position: 'left',
            label: 'Guides',
          },
          {
            type: 'docsVersionDropdown',
            position: 'right',
          },
          {
            href: 'https://pgkinetic.dev',
            label: 'Website',
            position: 'right',
          },
          {
            href: 'https://github.com/hookwoods/pg-kinetic',
            label: 'GitHub',
            position: 'right',
          },
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Documentation',
            items: [
              { label: 'Guides', to: '/' },
              { label: 'Website', href: 'https://pgkinetic.dev' },
              { label: 'Docs workflow', to: '/docs-site' },
            ],
          },
          {
            title: 'Project',
            items: [
              {
                label: 'GitHub',
                href: 'https://github.com/hookwoods/pg-kinetic',
              },
            ],
          },
        ],
        copyright: `Copyright ${new Date().getFullYear()} pg-kinetic contributors.`,
      },
    }),

  headTags: [
    { tagName: 'meta', attributes: { property: 'og:type', content: 'website' } },
    { tagName: 'meta', attributes: { property: 'og:site_name', content: 'pg-kinetic Documentation' } },
    { tagName: 'meta', attributes: { property: 'og:title', content: 'pg-kinetic PostgreSQL Wire Proxy Documentation' } },
    {
      tagName: 'meta',
      attributes: {
        property: 'og:description',
        content:
          'Evaluate, benchmark, install, and operate pg-kinetic for PostgreSQL connection pooling, backpressure, read routing, health checks, metrics, and admin visibility.',
      },
    },
    { tagName: 'meta', attributes: { property: 'og:image', content: socialImage } },
    { tagName: 'meta', attributes: { property: 'og:image:width', content: '1200' } },
    { tagName: 'meta', attributes: { property: 'og:image:height', content: '630' } },
    { tagName: 'meta', attributes: { property: 'og:image:alt', content: 'pg-kinetic PostgreSQL wire proxy documentation' } },
    {
      tagName: 'script',
      attributes: { type: 'application/ld+json' },
      innerHTML: JSON.stringify(structuredData),
    },
  ],
};

module.exports = config;

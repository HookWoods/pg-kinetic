// @ts-check

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'pg-kinetic',
  tagline: 'PostgreSQL wire proxy documentation',
  url: 'https://pg-kinetic.dev',
  baseUrl: '/',
  organizationName: 'hookwoods',
  projectName: 'pg-kinetic',
  trailingSlash: false,

  onBrokenLinks: 'throw',
  markdown: {
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
          routeBasePath: 'docs',
          sidebarPath: require.resolve('./sidebars.js'),
          editUrl: 'https://github.com/hookwoods/pg-kinetic/edit/main/docs/',
        },
        blog: false,
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      navbar: {
        title: 'pg-kinetic',
        items: [
          {
            type: 'docSidebar',
            sidebarId: 'docsSidebar',
            position: 'left',
            label: 'Documentation',
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
              { label: 'Guides', to: '/docs/admin' },
              { label: 'Docs workflow', to: '/docs/docs-site' },
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
};

module.exports = config;

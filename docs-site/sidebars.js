/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  docsSidebar: [
    {
      type: 'category',
      label: 'Guides',
      items: [
        'admin',
        'production-runtime',
        'read-routing',
        'sharding',
        'policy',
        'mirroring',
        'adaptive-ops',
        'benchmarking',
        'compatibility',
        'metrics',
        'kubernetes',
      ],
    },
    {
      type: 'category',
      label: 'Contributing',
      items: ['testing', 'regression', 'docs-site'],
    },
  ],
};

module.exports = sidebars;

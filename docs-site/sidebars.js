/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  docsSidebar: [
    'index',
    {
      type: 'category',
      label: 'Evaluate',
      link: {
        type: 'generated-index',
        title: 'Evaluate pg-kinetic',
        description: 'Decide whether pg-kinetic fits your PostgreSQL workload before configuring a deployment.',
        slug: '/evaluate',
      },
      collapsed: false,
      items: ['what-is-pg-kinetic', 'how-it-compares', 'is-it-ready', 'benchmark-results'],
    },
    {
      type: 'category',
      label: 'Start',
      link: {
        type: 'generated-index',
        title: 'Start With pg-kinetic',
        description: 'Install pg-kinetic, run the first proxy query, choose config, and understand the first production decisions.',
        slug: '/getting-started',
      },
      collapsed: false,
      items: ['installation', 'quickstart', 'configuration', 'commands', 'release-contract', 'compatibility', 'faq'],
    },
    {
      type: 'category',
      label: 'Concepts',
      link: {
        type: 'generated-index',
        title: 'Core Concepts',
        description: 'How pg-kinetic treats PostgreSQL wire traffic, sessions, routing, pooling, and overload.',
        slug: '/concepts',
      },
      collapsed: false,
      items: ['architecture', 'transaction-pooling', 'backpressure', 'prepared-statements'],
    },
    {
      type: 'category',
      label: 'Runtime Features And Preview Tooling',
      link: {
        type: 'generated-index',
        title: 'Runtime Features And Preview Tooling',
        description: 'Implemented runtime features and clearly labeled preview or inactive tooling surfaces.',
        slug: '/features',
      },
      items: [
        'admin',
        'read-routing',
        { type: 'doc', id: 'sharding', label: 'Sharding (Preview)' },
        { type: 'doc', id: 'policy', label: 'Policy (Preview)' },
        { type: 'doc', id: 'mirroring', label: 'Mirroring (Not active)' },
        { type: 'doc', id: 'adaptive-ops', label: 'Adaptive Operations (Simulation)' },
        'metrics',
      ],
    },
    {
      type: 'category',
      label: 'Operations',
      link: {
        type: 'generated-index',
        title: 'Operations',
        description: 'Run pg-kinetic in production with TLS, authentication, health checks, drain, and Kubernetes.',
        slug: '/operations',
      },
      items: [
        'production-runtime',
        'tls-and-auth',
        'backend-service-auth',
        'health-and-drain',
        'migration',
        'kubernetes',
        'deployment-escape-hatches',
        'troubleshooting',
      ],
    },
    {
      type: 'category',
      label: 'Benchmarking And Regression',
      link: {
        type: 'generated-index',
        title: 'Benchmarking And Regression',
        description: 'Maintain performance confidence with benchmark scenarios, compatibility reports, and regression manifests.',
        slug: '/benchmarking-and-regression',
      },
      items: ['benchmarking', 'regression'],
    },
    {
      type: 'category',
      label: 'Contributing',
      link: {
        type: 'generated-index',
        title: 'Contributing',
        description: 'Local validation, documentation workflow, and CI mapping for contributors.',
        slug: '/contributing',
      },
      items: ['testing', 'docs-site'],
    },
  ],
};

module.exports = sidebars;

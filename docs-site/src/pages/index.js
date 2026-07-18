import Link from '@docusaurus/Link';
import Layout from '@theme/Layout';

const guides = [
  {
    label: 'Admin',
    path: '/docs/admin',
    description: 'Listeners, pools, TLS, auth, reload, and runtime controls.',
  },
  {
    label: 'Routing',
    path: '/docs/read-routing',
    description: 'Replica reads, statement hints, LSN safety, and route views.',
  },
  {
    label: 'Sharding',
    path: '/docs/sharding',
    description: 'Route maps, shard lifecycle, and shard-key extraction.',
  },
  {
    label: 'Benchmarks',
    path: '/docs/benchmarking',
    description: 'Harness layout, local noise, baselines, and update workflow.',
  },
];

export default function Home() {
  return (
    <Layout title="Documentation" description="pg-kinetic documentation">
      <main className="docs-home">
        <div className="docs-home__gridline" aria-hidden="true" />
        <div className="container docs-home__shell">
          <section className="docs-home__hero">
            <div className="docs-home__copy">
              <p className="docs-home__eyebrow">
                <span className="docs-home__status" aria-hidden="true" />
                PostgreSQL wire proxy
              </p>
              <h1>pg-kinetic documentation</h1>
              <p className="docs-home__summary">
                Operator guides for pooling, routing, sharding, policy,
                observability, compatibility, and performance regression testing.
              </p>
              <div className="docs-home__actions">
                <Link className="docs-button docs-button--primary" to="/docs/admin">
                  Read the docs
                </Link>
                <Link className="docs-button docs-button--ghost" to="/docs/compatibility">
                  Compatibility matrix
                </Link>
              </div>
            </div>

            <aside className="docs-home__terminal" aria-label="Smoke command example">
              <div className="docs-home__chrome">
                <span />
                <span />
                <span />
                <strong>wire smoke</strong>
              </div>
              <pre>
                <code>{`$ docker compose -f bench/compose.yml up -d postgres pg-kinetic
$ cargo run -p xtask -- ci-linux
PASS: psql smoke passed on 127.0.0.1:58432
PASS: compatibility report complete`}</code>
              </pre>
            </aside>
          </section>

          <section className="docs-home__guides" aria-label="Documentation entry points">
            {guides.map((guide) => (
              <Link className="docs-home__card" key={guide.path} to={guide.path}>
                <span>{guide.label}</span>
                <p>{guide.description}</p>
              </Link>
            ))}
          </section>
        </div>
      </main>
    </Layout>
  );
}

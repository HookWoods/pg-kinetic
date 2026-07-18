import Link from '@docusaurus/Link';
import Layout from '@theme/Layout';

export default function Home() {
  return (
    <Layout title="Documentation" description="pg-kinetic documentation">
      <main className="docs-home">
        <div className="container">
          <p className="docs-home__eyebrow">PostgreSQL wire proxy</p>
          <h1>pg-kinetic documentation</h1>
          <p className="docs-home__summary">
            Operational guides for running, routing, observing, and benchmarking pg-kinetic.
          </p>
          <Link className="button button--primary button--lg" to="/docs/admin">
            Browse guides
          </Link>
        </div>
      </main>
    </Layout>
  );
}

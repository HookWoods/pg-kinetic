import pg from 'pg';
import { Kysely, PostgresDialect } from 'kysely';

const marker = 'compatibility report complete';
const target = process.env.PG_KINETIC_COMPAT_TARGET;
const url = target === 'pg-kinetic' ? process.env.DATABASE_URL_PROXY : process.env.DATABASE_URL_DIRECT;
const requested = process.env.PG_KINETIC_COMPAT_LIBRARY || 'pg';
const libraries = ['pg', 'kysely', 'prisma'];

const report = (payload) => console.log(JSON.stringify({
  ok: payload.outcome !== 'fail',
  success_marker: marker,
  language: 'javascript',
  libraries,
  target,
  ...payload,
}));

if (process.env.PG_KINETIC_COMPAT_LIVE !== '1') {
  report({ outcome: 'skip', skip_reason: 'live-stack-unavailable', error_summary: 'PG_KINETIC_COMPAT_LIVE=1 is required' });
  process.exit(0);
}
if (!url) {
  report({ outcome: 'skip', skip_reason: 'live-stack-unavailable', error_summary: 'target database URL is not configured' });
  process.exit(0);
}
if (requested === 'prisma') {
  report({
    outcome: 'skip',
    skip_reason: 'feature-unsupported',
    error_summary: 'Prisma ORM mapping is optional for this protocol smoke',
    cases: [],
  });
  process.exit(0);
}

const started = Date.now();
const pool = new pg.Pool({ connectionString: url, connectionTimeoutMillis: 5000, query_timeout: 5000, max: 1 });
let kysely;
try {
  if (requested === 'pg') {
    const connected = await pool.query('SELECT 1 AS connected');
    const parameterized = await pool.query('SELECT id, name FROM compat_items WHERE id = $1', [2]);
    if (connected.rows[0].connected !== 1) throw new Error('startup-connect returned unexpected row');
    if (parameterized.rows.length !== 1 || parameterized.rows[0].name !== 'beta') throw new Error('parameterized-query returned unexpected row');
    try {
      await pool.query('SELECT * FROM compat_missing_relation');
      throw new Error('error-propagation did not fail');
    } catch (error) {
      if (error.code !== '42P01') throw error;
    }
  } else if (requested === 'kysely') {
    kysely = new Kysely({ dialect: new PostgresDialect({ pool }) });
    const built = await kysely.selectFrom('compat_items').select(['id', 'name']).where('id', '=', 1).execute();
    if (built.length !== 1 || built[0].name !== 'alpha') throw new Error('kysely query returned unexpected row');
  } else {
    throw new Error(`unsupported library ${requested}`);
  }
  report({
    outcome: 'pass',
    duration_ms: Date.now() - started,
    cases: requested === 'kysely'
      ? [
        { case_id: 'startup-connect', outcome: 'connected' },
        { case_id: 'simple-query', outcome: 'one-row' },
      ]
      : [
        { case_id: 'startup-connect', outcome: 'connected' },
        { case_id: 'parameterized-query', outcome: 'one-row' },
        { case_id: 'error-propagation', outcome: 'sqlstate' },
      ],
  });
} catch (error) {
  report({ outcome: 'fail', duration_ms: Date.now() - started, error_summary: `${error.name}: ${error.message}` });
  process.exitCode = 1;
} finally {
  if (kysely) await kysely.destroy();
  await pool.end();
}

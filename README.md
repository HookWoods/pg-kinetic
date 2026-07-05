# pg-kinetic

pg-kinetic is a low-overhead PostgreSQL wire proxy for high-concurrency applications.

The first milestone focuses on:

- PostgreSQL startup and message forwarding
- a typed wire protocol parser
- transaction state tracking
- reproducible benchmarks against direct PostgreSQL, PgBouncer, and PgDog

## Local Benchmark Stack

Start the local stack:

```bash
docker compose -f bench/compose.yml up -d --build postgres pgbouncer pgdog pg-kinetic
```

Run smoke checks:

```bash
PGPASSWORD=postgres psql -h 127.0.0.1 -p 55432 -U postgres -d pgkinetic -c "select count(*) from accounts;"
PGPASSWORD=postgres psql -h 127.0.0.1 -p 58432 -U postgres -d pgkinetic -c "select count(*) from accounts;"
```

Run simple pgbench baselines:

```bash
PGPASSWORD=postgres pgbench -h 127.0.0.1 -p 55432 -U postgres -d pgkinetic -n -f bench/pgbench/basic.sql -c 32 -j 4 -T 30
PGPASSWORD=postgres pgbench -h 127.0.0.1 -p 56432 -U postgres -d pgkinetic -n -f bench/pgbench/basic.sql -c 32 -j 4 -T 30
PGPASSWORD=postgres pgbench -h 127.0.0.1 -p 57432 -U postgres -d pgkinetic -n -f bench/pgbench/basic.sql -c 32 -j 4 -T 30
PGPASSWORD=postgres pgbench -h 127.0.0.1 -p 58432 -U postgres -d pgkinetic -n -f bench/pgbench/basic.sql -c 32 -j 4 -T 30
```

Ports:

- `55432`: direct PostgreSQL
- `56432`: PgBouncer
- `57432`: PgDog
- `58432`: pg-kinetic

## Driver Compatibility Smoke Tests

Start the local stack before running compatibility checks:

```bash
docker compose -f bench/compose.yml up -d --build postgres pg-kinetic
```

Windows:

```powershell
powershell.exe -ExecutionPolicy Bypass -File scripts\smoke\compat.ps1
```

Unix:

```bash
chmod +x scripts/smoke/compat.sh
scripts/smoke/compat.sh
```

The smoke clients exercise prepared queries through:

- Rust `tokio-postgres`
- Go `pgx`
- Node.js `pg`
- Python `psycopg`

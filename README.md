# pg-kinetic

pg-kinetic is a low-overhead PostgreSQL wire proxy for high-concurrency applications.

The first milestone focuses on:

- PostgreSQL startup and message forwarding
- a typed wire protocol parser
- transaction state tracking
- reproducible benchmarks against direct PostgreSQL, PgBouncer, and PgDog

The project is intentionally small at the start. Advanced transaction pooling, prepared statement virtualization, read routing, and io_uring experiments build on this foundation after the protocol core is measurable and correct.

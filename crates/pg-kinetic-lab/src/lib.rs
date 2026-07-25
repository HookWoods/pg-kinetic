//! Offline tooling for pg-kinetic: benchmarking, regression runs, client
//! compatibility suites, and profiling.
//!
//! None of this is on the request path. It lives outside `pg-kinetic-proxy` so
//! the library that ships in the container carries only runtime code, and so the
//! module names here (`benchmark`, `compatibility`, `regression`) no longer
//! collide with the runtime models of the same name in `pg-kinetic-core`.

pub mod benchmark;
pub mod compatibility;
pub mod profile;
pub mod regression;

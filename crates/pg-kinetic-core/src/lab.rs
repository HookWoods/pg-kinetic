//! Models backing the offline tooling in the `pg-kinetic-lab` crate: benchmark
//! scenarios, the client compatibility matrix, and the regression manifest.
//!
//! The models live in core because the proxy reports on them through admin
//! snapshots; the runners that use them live in `pg-kinetic-lab`.

pub mod benchmark;
pub mod compatibility;
pub mod regression;

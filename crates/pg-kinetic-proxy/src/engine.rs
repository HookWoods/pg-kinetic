//! Runtime engines and the byte-stream abstraction they share.
//!
//! Named `engine` rather than `runtime` so it does not collide with
//! `pg_kinetic_core::cluster::runtime`, which models lifecycle state.

pub mod io_runtime;
pub mod io_uring;
#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub mod io_uring_transport;
pub mod runtime_engine;

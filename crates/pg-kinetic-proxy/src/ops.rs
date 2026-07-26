//! Operational control surfaces: the admin listener, the control plane, adaptive
//! tuning, and traffic mirroring.
//!
//! Grouped under `ops` so these names no longer collide with the runtime models
//! of the same name in `pg-kinetic-core`. `pg_kinetic_proxy::ops::admin` is the
//! listener and its wiring; `pg_kinetic_core::admin` is the command/view model it
//! serves.

pub mod adaptive;
pub mod admin;
pub mod control;
pub mod drain;
pub mod lifecycle;
pub mod mirror;
pub mod pause;
pub mod preflight;
pub mod reload;

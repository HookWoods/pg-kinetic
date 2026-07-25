//! Where a query goes and whether it is allowed to go there: route keys, read
//! routing, shard selection, policy rules, mirroring, and backpressure. Decision
//! models only; the proxy's `routing` module wires them to config and I/O.

pub mod backpressure;
pub mod mirror;
pub mod policy;
pub mod policy_rule;
pub mod route;
pub mod routing;
pub mod shard_extract;
pub mod sharding;

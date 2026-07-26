//! Node and cluster state: runtime lifecycle, replica health and lag, LSN
//! freshness, recovery decisions, connection cleanup, and adaptive tuning.

pub mod adaptive;
pub mod cleanup;
pub mod control;
pub mod ha;
pub mod lsn;
pub mod recovery;
pub mod runtime;

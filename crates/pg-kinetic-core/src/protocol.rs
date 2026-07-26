//! PostgreSQL session and statement models: SQL classification, prepared
//! statement tracking, and the virtual session state the proxy maintains on a
//! client's behalf. Pure logic — no I/O.

pub mod pin;
pub mod prepare;
pub mod session;
pub mod sql;
pub mod sql_classify;
pub mod virtual_session;

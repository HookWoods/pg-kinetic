//! Transport plumbing: sockets, TLS, and the buffer/limit machinery that sits
//! between the kernel and the protocol layer. Nothing here knows about the
//! PostgreSQL wire format.

pub mod buffers;
pub mod limits;
pub mod pressure;
pub mod socket;
pub mod tls;

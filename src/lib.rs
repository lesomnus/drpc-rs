//! A reliable dRPC server endpoint for one ordered, message-preserving peer.
#[path = "generated/mod.rs"]
mod wire;
pub use wire::drpc;
mod metadata;
mod server;
pub use server::Session;
#[cfg(target_arch = "wasm32")]
mod port;
#[cfg(target_arch = "wasm32")]
pub use port::PortServer;

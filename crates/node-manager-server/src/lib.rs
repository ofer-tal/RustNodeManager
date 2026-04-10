//! Node Manager gRPC service implementation.
//!
//! This module provides the gRPC service that handles heartbeat requests from data nodes
//! and status queries from clients. It integrates with the core business logic through
//! the HeartbeatProcessor and NodeManagerState.

pub mod service;
pub mod conversion;
pub mod server;

pub use service::NodeManagerServiceImpl;
pub use server::NodeManagerServer;

//! Node Manager Core - Domain model and business logic.
//!
//! This crate contains the core domain model, error types, configuration,
//! and business logic for the Node Manager service. It has ZERO dependencies
//! on gRPC frameworks (tonic/prost), making it suitable for testing and reuse.
//!
//! # Modules
//!
//! - [`error`] - Domain-specific error types
//! - [`config`] - Configuration structures with YAML/ENV support
//! - [`model`] - Core domain models (Node, Cluster, State)
//! - [`storage`] - State persistence abstraction
//! - [`engine`] - Role assignment and heartbeat processing
//! - [`monitor`] - Heartbeat monitoring for missed heartbeats

pub mod config;
pub mod engine;
pub mod error;
pub mod model;
pub mod monitor;
pub mod storage;

// Re-export commonly used types
pub use error::NodeManagerError;
pub use config::{AppConfig, HeartbeatConfig, RolesConfig};

pub use model::{
    cluster::Cluster,
    node::{HealthStatus, Node, NodeStatus, RoleLease, RoleType},
    node_manager_state::NodeManagerState,
};

pub use storage::{
    in_memory::InMemoryStore,
    json_file::JsonFileStore,
    trait_::{StateStore, EMPTY_STATE_MARKER},
};

pub use engine::{ClusterNodeInfo, HeartbeatProcessor, HeartbeatResult, RoleAction, RoleAssignmentEngine, RoleChange};

pub use monitor::HeartbeatMonitor;

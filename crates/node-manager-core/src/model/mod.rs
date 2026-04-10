//! Core domain models for the Node Manager.
//!
//! These models represent the state of nodes, clusters, and role assignments.
//! They are pure Rust types with no framework dependencies.

pub mod cluster;
pub mod node;
pub mod node_manager_state;

pub use cluster::Cluster;
pub use node::{HealthStatus, Node, NodeStatus, RoleLease, RoleType};
pub use node_manager_state::NodeManagerState;

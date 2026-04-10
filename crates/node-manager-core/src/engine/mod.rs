//! Engine module for role assignment and heartbeat processing.

pub mod heartbeat_processor;
pub mod role_assignment;

pub use heartbeat_processor::{ClusterNodeInfo, HeartbeatProcessor, HeartbeatResult};
pub use role_assignment::{RoleAction, RoleAssignmentEngine, RoleChange};

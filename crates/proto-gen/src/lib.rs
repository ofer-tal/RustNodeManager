//! Generated protobuf code for the Node Manager service.
//!
//! This crate contains the message types and service trait generated from `node_manager.proto`.

pub mod node_manager {
    tonic::include_proto!("node_manager");
}

/// Re-exported types for convenient access.
pub use node_manager::{
    node_manager_service_client::NodeManagerServiceClient,
    node_manager_service_server::NodeManagerService,
    node_manager_service_server::NodeManagerServiceServer,
};

pub use node_manager::{
    ClusterStatus, HealthStatus, HeartbeatRequest, HeartbeatResponse, NodeStatus,
    PeerNodeInfo, RoleLease, RoleType, StatusRequest, StatusResponse,
};

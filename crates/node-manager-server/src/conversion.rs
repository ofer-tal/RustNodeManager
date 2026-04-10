//! Conversion between protobuf and domain model types.

use proto_gen::node_manager::{
    ClusterStatus, HealthStatus as ProtoHealthStatus, HeartbeatResponse, NodeStatus as ProtoNodeStatus,
    PeerNodeInfo, RoleLease as ProtoRoleLease, RoleType as ProtoRoleType,
};
use node_manager_core::{
    model::{HealthStatus, NodeStatus, RoleLease, RoleType},
    HeartbeatResult,
};

/// Converts a protobuf health status i32 to a domain health status.
///
/// Returns `None` for unrecognized values, allowing callers to reject
/// the request rather than silently defaulting.
pub fn proto_to_domain_health_status(status: i32) -> Option<HealthStatus> {
    match ProtoHealthStatus::try_from(status) {
        Ok(ProtoHealthStatus::Healthy) => Some(HealthStatus::Healthy),
        Ok(ProtoHealthStatus::Unhealthy) => Some(HealthStatus::Unhealthy),
        Ok(ProtoHealthStatus::Draining) => Some(HealthStatus::Draining),
        Ok(ProtoHealthStatus::Unspecified) => {
            // Unspecified is the proto3 default (0) — treat as healthy
            Some(HealthStatus::Healthy)
        }
        Err(_) => None,
    }
}

/// Converts a domain node status to a protobuf node status.
pub fn domain_to_proto_node_status(status: NodeStatus) -> ProtoNodeStatus {
    match status {
        NodeStatus::Assignable => ProtoNodeStatus::Assignable,
        NodeStatus::Suspect => ProtoNodeStatus::Suspect,
        NodeStatus::Unassignable => ProtoNodeStatus::Unassignable,
    }
}

/// Converts a domain role type to a protobuf role type.
pub fn domain_to_proto_role_type(role: RoleType) -> ProtoRoleType {
    match role {
        RoleType::Metadata => ProtoRoleType::Metadata,
        RoleType::Data => ProtoRoleType::Data,
        RoleType::Storage => ProtoRoleType::Storage,
    }
}

/// Converts a domain role lease to a protobuf role lease.
pub fn domain_to_proto_role_lease(lease: &RoleLease) -> ProtoRoleLease {
    ProtoRoleLease {
        role: domain_to_proto_role_type(lease.role) as i32,
        fencing_token: lease.fencing_token,
        lease_expires_at: lease.expires_at().timestamp_millis() as u64,
    }
}

/// Converts a domain heartbeat result to a protobuf heartbeat response.
pub fn domain_to_proto_heartbeat_response(
    result: HeartbeatResult,
) -> HeartbeatResponse {
    let cluster_nodes = result
        .cluster_nodes
        .into_iter()
        .map(|node_info| PeerNodeInfo {
            node_id: node_info.node_id,
            status: domain_to_proto_node_status(node_info.status) as i32,
            roles: node_info
                .roles
                .iter()
                .map(domain_to_proto_role_lease)
                .collect(),
        })
        .collect();

    HeartbeatResponse {
        node_status: domain_to_proto_node_status(result.node_status) as i32,
        assigned_roles: result
            .assigned_roles
            .iter()
            .map(domain_to_proto_role_lease)
            .collect(),
        cluster_nodes,
    }
}

/// Converts domain cluster state to a protobuf cluster status.
pub fn domain_to_proto_cluster_status(
    cluster_id: String,
    nodes: Vec<node_manager_core::engine::ClusterNodeInfo>,
) -> ClusterStatus {
    ClusterStatus {
        cluster_id,
        nodes: nodes
            .into_iter()
            .map(|node_info| PeerNodeInfo {
                node_id: node_info.node_id,
                status: domain_to_proto_node_status(node_info.status) as i32,
                roles: node_info
                    .roles
                    .iter()
                    .map(domain_to_proto_role_lease)
                    .collect(),
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use node_manager_core::model::RoleLease;

    #[test]
    fn test_proto_to_domain_health_status() {
        assert_eq!(
            proto_to_domain_health_status(ProtoHealthStatus::Healthy as i32),
            Some(HealthStatus::Healthy)
        );
        assert_eq!(
            proto_to_domain_health_status(ProtoHealthStatus::Unhealthy as i32),
            Some(HealthStatus::Unhealthy)
        );
        assert_eq!(
            proto_to_domain_health_status(ProtoHealthStatus::Draining as i32),
            Some(HealthStatus::Draining)
        );
        // Invalid value should return None
        assert_eq!(proto_to_domain_health_status(999), None);
    }

    #[test]
    fn test_domain_to_proto_node_status() {
        assert_eq!(
            domain_to_proto_node_status(NodeStatus::Assignable),
            ProtoNodeStatus::Assignable
        );
        assert_eq!(
            domain_to_proto_node_status(NodeStatus::Suspect),
            ProtoNodeStatus::Suspect
        );
        assert_eq!(
            domain_to_proto_node_status(NodeStatus::Unassignable),
            ProtoNodeStatus::Unassignable
        );
    }

    #[test]
    fn test_domain_to_proto_role_type() {
        assert_eq!(
            domain_to_proto_role_type(RoleType::Metadata),
            ProtoRoleType::Metadata
        );
        assert_eq!(
            domain_to_proto_role_type(RoleType::Data),
            ProtoRoleType::Data
        );
        assert_eq!(
            domain_to_proto_role_type(RoleType::Storage),
            ProtoRoleType::Storage
        );
    }

    #[test]
    fn test_domain_to_proto_role_lease() {
        let now = Utc::now();
        let domain_lease = RoleLease::new(RoleType::Metadata, 42, now, 60);

        let proto_lease = domain_to_proto_role_lease(&domain_lease);

        assert_eq!(proto_lease.role, ProtoRoleType::Metadata as i32);
        assert_eq!(proto_lease.fencing_token, 42);
        // The expires_at should be approximately now + 60 seconds
        assert!(proto_lease.lease_expires_at > now.timestamp_millis() as u64);
    }

    #[test]
    fn test_domain_to_proto_heartbeat_response() {
        let result = HeartbeatResult {
            node_status: NodeStatus::Assignable,
            assigned_roles: vec![RoleLease::new(
                RoleType::Metadata,
                1,
                Utc::now(),
                60,
            )],
            cluster_nodes: vec![],
        };

        let response = domain_to_proto_heartbeat_response(result);

        assert_eq!(response.node_status, ProtoNodeStatus::Assignable as i32);
        assert_eq!(response.assigned_roles.len(), 1);
        assert_eq!(response.assigned_roles[0].role, ProtoRoleType::Metadata as i32);
    }
}

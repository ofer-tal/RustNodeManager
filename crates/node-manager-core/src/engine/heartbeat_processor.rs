//! Heartbeat processor for orchestrating the full heartbeat handling flow.
//!
//! The heartbeat processor coordinates:
//! - Updating node state with new heartbeat information
//! - Computing role assignments based on current cluster state
//! - Applying role changes to nodes
//! - Extending lease durations for active roles
//! - Building the response with all cluster information

use crate::config::RolesConfig;
use crate::engine::RoleAssignmentEngine;
use crate::engine::{RoleAction, RoleChange};
use crate::model::{HealthStatus, NodeStatus, RoleLease};
use crate::NodeManagerState;
use chrono::{DateTime, Utc};
use tracing::debug;

/// Result of processing a heartbeat, containing all information needed for the response.
#[derive(Debug, Clone)]
pub struct HeartbeatResult {
    /// The node's status after processing.
    pub node_status: NodeStatus,
    /// All role leases currently assigned to the node.
    pub assigned_roles: Vec<RoleLease>,
    /// All nodes in the cluster with their status and roles.
    pub cluster_nodes: Vec<ClusterNodeInfo>,
}

/// Information about a node in the cluster for the heartbeat response.
#[derive(Debug, Clone)]
pub struct ClusterNodeInfo {
    /// Node ID.
    pub node_id: String,
    /// Current node status.
    pub status: NodeStatus,
    /// Roles currently assigned to this node.
    pub roles: Vec<RoleLease>,
}

/// Heartbeat processor that orchestrates the full heartbeat handling flow.
///
/// The processor maintains both the node manager state and the role assignment engine,
/// coordinating all state transitions and role computations.
#[derive(Debug)]
pub struct HeartbeatProcessor {
    /// Current node manager state.
    state: NodeManagerState,
    /// Role assignment engine for computing role changes.
    role_engine: RoleAssignmentEngine,
}

impl HeartbeatProcessor {
    /// Creates a new heartbeat processor.
    ///
    /// # Arguments
    ///
    /// * `state` - Initial node manager state
    /// * `config` - Role assignment configuration
    pub fn new(state: NodeManagerState, config: RolesConfig) -> Self {
        let role_engine = RoleAssignmentEngine::new(config);
        Self { state, role_engine }
    }

    /// Creates a new heartbeat processor with an existing role assignment engine.
    pub fn with_engine(state: NodeManagerState, role_engine: RoleAssignmentEngine) -> Self {
        Self { state, role_engine }
    }

    /// Gets a reference to the current state.
    pub fn state(&self) -> &NodeManagerState {
        &self.state
    }

    /// Gets a mutable reference to the current state.
    pub fn state_mut(&mut self) -> &mut NodeManagerState {
        &mut self.state
    }

    /// Gets a reference to the role assignment engine.
    pub fn role_engine(&self) -> &RoleAssignmentEngine {
        &self.role_engine
    }

    /// Returns simultaneous mutable state and immutable role engine references.
    ///
    /// This method exists to satisfy the borrow checker when both references
    /// are needed at the same time (e.g., for the heartbeat monitor).
    pub fn state_and_engine(&mut self) -> (&mut NodeManagerState, &RoleAssignmentEngine) {
        (&mut self.state, &self.role_engine)
    }

    /// Processes a heartbeat from a node.
    ///
    /// This is the main entry point for heartbeat handling. It orchestrates:
    /// 1. Updates or creates the node with the new health status
    /// 2. Computes role assignments for the cluster
    /// 3. Applies role changes to nodes
    /// 4. Extends lease durations for the heartbeat node
    /// 5. Builds the response with cluster information
    ///
    /// # Arguments
    ///
    /// * `node_id` - ID of the node sending the heartbeat
    /// * `cluster_id` - ID of the cluster the node belongs to
    /// * `health_status` - Self-reported health status from the node
    /// * `now` - Current timestamp for the heartbeat
    ///
    /// # Returns
    ///
    /// A `HeartbeatResult` containing the node's status, assigned roles, and cluster info.
    pub fn process_heartbeat(
        &mut self,
        node_id: String,
        cluster_id: String,
        health_status: HealthStatus,
        now: DateTime<Utc>,
    ) -> HeartbeatResult {
        debug!(
            node_id = %node_id,
            cluster_id = %cluster_id,
            health_status = ?health_status,
            "Processing heartbeat"
        );

        // Step 1: Update or create the node with the new heartbeat
        self.state.process_heartbeat(
            node_id.clone(),
            cluster_id.clone(),
            health_status,
            now,
        );

        // Step 2: Get the cluster and compute role assignments
        // Safe to unwrap because process_heartbeat always creates the cluster
        #[allow(clippy::unwrap_used)]
        let cluster = self.state.get_cluster(&cluster_id).unwrap();

        let role_changes = self.role_engine.compute_assignments(cluster);

        debug!(
            node_id = %node_id,
            changes_count = role_changes.len(),
            "Computed {} role changes",
            role_changes.len()
        );

        // Step 3: Apply all role changes to the state using the processor's apply_role_change method
        for change in &role_changes {
            self.apply_role_change(change, now);
        }

        // Step 4: Extend lease durations for all existing roles of the heartbeat node
        if let Some(node) = self.state.get_node_mut(&node_id) {
            let lease_duration_secs = self.role_engine.config().lease_duration_secs;
            node.extend_leases(now, lease_duration_secs);
            debug!(
                node_id = %node_id,
                lease_duration_secs = lease_duration_secs,
                role_count = node.roles.len(),
                "Extended leases for {} roles",
                node.roles.len()
            );
        }

        // Step 5: Collect cluster node info AFTER all mutations for accurate response
        #[allow(clippy::unwrap_used)]
        let cluster = self.state.get_cluster(&cluster_id).unwrap();
        let cluster_nodes_snapshot: Vec<ClusterNodeInfo> = cluster
            .sorted_nodes()
            .iter()
            .map(|n| ClusterNodeInfo {
                node_id: n.node_id.clone(),
                status: n.status,
                roles: n.roles.clone(),
            })
            .collect();

        // Step 6: Build the response
        // Safe to unwrap because process_heartbeat always creates/updates the node
        #[allow(clippy::unwrap_used)]
        let node = self.state.get_node(&node_id).unwrap();
        let node_status = node.status;
        let assigned_roles = node.roles.clone();

        debug!(
            node_id = %node_id,
            node_status = ?node_status,
            role_count = assigned_roles.len(),
            cluster_nodes_count = cluster_nodes_snapshot.len(),
            "Heartbeat processed successfully"
        );

        HeartbeatResult {
            node_status,
            assigned_roles,
            cluster_nodes: cluster_nodes_snapshot,
        }
    }

    /// Applies a single role change to the state.
    fn apply_role_change(&mut self, change: &RoleChange, now: DateTime<Utc>) {
        match &change.action {
            RoleAction::Assign(role_type, fencing_token) => {
                if let Some(node) = self.state.get_node_mut(&change.node_id) {
                    if !node.has_role(*role_type) {
                        let lease = RoleLease::new(
                            *role_type,
                            *fencing_token,
                            now,
                            self.role_engine.config().lease_duration_secs,
                        );
                        node.add_role(lease);

                        // Update max fencing token for persistence (only for non-zero tokens)
                        if *fencing_token > 0 {
                            self.state.update_max_fencing_token(*fencing_token);
                        }

                        debug!(
                            node_id = %change.node_id,
                            role = ?role_type,
                            fencing_token = *fencing_token,
                            "Assigned role to node"
                        );
                    }
                }
            }
            RoleAction::Revoke(role_type) => {
                if let Some(node) = self.state.get_node_mut(&change.node_id) {
                    node.remove_role(*role_type);
                    debug!(
                        node_id = %change.node_id,
                        role = ?role_type,
                        "Revoked role from node"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RolesConfig;
    use crate::model::RoleType;

    fn create_test_processor() -> HeartbeatProcessor {
        let state = NodeManagerState::new();
        let config = RolesConfig::default();
        HeartbeatProcessor::new(state, config)
    }

    #[test]
    fn test_first_heartbeat_creates_node_and_assigns_all_roles() {
        let mut processor = create_test_processor();
        let now = Utc::now();

        let result = processor.process_heartbeat(
            "node-1".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Healthy,
            now,
        );

        // Node should be assignable
        assert_eq!(result.node_status, NodeStatus::Assignable);

        // Should have all three roles (metadata, data, storage)
        assert_eq!(result.assigned_roles.len(), 3);
        assert!(result.assigned_roles.iter().any(|r| r.role == RoleType::Metadata));
        assert!(result.assigned_roles.iter().any(|r| r.role == RoleType::Data));
        assert!(result.assigned_roles.iter().any(|r| r.role == RoleType::Storage));

        // Cluster should have exactly one node
        assert_eq!(result.cluster_nodes.len(), 1);
        assert_eq!(result.cluster_nodes[0].node_id, "node-1");
    }

    #[test]
    fn test_second_heartbeat_extends_leases() {
        let mut processor = create_test_processor();
        let now = Utc::now();

        // First heartbeat
        processor.process_heartbeat(
            "node-1".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Healthy,
            now,
        );

        let later = now + chrono::Duration::seconds(30);

        // Second heartbeat
        let result = processor.process_heartbeat(
            "node-1".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Healthy,
            later,
        );

        // Should still have all three roles
        assert_eq!(result.assigned_roles.len(), 3);

        // All leases should have been extended (granted_at should be 'later')
        for lease in &result.assigned_roles {
            assert_eq!(lease.granted_at, later);
        }
    }

    #[test]
    fn test_second_node_gets_data_and_storage_first_keeps_metadata() {
        let mut processor = create_test_processor();
        let now = Utc::now();

        // First node
        processor.process_heartbeat(
            "node-1".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Healthy,
            now,
        );

        // Second node
        let result = processor.process_heartbeat(
            "node-2".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Healthy,
            now,
        );

        // node-2 should have data role only (storage is at 30% cap, so only 1 of 2 nodes gets it)
        assert!(result.assigned_roles.iter().any(|r| r.role == RoleType::Data));
        // With 2 nodes and 30% cap, max_storage = max(1, (2*30)/100) = 1
        // So only the first node (node-1) gets storage
        assert!(!result.assigned_roles.iter().any(|r| r.role == RoleType::Storage));
        assert!(!result.assigned_roles.iter().any(|r| r.role == RoleType::Metadata));

        // Cluster should have two nodes
        assert_eq!(result.cluster_nodes.len(), 2);

        // node-1 should still have metadata
        let node1_info = result.cluster_nodes.iter().find(|n| n.node_id == "node-1").unwrap();
        assert!(node1_info.roles.iter().any(|r| r.role == RoleType::Metadata));
    }

    #[test]
    fn test_unhealthy_heartbeat_makes_node_unassignable_and_revokes_roles() {
        let mut processor = create_test_processor();
        let now = Utc::now();

        // First heartbeat - healthy
        processor.process_heartbeat(
            "node-1".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Healthy,
            now,
        );

        // Second heartbeat - unhealthy
        let result = processor.process_heartbeat(
            "node-1".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Unhealthy,
            now + chrono::Duration::seconds(10),
        );

        // Node should be unassignable
        assert_eq!(result.node_status, NodeStatus::Unassignable);

        // Should have no roles (all revoked)
        assert_eq!(result.assigned_roles.len(), 0);
    }

    #[test]
    fn test_draining_heartbeat_makes_node_unassignable() {
        let mut processor = create_test_processor();
        let now = Utc::now();

        let result = processor.process_heartbeat(
            "node-1".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Draining,
            now,
        );

        // Node should be unassignable
        assert_eq!(result.node_status, NodeStatus::Unassignable);

        // Should have no roles
        assert_eq!(result.assigned_roles.len(), 0);
    }

    #[test]
    fn test_suspect_node_recovery_to_assignable() {
        let mut processor = create_test_processor();
        let now = Utc::now();

        // Create node and mark it as suspect
        processor.state.process_heartbeat(
            "node-1".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Healthy,
            now,
        );
        if let Some(node) = processor.state.get_node_mut("node-1") {
            node.mark_suspect();
        }

        // Send healthy heartbeat - should recover to assignable
        let result = processor.process_heartbeat(
            "node-1".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Healthy,
            now + chrono::Duration::seconds(5),
        );

        assert_eq!(result.node_status, NodeStatus::Assignable);
    }

    #[test]
    fn test_unassignable_node_recovery_to_assignable_gets_roles_back() {
        let mut processor = create_test_processor();
        let now = Utc::now();

        // Create node with unhealthy status (becomes unassignable)
        processor.state.process_heartbeat(
            "node-1".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Unhealthy,
            now,
        );

        // Send healthy heartbeat - should recover to assignable
        let result = processor.process_heartbeat(
            "node-1".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Healthy,
            now + chrono::Duration::seconds(5),
        );

        assert_eq!(result.node_status, NodeStatus::Assignable);

        // Should have roles assigned again
        assert!(result.assigned_roles.iter().any(|r| r.role == RoleType::Metadata));
        assert!(result.assigned_roles.iter().any(|r| r.role == RoleType::Data));
        assert!(result.assigned_roles.iter().any(|r| r.role == RoleType::Storage));
    }

    #[test]
    fn test_multiple_clusters_are_independent() {
        let mut processor = create_test_processor();
        let now = Utc::now();

        // Add nodes to two different clusters
        processor.process_heartbeat(
            "node-1".to_string(),
            "cluster-a".to_string(),
            HealthStatus::Healthy,
            now,
        );

        processor.process_heartbeat(
            "node-2".to_string(),
            "cluster-b".to_string(),
            HealthStatus::Healthy,
            now,
        );

        // Each cluster should have its own metadata holder
        let result_a = processor.process_heartbeat(
            "node-1".to_string(),
            "cluster-a".to_string(),
            HealthStatus::Healthy,
            now,
        );

        let result_b = processor.process_heartbeat(
            "node-2".to_string(),
            "cluster-b".to_string(),
            HealthStatus::Healthy,
            now,
        );

        // node-1 should have metadata in cluster-a
        assert!(result_a.assigned_roles.iter().any(|r| r.role == RoleType::Metadata));

        // node-2 should have metadata in cluster-b
        assert!(result_b.assigned_roles.iter().any(|r| r.role == RoleType::Metadata));

        // Each cluster should have only one node
        assert_eq!(result_a.cluster_nodes.len(), 1);
        assert_eq!(result_b.cluster_nodes.len(), 1);
    }

    #[test]
    fn test_fencing_token_increments_on_metadata_reassignment() {
        let mut processor = create_test_processor();
        let now = Utc::now();

        // First node gets metadata with token 1
        processor.process_heartbeat(
            "node-1".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Healthy,
            now,
        );

        let node1 = processor.state.get_node("node-1").unwrap();
        let metadata_lease1 = node1.get_role_lease(RoleType::Metadata).unwrap();
        let token1 = metadata_lease1.fencing_token;

        // Second node joins, but metadata should stay with node-1
        processor.process_heartbeat(
            "node-2".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Healthy,
            now,
        );

        // Mark node-1 as unassignable (manually, to trigger reassignment)
        if let Some(node) = processor.state.get_node_mut("node-1") {
            node.mark_unassignable();
        }

        // Heartbeat from node-2 should trigger reassignment
        processor.process_heartbeat(
            "node-2".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Healthy,
            now + chrono::Duration::seconds(1),
        );

        // node-2 should now have metadata
        let node2 = processor.state.get_node("node-2").unwrap();
        let metadata_lease2 = node2.get_role_lease(RoleType::Metadata).unwrap();
        let token2 = metadata_lease2.fencing_token;

        // Token should have incremented
        assert!(token2 > token1);
    }

    #[test]
    fn test_cluster_nodes_response_includes_all_nodes() {
        let mut processor = create_test_processor();
        let now = Utc::now();

        // Add multiple nodes to the same cluster
        for i in 1..=3 {
            processor.process_heartbeat(
                format!("node-{}", i),
                "cluster-1".to_string(),
                HealthStatus::Healthy,
                now,
            );
        }

        let result = processor.process_heartbeat(
            "node-1".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Healthy,
            now,
        );

        // All three nodes should be in the response
        assert_eq!(result.cluster_nodes.len(), 3);

        // All nodes should be sorted by node_id
        assert_eq!(result.cluster_nodes[0].node_id, "node-1");
        assert_eq!(result.cluster_nodes[1].node_id, "node-2");
        assert_eq!(result.cluster_nodes[2].node_id, "node-3");

        // All nodes should be assignable
        assert!(result.cluster_nodes.iter().all(|n| n.status == NodeStatus::Assignable));
    }
}

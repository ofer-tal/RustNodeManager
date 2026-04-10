//! Node Manager aggregate root with dual-indexed state.
//!
//! This is the central state structure maintaining all clusters and nodes.
//! It provides dual indexes: cluster_id -> Cluster and node_id -> cluster_id.

use crate::{
    error::NodeManagerError,
    model::{Cluster, HealthStatus, Node, NodeStatus},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info, warn};

/// The aggregate root for all Node Manager state.
///
/// Maintains dual indexes for efficient lookup:
/// - `clusters`: cluster_id -> Cluster
/// - `node_to_cluster`: node_id -> cluster_id (for reverse lookup)
/// - `max_fencing_token`: highest fencing token ever issued (persisted for recovery)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeManagerState {
    /// All clusters indexed by cluster_id.
    clusters: HashMap<String, Cluster>,
    /// Reverse index: node_id -> cluster_id for O(1) node lookup.
    node_to_cluster: HashMap<String, String>,
    /// Highest fencing token ever issued (persists across restarts).
    max_fencing_token: u64,
}

impl Default for NodeManagerState {
    fn default() -> Self {
        Self::new()
    }
}

impl NodeManagerState {
    /// Creates a new empty state.
    pub fn new() -> Self {
        Self {
            clusters: HashMap::new(),
            node_to_cluster: HashMap::new(),
            max_fencing_token: 0,
        }
    }

    /// Gets the current maximum fencing token.
    pub fn max_fencing_token(&self) -> u64 {
        self.max_fencing_token
    }

    /// Updates the maximum fencing token if the given value is higher.
    pub fn update_max_fencing_token(&mut self, token: u64) {
        if token > self.max_fencing_token {
            self.max_fencing_token = token;
        }
    }

    /// Processes a heartbeat from a node.
    ///
    /// This is the main state transition method. It:
    /// 1. Creates or updates the node with the new health status
    /// 2. Resets missed_heartbeats to 0
    /// 3. Transitions status based on health:
    ///    - Unhealthy/Draining → Unassignable
    ///    - Healthy + Suspect → Assignable (recovery)
    ///    - Healthy + Unassignable → Assignable (recovery)
    /// 4. Creates cluster if it doesn't exist
    ///
    /// Returns the node_id of the processed node.
    pub fn process_heartbeat(
        &mut self,
        node_id: String,
        cluster_id: String,
        health_status: HealthStatus,
        now: chrono::DateTime<chrono::Utc>,
    ) -> String {
        // Get or create the cluster
        let cluster = self.get_or_create_cluster_mut(&cluster_id);

        // Update or create the node
        let result_node_id = if let Some(existing) = cluster.get_node_mut(&node_id) {
            // Update existing node
            existing.health_status = health_status;
            existing.record_heartbeat(now);

            // Handle status transitions based on health
            match health_status {
                HealthStatus::Unhealthy | HealthStatus::Draining => {
                    if existing.status != NodeStatus::Unassignable {
                        warn!(
                            node_id = %node_id,
                            cluster_id = %cluster_id,
                            health = ?health_status,
                            "Marking node Unassignable due to health status"
                        );
                        existing.mark_unassignable();
                    }
                }
                HealthStatus::Healthy => {
                    // Recovery transitions: Suspect/Unassignable -> Assignable
                    if existing.status == NodeStatus::Suspect || existing.status == NodeStatus::Unassignable {
                        info!(
                            node_id = %node_id,
                            cluster_id = %cluster_id,
                            previous_status = ?existing.status,
                            "Node recovered, marking Assignable"
                        );
                        existing.mark_assignable();
                    }
                }
            }

            debug!(
                node_id = %node_id,
                cluster_id = %cluster_id,
                status = ?existing.status,
                missed_heartbeats = existing.missed_heartbeats,
                "Processed heartbeat for existing node"
            );

            node_id.clone()
        } else {
            // Create new node
            let node = Node::new(node_id.clone(), cluster_id.clone(), health_status, now);
            debug!(
                node_id = %node_id,
                cluster_id = %cluster_id,
                status = ?node.status,
                "Registered new node"
            );
            cluster.add_node(node);
            self.node_to_cluster.insert(node_id.clone(), cluster_id.clone());
            node_id
        };

        result_node_id
    }

    /// Gets an immutable reference to a node by ID.
    pub fn get_node(&self, node_id: &str) -> Option<&Node> {
        let cluster_id = self.node_to_cluster.get(node_id)?;
        self.clusters.get(cluster_id)?.get_node(node_id)
    }

    /// Gets a mutable reference to a node by ID.
    pub fn get_node_mut(&mut self, node_id: &str) -> Option<&mut Node> {
        let cluster_id = self.node_to_cluster.get(node_id)?.clone();
        self.clusters.get_mut(&cluster_id)?.get_node_mut(node_id)
    }

    /// Gets an immutable reference to a cluster by ID.
    pub fn get_cluster(&self, cluster_id: &str) -> Option<&Cluster> {
        self.clusters.get(cluster_id)
    }

    /// Gets a mutable reference to a cluster by ID.
    pub fn get_cluster_mut(&mut self, cluster_id: &str) -> Option<&mut Cluster> {
        self.clusters.get_mut(cluster_id)
    }

    /// Gets an existing cluster or creates a new one.
    pub fn get_or_create_cluster_mut(&mut self, cluster_id: &str) -> &mut Cluster {
        self.clusters
            .entry(cluster_id.to_string())
            .or_insert_with(|| {
                debug!(cluster_id = %cluster_id, "Creating new cluster");
                Cluster::new(cluster_id.to_string())
            })
    }

    /// Returns an iterator over all nodes across all clusters.
    pub fn all_nodes(&self) -> impl Iterator<Item = &Node> {
        self.clusters.values().flat_map(|c| c.nodes_iter())
    }

    /// Removes a node from the state.
    ///
    /// Returns the removed node if it existed.
    pub fn remove_node(&mut self, node_id: &str) -> Option<Node> {
        let cluster_id = self.node_to_cluster.remove(node_id)?;
        let cluster = self.clusters.get_mut(&cluster_id)?;
        cluster.remove_node(node_id)
    }

    /// Gets all cluster IDs.
    pub fn cluster_ids(&self) -> impl Iterator<Item = &String> {
        self.clusters.keys()
    }

    /// Gets all node IDs.
    pub fn node_ids(&self) -> impl Iterator<Item = &String> {
        self.node_to_cluster.keys()
    }

    /// Returns the number of clusters.
    pub fn cluster_count(&self) -> usize {
        self.clusters.len()
    }

    /// Returns the total number of nodes across all clusters.
    pub fn node_count(&self) -> usize {
        self.node_to_cluster.len()
    }

    /// Gets the cluster ID for a given node ID.
    pub fn get_node_cluster_id(&self, node_id: &str) -> Option<&String> {
        self.node_to_cluster.get(node_id)
    }

    /// Returns true if the state is empty (no clusters or nodes).
    pub fn is_empty(&self) -> bool {
        self.clusters.is_empty()
    }

    /// Applies role changes to nodes.
    ///
    /// Used by the heartbeat processor to apply computed role changes from the engine.
    pub fn apply_role_change(
        &mut self,
        node_id: &str,
        change: &crate::engine::RoleChange,
        lease_duration_secs: u64,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), NodeManagerError> {
        let node = self
            .get_node_mut(node_id)
            .ok_or_else(|| NodeManagerError::NodeNotFound {
                node_id: node_id.to_string(),
            })?;

        match &change.action {
            crate::engine::RoleAction::Assign(role_type, fencing_token) => {
                if !node.has_role(*role_type) {
                    let lease = crate::model::RoleLease::new(
                        *role_type,
                        *fencing_token,
                        now,
                        lease_duration_secs,
                    );
                    node.add_role(lease);

                    // Update max fencing token for persistence
                    self.update_max_fencing_token(*fencing_token);
                }
            }
            crate::engine::RoleAction::Revoke(role_type) => {
                node.remove_role(*role_type);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::HealthStatus;
    use chrono::Utc;

    #[test]
    fn test_new_state() {
        let state = NodeManagerState::new();
        assert!(state.is_empty());
        assert_eq!(state.cluster_count(), 0);
        assert_eq!(state.node_count(), 0);
        assert_eq!(state.max_fencing_token(), 0);
    }

    #[test]
    fn test_default_state() {
        let state = NodeManagerState::default();
        assert!(state.is_empty());
        assert_eq!(state.max_fencing_token(), 0);
    }

    #[test]
    fn test_max_fencing_token() {
        let mut state = NodeManagerState::new();
        assert_eq!(state.max_fencing_token(), 0);

        state.update_max_fencing_token(5);
        assert_eq!(state.max_fencing_token(), 5);

        state.update_max_fencing_token(3);
        assert_eq!(state.max_fencing_token(), 5); // Should stay at 5

        state.update_max_fencing_token(10);
        assert_eq!(state.max_fencing_token(), 10);
    }

    #[test]
    fn test_process_heartbeat_new_node() {
        let mut state = NodeManagerState::new();
        let now = Utc::now();

        let node_id = state.process_heartbeat(
            "node-1".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Healthy,
            now,
        );

        assert_eq!(node_id, "node-1");
        assert_eq!(state.cluster_count(), 1);
        assert_eq!(state.node_count(), 1);

        let node = state.get_node("node-1").unwrap();
        assert_eq!(node.status, NodeStatus::Assignable);
        assert_eq!(node.missed_heartbeats, 0);
        assert_eq!(node.health_status, HealthStatus::Healthy);
    }

    #[test]
    fn test_process_heartbeat_new_node_unhealthy() {
        let mut state = NodeManagerState::new();
        let now = Utc::now();

        state.process_heartbeat(
            "node-1".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Unhealthy,
            now,
        );

        let node = state.get_node("node-1").unwrap();
        assert_eq!(node.status, NodeStatus::Unassignable);
        assert_eq!(node.health_status, HealthStatus::Unhealthy);
    }

    #[test]
    fn test_process_heartbeat_new_node_draining() {
        let mut state = NodeManagerState::new();
        let now = Utc::now();

        state.process_heartbeat(
            "node-1".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Draining,
            now,
        );

        let node = state.get_node("node-1").unwrap();
        assert_eq!(node.status, NodeStatus::Unassignable);
        assert_eq!(node.health_status, HealthStatus::Draining);
    }

    #[test]
    fn test_process_heartbeat_existing_node_updates() {
        let mut state = NodeManagerState::new();
        let now = Utc::now();

        state.process_heartbeat(
            "node-1".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Healthy,
            now,
        );

        // Simulate missed heartbeats
        if let Some(node) = state.get_node_mut("node-1") {
            node.missed_heartbeats = 3;
            node.mark_suspect();
        }

        let later = now + chrono::Duration::seconds(30);
        state.process_heartbeat(
            "node-1".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Healthy,
            later,
        );

        let node = state.get_node("node-1").unwrap();
        assert_eq!(node.missed_heartbeats, 0);
        assert_eq!(node.status, NodeStatus::Assignable); // Recovered
    }

    #[test]
    fn test_process_heartbeat_unhealthy_to_unassignable() {
        let mut state = NodeManagerState::new();
        let now = Utc::now();

        state.process_heartbeat(
            "node-1".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Healthy,
            now,
        );

        let later = now + chrono::Duration::seconds(30);
        state.process_heartbeat(
            "node-1".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Unhealthy,
            later,
        );

        let node = state.get_node("node-1").unwrap();
        assert_eq!(node.status, NodeStatus::Unassignable);
    }

    #[test]
    fn test_process_heartbeat_recovery_from_suspect() {
        let mut state = NodeManagerState::new();
        let now = Utc::now();

        state.process_heartbeat(
            "node-1".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Healthy,
            now,
        );

        // Mark as suspect
        if let Some(node) = state.get_node_mut("node-1") {
            node.mark_suspect();
        }

        let later = now + chrono::Duration::seconds(30);
        state.process_heartbeat(
            "node-1".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Healthy,
            later,
        );

        let node = state.get_node("node-1").unwrap();
        assert_eq!(node.status, NodeStatus::Assignable);
    }

    #[test]
    fn test_process_heartbeat_recovery_from_unassignable() {
        let mut state = NodeManagerState::new();
        let now = Utc::now();

        state.process_heartbeat(
            "node-1".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Unhealthy,
            now,
        );

        let later = now + chrono::Duration::seconds(30);
        state.process_heartbeat(
            "node-1".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Healthy,
            later,
        );

        let node = state.get_node("node-1").unwrap();
        assert_eq!(node.status, NodeStatus::Assignable);
    }

    #[test]
    fn test_get_node_nonexistent() {
        let state = NodeManagerState::new();
        assert!(state.get_node("nonexistent").is_none());
    }

    #[test]
    fn test_get_node_mut_nonexistent() {
        let mut state = NodeManagerState::new();
        assert!(state.get_node_mut("nonexistent").is_none());
    }

    #[test]
    fn test_get_cluster_nonexistent() {
        let state = NodeManagerState::new();
        assert!(state.get_cluster("nonexistent").is_none());
    }

    #[test]
    fn test_get_or_create_cluster_mut() {
        let mut state = NodeManagerState::new();

        let cluster1 = state.get_or_create_cluster_mut("cluster-1");
        assert_eq!(cluster1.cluster_id, "cluster-1");
        assert_eq!(state.cluster_count(), 1);

        let cluster2 = state.get_or_create_cluster_mut("cluster-1");
        assert_eq!(cluster2.cluster_id, "cluster-1");
        assert_eq!(state.cluster_count(), 1); // No duplicate created

        state.get_or_create_cluster_mut("cluster-2");
        assert_eq!(state.cluster_count(), 2);
    }

    #[test]
    fn test_all_nodes_iterator() {
        let mut state = NodeManagerState::new();
        let now = Utc::now();

        state.process_heartbeat("node-1".to_string(), "cluster-1".to_string(), HealthStatus::Healthy, now);
        state.process_heartbeat("node-2".to_string(), "cluster-1".to_string(), HealthStatus::Healthy, now);
        state.process_heartbeat("node-3".to_string(), "cluster-2".to_string(), HealthStatus::Healthy, now);

        assert_eq!(state.all_nodes().count(), 3);
    }

    #[test]
    fn test_remove_node() {
        let mut state = NodeManagerState::new();
        let now = Utc::now();

        state.process_heartbeat("node-1".to_string(), "cluster-1".to_string(), HealthStatus::Healthy, now);

        let removed = state.remove_node("node-1");
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().node_id, "node-1");
        assert_eq!(state.node_count(), 0);
        assert!(state.get_node_cluster_id("node-1").is_none());
    }

    #[test]
    fn test_remove_nonexistent_node() {
        let mut state = NodeManagerState::new();
        assert!(state.remove_node("nonexistent").is_none());
    }

    #[test]
    fn test_get_node_cluster_id() {
        let mut state = NodeManagerState::new();
        let now = Utc::now();

        state.process_heartbeat("node-1".to_string(), "cluster-1".to_string(), HealthStatus::Healthy, now);

        let cluster_id = state.get_node_cluster_id("node-1");
        assert_eq!(cluster_id, Some(&"cluster-1".to_string()));
        assert!(state.get_node_cluster_id("nonexistent").is_none());
    }

    #[test]
    fn test_multiple_clusters() {
        let mut state = NodeManagerState::new();
        let now = Utc::now();

        state.process_heartbeat("node-1".to_string(), "cluster-1".to_string(), HealthStatus::Healthy, now);
        state.process_heartbeat("node-2".to_string(), "cluster-2".to_string(), HealthStatus::Healthy, now);

        assert_eq!(state.cluster_count(), 2);
        assert_eq!(state.node_count(), 2);

        let cluster1 = state.get_cluster("cluster-1").unwrap();
        assert_eq!(cluster1.node_count(), 1);
        assert!(cluster1.contains_node("node-1"));

        let cluster2 = state.get_cluster("cluster-2").unwrap();
        assert_eq!(cluster2.node_count(), 1);
        assert!(cluster2.contains_node("node-2"));
    }

    #[test]
    fn test_cluster_ids_iterator() {
        let mut state = NodeManagerState::new();
        let now = Utc::now();

        state.process_heartbeat("node-1".to_string(), "cluster-1".to_string(), HealthStatus::Healthy, now);
        state.process_heartbeat("node-2".to_string(), "cluster-2".to_string(), HealthStatus::Healthy, now);

        let ids: Vec<&String> = state.cluster_ids().collect();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&&"cluster-1".to_string()));
        assert!(ids.contains(&&"cluster-2".to_string()));
    }

    #[test]
    fn test_node_ids_iterator() {
        let mut state = NodeManagerState::new();
        let now = Utc::now();

        state.process_heartbeat("node-1".to_string(), "cluster-1".to_string(), HealthStatus::Healthy, now);
        state.process_heartbeat("node-2".to_string(), "cluster-1".to_string(), HealthStatus::Healthy, now);

        let ids: Vec<&String> = state.node_ids().collect();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&&"node-1".to_string()));
        assert!(ids.contains(&&"node-2".to_string()));
    }

    #[test]
    fn test_draining_node_stays_unassignable_on_heartbeat() {
        let mut state = NodeManagerState::new();
        let now = Utc::now();

        state.process_heartbeat(
            "node-1".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Draining,
            now,
        );

        let later = now + chrono::Duration::seconds(30);
        state.process_heartbeat(
            "node-1".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Draining,
            later,
        );

        let node = state.get_node("node-1").unwrap();
        assert_eq!(node.status, NodeStatus::Unassignable);
        assert_eq!(node.missed_heartbeats, 0);
    }

    #[test]
    fn test_heartbeat_resets_missed_count() {
        let mut state = NodeManagerState::new();
        let now = Utc::now();

        state.process_heartbeat("node-1".to_string(), "cluster-1".to_string(), HealthStatus::Healthy, now);

        // Manually set missed heartbeats
        if let Some(node) = state.get_node_mut("node-1") {
            node.missed_heartbeats = 5;
        }

        let later = now + chrono::Duration::seconds(30);
        state.process_heartbeat("node-1".to_string(), "cluster-1".to_string(), HealthStatus::Healthy, later);

        let node = state.get_node("node-1").unwrap();
        assert_eq!(node.missed_heartbeats, 0);
    }

    #[test]
    fn test_apply_role_change_assign() {
        let mut state = NodeManagerState::new();
        let now = Utc::now();

        state.process_heartbeat("node-1".to_string(), "cluster-1".to_string(), HealthStatus::Healthy, now);

        // Create a role change using the engine's RoleChange
        let change = crate::engine::RoleChange::assign(
            "node-1".to_string(),
            crate::model::RoleType::Metadata,
            42,
        );

        state
            .apply_role_change(&change.node_id, &change, 60, now)
            .unwrap();

        let node = state.get_node("node-1").unwrap();
        assert!(node.has_role(crate::model::RoleType::Metadata));
        assert_eq!(node.get_role_lease(crate::model::RoleType::Metadata).unwrap().fencing_token, 42);
        assert_eq!(state.max_fencing_token(), 42);
    }

    #[test]
    fn test_apply_role_change_revoke() {
        let mut state = NodeManagerState::new();
        let now = Utc::now();

        state.process_heartbeat("node-1".to_string(), "cluster-1".to_string(), HealthStatus::Healthy, now);

        // First assign a role
        let change = crate::engine::RoleChange::assign(
            "node-1".to_string(),
            crate::model::RoleType::Data,
            1,
        );
        state.apply_role_change(&change.node_id, &change, 60, now).unwrap();

        // Now revoke it
        let revoke_change = crate::engine::RoleChange::revoke("node-1".to_string(), crate::model::RoleType::Data);
        state
            .apply_role_change(&revoke_change.node_id, &revoke_change, 60, now)
            .unwrap();

        let node = state.get_node("node-1").unwrap();
        assert!(!node.has_role(crate::model::RoleType::Data));
    }

    #[test]
    fn test_apply_role_change_idempotent() {
        let mut state = NodeManagerState::new();
        let now = Utc::now();

        state.process_heartbeat("node-1".to_string(), "cluster-1".to_string(), HealthStatus::Healthy, now);

        let change = crate::engine::RoleChange::assign(
            "node-1".to_string(),
            crate::model::RoleType::Metadata,
            10,
        );

        // Apply the same change twice
        state.apply_role_change(&change.node_id, &change, 60, now).unwrap();
        state.apply_role_change(&change.node_id, &change, 60, now).unwrap();

        let node = state.get_node("node-1").unwrap();
        assert!(node.has_role(crate::model::RoleType::Metadata));
        assert_eq!(node.roles.len(), 1); // Only one role, not duplicated
        assert_eq!(state.max_fencing_token(), 10); // Token only updated once
    }
}

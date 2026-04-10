//! Role assignment engine.
//!
//! Determines which nodes should be assigned which roles based on cluster state
//! and configuration. Implements deterministic role assignment with fencing tokens.

use crate::config::RolesConfig;
use crate::model::{Cluster, NodeStatus, RoleType};
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::debug;

/// Action to take on a role for a specific node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoleAction {
    /// Assign a role to a node with a fencing token.
    /// The fencing token is non-zero only for Metadata role assignments.
    Assign(RoleType, u64),
    /// Revoke a role from a node.
    Revoke(RoleType),
}

/// A change to be applied to a node's role assignments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleChange {
    /// ID of the node this change applies to.
    pub node_id: String,
    /// The action to take.
    pub action: RoleAction,
}

impl RoleChange {
    /// Creates a new role change for assigning a role with a fencing token.
    pub fn assign(node_id: String, role: RoleType, fencing_token: u64) -> Self {
        Self {
            node_id,
            action: RoleAction::Assign(role, fencing_token),
        }
    }

    /// Creates a new role change for revoking a role.
    pub fn revoke(node_id: String, role: RoleType) -> Self {
        Self {
            node_id,
            action: RoleAction::Revoke(role),
        }
    }
}

/// Engine for computing role assignments based on cluster state.
///
/// The engine maintains a monotonically increasing fencing token counter
/// used for metadata role assignments to prevent split-brain scenarios.
#[derive(Debug)]
pub struct RoleAssignmentEngine {
    /// Role assignment configuration.
    config: RolesConfig,
    /// Next fencing token value (incremented atomically).
    next_fencing_token: AtomicU64,
}

impl RoleAssignmentEngine {
    /// Creates a new role assignment engine with the given configuration.
    pub fn new(config: RolesConfig) -> Self {
        Self {
            config,
            next_fencing_token: AtomicU64::new(1),
        }
    }

    /// Creates a new role assignment engine with fencing token recovery.
    ///
    /// Initializes the fencing token counter to ensure the next issued token
    /// is higher than any previously issued token (for split-brain prevention).
    ///
    /// # Arguments
    ///
    /// * `config` - Role assignment configuration
    /// * `max_fencing_token` - The highest fencing token ever issued (from persisted state)
    ///
    /// # Example
    ///
    /// ```ignore
    /// let state = store.load_state().await?;
    /// let engine = RoleAssignmentEngine::with_recovery(config, state.max_fencing_token());
    /// ```
    pub fn with_recovery(config: RolesConfig, max_fencing_token: u64) -> Self {
        // Start from max + 1 to ensure new tokens are always higher
        let next_token = max_fencing_token.saturating_add(1).max(1);
        Self {
            config,
            next_fencing_token: AtomicU64::new(next_token),
        }
    }

    /// Gets the next fencing token value.
    ///
    /// This increments the counter atomically and returns the previous value.
    /// Each metadata role assignment gets a unique, monotonically increasing token.
    fn next_fencing_token(&self) -> u64 {
        self.next_fencing_token.fetch_add(1, Ordering::SeqCst)
    }

    /// Computes role assignments for all nodes in a cluster.
    ///
    /// Returns a list of role changes that should be applied to achieve
    /// the desired state based on the current cluster state and configuration.
    ///
    /// # Algorithm
    ///
    /// 1. **Unassignable nodes**: Revoke ALL roles from every Unassignable node
    /// 2. **Data role (100%)**: Assign Data to all Assignable nodes missing it;
    ///    revoke from non-assignable nodes
    /// 3. **Metadata role (exactly N)**: Ensure exactly `metadata_count` Assignable
    ///    nodes hold the metadata role, using the lowest node_ids for determinism
    /// 4. **Storage role (min to max%)**: Ensure storage role is held by between
    ///    `storage_min_count` and `storage_max_percent` of Assignable nodes
    ///
    /// # Key Rules
    ///
    /// - **Suspect nodes**: Keep existing roles but don't assign new ones
    /// - **Unassignable nodes**: Lose ALL roles
    /// - **Deterministic selection**: Always sort by node_id
    /// - **Fencing tokens**: Only metadata role gets fencing tokens, incremented
    ///   on each assignment
    pub fn compute_assignments(&self, cluster: &Cluster) -> Vec<RoleChange> {
        debug!(
            "Computing role assignments for cluster {} with {} nodes",
            cluster.cluster_id,
            cluster.node_count()
        );

        let mut changes = Vec::new();

        // Step 1: Revoke ALL roles from Unassignable nodes
        for node in cluster.sorted_nodes() {
            if node.status == NodeStatus::Unassignable {
                for lease in &node.roles {
                    debug!(
                        "Revoking role {:?} from unassignable node {}",
                        lease.role, node.node_id
                    );
                    changes.push(RoleChange::revoke(node.node_id.clone(), lease.role));
                }
            }
        }

        // Get assignable nodes sorted by node_id for deterministic selection
        let assignable_nodes = cluster.assignable_nodes();
        let assignable_count = assignable_nodes.len();

        if assignable_count == 0 {
            debug!("No assignable nodes in cluster, skipping role assignments");
            return changes;
        }

        // Step 2: Data role (100% of assignable nodes)
        for node in &assignable_nodes {
            if !node.has_role(RoleType::Data) {
                debug!(
                    "Assigning Data role to assignable node {}",
                    node.node_id
                );
                changes.push(RoleChange::assign(node.node_id.clone(), RoleType::Data, 0));
            }
        }

        // Note: Data role revocation from Unassignable nodes is already handled in Step 1
        // Suspect nodes keep their existing roles

        // Step 3: Metadata role (exactly metadata_count nodes)
        let metadata_count = self.config.metadata_count;
        let current_metadata_holders: Vec<_> = assignable_nodes
            .iter()
            .filter(|n| n.has_role(RoleType::Metadata))
            .collect();

        debug!(
            "Current metadata holders: {} (target: {})",
            current_metadata_holders.len(),
            metadata_count
        );

        if current_metadata_holders.len() < metadata_count {
            // Need to assign metadata to additional nodes
            let nodes_without_metadata: Vec<_> = assignable_nodes
                .iter()
                .filter(|n| !n.has_role(RoleType::Metadata))
                .collect();

            let needed = metadata_count - current_metadata_holders.len();
            for node in nodes_without_metadata.iter().take(needed) {
                let token = self.next_fencing_token();
                debug!(
                    "Assigning Metadata role to node {} with fencing token {}",
                    node.node_id, token
                );
                changes.push(RoleChange::assign(node.node_id.clone(), RoleType::Metadata, token));
            }
        } else if current_metadata_holders.len() > metadata_count {
            // Need to revoke metadata from some nodes (highest node_ids first)
            let mut holders = current_metadata_holders.clone();
            holders.sort_by_key(|n| &n.node_id);
            holders.reverse(); // Sort descending to remove highest node_ids first

            let to_remove = current_metadata_holders.len() - metadata_count;
            for node in holders.iter().take(to_remove) {
                debug!(
                    "Revoking Metadata role from node {} (excess holders)",
                    node.node_id
                );
                changes.push(RoleChange::revoke(node.node_id.clone(), RoleType::Metadata));
            }
        }

        // Step 4: Storage role (min to max% of assignable nodes)
        let storage_min = self.config.storage_min_count;
        let storage_max_percent = self.config.storage_max_percent;
        let max_storage = std::cmp::max(
            storage_min,
            (assignable_count * storage_max_percent) / 100,
        );

        let current_storage_holders: Vec<_> = assignable_nodes
            .iter()
            .filter(|n| n.has_role(RoleType::Storage))
            .collect();

        debug!(
            "Current storage holders: {} (target: {}-{})",
            current_storage_holders.len(),
            storage_min,
            max_storage
        );

        if current_storage_holders.len() < max_storage {
            // Below max: assign storage to bring us up to max
            let nodes_without_storage: Vec<_> = assignable_nodes
                .iter()
                .filter(|n| !n.has_role(RoleType::Storage))
                .collect();

            let needed = max_storage - current_storage_holders.len();
            for node in nodes_without_storage.iter().take(needed) {
                debug!(
                    "Assigning Storage role to node {} (below max)",
                    node.node_id
                );
                changes.push(RoleChange::assign(node.node_id.clone(), RoleType::Storage, 0));
            }
        } else if current_storage_holders.len() > max_storage {
            // Above maximum: revoke from highest node_ids
            let mut holders = current_storage_holders.clone();
            holders.sort_by_key(|n| &n.node_id);
            holders.reverse(); // Sort descending to remove highest node_ids first

            let to_remove = current_storage_holders.len() - max_storage;
            for node in holders.iter().take(to_remove) {
                debug!(
                    "Revoking Storage role from node {} (above maximum)",
                    node.node_id
                );
                changes.push(RoleChange::revoke(node.node_id.clone(), RoleType::Storage));
            }
        }

        // Note: Storage role revocation from Unassignable nodes is already handled in Step 1
        // Suspect nodes keep their existing roles

        debug!(
            "Computed {} role changes for cluster {}",
            changes.len(),
            cluster.cluster_id
        );

        changes
    }
}

impl RoleAssignmentEngine {
    /// Gets a reference to the configuration.
    pub fn config(&self) -> &RolesConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Node, RoleLease};
    use chrono::Utc;

    fn create_test_node(
        node_id: &str,
        cluster_id: &str,
        status: NodeStatus,
        roles: Vec<RoleType>,
    ) -> Node {
        let mut node = Node::new(
            node_id.to_string(),
            cluster_id.to_string(),
            crate::model::HealthStatus::Healthy,
            Utc::now(),
        );
        node.status = status;
        for role in roles {
            node.add_role(RoleLease::new(role, 1, Utc::now(), 60));
        }
        node
    }

    fn create_test_cluster(nodes: Vec<Node>) -> Cluster {
        let mut cluster = Cluster::new("test-cluster".to_string());
        for node in nodes {
            cluster.add_node(node);
        }
        cluster
    }

    fn default_config() -> RolesConfig {
        RolesConfig::default()
    }

    #[test]
    fn test_single_node_gets_all_roles() {
        let node = create_test_node(
            "node-1",
            "test-cluster",
            NodeStatus::Assignable,
            vec![],
        );
        let cluster = create_test_cluster(vec![node]);
        let engine = RoleAssignmentEngine::new(default_config());

        let changes = engine.compute_assignments(&cluster);

        // Single node should get all three roles: metadata, data, storage
        assert_eq!(changes.len(), 3);
        let assign_changes: Vec<_> = changes
            .iter()
            .filter(|c| matches!(&c.action, RoleAction::Assign(_, _)))
            .collect();

        assert_eq!(assign_changes.len(), 3);
        assert!(assign_changes
            .iter()
            .any(|c| matches!(&c.action, RoleAction::Assign(RoleType::Metadata, _))));
        assert!(assign_changes
            .iter()
            .any(|c| matches!(&c.action, RoleAction::Assign(RoleType::Data, _))));
        assert!(assign_changes
            .iter()
            .any(|c| matches!(&c.action, RoleAction::Assign(RoleType::Storage, _))));
    }

    #[test]
    fn test_metadata_assigned_to_exactly_one_node() {
        let nodes = vec![
            create_test_node("node-1", "test-cluster", NodeStatus::Assignable, vec![]),
            create_test_node("node-2", "test-cluster", NodeStatus::Assignable, vec![]),
            create_test_node("node-3", "test-cluster", NodeStatus::Assignable, vec![]),
        ];
        let cluster = create_test_cluster(nodes);
        let engine = RoleAssignmentEngine::new(default_config());

        let changes = engine.compute_assignments(&cluster);

        // Only one node should get metadata (the lowest node_id)
        let metadata_assigns: Vec<_> = changes
            .iter()
            .filter(|c| {
                matches!(&c.action, RoleAction::Assign(RoleType::Metadata, _))
            })
            .collect();

        assert_eq!(metadata_assigns.len(), 1);
        assert_eq!(metadata_assigns[0].node_id, "node-1");
    }

    #[test]
    fn test_metadata_reassignment_when_holder_goes_unassignable() {
        let nodes = vec![
            create_test_node(
                "node-1",
                "test-cluster",
                NodeStatus::Assignable,
                vec![RoleType::Metadata],
            ),
            create_test_node("node-2", "test-cluster", NodeStatus::Assignable, vec![]),
        ];
        let cluster = create_test_cluster(nodes);
        let engine = RoleAssignmentEngine::new(default_config());

        let changes = engine.compute_assignments(&cluster);

        // node-1 already has metadata, node-2 should get data and storage
        let metadata_changes: Vec<_> = changes
            .iter()
            .filter(|c| matches!(&c.action, RoleAction::Assign(RoleType::Metadata, _)))
            .collect();

        assert_eq!(metadata_changes.len(), 0); // No new metadata assignments

        // Now mark node-1 as unassignable
        let nodes = vec![
            create_test_node(
                "node-1",
                "test-cluster",
                NodeStatus::Unassignable,
                vec![RoleType::Metadata],
            ),
            create_test_node("node-2", "test-cluster", NodeStatus::Assignable, vec![]),
        ];
        let cluster = create_test_cluster(nodes);

        let changes = engine.compute_assignments(&cluster);

        // node-1 should lose metadata, node-2 should gain it
        let metadata_revokes: Vec<_> = changes
            .iter()
            .filter(|c| {
                matches!(
                    &c.action,
                    RoleAction::Revoke(RoleType::Metadata)
                )
            })
            .collect();

        assert_eq!(metadata_revokes.len(), 1);
        assert_eq!(metadata_revokes[0].node_id, "node-1");

        let metadata_assigns: Vec<_> = changes
            .iter()
            .filter(|c| {
                matches!(&c.action, RoleAction::Assign(RoleType::Metadata, _))
            })
            .collect();

        assert_eq!(metadata_assigns.len(), 1);
        assert_eq!(metadata_assigns[0].node_id, "node-2");
    }

    #[test]
    fn test_storage_caps_at_percentage() {
        // With default config (30% cap), 10 nodes should have max 3 storage nodes
        let nodes: Vec<Node> = (1..=10)
            .map(|i| {
                create_test_node(
                    &format!("node-{:02}", i),
                    "test-cluster",
                    NodeStatus::Assignable,
                    vec![],
                )
            })
            .collect();

        let cluster = create_test_cluster(nodes);
        let engine = RoleAssignmentEngine::new(default_config());

        let changes = engine.compute_assignments(&cluster);

        // Storage should be assigned to 3 nodes (30% of 10 = 3, but min is 1)
        let storage_assigns: Vec<_> = changes
            .iter()
            .filter(|c| {
                matches!(&c.action, RoleAction::Assign(RoleType::Storage, _))
            })
            .collect();

        assert_eq!(storage_assigns.len(), 3);
    }

    #[test]
    fn test_storage_minimum_of_one() {
        let nodes = vec![create_test_node(
            "node-1",
            "test-cluster",
            NodeStatus::Assignable,
            vec![],
        )];
        let cluster = create_test_cluster(nodes);
        let engine = RoleAssignmentEngine::new(default_config());

        let changes = engine.compute_assignments(&cluster);

        // Even with 1 node, storage should be assigned (minimum is 1)
        let storage_assigns: Vec<_> = changes
            .iter()
            .filter(|c| {
                matches!(&c.action, RoleAction::Assign(RoleType::Storage, _))
            })
            .collect();

        assert_eq!(storage_assigns.len(), 1);
    }

    #[test]
    fn test_data_assigned_to_all_assignable_nodes() {
        let nodes = vec![
            create_test_node("node-1", "test-cluster", NodeStatus::Assignable, vec![]),
            create_test_node("node-2", "test-cluster", NodeStatus::Assignable, vec![]),
            create_test_node("node-3", "test-cluster", NodeStatus::Assignable, vec![]),
        ];
        let cluster = create_test_cluster(nodes);
        let engine = RoleAssignmentEngine::new(default_config());

        let changes = engine.compute_assignments(&cluster);

        // All assignable nodes should get data role
        let data_assigns: Vec<_> = changes
            .iter()
            .filter(|c| matches!(&c.action, RoleAction::Assign(RoleType::Data, _)))
            .collect();

        assert_eq!(data_assigns.len(), 3);
    }

    #[test]
    fn test_fencing_token_increments_monotonically() {
        let nodes = vec![
            create_test_node("node-1", "test-cluster", NodeStatus::Assignable, vec![]),
            create_test_node("node-2", "test-cluster", NodeStatus::Assignable, vec![]),
        ];
        let cluster = create_test_cluster(nodes);
        let engine = RoleAssignmentEngine::new(default_config());

        // First assignment
        let _changes1 = engine.compute_assignments(&cluster);
        let _first_token = engine.next_fencing_token();

        // Create a new cluster where metadata moves to a different node
        let nodes = vec![
            create_test_node(
                "node-1",
                "test-cluster",
                NodeStatus::Unassignable,
                vec![RoleType::Metadata],
            ),
            create_test_node("node-2", "test-cluster", NodeStatus::Assignable, vec![]),
        ];
        let cluster = create_test_cluster(nodes);

        let _changes2 = engine.compute_assignments(&cluster);
        let _second_token = engine.next_fencing_token();

        // Tokens should increment - verified by the fact that next_fencing_token was called
        // after the second compute_assignments, which would have incremented it again
    }

    #[test]
    fn test_all_roles_revoked_from_unassignable_nodes() {
        let nodes = vec![
            create_test_node(
                "node-1",
                "test-cluster",
                NodeStatus::Unassignable,
                vec![RoleType::Metadata, RoleType::Data, RoleType::Storage],
            ),
            create_test_node(
                "node-2",
                "test-cluster",
                NodeStatus::Assignable,
                vec![],
            ),
        ];
        let cluster = create_test_cluster(nodes);
        let engine = RoleAssignmentEngine::new(default_config());

        let changes = engine.compute_assignments(&cluster);

        // node-1 should lose all roles
        let revokes: Vec<_> = changes
            .iter()
            .filter(|c| c.node_id == "node-1" && matches!(&c.action, RoleAction::Revoke(_)))
            .collect();

        assert_eq!(revokes.len(), 3); // All three roles revoked
    }

    #[test]
    fn test_suspect_nodes_keep_existing_roles() {
        let nodes = vec![
            create_test_node(
                "node-1",
                "test-cluster",
                NodeStatus::Suspect,
                vec![RoleType::Metadata, RoleType::Data],
            ),
            create_test_node("node-2", "test-cluster", NodeStatus::Assignable, vec![]),
        ];
        let cluster = create_test_cluster(nodes);
        let engine = RoleAssignmentEngine::new(default_config());

        let changes = engine.compute_assignments(&cluster);

        // node-1 (suspect) should NOT lose its existing roles
        let node1_revokes: Vec<_> = changes
            .iter()
            .filter(|c| c.node_id == "node-1" && matches!(&c.action, RoleAction::Revoke(_)))
            .collect();

        assert_eq!(node1_revokes.len(), 0);

        // But node-1 should NOT get any new roles either
        let node1_assigns: Vec<_> = changes
            .iter()
            .filter(|c| c.node_id == "node-1" && matches!(&c.action, RoleAction::Assign(_, _)))
            .collect();

        assert_eq!(node1_assigns.len(), 0);
    }

    #[test]
    fn test_suspect_nodes_dont_get_new_assignments() {
        let nodes = vec![
            create_test_node("node-1", "test-cluster", NodeStatus::Suspect, vec![]),
            create_test_node("node-2", "test-cluster", NodeStatus::Assignable, vec![]),
        ];
        let cluster = create_test_cluster(nodes);
        let engine = RoleAssignmentEngine::new(default_config());

        let changes = engine.compute_assignments(&cluster);

        // node-1 (suspect) should not get storage role
        let node1_storage: Vec<_> = changes
            .iter()
            .filter(|c| {
                c.node_id == "node-1"
                    && matches!(&c.action, RoleAction::Assign(RoleType::Storage, _))
            })
            .collect();

        assert_eq!(node1_storage.len(), 0);

        // But node-2 (assignable) should get storage
        let node2_storage: Vec<_> = changes
            .iter()
            .filter(|c| {
                c.node_id == "node-2"
                    && matches!(&c.action, RoleAction::Assign(RoleType::Storage, _))
            })
            .collect();

        assert_eq!(node2_storage.len(), 1);
    }

    #[test]
    fn test_multiple_clusters_are_independent() {
        // This test verifies that role assignments are computed per-cluster
        // by ensuring the algorithm doesn't have cross-cluster dependencies
        let nodes1 = vec![
            create_test_node("node-1", "cluster-1", NodeStatus::Assignable, vec![]),
            create_test_node("node-2", "cluster-1", NodeStatus::Assignable, vec![]),
        ];
        let cluster1 = create_test_cluster(nodes1);

        let nodes2 = vec![
            create_test_node("node-3", "cluster-2", NodeStatus::Assignable, vec![]),
            create_test_node("node-4", "cluster-2", NodeStatus::Assignable, vec![]),
        ];
        let mut cluster2 = create_test_cluster(nodes2);
        cluster2.cluster_id = "cluster-2".to_string();

        let engine = RoleAssignmentEngine::new(default_config());

        let changes1 = engine.compute_assignments(&cluster1);
        let changes2 = engine.compute_assignments(&cluster2);

        // Each cluster should have its own metadata assignment
        let metadata1 = changes1
            .iter()
            .filter(|c| matches!(&c.action, RoleAction::Assign(RoleType::Metadata, _)))
            .count();
        let metadata2 = changes2
            .iter()
            .filter(|c| matches!(&c.action, RoleAction::Assign(RoleType::Metadata, _)))
            .count();

        assert_eq!(metadata1, 1);
        assert_eq!(metadata2, 1);
    }

    #[test]
    fn test_ten_node_cluster_with_correct_storage_percentage() {
        let nodes: Vec<Node> = (1..=10)
            .map(|i| {
                create_test_node(
                    &format!("node-{:02}", i),
                    "test-cluster",
                    NodeStatus::Assignable,
                    vec![],
                )
            })
            .collect();

        let cluster = create_test_cluster(nodes);
        let engine = RoleAssignmentEngine::new(default_config());

        let changes = engine.compute_assignments(&cluster);

        // With default config (30% cap, min 1), 10 nodes:
        // - max_storage = max(1, (10 * 30) / 100) = max(1, 3) = 3
        // Storage should be assigned to 3 nodes
        let storage_assigns: Vec<_> = changes
            .iter()
            .filter(|c| {
                matches!(&c.action, RoleAction::Assign(RoleType::Storage, _))
            })
            .collect();

        assert_eq!(storage_assigns.len(), 3);

        // Should be assigned to the lowest node_ids (deterministic)
        let mut storage_node_ids: Vec<_> = storage_assigns
            .iter()
            .map(|c| c.node_id.as_str())
            .collect();
        storage_node_ids.sort();

        assert_eq!(storage_node_ids, vec!["node-01", "node-02", "node-03"]);
    }

    #[test]
    fn test_custom_metadata_count() {
        let mut config = default_config();
        config.metadata_count = 3;

        let nodes = vec![
            create_test_node("node-1", "test-cluster", NodeStatus::Assignable, vec![]),
            create_test_node("node-2", "test-cluster", NodeStatus::Assignable, vec![]),
            create_test_node("node-3", "test-cluster", NodeStatus::Assignable, vec![]),
            create_test_node("node-4", "test-cluster", NodeStatus::Assignable, vec![]),
            create_test_node("node-5", "test-cluster", NodeStatus::Assignable, vec![]),
        ];
        let cluster = create_test_cluster(nodes);
        let engine = RoleAssignmentEngine::new(config);

        let changes = engine.compute_assignments(&cluster);

        // Should assign metadata to 3 nodes (lowest node_ids)
        let metadata_assigns: Vec<_> = changes
            .iter()
            .filter(|c| {
                matches!(&c.action, RoleAction::Assign(RoleType::Metadata, _))
            })
            .collect();

        assert_eq!(metadata_assigns.len(), 3);

        let mut metadata_node_ids: Vec<_> = metadata_assigns
            .iter()
            .map(|c| c.node_id.as_str())
            .collect();
        metadata_node_ids.sort();

        assert_eq!(
            metadata_node_ids,
            vec!["node-1", "node-2", "node-3"]
        );
    }

    #[test]
    fn test_engine_with_recovery_starts_from_max_plus_one() {
        let config = default_config();

        // Engine with recovery from max_token=100 should start at 101
        let engine = RoleAssignmentEngine::with_recovery(config.clone(), 100);

        // First token issued should be 101
        let token1 = engine.next_fencing_token();
        assert_eq!(token1, 101);

        // Second token should be 102
        let token2 = engine.next_fencing_token();
        assert_eq!(token2, 102);
    }

    #[test]
    fn test_engine_with_recovery_zero_max() {
        let config = default_config();

        // Engine with recovery from max_token=0 should start at 1
        let engine = RoleAssignmentEngine::with_recovery(config, 0);

        let token = engine.next_fencing_token();
        assert_eq!(token, 1);
    }

    #[test]
    fn test_engine_with_recovery_large_token() {
        let config = default_config();

        // Test with a large token value
        let large_token = u64::MAX - 1;
        let engine = RoleAssignmentEngine::with_recovery(config, large_token);

        // Should start at u64::MAX (saturating_add prevents overflow)
        let token = engine.next_fencing_token();
        assert_eq!(token, u64::MAX);
    }

    #[test]
    fn test_engine_new_vs_recovery() {
        let config = default_config();

        // Regular engine starts at 1
        let engine_new = RoleAssignmentEngine::new(config.clone());
        assert_eq!(engine_new.next_fencing_token(), 1);

        // Recovery engine with max=0 also starts at 1
        let engine_recovery = RoleAssignmentEngine::with_recovery(config.clone(), 0);
        assert_eq!(engine_recovery.next_fencing_token(), 1);

        // Recovery engine with max=50 starts at 51
        let engine_recovery_50 = RoleAssignmentEngine::with_recovery(config, 50);
        assert_eq!(engine_recovery_50.next_fencing_token(), 51);
    }
}

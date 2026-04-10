//! Heartbeat monitor for detecting missed heartbeats.
//!
//! The monitor tracks the time since each node's last heartbeat and
//! increments missed heartbeat counters. Nodes transition through
//! states based on configurable thresholds.

use crate::config::HeartbeatConfig;
use crate::engine::{RoleAssignmentEngine, RoleChange};
use crate::model::NodeStatus;
use crate::NodeManagerState;
use chrono::{DateTime, Utc};
use std::collections::HashSet;
use tracing::{debug, info, warn};

/// Monitor that checks for missed heartbeats and updates node status.
///
/// The monitor runs periodically (typically every `check_interval_secs`)
/// and increments the `missed_heartbeats` counter for nodes that haven't
/// sent heartbeats recently enough.
pub struct HeartbeatMonitor {
    /// Heartbeat monitoring configuration.
    config: HeartbeatConfig,
}

impl HeartbeatMonitor {
    /// Creates a new heartbeat monitor with the given configuration.
    pub fn new(config: HeartbeatConfig) -> Self {
        Self { config }
    }

    /// Checks all nodes for missed heartbeats and computes role changes.
    ///
    /// This method:
    /// 1. Iterates over all nodes in the state
    /// 2. For nodes with `last_heartbeat` set, computes elapsed time
    /// 3. If enough time has passed, increments `missed_heartbeats`
    /// 4. Updates node status based on missed heartbeat count:
    ///    - 1 to (threshold-1) missed → Suspect
    ///    - threshold or more missed → Unassignable
    /// 5. For clusters with status changes, recomputes role assignments
    /// 6. Returns all role changes that should be applied
    ///
    /// # Arguments
    ///
    /// * `state` - Mutable reference to the node manager state
    /// * `role_engine` - Reference to the role assignment engine
    /// * `now` - Current timestamp for elapsed time calculations
    ///
    /// # Returns
    ///
    /// A vector of role changes that should be applied to the state.
    pub fn check_heartbeats(
        &self,
        state: &mut NodeManagerState,
        role_engine: &RoleAssignmentEngine,
        now: DateTime<Utc>,
    ) -> Vec<RoleChange> {
        let threshold = self.config.missed_threshold;

        debug!(
            check_interval_secs = self.config.check_interval_secs,
            missed_threshold = threshold,
            "Checking heartbeats for {} nodes",
            state.node_count()
        );

        // Track which clusters had status changes
        let mut clusters_with_changes: HashSet<String> = HashSet::new();

        // Iterate over all nodes and check for missed heartbeats
        // We need to collect node_ids first since we'll be modifying state
        let node_ids: Vec<_> = state.node_ids().cloned().collect();

        for node_id in node_ids {
            // Get the cluster ID for this node
            let cluster_id = match state.get_node_cluster_id(&node_id).cloned() {
                Some(id) => id,
                None => continue,
            };

            // Get mutable reference to the node
            let node = match state.get_node_mut(&node_id) {
                Some(n) => n,
                None => continue,
            };

            // Skip nodes that haven't sent any heartbeat yet
            let last_heartbeat = match node.last_heartbeat {
                Some(t) => t,
                None => continue,
            };

            // Compute elapsed time since last heartbeat
            // Use Duration::from() to handle both past and future timestamps
            let elapsed = now - last_heartbeat;

            // Calculate how many heartbeat intervals have been missed
            // Each check_interval seconds represents one potential heartbeat
            // If elapsed >= check_interval, at least one heartbeat was missed
            // Handle clock skew by treating negative elapsed as zero
            let elapsed_secs = elapsed.num_seconds().max(0) as u64;
            let interval_secs = self.config.check_interval_secs;

            // Calculate the total number of missed intervals
            // If 35 seconds have passed with 10s intervals, we've missed 3 intervals (at 10s, 20s, 30s)
            let total_missed_intervals = elapsed_secs / interval_secs;

            // Always update to the actual number of missed intervals based on elapsed time
            if total_missed_intervals > 0 {
                let previous_missed = node.missed_heartbeats;
                node.missed_heartbeats = total_missed_intervals as u32;

                warn!(
                    node_id = %node.node_id,
                    cluster_id = %node.cluster_id,
                    previous_missed = previous_missed,
                    new_missed = node.missed_heartbeats,
                    elapsed_secs = elapsed_secs,
                    "Node missed heartbeat intervals"
                );

                // Update status based on missed count
                let previous_status = node.status;
                if node.missed_heartbeats >= threshold {
                    // Threshold exceeded → Unassignable
                    if node.status != NodeStatus::Unassignable {
                        node.mark_unassignable();
                        info!(
                            node_id = %node.node_id,
                            cluster_id = %node.cluster_id,
                            missed_heartbeats = node.missed_heartbeats,
                            threshold = threshold,
                            "Node marked Unassignable due to missed heartbeats"
                        );
                        clusters_with_changes.insert(cluster_id.clone());
                    }
                } else if node.missed_heartbeats >= 1 {
                    // At least one missed → Suspect (if not already)
                    if node.status == NodeStatus::Assignable {
                        node.mark_suspect();
                        info!(
                            node_id = %node.node_id,
                            cluster_id = %node.cluster_id,
                            missed_heartbeats = node.missed_heartbeats,
                            "Node marked Suspect due to missed heartbeat"
                        );
                        clusters_with_changes.insert(cluster_id.clone());
                    }
                }

                debug!(
                    node_id = %node.node_id,
                    previous_status = ?previous_status,
                    new_status = ?node.status,
                    missed_heartbeats = node.missed_heartbeats,
                    "Node status updated after missed heartbeat"
                );
            }
        }

        // Compute role assignments for clusters that had changes
        let mut all_role_changes = Vec::new();

        for cluster_id in &clusters_with_changes {
            if let Some(cluster) = state.get_cluster(cluster_id) {
                debug!(
                    cluster_id = %cluster_id,
                    "Recomputing role assignments after heartbeat check"
                );
                let changes = role_engine.compute_assignments(cluster);
                all_role_changes.extend(changes);
            }
        }

        debug!(
            clusters_checked = clusters_with_changes.len(),
            total_role_changes = all_role_changes.len(),
            "Heartbeat check complete"
        );

        all_role_changes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HeartbeatConfig;
    use crate::engine::RoleAssignmentEngine;
    use crate::model::{HealthStatus, RoleType};
    use crate::RolesConfig;
    use chrono::Duration;

    /// Helper to create a test config with short intervals for testing
    fn test_config() -> HeartbeatConfig {
        HeartbeatConfig {
            check_interval_secs: 10,
            missed_threshold: 3,
        }
    }

    #[test]
    fn test_heartbeat_monitor_new() {
        let config = HeartbeatConfig {
            check_interval_secs: 10,
            missed_threshold: 3,
        };
        let monitor = HeartbeatMonitor::new(config);

        assert_eq!(monitor.config.check_interval_secs, 10);
        assert_eq!(monitor.config.missed_threshold, 3);
    }

    #[test]
    fn test_node_becomes_suspect_after_one_missed_interval() {
        let config = test_config();
        let monitor = HeartbeatMonitor::new(config);
        let role_engine = RoleAssignmentEngine::new(RolesConfig::default());

        let mut state = NodeManagerState::new();
        let now = Utc::now();

        // Register a node with a heartbeat 15 seconds ago (past one 10s interval)
        state.process_heartbeat(
            "node-1".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Healthy,
            now - Duration::seconds(15),
        );

        // Run check at current time
        let _changes = monitor.check_heartbeats(&mut state, &role_engine, now);

        // Node should be marked suspect
        let node = state.get_node("node-1").unwrap();
        assert_eq!(node.status, NodeStatus::Suspect);
        assert_eq!(node.missed_heartbeats, 1);
    }

    #[test]
    fn test_node_becomes_unassignable_after_threshold() {
        let config = test_config();
        let monitor = HeartbeatMonitor::new(config);
        let role_engine = RoleAssignmentEngine::new(RolesConfig::default());

        let mut state = NodeManagerState::new();
        let now = Utc::now();

        // Register a node with heartbeat 35 seconds ago (past 3 intervals of 10s)
        state.process_heartbeat(
            "node-1".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Healthy,
            now - Duration::seconds(35),
        );

        // Run check at current time
        let _changes = monitor.check_heartbeats(&mut state, &role_engine, now);

        // Node should be marked unassignable
        let node = state.get_node("node-1").unwrap();
        assert_eq!(node.status, NodeStatus::Unassignable);
        assert_eq!(node.missed_heartbeats, 3);
    }

    #[test]
    fn test_active_node_not_affected() {
        let config = test_config();
        let monitor = HeartbeatMonitor::new(config);
        let role_engine = RoleAssignmentEngine::new(RolesConfig::default());

        let mut state = NodeManagerState::new();
        let now = Utc::now();

        // Register a node with a recent heartbeat (5 seconds ago)
        state.process_heartbeat(
            "node-1".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Healthy,
            now - Duration::seconds(5),
        );

        // Run check at current time
        let _changes = monitor.check_heartbeats(&mut state, &role_engine, now);

        // Node should remain assignable with no missed heartbeats
        let node = state.get_node("node-1").unwrap();
        assert_eq!(node.status, NodeStatus::Assignable);
        assert_eq!(node.missed_heartbeats, 0);
    }

    #[test]
    fn test_suspect_node_with_roles_keeps_them() {
        let config = test_config();
        let monitor = HeartbeatMonitor::new(config);
        let role_engine = RoleAssignmentEngine::new(RolesConfig::default());

        let mut state = NodeManagerState::new();
        let now = Utc::now();

        // Register a node and give it roles (15 seconds ago = 1 missed interval = Suspect)
        state.process_heartbeat(
            "node-1".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Healthy,
            now - Duration::seconds(15),
        );

        // Manually add roles to the node
        if let Some(node) = state.get_node_mut("node-1") {
            node.add_role(crate::model::RoleLease::new(
                RoleType::Metadata,
                1,
                now - Duration::seconds(15),
                60,
            ));
            node.add_role(crate::model::RoleLease::new(
                RoleType::Data,
                1,
                now - Duration::seconds(15),
                60,
            ));
        }

        // Run check - node should become suspect but keep roles
        let _changes = monitor.check_heartbeats(&mut state, &role_engine, now);

        let node = state.get_node("node-1").unwrap();
        assert_eq!(node.status, NodeStatus::Suspect);
        // Suspect nodes keep their existing roles
        assert!(node.has_role(RoleType::Metadata));
        assert!(node.has_role(RoleType::Data));
    }

    #[test]
    fn test_unassignable_node_loses_all_roles() {
        let config = test_config();
        let monitor = HeartbeatMonitor::new(config);
        let role_engine = RoleAssignmentEngine::new(RolesConfig::default());

        let mut state = NodeManagerState::new();
        let now = Utc::now();

        // Register two nodes
        state.process_heartbeat(
            "node-1".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Healthy,
            now - Duration::seconds(100),
        );
        state.process_heartbeat(
            "node-2".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Healthy,
            now,
        );

        // Give node-1 the metadata role
        if let Some(node) = state.get_node_mut("node-1") {
            node.add_role(crate::model::RoleLease::new(
                RoleType::Metadata,
                1,
                now,
                60,
            ));
        }

        // Run check - node-1 becomes unassignable
        let changes = monitor.check_heartbeats(&mut state, &role_engine, now);

        // node-1 should have metadata revoked
        let metadata_revoked = changes.iter().any(|c| {
            c.node_id == "node-1"
                && matches!(
                    &c.action,
                    crate::engine::RoleAction::Revoke(RoleType::Metadata)
                )
        });
        assert!(metadata_revoked);

        // node-2 should get metadata assigned
        let metadata_assigned = changes.iter().any(|c| {
            c.node_id == "node-2"
                && matches!(
                    &c.action,
                    crate::engine::RoleAction::Assign(RoleType::Metadata, _)
                )
        });
        assert!(metadata_assigned);
    }

    #[test]
    fn test_multiple_nodes_missing_heartbeats_in_same_cluster() {
        let config = test_config();
        let monitor = HeartbeatMonitor::new(config);
        let role_engine = RoleAssignmentEngine::new(RolesConfig::default());

        let mut state = NodeManagerState::new();
        let now = Utc::now();

        // Register three nodes with different heartbeat times
        state.process_heartbeat(
            "node-1".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Healthy,
            now - Duration::seconds(35), // Will be unassignable
        );
        state.process_heartbeat(
            "node-2".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Healthy,
            now - Duration::seconds(15), // Will be suspect
        );
        state.process_heartbeat(
            "node-3".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Healthy,
            now, // Active
        );

        // Run check
        let _changes = monitor.check_heartbeats(&mut state, &role_engine, now);

        // Verify statuses
        assert_eq!(
            state.get_node("node-1").unwrap().status,
            NodeStatus::Unassignable
        );
        assert_eq!(state.get_node("node-2").unwrap().status, NodeStatus::Suspect);
        assert_eq!(
            state.get_node("node-3").unwrap().status,
            NodeStatus::Assignable
        );

        // Verify missed counts
        assert_eq!(state.get_node("node-1").unwrap().missed_heartbeats, 3);
        assert_eq!(state.get_node("node-2").unwrap().missed_heartbeats, 1);
        assert_eq!(state.get_node("node-3").unwrap().missed_heartbeats, 0);
    }

    #[test]
    fn test_nodes_in_different_clusters_are_independent() {
        let config = test_config();
        let monitor = HeartbeatMonitor::new(config);
        let role_engine = RoleAssignmentEngine::new(RolesConfig::default());

        let mut state = NodeManagerState::new();
        let now = Utc::now();

        // Register nodes in different clusters
        state.process_heartbeat(
            "node-1".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Healthy,
            now - Duration::seconds(35), // Will be unassignable
        );
        state.process_heartbeat(
            "node-2".to_string(),
            "cluster-2".to_string(),
            HealthStatus::Healthy,
            now, // Active
        );

        // Run check
        let _changes = monitor.check_heartbeats(&mut state, &role_engine, now);

        // cluster-1 node should be unassignable
        assert_eq!(
            state.get_node("node-1").unwrap().status,
            NodeStatus::Unassignable
        );

        // cluster-2 node should still be assignable
        assert_eq!(
            state.get_node("node-2").unwrap().status,
            NodeStatus::Assignable
        );
    }

    #[test]
    fn test_incremental_missed_heartbeat_counting() {
        let config = test_config();
        let monitor = HeartbeatMonitor::new(config);
        let role_engine = RoleAssignmentEngine::new(RolesConfig::default());

        let mut state = NodeManagerState::new();
        let base_time = Utc::now();

        // Register a node
        state.process_heartbeat(
            "node-1".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Healthy,
            base_time,
        );

        // First check: 5 seconds later - no missed heartbeats
        let _changes = monitor.check_heartbeats(
            &mut state,
            &role_engine,
            base_time + Duration::seconds(5),
        );
        assert_eq!(state.get_node("node-1").unwrap().missed_heartbeats, 0);

        // Second check: 15 seconds later - 1 missed
        let _changes = monitor.check_heartbeats(
            &mut state,
            &role_engine,
            base_time + Duration::seconds(15),
        );
        assert_eq!(state.get_node("node-1").unwrap().missed_heartbeats, 1);
        assert_eq!(
            state.get_node("node-1").unwrap().status,
            NodeStatus::Suspect
        );

        // Third check: 25 seconds later - 2 missed
        let _changes = monitor.check_heartbeats(
            &mut state,
            &role_engine,
            base_time + Duration::seconds(25),
        );
        assert_eq!(state.get_node("node-1").unwrap().missed_heartbeats, 2);
        assert_eq!(
            state.get_node("node-1").unwrap().status,
            NodeStatus::Suspect
        );

        // Fourth check: 35 seconds later - 3 missed (unassignable)
        let _changes = monitor.check_heartbeats(
            &mut state,
            &role_engine,
            base_time + Duration::seconds(35),
        );
        assert_eq!(state.get_node("node-1").unwrap().missed_heartbeats, 3);
        assert_eq!(
            state.get_node("node-1").unwrap().status,
            NodeStatus::Unassignable
        );
    }

    #[test]
    fn test_role_reassignment_triggered_when_node_becomes_unassignable() {
        let config = test_config();
        let monitor = HeartbeatMonitor::new(config);
        let role_engine = RoleAssignmentEngine::new(RolesConfig::default());

        let mut state = NodeManagerState::new();
        let now = Utc::now();

        // Register two nodes
        state.process_heartbeat(
            "node-1".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Healthy,
            now - Duration::seconds(5), // Active, will get metadata
        );
        state.process_heartbeat(
            "node-2".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Healthy,
            now - Duration::seconds(5), // Active
        );

        // Manually assign metadata to node-1
        if let Some(node) = state.get_node_mut("node-1") {
            node.add_role(crate::model::RoleLease::new(
                RoleType::Metadata,
                1,
                now,
                60,
            ));
        }

        // Now node-1 stops sending heartbeats (simulate time passing)
        if let Some(node) = state.get_node_mut("node-1") {
            node.last_heartbeat = Some(now - Duration::seconds(35));
        }

        // Run check - node-1 becomes unassignable, metadata should transfer
        let changes = monitor.check_heartbeats(&mut state, &role_engine, now);

        // Verify metadata transfer happened
        let metadata_revoked_from_1 = changes.iter().any(|c| {
            c.node_id == "node-1"
                && matches!(
                    &c.action,
                    crate::engine::RoleAction::Revoke(RoleType::Metadata)
                )
        });
        assert!(metadata_revoked_from_1);

        let metadata_assigned_to_2 = changes.iter().any(|c| {
            c.node_id == "node-2"
                && matches!(
                    &c.action,
                    crate::engine::RoleAction::Assign(RoleType::Metadata, _)
                )
        });
        assert!(metadata_assigned_to_2);
    }

    #[test]
    fn test_node_without_last_heartbeat_is_skipped() {
        let config = test_config();
        let monitor = HeartbeatMonitor::new(config);
        let role_engine = RoleAssignmentEngine::new(RolesConfig::default());

        let mut state = NodeManagerState::new();
        let now = Utc::now();

        // Create a node via process_heartbeat, then set last_heartbeat to None
        // to test the skip logic for nodes without last_heartbeat
        state.process_heartbeat(
            "node-1".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Healthy,
            now,
        );

        // Now set last_heartbeat to None to test the skip logic
        if let Some(node) = state.get_node_mut("node-1") {
            node.last_heartbeat = None;
        }

        // Run check - should not crash, should skip the node
        let _changes = monitor.check_heartbeats(&mut state, &role_engine, now);

        // Node should still be assignable with no missed heartbeats
        let node = state.get_node("node-1").unwrap();
        assert_eq!(node.status, NodeStatus::Assignable);
        assert_eq!(node.missed_heartbeats, 0);
    }

    #[test]
    fn test_custom_threshold() {
        let config = HeartbeatConfig {
            check_interval_secs: 5,
            missed_threshold: 2, // Lower threshold
        };
        let monitor = HeartbeatMonitor::new(config);
        let role_engine = RoleAssignmentEngine::new(RolesConfig::default());

        let mut state = NodeManagerState::new();
        let now = Utc::now();

        // Register a node with heartbeat 12 seconds ago (past 2 intervals of 5s)
        state.process_heartbeat(
            "node-1".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Healthy,
            now - Duration::seconds(12),
        );

        // Run check
        let _changes = monitor.check_heartbeats(&mut state, &role_engine, now);

        // Node should be unassignable with threshold of 2
        let node = state.get_node("node-1").unwrap();
        assert_eq!(node.status, NodeStatus::Unassignable);
        assert_eq!(node.missed_heartbeats, 2);
    }
}

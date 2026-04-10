//! Core node-related types: status enums, roles, and the Node struct.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Node assignability status as determined by the node manager.
///
/// The state machine: Assignable ↔ Suspect → Unassignable
/// - Assignable: Node is healthy and can be assigned roles
/// - Suspect: Node missed at least one heartbeat but under threshold
/// - Unassignable: Node missed threshold heartbeats or reported unhealthy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeStatus {
    Assignable,
    Suspect,
    Unassignable,
}

impl NodeStatus {
    /// Returns true if the node can be assigned new roles.
    pub fn is_assignable(&self) -> bool {
        matches!(self, Self::Assignable)
    }

    /// Returns true if the node cannot be assigned roles.
    pub fn is_unassignable(&self) -> bool {
        matches!(self, Self::Unassignable)
    }
}

/// Roles that can be assigned to data nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RoleType {
    /// Metadata role: coordinates cluster metadata (typically 1 per cluster)
    Metadata,
    /// Data role: stores and serves actual data (all assignable nodes)
    Data,
    /// Storage role: manages storage allocation (subset of nodes)
    Storage,
}

/// Self-reported health status from a data node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthStatus {
    Healthy,
    Unhealthy,
    Draining,
}

impl HealthStatus {
    /// Returns true if the health status prevents role assignment.
    pub fn prevents_assignment(&self) -> bool {
        matches!(self, Self::Unhealthy | Self::Draining)
    }
}

/// A role lease with fencing token and expiration time.
///
/// Leases are granted by the node manager and must be renewed via heartbeats.
/// The fencing token increments on each metadata role reassignment to prevent split-brain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleLease {
    /// The type of role being leased.
    pub role: RoleType,
    /// Monotonically increasing fencing token (used for Metadata role).
    pub fencing_token: u64,
    /// When this lease was granted.
    pub granted_at: DateTime<Utc>,
    /// Duration of the lease in seconds.
    pub lease_duration_secs: u64,
}

impl RoleLease {
    /// Creates a new role lease.
    pub fn new(role: RoleType, fencing_token: u64, granted_at: DateTime<Utc>, lease_duration_secs: u64) -> Self {
        Self {
            role,
            fencing_token,
            granted_at,
            lease_duration_secs,
        }
    }

    /// Returns the timestamp when this lease expires.
    pub fn expires_at(&self) -> DateTime<Utc> {
        self.granted_at + chrono::Duration::seconds(self.lease_duration_secs as i64)
    }

    /// Returns true if the lease is expired at the given timestamp.
    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        now > self.expires_at()
    }

    /// Returns true if the lease is still valid at the given timestamp.
    pub fn is_valid(&self, now: DateTime<Utc>) -> bool {
        !self.is_expired(now)
    }
}

/// Represents a data node in the system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    /// Unique identifier for this node.
    pub node_id: String,
    /// ID of the cluster this node belongs to.
    pub cluster_id: String,
    /// Current assignability status.
    pub status: NodeStatus,
    /// Self-reported health status.
    pub health_status: HealthStatus,
    /// Currently assigned role leases.
    pub roles: Vec<RoleLease>,
    /// Timestamp of the last heartbeat received.
    pub last_heartbeat: Option<DateTime<Utc>>,
    /// Number of consecutive missed heartbeats.
    pub missed_heartbeats: u32,
    /// When this node was first registered.
    pub registered_at: DateTime<Utc>,
}

impl Node {
    /// Creates a new node with default values.
    pub fn new(node_id: String, cluster_id: String, health_status: HealthStatus, now: DateTime<Utc>) -> Self {
        let status = if health_status.prevents_assignment() {
            NodeStatus::Unassignable
        } else {
            NodeStatus::Assignable
        };

        Self {
            node_id,
            cluster_id,
            status,
            health_status,
            roles: Vec::new(),
            last_heartbeat: Some(now),
            missed_heartbeats: 0,
            registered_at: now,
        }
    }

    /// Returns true if this node has a specific role.
    pub fn has_role(&self, role: RoleType) -> bool {
        self.roles.iter().any(|r| r.role == role)
    }

    /// Returns the lease for a specific role if present.
    pub fn get_role_lease(&self, role: RoleType) -> Option<&RoleLease> {
        self.roles.iter().find(|r| r.role == role)
    }

    /// Adds a role lease to this node.
    pub fn add_role(&mut self, lease: RoleLease) {
        if !self.has_role(lease.role) {
            self.roles.push(lease);
        }
    }

    /// Removes a role lease from this node.
    pub fn remove_role(&mut self, role: RoleType) {
        self.roles.retain(|r| r.role != role);
    }

    /// Removes all roles from this node.
    pub fn remove_all_roles(&mut self) {
        self.roles.clear();
    }

    /// Extends all role leases to expire at the given duration from now.
    /// This is called when a heartbeat is received to renew active leases.
    pub fn extend_leases(&mut self, now: DateTime<Utc>, lease_duration_secs: u64) {
        for lease in &mut self.roles {
            lease.granted_at = now;
            lease.lease_duration_secs = lease_duration_secs;
        }
    }

    /// Records a heartbeat, resetting the missed counter.
    pub fn record_heartbeat(&mut self, now: DateTime<Utc>) {
        self.last_heartbeat = Some(now);
        self.missed_heartbeats = 0;
    }

    /// Marks this node as suspect (transition from Assignable).
    pub fn mark_suspect(&mut self) {
        if self.status == NodeStatus::Assignable {
            self.status = NodeStatus::Suspect;
        }
    }

    /// Marks this node as unassignable.
    pub fn mark_unassignable(&mut self) {
        self.status = NodeStatus::Unassignable;
    }

    /// Marks this node as assignable (recovery from Suspect/Unassignable).
    pub fn mark_assignable(&mut self) {
        self.status = NodeStatus::Assignable;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> DateTime<Utc> {
        Utc::now()
    }

    #[test]
    fn test_role_lease_expiration() {
        let lease = RoleLease::new(RoleType::Metadata, 1, now(), 60);
        assert!(!lease.is_expired(now()));
        assert!(lease.is_valid(now()));

        let future = now() + chrono::Duration::seconds(61);
        assert!(lease.is_expired(future));
        assert!(!lease.is_valid(future));
    }

    #[test]
    fn test_node_new_healthy() {
        let node = Node::new("node-1".to_string(), "cluster-1".to_string(), HealthStatus::Healthy, now());
        assert_eq!(node.status, NodeStatus::Assignable);
        assert_eq!(node.missed_heartbeats, 0);
        assert!(node.roles.is_empty());
    }

    #[test]
    fn test_node_new_unhealthy() {
        let node = Node::new("node-1".to_string(), "cluster-1".to_string(), HealthStatus::Unhealthy, now());
        assert_eq!(node.status, NodeStatus::Unassignable);
    }

    #[test]
    fn test_node_new_draining() {
        let node = Node::new("node-1".to_string(), "cluster-1".to_string(), HealthStatus::Draining, now());
        assert_eq!(node.status, NodeStatus::Unassignable);
    }

    #[test]
    fn test_add_role() {
        let mut node = Node::new("node-1".to_string(), "cluster-1".to_string(), HealthStatus::Healthy, now());
        let lease = RoleLease::new(RoleType::Data, 1, now(), 60);
        node.add_role(lease);
        assert!(node.has_role(RoleType::Data));
        assert_eq!(node.roles.len(), 1);
    }

    #[test]
    fn test_remove_role() {
        let mut node = Node::new("node-1".to_string(), "cluster-1".to_string(), HealthStatus::Healthy, now());
        node.add_role(RoleLease::new(RoleType::Data, 1, now(), 60));
        node.add_role(RoleLease::new(RoleType::Metadata, 2, now(), 60));
        assert_eq!(node.roles.len(), 2);

        node.remove_role(RoleType::Data);
        assert!(!node.has_role(RoleType::Data));
        assert!(node.has_role(RoleType::Metadata));
        assert_eq!(node.roles.len(), 1);
    }

    #[test]
    fn test_remove_all_roles() {
        let mut node = Node::new("node-1".to_string(), "cluster-1".to_string(), HealthStatus::Healthy, now());
        node.add_role(RoleLease::new(RoleType::Data, 1, now(), 60));
        node.add_role(RoleLease::new(RoleType::Metadata, 2, now(), 60));
        node.remove_all_roles();
        assert!(node.roles.is_empty());
    }

    #[test]
    fn test_record_heartbeat() {
        let mut node = Node::new("node-1".to_string(), "cluster-1".to_string(), HealthStatus::Healthy, now());
        node.missed_heartbeats = 3;
        node.record_heartbeat(now());
        assert_eq!(node.missed_heartbeats, 0);
        assert!(node.last_heartbeat.is_some());
    }

    #[test]
    fn test_status_transitions() {
        let mut node = Node::new("node-1".to_string(), "cluster-1".to_string(), HealthStatus::Healthy, now());
        assert!(node.status.is_assignable());

        node.mark_suspect();
        assert_eq!(node.status, NodeStatus::Suspect);
        assert!(!node.status.is_assignable());

        node.mark_unassignable();
        assert_eq!(node.status, NodeStatus::Unassignable);
        assert!(node.status.is_unassignable());

        node.mark_assignable();
        assert_eq!(node.status, NodeStatus::Assignable);
        assert!(node.status.is_assignable());
    }

    #[test]
    fn test_extend_leases() {
        let mut node = Node::new("node-1".to_string(), "cluster-1".to_string(), HealthStatus::Healthy, now());
        node.add_role(RoleLease::new(RoleType::Data, 1, now(), 60));

        let later = now() + chrono::Duration::seconds(30);
        node.extend_leases(later, 120);

        let lease = node.get_role_lease(RoleType::Data).unwrap();
        assert_eq!(lease.lease_duration_secs, 120);
        assert_eq!(lease.granted_at, later);
    }

    #[test]
    fn test_health_status_prevents_assignment() {
        assert!(!HealthStatus::Healthy.prevents_assignment());
        assert!(HealthStatus::Unhealthy.prevents_assignment());
        assert!(HealthStatus::Draining.prevents_assignment());
    }

    #[test]
    fn test_suspect_does_not_transition_from_unassignable() {
        let mut node = Node::new("node-1".to_string(), "cluster-1".to_string(), HealthStatus::Healthy, now());
        node.mark_unassignable();
        node.mark_suspect(); // Should have no effect
        assert_eq!(node.status, NodeStatus::Unassignable);
    }

    #[test]
    fn test_get_role_lease() {
        let mut node = Node::new("node-1".to_string(), "cluster-1".to_string(), HealthStatus::Healthy, now());
        node.add_role(RoleLease::new(RoleType::Metadata, 5, now(), 60));

        let metadata_lease = node.get_role_lease(RoleType::Metadata);
        assert!(metadata_lease.is_some());
        assert_eq!(metadata_lease.unwrap().fencing_token, 5);

        assert!(node.get_role_lease(RoleType::Data).is_none());
    }

    #[test]
    fn test_add_duplicate_role_idempotent() {
        let mut node = Node::new("node-1".to_string(), "cluster-1".to_string(), HealthStatus::Healthy, now());
        node.add_role(RoleLease::new(RoleType::Data, 1, now(), 60));
        node.add_role(RoleLease::new(RoleType::Data, 2, now(), 60));
        assert_eq!(node.roles.len(), 1);
    }
}

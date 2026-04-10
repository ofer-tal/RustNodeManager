//! Cluster model representing a collection of nodes.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::Node;

/// Represents a cluster containing multiple nodes.
///
/// A cluster groups nodes that work together on the same data set.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cluster {
    /// Unique identifier for this cluster.
    pub cluster_id: String,
    /// All nodes in this cluster, keyed by node_id.
    pub nodes: HashMap<String, Node>,
}

impl Cluster {
    /// Creates a new empty cluster.
    pub fn new(cluster_id: String) -> Self {
        Self {
            cluster_id,
            nodes: HashMap::new(),
        }
    }

    /// Adds or updates a node in the cluster.
    pub fn add_node(&mut self, node: Node) {
        self.nodes.insert(node.node_id.clone(), node);
    }

    /// Removes a node from the cluster, returning it if present.
    pub fn remove_node(&mut self, node_id: &str) -> Option<Node> {
        self.nodes.remove(node_id)
    }

    /// Gets a reference to a node by ID.
    pub fn get_node(&self, node_id: &str) -> Option<&Node> {
        self.nodes.get(node_id)
    }

    /// Gets a mutable reference to a node by ID.
    pub fn get_node_mut(&mut self, node_id: &str) -> Option<&mut Node> {
        self.nodes.get_mut(node_id)
    }

    /// Returns true if the cluster contains the node.
    pub fn contains_node(&self, node_id: &str) -> bool {
        self.nodes.contains_key(node_id)
    }

    /// Returns the number of nodes in the cluster.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Returns an iterator over all nodes in the cluster.
    pub fn nodes_iter(&self) -> impl Iterator<Item = &Node> {
        self.nodes.values()
    }

    /// Counts nodes with Assignable status.
    pub fn assignable_count(&self) -> usize {
        self.nodes_iter().filter(|n| n.status.is_assignable()).count()
    }

    /// Counts nodes with Suspect status.
    pub fn suspect_count(&self) -> usize {
        self.nodes_iter()
            .filter(|n| n.status == crate::model::NodeStatus::Suspect)
            .count()
    }

    /// Counts nodes with Unassignable status.
    pub fn unassignable_count(&self) -> usize {
        self.nodes_iter().filter(|n| n.status.is_unassignable()).count()
    }

    /// Gets all assignable nodes sorted by node_id.
    pub fn assignable_nodes(&self) -> Vec<&Node> {
        let mut nodes: Vec<&Node> = self.nodes_iter().filter(|n| n.status.is_assignable()).collect();
        nodes.sort_by_key(|n| &n.node_id);
        nodes
    }

    /// Gets all nodes sorted by node_id.
    pub fn sorted_nodes(&self) -> Vec<&Node> {
        let mut nodes: Vec<&Node> = self.nodes_iter().collect();
        nodes.sort_by_key(|n| &n.node_id);
        nodes
    }

    /// Checks if the cluster is empty.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::HealthStatus;
    use chrono::Utc;

    fn create_test_node(node_id: &str, health: HealthStatus) -> Node {
        Node::new(node_id.to_string(), "test-cluster".to_string(), health, Utc::now())
    }

    #[test]
    fn test_new_cluster() {
        let cluster = Cluster::new("cluster-1".to_string());
        assert_eq!(cluster.cluster_id, "cluster-1");
        assert!(cluster.is_empty());
        assert_eq!(cluster.node_count(), 0);
    }

    #[test]
    fn test_add_node() {
        let mut cluster = Cluster::new("cluster-1".to_string());
        let node = create_test_node("node-1", HealthStatus::Healthy);
        cluster.add_node(node);

        assert_eq!(cluster.node_count(), 1);
        assert!(cluster.contains_node("node-1"));
    }

    #[test]
    fn test_remove_node() {
        let mut cluster = Cluster::new("cluster-1".to_string());
        let node = create_test_node("node-1", HealthStatus::Healthy);
        cluster.add_node(node);

        let removed = cluster.remove_node("node-1");
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().node_id, "node-1");
        assert!(cluster.is_empty());
    }

    #[test]
    fn test_remove_nonexistent_node() {
        let mut cluster = Cluster::new("cluster-1".to_string());
        assert!(cluster.remove_node("nonexistent").is_none());
    }

    #[test]
    fn test_get_node() {
        let mut cluster = Cluster::new("cluster-1".to_string());
        let node = create_test_node("node-1", HealthStatus::Healthy);
        cluster.add_node(node);

        let found = cluster.get_node("node-1");
        assert!(found.is_some());
        assert_eq!(found.unwrap().node_id, "node-1");

        assert!(cluster.get_node("nonexistent").is_none());
    }

    #[test]
    fn test_get_node_mut() {
        let mut cluster = Cluster::new("cluster-1".to_string());
        let node = create_test_node("node-1", HealthStatus::Healthy);
        cluster.add_node(node);

        if let Some(node_mut) = cluster.get_node_mut("node-1") {
            node_mut.missed_heartbeats = 5;
        }

        assert_eq!(cluster.get_node("node-1").unwrap().missed_heartbeats, 5);
    }

    #[test]
    fn test_status_counts() {
        let mut cluster = Cluster::new("cluster-1".to_string());

        cluster.add_node(create_test_node("node-1", HealthStatus::Healthy));
        cluster.add_node(create_test_node("node-2", HealthStatus::Healthy));

        let mut node3 = create_test_node("node-3", HealthStatus::Healthy);
        node3.mark_suspect();
        cluster.add_node(node3);

        let node4 = create_test_node("node-4", HealthStatus::Unhealthy);
        cluster.add_node(node4);

        assert_eq!(cluster.assignable_count(), 2);
        assert_eq!(cluster.suspect_count(), 1);
        assert_eq!(cluster.unassignable_count(), 1);
    }

    #[test]
    fn test_assignable_nodes_sorted() {
        let mut cluster = Cluster::new("cluster-1".to_string());
        cluster.add_node(create_test_node("node-3", HealthStatus::Healthy));
        cluster.add_node(create_test_node("node-1", HealthStatus::Healthy));
        cluster.add_node(create_test_node("node-2", HealthStatus::Healthy));

        let assignable = cluster.assignable_nodes();
        assert_eq!(assignable.len(), 3);
        assert_eq!(assignable[0].node_id, "node-1");
        assert_eq!(assignable[1].node_id, "node-2");
        assert_eq!(assignable[2].node_id, "node-3");
    }

    #[test]
    fn test_assignable_nodes_filters_non_assignable() {
        let mut cluster = Cluster::new("cluster-1".to_string());
        cluster.add_node(create_test_node("node-1", HealthStatus::Healthy));

        let mut node2 = create_test_node("node-2", HealthStatus::Healthy);
        node2.mark_suspect();
        cluster.add_node(node2);

        cluster.add_node(create_test_node("node-3", HealthStatus::Unhealthy));

        let assignable = cluster.assignable_nodes();
        assert_eq!(assignable.len(), 1);
        assert_eq!(assignable[0].node_id, "node-1");
    }

    #[test]
    fn test_sorted_nodes() {
        let mut cluster = Cluster::new("cluster-1".to_string());
        cluster.add_node(create_test_node("node-3", HealthStatus::Healthy));
        cluster.add_node(create_test_node("node-1", HealthStatus::Unhealthy));
        cluster.add_node(create_test_node("node-2", HealthStatus::Healthy));

        let sorted = cluster.sorted_nodes();
        assert_eq!(sorted.len(), 3);
        assert_eq!(sorted[0].node_id, "node-1");
        assert_eq!(sorted[1].node_id, "node-2");
        assert_eq!(sorted[2].node_id, "node-3");
    }

    #[test]
    fn test_update_existing_node() {
        let mut cluster = Cluster::new("cluster-1".to_string());
        let mut node = create_test_node("node-1", HealthStatus::Healthy);
        node.missed_heartbeats = 5;
        cluster.add_node(node);

        let mut updated = create_test_node("node-1", HealthStatus::Healthy);
        updated.missed_heartbeats = 0;
        cluster.add_node(updated);

        assert_eq!(cluster.node_count(), 1);
        assert_eq!(cluster.get_node("node-1").unwrap().missed_heartbeats, 0);
    }
}

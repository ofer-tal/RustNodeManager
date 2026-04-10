//! gRPC service implementation for the Node Manager.

use node_manager_core::{HeartbeatMonitor, HeartbeatProcessor, NodeManagerState, RoleAssignmentEngine, RolesConfig, StateStore};
use proto_gen::node_manager::node_manager_service_server::{NodeManagerService as NodeManagerServiceTrait, NodeManagerServiceServer};
use proto_gen::{HeartbeatRequest, HeartbeatResponse, StatusRequest, StatusResponse};
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status as TonicStatus};
use tracing::{debug, error};

/// Shared state for the Node Manager service.
///
/// This struct contains all the core components needed for heartbeat processing,
/// monitoring, and state persistence. It is wrapped in an Arc<RwLock> to allow
/// concurrent access from multiple tasks (RPC handlers and background monitor).
pub struct SharedState {
    /// Heartbeat processor for handling heartbeats and computing role assignments.
    pub processor: HeartbeatProcessor,
    /// Heartbeat monitor for detecting missed heartbeats and updating node status.
    pub monitor: HeartbeatMonitor,
    /// State store for persisting node manager state.
    pub store: Box<dyn StateStore>,
}

impl SharedState {
    /// Creates a new shared state instance.
    pub fn new(
        processor: HeartbeatProcessor,
        monitor: HeartbeatMonitor,
        store: Box<dyn StateStore>,
    ) -> Self {
        Self {
            processor,
            monitor,
            store,
        }
    }

    /// Creates a new shared state from components.
    pub fn from_components(
        state: NodeManagerState,
        roles_config: RolesConfig,
        heartbeat_config: node_manager_core::HeartbeatConfig,
        store: Box<dyn StateStore>,
    ) -> Self {
        // Create role assignment engine with fencing token recovery
        let max_fencing_token = state.max_fencing_token();
        let role_engine = if max_fencing_token > 0 {
            RoleAssignmentEngine::with_recovery(roles_config, max_fencing_token)
        } else {
            RoleAssignmentEngine::new(roles_config)
        };

        let processor = HeartbeatProcessor::with_engine(state, role_engine);
        let monitor = HeartbeatMonitor::new(heartbeat_config);

        Self::new(processor, monitor, store)
    }

    /// Gets a reference to the node manager state.
    pub fn state(&self) -> &NodeManagerState {
        self.processor.state()
    }

    /// Gets a mutable reference to the node manager state.
    ///
    /// This method provides mutable access to the state for the monitor
    /// and other components that need to update state.
    pub fn state_mut(&mut self) -> &mut NodeManagerState {
        self.processor.state_mut()
    }

    /// Gets the roles config from the processor.
    pub fn roles_config(&self) -> RolesConfig {
        self.processor.role_engine().config().clone()
    }

    /// Runs heartbeat check and returns the role changes.
    ///
    /// Destructures self to separate the processor from the monitor, then uses
    /// `state_and_engine()` to obtain simultaneous mutable state and immutable
    /// role engine references without borrow conflicts.
    pub fn check_heartbeats(&mut self, now: chrono::DateTime<chrono::Utc>) -> Vec<node_manager_core::engine::RoleChange> {
        let Self { processor, monitor, store: _ } = self;
        let (state, role_engine) = processor.state_and_engine();
        monitor.check_heartbeats(state, role_engine, now)
    }
}

/// The gRPC service implementation for the Node Manager.
///
/// This service handles heartbeat requests from data nodes and status queries from clients.
/// It uses an Arc<RwLock<>> wrapper to allow concurrent read/write access to the shared state.
pub struct NodeManagerServiceImpl {
    /// The shared state wrapped in a RwLock for concurrent access.
    state: Arc<RwLock<SharedState>>,
}

impl NodeManagerServiceImpl {
    /// Creates a new service implementation with the given shared state.
    pub fn new(state: SharedState) -> Self {
        Self {
            state: Arc::new(RwLock::new(state)),
        }
    }

    /// Creates a new service implementation from components.
    pub fn from_components(
        node_manager_state: NodeManagerState,
        roles_config: RolesConfig,
        heartbeat_config: node_manager_core::HeartbeatConfig,
        store: Box<dyn StateStore>,
    ) -> Self {
        let shared_state = SharedState::from_components(
            node_manager_state,
            roles_config,
            heartbeat_config,
            store,
        );
        Self::new(shared_state)
    }

    /// Gets a reference to the shared state.
    pub fn get_state(&self) -> std::sync::Arc<tokio::sync::RwLock<SharedState>> {
        self.state.clone()
    }

    /// Converts this service into a tonic server.
    pub fn into_server(self) -> NodeManagerServiceServer<Self> {
        NodeManagerServiceServer::new(self)
    }

    /// Persists the current state to storage in the background.
    ///
    /// This is a fire-and-forget operation that logs errors but doesn't propagate them.
    async fn persist_state(&self) {
        let state = self.state.clone();
        tokio::spawn(async move {
            let state_guard = state.read().await;
            let node_state = state_guard.state();

            match state_guard.store.save_state(node_state).await {
                Ok(_) => {
                    debug!(
                        clusters = node_state.cluster_count(),
                        nodes = node_state.node_count(),
                        "State persisted successfully"
                    );
                }
                Err(e) => {
                    error!(error = %e, "Failed to persist state");
                }
            }
        });
    }
}

#[async_trait::async_trait]
impl NodeManagerServiceTrait for NodeManagerServiceImpl {
    /// Handles heartbeat requests from data nodes.
    ///
    /// This RPC is called by data nodes to report their liveness and health status.
    /// The service processes the heartbeat, updates node state, computes role assignments,
    /// and returns the node's current status and assigned roles.
    ///
    /// State persistence happens asynchronously after the response is returned.
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, TonicStatus> {
        let req = request.into_inner();

        // Validate input: reject empty or excessively long IDs
        if req.node_id.is_empty() || req.node_id.len() > 256 {
            return Err(TonicStatus::invalid_argument(
                "node_id must be between 1 and 256 characters",
            ));
        }
        if req.cluster_id.is_empty() || req.cluster_id.len() > 256 {
            return Err(TonicStatus::invalid_argument(
                "cluster_id must be between 1 and 256 characters",
            ));
        }

        debug!(
            node_id = %req.node_id,
            cluster_id = %req.cluster_id,
            health_status = req.health_status,
            "Received heartbeat request"
        );

        // Convert protobuf health status to domain health status
        let health_status = match crate::conversion::proto_to_domain_health_status(req.health_status) {
            Some(hs) => hs,
            None => {
                return Err(TonicStatus::invalid_argument(format!(
                    "Invalid health_status value: {}",
                    req.health_status
                )));
            }
        };

        // Get current time
        let now = chrono::Utc::now();

        // Acquire write lock and process heartbeat
        let result = {
            let mut state_guard = self.state.write().await;

            state_guard.processor.process_heartbeat(
                req.node_id.clone(),
                req.cluster_id.clone(),
                health_status,
                now,
            )
        };

        // Spawn fire-and-forget persist task
        self.persist_state().await;

        // Convert domain result to protobuf response
        let response = crate::conversion::domain_to_proto_heartbeat_response(result);

        debug!(
            node_status = response.node_status,
            role_count = response.assigned_roles.len(),
            cluster_nodes_count = response.cluster_nodes.len(),
            "Heartbeat processed successfully"
        );

        Ok(Response::new(response))
    }

    /// Handles status queries from clients.
    ///
    /// This RPC returns the current status of all clusters (or a specific cluster if filtered).
    async fn get_status(
        &self,
        request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, TonicStatus> {
        let req = request.into_inner();

        debug!(
            cluster_filter = ?req.cluster_id,
            "Received status request"
        );

        // Acquire read lock and get state
        let state_guard = self.state.read().await;

        let node_state = state_guard.state();

        let clusters = if let Some(cluster_id) = req.cluster_id {
            // Filter by specific cluster
            if let Some(cluster) = node_state.get_cluster(&cluster_id) {
                vec![crate::conversion::domain_to_proto_cluster_status(
                    cluster_id,
                    cluster.sorted_nodes().iter().map(|n| {
                        node_manager_core::engine::ClusterNodeInfo {
                            node_id: n.node_id.clone(),
                            status: n.status,
                            roles: n.roles.clone(),
                        }
                    }).collect(),
                )]
            } else {
                vec![]
            }
        } else {
            // Return all clusters
            node_state.cluster_ids()
                .filter_map(|id| {
                    let cluster_id = id.clone();
                    node_state.get_cluster(&cluster_id).map(|cluster| {
                        crate::conversion::domain_to_proto_cluster_status(
                            cluster_id,
                            cluster.sorted_nodes().iter().map(|n| {
                                node_manager_core::engine::ClusterNodeInfo {
                                    node_id: n.node_id.clone(),
                                    status: n.status,
                                    roles: n.roles.clone(),
                                }
                            }).collect(),
                        )
                    })
                })
                .collect()
        };

        let response = StatusResponse { clusters };

        debug!(
            cluster_count = response.clusters.len(),
            "Status request completed"
        );

        Ok(Response::new(response))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use node_manager_core::{NodeStatus, RolesConfig};
    use proto_gen::node_manager::{HealthStatus as ProtoHealthStatus, NodeStatus as ProtoNodeStatus};

    #[test]
    fn test_shared_state_creation() {
        let state = NodeManagerState::new();
        let roles_config = RolesConfig::default();
        let heartbeat_config = node_manager_core::HeartbeatConfig::default();
        let store = Box::new(node_manager_core::InMemoryStore::new());

        let shared_state = SharedState::from_components(
            state,
            roles_config,
            heartbeat_config,
            store,
        );

        assert_eq!(shared_state.state().cluster_count(), 0);
        assert_eq!(shared_state.state().node_count(), 0);
    }

    #[test]
    fn test_service_creation() {
        let state = NodeManagerState::new();
        let roles_config = RolesConfig::default();
        let heartbeat_config = node_manager_core::HeartbeatConfig::default();
        let store = Box::new(node_manager_core::InMemoryStore::new());

        let service = NodeManagerServiceImpl::from_components(
            state,
            roles_config,
            heartbeat_config,
            store,
        );

        assert!(Arc::strong_count(&service.state) >= 1);
    }

    #[tokio::test]
    async fn test_heartbeat_request_creates_node() {
        let state = NodeManagerState::new();
        let roles_config = RolesConfig::default();
        let heartbeat_config = node_manager_core::HeartbeatConfig::default();
        let store = Box::new(node_manager_core::InMemoryStore::new());

        let service = NodeManagerServiceImpl::from_components(
            state,
            roles_config,
            heartbeat_config,
            store,
        );

        let request = HeartbeatRequest {
            node_id: "node-1".to_string(),
            cluster_id: "cluster-1".to_string(),
            health_status: ProtoHealthStatus::Healthy as i32,
        };

        let response = service
            .heartbeat(Request::new(request))
            .await
            .expect("Heartbeat should succeed");

        // Node should be assignable and have all three roles
        assert_eq!(response.into_inner().node_status, ProtoNodeStatus::Assignable as i32);

        // Verify the node was created in the state
        let state_guard = service.state.read().await;
        let node = state_guard.state().get_node("node-1").expect("Node should exist");
        assert_eq!(node.status, NodeStatus::Assignable);
    }

    #[tokio::test]
    async fn test_heartbeat_unhealthy_marks_unassignable() {
        let state = NodeManagerState::new();
        let roles_config = RolesConfig::default();
        let heartbeat_config = node_manager_core::HeartbeatConfig::default();
        let store = Box::new(node_manager_core::InMemoryStore::new());

        let service = NodeManagerServiceImpl::from_components(
            state,
            roles_config,
            heartbeat_config,
            store,
        );

        let request = HeartbeatRequest {
            node_id: "node-1".to_string(),
            cluster_id: "cluster-1".to_string(),
            health_status: ProtoHealthStatus::Unhealthy as i32,
        };

        let response = service
            .heartbeat(Request::new(request))
            .await
            .expect("Heartbeat should succeed");

        assert_eq!(response.into_inner().node_status, ProtoNodeStatus::Unassignable as i32);
    }

    #[tokio::test]
    async fn test_get_status_empty_state() {
        let state = NodeManagerState::new();
        let roles_config = RolesConfig::default();
        let heartbeat_config = node_manager_core::HeartbeatConfig::default();
        let store = Box::new(node_manager_core::InMemoryStore::new());

        let service = NodeManagerServiceImpl::from_components(
            state,
            roles_config,
            heartbeat_config,
            store,
        );

        let request = StatusRequest { cluster_id: None };

        let response = service
            .get_status(Request::new(request))
            .await
            .expect("Get status should succeed");

        assert_eq!(response.into_inner().clusters.len(), 0);
    }
}

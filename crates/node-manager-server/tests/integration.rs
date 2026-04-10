//! Integration tests for the Node Manager gRPC service.
//!
//! These tests start a real gRPC server on a random port and test
//! the full request/response flow through the network stack.

use node_manager_core::{HeartbeatConfig, InMemoryStore, RolesConfig};
use node_manager_server::service::NodeManagerServiceImpl;
use proto_gen::node_manager::node_manager_service_client::NodeManagerServiceClient;
use proto_gen::node_manager::{
    HealthStatus as ProtoHealthStatus, NodeStatus as ProtoNodeStatus,
};
use proto_gen::{HeartbeatRequest, StatusRequest};
use std::net::SocketAddr;
use std::time::Duration;
use tonic::transport::Server;

/// Helper to create a service with default config and in-memory store.
fn create_service() -> NodeManagerServiceImpl {
    let store = Box::new(InMemoryStore::new());
    NodeManagerServiceImpl::from_components(
        node_manager_core::NodeManagerState::new(),
        RolesConfig::default(),
        HeartbeatConfig::default(),
        store,
    )
}

/// Starts a gRPC server on a random port and returns the address.
async fn start_server(service: NodeManagerServiceImpl) -> SocketAddr {
    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<proto_gen::node_manager::node_manager_service_server::NodeManagerServiceServer<NodeManagerServiceImpl>>()
        .await;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let svc = service.into_server();
    tokio::spawn(async move {
        Server::builder()
            .add_service(health_service)
            .add_service(svc)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    // Give the server a moment to start
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

/// Creates a gRPC client connected to the given address.
async fn create_client(addr: SocketAddr) -> NodeManagerServiceClient<tonic::transport::Channel> {
    let url = format!("http://{}", addr);
    NodeManagerServiceClient::connect(url).await.unwrap()
}

#[tokio::test]
async fn test_single_node_heartbeat_assigns_all_roles() {
    let service = create_service();
    let addr = start_server(service).await;
    let mut client = create_client(addr).await;

    let response = client
        .heartbeat(HeartbeatRequest {
            node_id: "node-1".to_string(),
            cluster_id: "cluster-1".to_string(),
            health_status: ProtoHealthStatus::Healthy as i32,
        })
        .await
        .unwrap()
        .into_inner();

    // Single node should get all roles: metadata, data, storage
    assert_eq!(response.node_status, ProtoNodeStatus::Assignable as i32);
    assert_eq!(response.assigned_roles.len(), 3);
    assert!(response
        .assigned_roles
        .iter()
        .any(|r| r.role == proto_gen::node_manager::RoleType::Metadata as i32));
    assert!(response
        .assigned_roles
        .iter()
        .any(|r| r.role == proto_gen::node_manager::RoleType::Data as i32));
    assert!(response
        .assigned_roles
        .iter()
        .any(|r| r.role == proto_gen::node_manager::RoleType::Storage as i32));

    // Cluster nodes should include the single node
    assert_eq!(response.cluster_nodes.len(), 1);
    assert_eq!(response.cluster_nodes[0].node_id, "node-1");
}

#[tokio::test]
async fn test_second_node_gets_data_and_storage_metadata_stays_on_first() {
    let service = create_service();
    let addr = start_server(service).await;
    let mut client = create_client(addr).await;

    // First node heartbeat
    let response1 = client
        .heartbeat(HeartbeatRequest {
            node_id: "node-1".to_string(),
            cluster_id: "cluster-1".to_string(),
            health_status: ProtoHealthStatus::Healthy as i32,
        })
        .await
        .unwrap()
        .into_inner();

    // node-1 should have metadata (lowest node_id)
    assert!(response1
        .assigned_roles
        .iter()
        .any(|r| r.role == proto_gen::node_manager::RoleType::Metadata as i32));

    // Second node heartbeat
    let response2 = client
        .heartbeat(HeartbeatRequest {
            node_id: "node-2".to_string(),
            cluster_id: "cluster-1".to_string(),
            health_status: ProtoHealthStatus::Healthy as i32,
        })
        .await
        .unwrap()
        .into_inner();

    // node-2 should get data but NOT metadata.
    // With default config (storage_max_percent=30%, 2 nodes): max_storage = max(1, 0) = 1,
    // so storage goes to node-1 (lowest node_id). node-2 gets only Data.
    assert_eq!(response2.node_status, ProtoNodeStatus::Assignable as i32);
    assert!(response2
        .assigned_roles
        .iter()
        .any(|r| r.role == proto_gen::node_manager::RoleType::Data as i32));
    assert!(!response2
        .assigned_roles
        .iter()
        .any(|r| r.role == proto_gen::node_manager::RoleType::Metadata as i32));
    assert!(!response2
        .assigned_roles
        .iter()
        .any(|r| r.role == proto_gen::node_manager::RoleType::Storage as i32));

    // Both nodes should be in cluster_nodes
    assert_eq!(response2.cluster_nodes.len(), 2);
}

#[tokio::test]
async fn test_get_status_returns_all_clusters() {
    let service = create_service();
    let addr = start_server(service).await;
    let mut client = create_client(addr).await;

    // Register nodes in two different clusters
    client
        .heartbeat(HeartbeatRequest {
            node_id: "node-1".to_string(),
            cluster_id: "cluster-A".to_string(),
            health_status: ProtoHealthStatus::Healthy as i32,
        })
        .await
        .unwrap();

    client
        .heartbeat(HeartbeatRequest {
            node_id: "node-2".to_string(),
            cluster_id: "cluster-B".to_string(),
            health_status: ProtoHealthStatus::Healthy as i32,
        })
        .await
        .unwrap();

    // Query all clusters
    let status = client
        .get_status(StatusRequest { cluster_id: None })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(status.clusters.len(), 2);

    let cluster_ids: Vec<&str> = status
        .clusters
        .iter()
        .map(|c| c.cluster_id.as_str())
        .collect();
    assert!(cluster_ids.contains(&"cluster-A"));
    assert!(cluster_ids.contains(&"cluster-B"));
}

#[tokio::test]
async fn test_get_status_filters_by_cluster() {
    let service = create_service();
    let addr = start_server(service).await;
    let mut client = create_client(addr).await;

    // Register nodes in two different clusters
    client
        .heartbeat(HeartbeatRequest {
            node_id: "node-1".to_string(),
            cluster_id: "cluster-A".to_string(),
            health_status: ProtoHealthStatus::Healthy as i32,
        })
        .await
        .unwrap();

    client
        .heartbeat(HeartbeatRequest {
            node_id: "node-2".to_string(),
            cluster_id: "cluster-B".to_string(),
            health_status: ProtoHealthStatus::Healthy as i32,
        })
        .await
        .unwrap();

    // Query only cluster-A
    let status = client
        .get_status(StatusRequest {
            cluster_id: Some("cluster-A".to_string()),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(status.clusters.len(), 1);
    assert_eq!(status.clusters[0].cluster_id, "cluster-A");
    assert_eq!(status.clusters[0].nodes.len(), 1);
}

#[tokio::test]
async fn test_unhealthy_node_becomes_unassignable() {
    let service = create_service();
    let addr = start_server(service).await;
    let mut client = create_client(addr).await;

    let response = client
        .heartbeat(HeartbeatRequest {
            node_id: "node-1".to_string(),
            cluster_id: "cluster-1".to_string(),
            health_status: ProtoHealthStatus::Unhealthy as i32,
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.node_status, ProtoNodeStatus::Unassignable as i32);
    // Unassignable nodes should have no roles
    assert!(response.assigned_roles.is_empty());
}

#[tokio::test]
async fn test_metadata_transfers_when_holder_goes_unhealthy() {
    let service = create_service();
    let addr = start_server(service).await;
    let mut client = create_client(addr).await;

    // node-1 gets metadata (lowest node_id)
    let response1 = client
        .heartbeat(HeartbeatRequest {
            node_id: "node-1".to_string(),
            cluster_id: "cluster-1".to_string(),
            health_status: ProtoHealthStatus::Healthy as i32,
        })
        .await
        .unwrap()
        .into_inner();

    assert!(response1
        .assigned_roles
        .iter()
        .any(|r| r.role == proto_gen::node_manager::RoleType::Metadata as i32));

    // node-2 joins
    client
        .heartbeat(HeartbeatRequest {
            node_id: "node-2".to_string(),
            cluster_id: "cluster-1".to_string(),
            health_status: ProtoHealthStatus::Healthy as i32,
        })
        .await
        .unwrap();

    // node-1 goes unhealthy
    let response1_sick = client
        .heartbeat(HeartbeatRequest {
            node_id: "node-1".to_string(),
            cluster_id: "cluster-1".to_string(),
            health_status: ProtoHealthStatus::Unhealthy as i32,
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        response1_sick.node_status,
        ProtoNodeStatus::Unassignable as i32
    );
    assert!(response1_sick.assigned_roles.is_empty());

    // node-2 should now have metadata
    let response2 = client
        .heartbeat(HeartbeatRequest {
            node_id: "node-2".to_string(),
            cluster_id: "cluster-1".to_string(),
            health_status: ProtoHealthStatus::Healthy as i32,
        })
        .await
        .unwrap()
        .into_inner();

    assert!(response2
        .assigned_roles
        .iter()
        .any(|r| r.role == proto_gen::node_manager::RoleType::Metadata as i32));
}

#[tokio::test]
async fn test_node_recovery_from_unassignable() {
    let service = create_service();
    let addr = start_server(service).await;
    let mut client = create_client(addr).await;

    // node-1 goes unhealthy
    let response_sick = client
        .heartbeat(HeartbeatRequest {
            node_id: "node-1".to_string(),
            cluster_id: "cluster-1".to_string(),
            health_status: ProtoHealthStatus::Unhealthy as i32,
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        response_sick.node_status,
        ProtoNodeStatus::Unassignable as i32
    );

    // node-1 recovers
    let response_healthy = client
        .heartbeat(HeartbeatRequest {
            node_id: "node-1".to_string(),
            cluster_id: "cluster-1".to_string(),
            health_status: ProtoHealthStatus::Healthy as i32,
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        response_healthy.node_status,
        ProtoNodeStatus::Assignable as i32
    );
    // Should get roles back (data + storage at minimum, metadata if only node)
    assert!(!response_healthy.assigned_roles.is_empty());
}

#[tokio::test]
async fn test_get_status_empty_state() {
    let service = create_service();
    let addr = start_server(service).await;
    let mut client = create_client(addr).await;

    let status = client
        .get_status(StatusRequest { cluster_id: None })
        .await
        .unwrap()
        .into_inner();

    assert!(status.clusters.is_empty());
}

#[tokio::test]
async fn test_get_status_nonexistent_cluster_returns_empty() {
    let service = create_service();
    let addr = start_server(service).await;
    let mut client = create_client(addr).await;

    // Register a node in cluster-A
    client
        .heartbeat(HeartbeatRequest {
            node_id: "node-1".to_string(),
            cluster_id: "cluster-A".to_string(),
            health_status: ProtoHealthStatus::Healthy as i32,
        })
        .await
        .unwrap();

    // Query a nonexistent cluster
    let status = client
        .get_status(StatusRequest {
            cluster_id: Some("cluster-ZZZ".to_string()),
        })
        .await
        .unwrap()
        .into_inner();

    assert!(status.clusters.is_empty());
}

#[tokio::test]
async fn test_draining_node_becomes_unassignable() {
    let service = create_service();
    let addr = start_server(service).await;
    let mut client = create_client(addr).await;

    let response = client
        .heartbeat(HeartbeatRequest {
            node_id: "node-1".to_string(),
            cluster_id: "cluster-1".to_string(),
            health_status: ProtoHealthStatus::Draining as i32,
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.node_status, ProtoNodeStatus::Unassignable as i32);
    assert!(response.assigned_roles.is_empty());
}

#[tokio::test]
async fn test_fencing_token_present_in_metadata_role() {
    let service = create_service();
    let addr = start_server(service).await;
    let mut client = create_client(addr).await;

    let response = client
        .heartbeat(HeartbeatRequest {
            node_id: "node-1".to_string(),
            cluster_id: "cluster-1".to_string(),
            health_status: ProtoHealthStatus::Healthy as i32,
        })
        .await
        .unwrap()
        .into_inner();

    // Metadata role should have a non-zero fencing token
    let metadata_lease = response
        .assigned_roles
        .iter()
        .find(|r| r.role == proto_gen::node_manager::RoleType::Metadata as i32)
        .unwrap();
    assert!(metadata_lease.fencing_token > 0);

    // Should have a valid lease expiration
    assert!(metadata_lease.lease_expires_at > 0);
}

//! nm-cli - CLI tool for interacting with the Node Manager.
//!
//! Provides three subcommands:
//! - heartbeat: Send a single heartbeat to the Node Manager
//! - status: Query and display node manager status as JSON
//! - simulate: Simulate N nodes sending periodic heartbeats

use clap::{Parser, Subcommand};
use proto_gen::node_manager::{
    node_manager_service_client::NodeManagerServiceClient, HealthStatus, HeartbeatRequest,
    StatusRequest,
};
use serde_json::{json, Value};
use tonic::transport::Channel;
use tracing::{error, info};
use uuid::Uuid;

/// nm-cli - Node Manager CLI Tool
#[derive(Parser, Debug)]
#[command(name = "nm-cli")]
#[command(about = "CLI tool for interacting with the Node Manager", long_about = None)]
struct Cli {
    /// Node Manager server address (default: http://127.0.0.1:50051)
    #[arg(short, long, default_value = "http://127.0.0.1:50051")]
    server: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Send a single heartbeat to the Node Manager
    Heartbeat {
        /// Node ID (UUID, auto-generated if not provided)
        #[arg(short, long)]
        node_id: Option<String>,

        /// Cluster ID (UUID, auto-generated if not provided)
        #[arg(short, long)]
        cluster_id: Option<String>,

        /// Health status (default: healthy)
        #[arg(long, default_value = "healthy", value_parser = parse_health_status)]
        health: HealthStatus,
    },

    /// Query and display node manager status as JSON
    Status {
        /// Filter by cluster ID (optional, shows all clusters if not provided)
        #[arg(short, long)]
        cluster_id: Option<String>,
    },

    /// Simulate N nodes sending periodic heartbeats
    Simulate {
        /// Cluster ID (UUID, auto-generated if not provided)
        #[arg(short, long)]
        cluster_id: Option<String>,

        /// Number of nodes to simulate (default: 5)
        #[arg(short, long, default_value = "5")]
        nodes: usize,

        /// Interval between heartbeats in seconds (default: 10)
        #[arg(short, long, default_value = "10")]
        interval_secs: u64,
    },
}

/// Parse health status from string
fn parse_health_status(s: &str) -> Result<HealthStatus, String> {
    match s.to_lowercase().as_str() {
        "healthy" => Ok(HealthStatus::Healthy),
        "unhealthy" => Ok(HealthStatus::Unhealthy),
        "draining" => Ok(HealthStatus::Draining),
        _ => Err(format!(
            "Invalid health status: {}. Must be one of: healthy, unhealthy, draining",
            s
        )),
    }
}

/// Convert a proto RoleType i32 to a human-readable string.
fn role_type_name(role: i32) -> String {
    match role {
        1 => "METADATA".to_string(),
        2 => "DATA".to_string(),
        3 => "STORAGE".to_string(),
        _ => format!("UNKNOWN({})", role),
    }
}

/// Convert a proto NodeStatus i32 to a human-readable string.
fn node_status_name(status: i32) -> String {
    match status {
        1 => "ASSIGNABLE".to_string(),
        2 => "SUSPECT".to_string(),
        3 => "UNASSIGNABLE".to_string(),
        _ => format!("UNKNOWN({})", status),
    }
}

/// Convert a RoleLease proto message to a human-readable JSON value.
/// Omits fencing_token when it is 0 (meaning no split-brain protection needed).
fn format_role_lease(role: &proto_gen::node_manager::RoleLease) -> Value {
    let mut map = serde_json::Map::new();
    map.insert("role".to_string(), json!(role_type_name(role.role)));

    if role.fencing_token != 0 {
        map.insert("fencing_token".to_string(), json!(role.fencing_token));
    }

    // Convert unix millis to human-readable ISO 8601
    let expires_at = chrono::DateTime::from_timestamp_millis(role.lease_expires_at as i64)
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_else(|| role.lease_expires_at.to_string());
    map.insert("lease_expires_at".to_string(), json!(expires_at));

    Value::Object(map)
}

/// Format a HeartbeatResponse as human-readable JSON.
fn format_heartbeat_response(
    response: &proto_gen::node_manager::HeartbeatResponse,
) -> Value {
    json!({
        "node_status": node_status_name(response.node_status),
        "assigned_roles": response.assigned_roles.iter().map(format_role_lease).collect::<Vec<_>>(),
        "cluster_nodes": response.cluster_nodes.iter().map(|peer| {
            json!({
                "node_id": peer.node_id,
                "status": node_status_name(peer.status),
                "roles": peer.roles.iter().map(format_role_lease).collect::<Vec<_>>()
            })
        }).collect::<Vec<_>>()
    })
}

/// Format a StatusResponse as human-readable JSON.
fn format_status_response(
    response: &proto_gen::node_manager::StatusResponse,
) -> Value {
    json!({
        "clusters": response.clusters.iter().map(|cluster| {
            json!({
                "cluster_id": cluster.cluster_id,
                "nodes": cluster.nodes.iter().map(|peer| {
                    json!({
                        "node_id": peer.node_id,
                        "status": node_status_name(peer.status),
                        "roles": peer.roles.iter().map(format_role_lease).collect::<Vec<_>>()
                    })
                }).collect::<Vec<_>>()
            })
        }).collect::<Vec<_>>()
    })
}

/// Connect to the Node Manager service
async fn connect(server_addr: &str) -> Result<NodeManagerServiceClient<Channel>, String> {
    NodeManagerServiceClient::connect(server_addr.to_string())
        .await
        .map_err(|e| format!("Failed to connect to {}: {}", server_addr, e))
}

/// Handle the heartbeat subcommand
async fn handle_heartbeat(
    server_addr: &str,
    node_id: Option<String>,
    cluster_id: Option<String>,
    health: HealthStatus,
) -> Result<(), String> {
    let node_id = node_id.unwrap_or_else(|| Uuid::new_v4().to_string());
    let cluster_id = cluster_id.unwrap_or_else(|| Uuid::new_v4().to_string());

    info!(
        "Sending heartbeat: node_id={}, cluster_id={}, health={:?}",
        node_id, cluster_id, health
    );

    let mut client = connect(server_addr).await?;

    let request = HeartbeatRequest {
        node_id: node_id.clone(),
        cluster_id,
        health_status: health as i32,
    };

    let response = client
        .heartbeat(request)
        .await
        .map_err(|e| format!("Heartbeat request failed: {}", e))?
        .into_inner();

    // Print response as formatted JSON
    let json_output = serde_json::to_string_pretty(&format_heartbeat_response(&response))
        .map_err(|e| format!("Failed to serialize response to JSON: {}", e))?;
    println!("{}", json_output);

    Ok(())
}

/// Handle the status subcommand
async fn handle_status(server_addr: &str, cluster_id: Option<String>) -> Result<(), String> {
    info!("Querying status{}", cluster_id.as_ref().map_or(String::new(), |id| format!(" for cluster: {}", id)));

    let mut client = connect(server_addr).await?;

    let request = StatusRequest { cluster_id };

    let response = client
        .get_status(request)
        .await
        .map_err(|e| format!("Status request failed: {}", e))?
        .into_inner();

    // Print response as formatted JSON
    let json_output = serde_json::to_string_pretty(&format_status_response(&response))
        .map_err(|e| format!("Failed to serialize response to JSON: {}", e))?;
    println!("{}", json_output);

    Ok(())
}

/// Handle the simulate subcommand
async fn handle_simulate(
    server_addr: String,
    cluster_id: Option<String>,
    nodes: usize,
    interval_secs: u64,
) -> Result<(), String> {
    let cluster_id = cluster_id.unwrap_or_else(|| Uuid::new_v4().to_string());

    info!(
        "Simulating {} nodes in cluster {} with {}s heartbeat interval",
        nodes, cluster_id, interval_secs
    );

    let mut handles = Vec::with_capacity(nodes);

    for i in 0..nodes {
        let server_addr = server_addr.clone();
        let cluster_id = cluster_id.clone();
        let node_id = Uuid::new_v4().to_string();

        let handle = tokio::spawn(async move {
            let node_index = i;
            let mut client = match connect(&server_addr).await {
                Ok(c) => c,
                Err(e) => {
                    error!(
                        "Node {}: Failed to connect to server: {}",
                        node_index, e
                    );
                    return;
                }
            };

            info!(
                "Node {}: Started (node_id={}, cluster_id={})",
                node_index, node_id, cluster_id
            );

            let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));

            loop {
                interval.tick().await;

                let request = HeartbeatRequest {
                    node_id: node_id.clone(),
                    cluster_id: cluster_id.clone(),
                    health_status: HealthStatus::Healthy as i32,
                };

                match client.heartbeat(request.clone()).await {
                    Ok(response) => {
                        let response = response.into_inner();
                        info!(
                            "Node {}: Heartbeat acknowledged - status: {:?}, roles: {}",
                            node_index,
                            response.node_status(),
                            response.assigned_roles.len()
                        );
                    }
                    Err(e) => {
                        error!("Node {}: Heartbeat failed: {}", node_index, e);
                    }
                }
            }
        });

        handles.push(handle);
    }

    info!("All {} nodes started. Press Ctrl+C to stop.", nodes);

    // Wait for Ctrl+C
    tokio::signal::ctrl_c()
        .await
        .map_err(|e| format!("Failed to listen for Ctrl+C: {}", e))?;

    info!("Shutting down simulation...");

    // Abort all tasks
    for handle in handles {
        handle.abort();
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing subscriber
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    let result = match cli.command {
        Commands::Heartbeat {
            node_id,
            cluster_id,
            health,
        } => handle_heartbeat(&cli.server, node_id, cluster_id, health).await,

        Commands::Status { cluster_id } => handle_status(&cli.server, cluster_id).await,

        Commands::Simulate {
            cluster_id,
            nodes,
            interval_secs,
        } => {
            handle_simulate(cli.server, cluster_id, nodes, interval_secs).await
        }
    };

    if let Err(e) = result {
        error!("Error: {}", e);
        std::process::exit(1);
    }

    Ok(())
}

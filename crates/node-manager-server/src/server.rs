//! Node Manager gRPC server.

use node_manager_core::{AppConfig, HeartbeatConfig, StateStore};
use proto_gen::node_manager::node_manager_service_server::NodeManagerServiceServer;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::signal;
use tonic::transport::Server;
use tracing::{debug, info, warn};

use crate::service::{NodeManagerServiceImpl, SharedState};

/// The Node Manager gRPC server.
///
/// This server hosts the NodeManagerService and handles incoming gRPC connections.
pub struct NodeManagerServer {
    /// The service implementation.
    service: NodeManagerServiceImpl,
    /// The address to bind to.
    addr: SocketAddr,
    /// Heartbeat check interval for the monitor task.
    heartbeat_check_interval: Duration,
}

impl NodeManagerServer {
    /// Creates a new server with the given configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - The application configuration
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid.
    pub async fn with_config(config: &AppConfig) -> Result<Self, anyhow::Error> {
        // Create JsonFileStore
        let store = Box::new(node_manager_core::JsonFileStore::new(
            config.storage.json_file_path.clone(),
        ));

        // Load or create initial state
        let state = match store.load_state().await {
            Ok(state) => {
                info!(
                    clusters = state.cluster_count(),
                    nodes = state.node_count(),
                    max_fencing_token = state.max_fencing_token(),
                    "Loaded state from storage"
                );
                state
            }
            Err(e) => {
                warn!(error = %e, "Failed to load state, starting with empty state");
                node_manager_core::NodeManagerState::new()
            }
        };

        // Create the service with shared state
        let heartbeat_config = HeartbeatConfig {
            check_interval_secs: config.heartbeat.check_interval_secs,
            missed_threshold: config.heartbeat.missed_threshold,
        };

        let service = NodeManagerServiceImpl::from_components(
            state,
            config.roles.clone(),
            heartbeat_config,
            store,
        );

        // Parse the server address
        let addr = config.server.bind_address.parse()?;

        Ok(Self {
            service,
            addr,
            heartbeat_check_interval: Duration::from_secs(config.heartbeat.check_interval_secs),
        })
    }

    /// Creates a new server with the given service and address.
    pub fn new(service: NodeManagerServiceImpl, addr: SocketAddr, heartbeat_check_interval: Duration) -> Self {
        Self {
            service,
            addr,
            heartbeat_check_interval,
        }
    }

    /// Starts the server and runs until a shutdown signal is received.
    ///
    /// This method:
    /// 1. Spawns a background heartbeat monitor task
    /// 2. Binds to the configured address
    /// 3. Serves the gRPC service
    /// 4. Runs until a Ctrl+C signal is received
    /// 5. Shuts down gracefully
    ///
    /// # Errors
    ///
    /// Returns an error if the server fails to start or encounters an error.
    pub async fn run(self) -> Result<(), anyhow::Error> {
        let (health_reporter, health_service) = tonic_health::server::health_reporter();

        // Mark the server as serving
        health_reporter.set_serving::<NodeManagerServiceServer<NodeManagerServiceImpl>>().await;

        // Spawn heartbeat monitor background task
        let monitor_interval = self.heartbeat_check_interval;
        let service_state = self.service.get_state();
        let _monitor_handle = tokio::spawn(async move {
            heartbeat_monitor_task(service_state, monitor_interval).await;
        });

        let addr = self.addr;
        info!(address = %addr, "Starting Node Manager gRPC server");

        let svc = self.service.into_server();

        // Start server with shutdown signal
        Server::builder()
            .add_service(health_service)
            .add_service(svc)
            .serve_with_shutdown(addr, shutdown_signal())
            .await?;

        info!("Server shut down gracefully");
        Ok(())
    }
}

/// Background task that runs the heartbeat monitor at regular intervals.
async fn heartbeat_monitor_task(
    state: std::sync::Arc<tokio::sync::RwLock<SharedState>>,
    interval: Duration,
) {
    let mut timer = tokio::time::interval(interval);

    info!(
        interval_secs = interval.as_secs(),
        "Heartbeat monitor task started"
    );

    loop {
        timer.tick().await;

        let now = chrono::Utc::now();

        // Acquire write lock and check heartbeats
        let changes = {
            let mut state_guard = state.write().await;
            state_guard.check_heartbeats(now)
        };

        if !changes.is_empty() {
            info!(
                changes_count = changes.len(),
                "Heartbeat monitor detected node status changes"
            );

            // Persist state after changes
            let state_for_persist = state.clone();
            tokio::spawn(async move {
                let state_guard = state_for_persist.read().await;
                let node_state = state_guard.state();

                if let Err(e) = state_guard.store.save_state(node_state).await {
                    warn!(error = %e, "Failed to persist state after heartbeat check");
                } else {
                    debug!("State persisted after heartbeat check");
                }
            });
        }
    }
}

/// Waits for a shutdown signal (Ctrl+C).
async fn shutdown_signal() {
    let ctrl_c = async {
        #[allow(clippy::expect_used)]
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!("Received Ctrl+C signal");
        },
        _ = terminate => {
            info!("Received terminate signal");
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use node_manager_core::RolesConfig;

    #[test]
    fn test_server_with_custom_address() {
        let state = node_manager_core::NodeManagerState::new();
        let roles_config = RolesConfig::default();
        let heartbeat_config = HeartbeatConfig::default();
        let store = Box::new(node_manager_core::InMemoryStore::new());

        let service = NodeManagerServiceImpl::from_components(
            state,
            roles_config,
            heartbeat_config,
            store,
        );

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = NodeManagerServer::new(service, addr, Duration::from_secs(10));
        assert_eq!(server.addr, addr);
    }
}

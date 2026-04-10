//! Node Manager gRPC server binary.

use clap::Parser;
use node_manager_core::{AppConfig, JsonFileStore, NodeManagerState, StateStore};
use node_manager_server::NodeManagerServer;
use std::path::PathBuf;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Node Manager gRPC Server
///
/// A distributed node management service that coordinates role assignments
/// across data nodes in a cluster.
#[derive(Parser, Debug)]
#[command(name = "node-manager-server")]
#[command(about = "Node Manager gRPC Server", long_about = None)]
struct Args {
    /// Configuration file path (YAML)
    #[arg(short, long, default_value = "./config/node_manager.yaml")]
    config: PathBuf,

    /// Log level
    #[arg(short, long, default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| args.log_level.into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!("Starting Node Manager server");

    // Load or create configuration
    let config = if args.config.as_os_str().is_empty() {
        info!("Using default configuration");
        AppConfig::default()
    } else if args.config.exists() {
        info!(config = %args.config.display(), "Loading configuration from file");
        load_config(&args.config)?
    } else {
        info!(config = %args.config.display(), "Config file not found, using defaults");
        AppConfig::default()
    };

    // Validate configuration
    config.validate().map_err(|e| {
        tracing::error!(error = %e, "Invalid configuration");
        Box::new(e) as Box<dyn std::error::Error>
    })?;

    // Load or initialize state from storage
    let store: Box<dyn StateStore> = if config.storage.backend == "json_file" {
        info!(path = %config.storage.json_file_path, "Loading state from JSON file");
        Box::new(JsonFileStore::new(config.storage.json_file_path.clone()))
    } else {
        info!(
            backend = %config.storage.backend,
            "Unknown storage backend, using in-memory state"
        );
        Box::new(node_manager_core::InMemoryStore::new())
    };

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
            info!(error = %e, "Failed to load state, starting with empty state");
            NodeManagerState::new()
        }
    };

    // Create the service with all components
    let heartbeat_config = node_manager_core::HeartbeatConfig {
        check_interval_secs: config.heartbeat.check_interval_secs,
        missed_threshold: config.heartbeat.missed_threshold,
    };

    let service = node_manager_server::service::NodeManagerServiceImpl::from_components(
        state,
        config.roles.clone(),
        heartbeat_config,
        store,
    );

    // Create the server
    let addr = config.server.bind_address.parse()?;
    let heartbeat_interval = std::time::Duration::from_secs(config.heartbeat.check_interval_secs as u64);
    let server = NodeManagerServer::new(service, addr, heartbeat_interval);

    info!(
        address = %config.server.bind_address,
        "Node Manager server ready"
    );

    // Run the server
    server.run().await?;

    Ok(())
}

/// Loads configuration from a YAML file.
fn load_config(path: &PathBuf) -> Result<AppConfig, Box<dyn std::error::Error>> {
    let contents = std::fs::read_to_string(path)?;
    let config: AppConfig = serde_yaml::from_str(&contents)?;
    Ok(config)
}

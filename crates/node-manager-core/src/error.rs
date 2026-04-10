//! Domain-specific error types for the Node Manager.
//!
//! All errors are logged via tracing and provide context about what went wrong.

use std::path::PathBuf;

/// Domain-specific errors that can occur in the Node Manager.
#[derive(Debug, thiserror::Error)]
pub enum NodeManagerError {
    /// A requested node was not found in the cluster state.
    #[error("Node {node_id} not found")]
    NodeNotFound { node_id: String },

    /// A requested cluster was not found in the state.
    #[error("Cluster {cluster_id} not found")]
    ClusterNotFound { cluster_id: String },

    /// An error occurred during storage operations.
    #[error("Storage error: {0}")]
    StorageError(String),

    /// Failed to serialize or deserialize state.
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    /// An I/O error occurred during file operations.
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    /// Failed to read configuration file.
    #[error("Failed to read config from {path}: {source}")]
    ConfigReadError {
        path: PathBuf,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// Invalid configuration value provided.
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = NodeManagerError::NodeNotFound {
            node_id: "node-123".to_string(),
        };
        assert!(err.to_string().contains("node-123"));
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_cluster_not_found() {
        let err = NodeManagerError::ClusterNotFound {
            cluster_id: "cluster-abc".to_string(),
        };
        assert!(err.to_string().contains("cluster-abc"));
    }

    #[test]
    fn test_storage_error() {
        let err = NodeManagerError::StorageError("disk full".to_string());
        assert!(err.to_string().contains("disk full"));
    }
}

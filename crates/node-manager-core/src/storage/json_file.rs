//! JSON file-based state store with atomic writes.
//!
//! This implementation persists state to a JSON file on disk.
//! Writes are atomic (write to temp file, then rename) to prevent corruption.

use crate::{
    error::NodeManagerError,
    model::NodeManagerState,
    storage::{trait_::StateStore, EMPTY_STATE_MARKER},
};
use async_trait::async_trait;
use std::path::Path;
use tokio::fs;
use tracing::{debug, info};

/// File-based state store using JSON serialization with atomic writes.
pub struct JsonFileStore {
    file_path: String,
}

impl JsonFileStore {
    /// Creates a new JSON file store with the given path.
    ///
    /// The parent directory will be created if it doesn't exist.
    pub fn new(file_path: String) -> Self {
        Self { file_path }
    }

    /// Returns the file path where state is stored.
    pub fn file_path(&self) -> &str {
        &self.file_path
    }

    /// Ensures the parent directory exists, creating it if necessary.
    async fn ensure_parent_dir(&self) -> Result<(), NodeManagerError> {
        if let Some(parent) = Path::new(&self.file_path).parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)
                    .await
                    .map_err(NodeManagerError::IoError)?;
                debug!(path = %parent.display(), "Ensured parent directory exists");
            }
        }
        Ok(())
    }

    /// Writes the state to a temporary file, then atomically renames it.
    async fn atomic_write(&self, content: &[u8]) -> Result<(), NodeManagerError> {
        self.ensure_parent_dir().await?;

        let temp_path = format!("{}.tmp", self.file_path);

        // Write to temp file
        fs::write(&temp_path, content)
            .await
            .map_err(NodeManagerError::IoError)?;

        // Atomic rename
        fs::rename(&temp_path, &self.file_path)
            .await
            .map_err(NodeManagerError::IoError)?;

        debug!(path = %self.file_path, "Atomically wrote state to disk");
        Ok(())
    }
}

#[async_trait]
impl StateStore for JsonFileStore {
    /// Loads state from the JSON file.
    ///
    /// Returns an empty state if:
    /// - The file doesn't exist
    /// - The file contains only the EMPTY_STATE_MARKER
    async fn load_state(&self) -> Result<NodeManagerState, NodeManagerError> {
        match fs::metadata(&self.file_path).await {
            Ok(_) => {
                let content = fs::read_to_string(&self.file_path)
                    .await
                    .map_err(NodeManagerError::IoError)?;

                // Check for empty state marker
                if content.trim() == EMPTY_STATE_MARKER {
                    info!(path = %self.file_path, "Loaded empty state (marker found)");
                    return Ok(NodeManagerState::new());
                }

                // Parse JSON
                let state: NodeManagerState = serde_json::from_str(&content)
                    .map_err(|e| NodeManagerError::StorageError(format!("Failed to parse state from {}: {}", self.file_path, e)))?;

                info!(
                    path = %self.file_path,
                    clusters = state.cluster_count(),
                    nodes = state.node_count(),
                    "Loaded state from disk"
                );
                Ok(state)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                info!(path = %self.file_path, "State file not found, returning empty state");
                Ok(NodeManagerState::new())
            }
            Err(e) => {
                Err(NodeManagerError::IoError(e))
            }
        }
    }

    /// Saves state to the JSON file with atomic write.
    ///
    /// For truly empty state, writes the EMPTY_STATE_MARKER instead of "{}"
    /// to distinguish between explicit empty state and missing file.
    async fn save_state(&self, state: &NodeManagerState) -> Result<(), NodeManagerError> {
        if state.is_empty() {
            // Write the marker instead of empty JSON
            self.atomic_write(EMPTY_STATE_MARKER.as_bytes()).await?;
            info!(path = %self.file_path, "Saved empty state marker");
        } else {
            let json = serde_json::to_string_pretty(state)
                .map_err(NodeManagerError::SerializationError)?;

            self.atomic_write(json.as_bytes()).await?;
            info!(
                path = %self.file_path,
                clusters = state.cluster_count(),
                nodes = state.node_count(),
                "Saved state to disk"
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::HealthStatus;
    use chrono::Utc;

    async fn create_temp_store() -> JsonFileStore {
        let temp_dir = std::env::temp_dir();
        let test_path = temp_dir
            .join(format!("node_manager_test_{}.json", uuid::Uuid::new_v4()));
        JsonFileStore::new(test_path.to_string_lossy().to_string())
    }

    #[tokio::test]
    async fn test_load_nonexistent_file_returns_empty_state() {
        let store = JsonFileStore::new("/nonexistent/path/file.json".to_string());
        let state = store.load_state().await.unwrap();
        assert!(state.is_empty());
    }

    #[tokio::test]
    async fn test_save_and_load_round_trip() {
        let store = create_temp_store().await;

        let mut original_state = NodeManagerState::new();
        original_state.process_heartbeat(
            "node-1".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Healthy,
            Utc::now(),
        );

        store.save_state(&original_state).await.unwrap();
        let loaded_state = store.load_state().await.unwrap();

        assert_eq!(loaded_state.cluster_count(), 1);
        assert_eq!(loaded_state.node_count(), 1);
        assert!(loaded_state.get_node("node-1").is_some());

        // Cleanup
        let _ = fs::remove_file(store.file_path()).await;
    }

    #[tokio::test]
    async fn test_save_empty_state_writes_marker() {
        let store = create_temp_store().await;
        let empty_state = NodeManagerState::new();

        store.save_state(&empty_state).await.unwrap();

        let content = fs::read_to_string(store.file_path()).await.unwrap();
        assert_eq!(content.trim(), EMPTY_STATE_MARKER);

        // Cleanup
        let _ = fs::remove_file(store.file_path()).await;
    }

    #[tokio::test]
    async fn test_load_empty_state_marker() {
        let store = create_temp_store().await;

        // Write the marker directly
        store.ensure_parent_dir().await.unwrap();
        fs::write(store.file_path(), EMPTY_STATE_MARKER)
            .await
            .unwrap();

        let state = store.load_state().await.unwrap();
        assert!(state.is_empty());

        // Cleanup
        let _ = fs::remove_file(store.file_path()).await;
    }

    #[tokio::test]
    async fn test_multiple_clusters_and_nodes() {
        let store = create_temp_store().await;
        let now = Utc::now();

        let mut state = NodeManagerState::new();
        state.process_heartbeat("node-1".to_string(), "cluster-1".to_string(), HealthStatus::Healthy, now);
        state.process_heartbeat("node-2".to_string(), "cluster-1".to_string(), HealthStatus::Healthy, now);
        state.process_heartbeat("node-3".to_string(), "cluster-2".to_string(), HealthStatus::Healthy, now);

        store.save_state(&state).await.unwrap();
        let loaded = store.load_state().await.unwrap();

        assert_eq!(loaded.cluster_count(), 2);
        assert_eq!(loaded.node_count(), 3);

        // Cleanup
        let _ = fs::remove_file(store.file_path()).await;
    }

    #[tokio::test]
    async fn test_file_path() {
        let store = JsonFileStore::new("/tmp/test.json".to_string());
        assert_eq!(store.file_path(), "/tmp/test.json");
    }

    #[tokio::test]
    async fn test_creates_parent_directory() {
        let temp_dir = std::env::temp_dir();
        let nested_path = temp_dir
            .join(format!("test_nested_{}", uuid::Uuid::new_v4()))
            .join("subdir")
            .join("state.json");

        let store = JsonFileStore::new(nested_path.to_string_lossy().to_string());

        let state = NodeManagerState::new();
        store.save_state(&state).await.unwrap();

        // Verify file exists
        assert!(fs::metadata(store.file_path()).await.is_ok());

        // Cleanup
        let _ = fs::remove_file(store.file_path()).await;
        let parent = nested_path.parent().unwrap();
        let _ = fs::remove_dir(parent).await;
        let grandparent = parent.parent().unwrap();
        let _ = fs::remove_dir(grandparent).await;
    }

    #[tokio::test]
    async fn test_overwrite_existing_state() {
        let store = create_temp_store().await;
        let now = Utc::now();

        // Save initial state
        let mut state1 = NodeManagerState::new();
        state1.process_heartbeat("node-1".to_string(), "cluster-1".to_string(), HealthStatus::Healthy, now);
        store.save_state(&state1).await.unwrap();

        // Overwrite with different state
        let mut state2 = NodeManagerState::new();
        state2.process_heartbeat("node-2".to_string(), "cluster-2".to_string(), HealthStatus::Healthy, now);
        store.save_state(&state2).await.unwrap();

        let loaded = store.load_state().await.unwrap();
        assert_eq!(loaded.node_count(), 1);
        assert!(loaded.get_node("node-1").is_none());
        assert!(loaded.get_node("node-2").is_some());

        // Cleanup
        let _ = fs::remove_file(store.file_path()).await;
    }
}

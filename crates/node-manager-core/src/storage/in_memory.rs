//! In-memory state store for testing.

use crate::error::NodeManagerError;
use crate::model::NodeManagerState;
use crate::storage::StateStore;
use async_trait::async_trait;
use std::sync::{Arc, Mutex};

/// In-memory state store for testing purposes.
///
/// This implementation stores state in memory and is useful for tests
/// and scenarios where persistence is not required.
#[derive(Debug, Clone)]
pub struct InMemoryStore {
    /// The stored state, wrapped in Arc<Mutex> for interior mutability.
    state: Arc<Mutex<Option<NodeManagerState>>>,
}

impl InMemoryStore {
    /// Creates a new empty in-memory store.
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(None)),
        }
    }

    /// Sets the state directly (for testing).
    #[allow(clippy::expect_used)]
    pub fn set_state(&self, state: NodeManagerState) {
        let mut guard = self.state.lock().expect("InMemoryStore lock poisoned");
        *guard = Some(state);
    }

    /// Gets the current state (for testing).
    #[allow(clippy::expect_used)]
    pub fn get_state(&self) -> Option<NodeManagerState> {
        let guard = self.state.lock().expect("InMemoryStore lock poisoned");
        guard.clone()
    }
}

impl Default for InMemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StateStore for InMemoryStore {
    /// Loads state from memory.
    ///
    /// Returns an empty state if no state has been stored.
    #[allow(clippy::expect_used)]
    async fn load_state(&self) -> Result<NodeManagerState, NodeManagerError> {
        let guard = self.state.lock().expect("InMemoryStore lock poisoned");
        Ok(guard.clone().unwrap_or_default())
    }

    /// Saves state to memory.
    #[allow(clippy::expect_used)]
    async fn save_state(&self, state: &NodeManagerState) -> Result<(), NodeManagerError> {
        let mut guard = self.state.lock().expect("InMemoryStore lock poisoned");
        *guard = Some(state.clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::HealthStatus;
    use chrono::Utc;

    #[tokio::test]
    async fn test_in_memory_store_save_and_load() {
        let store = InMemoryStore::new();
        let now = Utc::now();

        // Load should return empty state initially
        let state = store.load_state().await.unwrap();
        assert!(state.is_empty());

        // Save a state with a node
        let mut state_to_save = NodeManagerState::new();
        state_to_save.process_heartbeat(
            "node-1".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Healthy,
            now,
        );

        store.save_state(&state_to_save).await.unwrap();

        // Load should return the saved state
        let loaded_state = store.load_state().await.unwrap();
        assert_eq!(loaded_state.cluster_count(), 1);
        assert_eq!(loaded_state.node_count(), 1);
        assert!(loaded_state.get_node("node-1").is_some());
    }

    #[tokio::test]
    async fn test_in_memory_store_set_and_get() {
        let store = InMemoryStore::new();
        let now = Utc::now();

        let mut state = NodeManagerState::new();
        state.process_heartbeat(
            "node-1".to_string(),
            "cluster-1".to_string(),
            HealthStatus::Healthy,
            now,
        );

        store.set_state(state.clone());

        let loaded = store.get_state().unwrap();
        assert_eq!(loaded.cluster_count(), 1);
    }

    #[tokio::test]
    async fn test_in_memory_store_default() {
        let store = InMemoryStore::default();
        let state = store.load_state().await.unwrap();
        assert!(state.is_empty());
    }
}

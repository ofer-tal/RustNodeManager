//! Async trait for state persistence.

use async_trait::async_trait;
use crate::{error::NodeManagerError, model::NodeManagerState};

/// Marker string written to JSON file to represent empty state.
///
/// This allows distinguishing between:
/// - File doesn't exist → empty state
/// - File exists with this marker → empty state was explicitly saved
pub const EMPTY_STATE_MARKER: &str = "__EMPTY_STATE__";

/// Async trait for persisting and loading Node Manager state.
///
/// Implementations must be thread-safe (Send + Sync) to allow
/// concurrent access from the heartbeat RPC and background monitor.
#[async_trait]
pub trait StateStore: Send + Sync {
    /// Loads the current state from storage.
    ///
    /// Returns an empty state if no persisted state exists.
    async fn load_state(&self) -> Result<NodeManagerState, NodeManagerError>;

    /// Persists the given state to storage.
    ///
    /// Must be atomic - either fully succeeds or fully fails.
    async fn save_state(&self, state: &NodeManagerState) -> Result<(), NodeManagerError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_state_marker_defined() {
        assert!(!EMPTY_STATE_MARKER.is_empty());
        assert_eq!(EMPTY_STATE_MARKER, "__EMPTY_STATE__");
    }
}

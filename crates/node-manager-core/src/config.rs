//! Configuration structures for the Node Manager.
//!
//! Configuration is loaded from layered sources (default YAML file + environment overrides).
//! All configuration has sensible defaults.

use crate::error::NodeManagerError;
use serde::Deserialize;

/// Main application configuration.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct AppConfig {
    /// Server network configuration.
    #[serde(default)]
    pub server: ServerConfig,
    /// Heartbeat monitoring configuration.
    #[serde(default)]
    pub heartbeat: HeartbeatConfig,
    /// Role assignment configuration.
    #[serde(default)]
    pub roles: RolesConfig,
    /// Storage backend configuration.
    #[serde(default)]
    pub storage: StorageConfig,
}

impl AppConfig {
    /// Validates that configuration values are within acceptable bounds.
    ///
    /// # Errors
    ///
    /// Returns `NodeManagerError::InvalidConfig` if any value is invalid.
    pub fn validate(&self) -> Result<(), NodeManagerError> {
        if self.roles.metadata_count == 0 {
            return Err(NodeManagerError::InvalidConfig(
                "metadata_count must be > 0".to_string(),
            ));
        }
        if self.roles.storage_max_percent > 100 {
            return Err(NodeManagerError::InvalidConfig(
                "storage_max_percent must be <= 100".to_string(),
            ));
        }
        if self.roles.lease_duration_secs == 0 {
            return Err(NodeManagerError::InvalidConfig(
                "lease_duration_secs must be > 0".to_string(),
            ));
        }
        if self.heartbeat.check_interval_secs == 0 {
            return Err(NodeManagerError::InvalidConfig(
                "check_interval_secs must be > 0".to_string(),
            ));
        }
        if self.heartbeat.missed_threshold == 0 {
            return Err(NodeManagerError::InvalidConfig(
                "missed_threshold must be > 0".to_string(),
            ));
        }
        Ok(())
    }
}

/// Server network binding configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    /// Address to bind the gRPC server (e.g., "0.0.0.0:50051").
    #[serde(default = "default_bind_address")]
    pub bind_address: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_address: default_bind_address(),
        }
    }
}

fn default_bind_address() -> String {
    "0.0.0.0:50051".to_string()
}

/// Heartbeat monitoring configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct HeartbeatConfig {
    /// Interval in seconds between heartbeat checks.
    #[serde(default = "default_check_interval")]
    pub check_interval_secs: u64,
    /// Number of missed heartbeats before marking as Unassignable.
    #[serde(default = "default_missed_threshold")]
    pub missed_threshold: u32,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            check_interval_secs: default_check_interval(),
            missed_threshold: default_missed_threshold(),
        }
    }
}

fn default_check_interval() -> u64 {
    10
}

fn default_missed_threshold() -> u32 {
    3
}

/// Role assignment configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct RolesConfig {
    /// Number of nodes that should hold the Metadata role.
    #[serde(default = "default_metadata_count")]
    pub metadata_count: usize,
    /// Minimum number of nodes that must hold the Storage role.
    #[serde(default = "default_storage_min")]
    pub storage_min_count: usize,
    /// Maximum percentage of assignable nodes that can hold Storage role.
    #[serde(default = "default_storage_max_percent")]
    pub storage_max_percent: usize,
    /// Duration in seconds for role leases before renewal required.
    #[serde(default = "default_lease_duration")]
    pub lease_duration_secs: u64,
}

impl Default for RolesConfig {
    fn default() -> Self {
        Self {
            metadata_count: default_metadata_count(),
            storage_min_count: default_storage_min(),
            storage_max_percent: default_storage_max_percent(),
            lease_duration_secs: default_lease_duration(),
        }
    }
}

fn default_metadata_count() -> usize {
    1
}

fn default_storage_min() -> usize {
    1
}

fn default_storage_max_percent() -> usize {
    30
}

fn default_lease_duration() -> u64 {
    60
}

/// Storage backend configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct StorageConfig {
    /// Type of storage backend ("json_file" is the initial implementation).
    #[serde(default = "default_backend")]
    pub backend: String,
    /// Path for JSON file storage.
    #[serde(default = "default_json_file_path")]
    pub json_file_path: String,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            backend: default_backend(),
            json_file_path: default_json_file_path(),
        }
    }
}

fn default_backend() -> String {
    "json_file".to_string()
}

fn default_json_file_path() -> String {
    "./data/node_manager_state.json".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = AppConfig::default();
        assert_eq!(config.server.bind_address, "0.0.0.0:50051");
        assert_eq!(config.heartbeat.check_interval_secs, 10);
        assert_eq!(config.heartbeat.missed_threshold, 3);
        assert_eq!(config.roles.metadata_count, 1);
        assert_eq!(config.roles.storage_min_count, 1);
        assert_eq!(config.roles.storage_max_percent, 30);
        assert_eq!(config.roles.lease_duration_secs, 60);
        assert_eq!(config.storage.backend, "json_file");
    }

    #[test]
    fn test_partial_deserialize() {
        let yaml = r#"
          server:
            bind_address: "127.0.0.1:8080"
        "#;
        let config: AppConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.server.bind_address, "127.0.0.1:8080");
        // Verify defaults applied
        assert_eq!(config.heartbeat.check_interval_secs, 10);
        assert_eq!(config.roles.metadata_count, 1);
    }

    #[test]
    fn test_validate_default_config() {
        let config = AppConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_rejects_zero_metadata_count() {
        let mut config = AppConfig::default();
        config.roles.metadata_count = 0;
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("metadata_count"));
    }

    #[test]
    fn test_validate_rejects_zero_lease_duration() {
        let mut config = AppConfig::default();
        config.roles.lease_duration_secs = 0;
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("lease_duration_secs"));
    }

    #[test]
    fn test_validate_rejects_over_100_percent_storage() {
        let mut config = AppConfig::default();
        config.roles.storage_max_percent = 150;
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("storage_max_percent"));
    }

    #[test]
    fn test_validate_rejects_zero_check_interval() {
        let mut config = AppConfig::default();
        config.heartbeat.check_interval_secs = 0;
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("check_interval_secs"));
    }
}

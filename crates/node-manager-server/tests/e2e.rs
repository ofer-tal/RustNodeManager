//! End-to-end tests for the Node Manager.
//!
//! These tests start the actual server binary and use the CLI tool
//! to interact with it, verifying the full system integration.

use std::process::Stdio;
use std::time::Duration;
use tokio::process::{Child, Command};

/// Path to the server binary (built via cargo).
fn server_bin() -> String {
    let suffix = if cfg!(windows) { ".exe" } else { "" };
    let target_dir = std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| {
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
        format!("{}/../../target", manifest_dir)
    });
    format!("{}/debug/node-manager-server{}", target_dir, suffix)
}

/// Path to the CLI binary (built via cargo).
fn cli_bin() -> String {
    let suffix = if cfg!(windows) { ".exe" } else { "" };
    let target_dir = std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| {
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
        format!("{}/../../target", manifest_dir)
    });
    format!("{}/debug/nm-cli{}", target_dir, suffix)
}

/// Convert a path to forward slashes for safe embedding in YAML strings.
fn to_forward_slashes(path: &std::path::Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

/// Finds a free TCP port on localhost.
async fn find_free_port() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().port()
}

/// Server handle that kills the process on drop.
struct ServerGuard {
    child: Child,
    _temp_dir: tempfile::TempDir,
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        // Kill the server process to prevent orphaned processes after tests
        let _ = self.child.start_kill();
    }
}

/// Starts the server binary on a random port with a temp config file.
/// Returns the (address, ServerGuard) for cleanup.
async fn start_server_binary(port: u16) -> (String, ServerGuard) {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let config_path = temp_dir.path().join("config.yaml");
    let state_path = temp_dir.path().join("state.json");

    // Use forward slashes in the YAML to avoid escape issues on Windows
    let state_path_str = to_forward_slashes(&state_path);

    let config_content = format!(
        r#"server:
  bind_address: "127.0.0.1:{port}"
heartbeat:
  check_interval_secs: 5
  missed_threshold: 3
roles:
  metadata_count: 1
  storage_min_count: 1
  storage_max_percent: 30
  lease_duration_secs: 60
storage:
  backend: json_file
  json_file_path: "{state_path}"
"#,
        port = port,
        state_path = state_path_str,
    );
    std::fs::write(&config_path, config_content).unwrap();

    let mut child = Command::new(server_bin())
        .arg("--config")
        .arg(&config_path)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    // Wait for the server to become ready by polling the TCP port
    let tcp_addr = format!("127.0.0.1:{}", port);
    let mut ready = false;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if tokio::net::TcpStream::connect(&tcp_addr).await.is_ok() {
            ready = true;
            break;
        }
    }

    if !ready {
        // Try to read stderr for diagnostics
        let _ = child.kill().await;
        panic!("Server failed to start on port {} within 5 seconds", port);
    }

    let addr = format!("http://127.0.0.1:{}", port);
    (
        addr,
        ServerGuard {
            child,
            _temp_dir: temp_dir,
        },
    )
}

/// Runs the CLI with the given arguments and returns stdout.
async fn run_cli(args: &[&str]) -> String {
    let output = Command::new(cli_bin())
        .args(args)
        .output()
        .await
        .unwrap();
    String::from_utf8_lossy(&output.stdout).to_string()
}

#[tokio::test]
async fn test_cli_heartbeat_creates_node() {
    let port = find_free_port().await;
    let (addr, _guard) = start_server_binary(port).await;

    let output = run_cli(&[
        "--server", &addr,
        "heartbeat",
        "--node-id", "test-node-1",
        "--cluster-id", "test-cluster-1",
        "--health", "healthy",
    ]).await;

    assert!(output.contains("ASSIGNABLE"),
        "Expected ASSIGNABLE status in output, got: {}", output);
}

#[tokio::test]
async fn test_cli_status_shows_cluster() {
    let port = find_free_port().await;
    let (addr, _guard) = start_server_binary(port).await;

    run_cli(&[
        "--server", &addr,
        "heartbeat",
        "--node-id", "e2e-node-1",
        "--cluster-id", "e2e-cluster-1",
        "--health", "healthy",
    ]).await;

    let output = run_cli(&[
        "--server", &addr,
        "status",
    ]).await;

    assert!(output.contains("e2e-cluster-1"),
        "Expected cluster ID in status output, got: {}", output);
    assert!(output.contains("e2e-node-1"),
        "Expected node ID in status output, got: {}", output);
}

#[tokio::test]
async fn test_cli_status_filters_by_cluster() {
    let port = find_free_port().await;
    let (addr, _guard) = start_server_binary(port).await;

    run_cli(&[
        "--server", &addr,
        "heartbeat",
        "--node-id", "node-a",
        "--cluster-id", "cluster-alpha",
        "--health", "healthy",
    ]).await;

    run_cli(&[
        "--server", &addr,
        "heartbeat",
        "--node-id", "node-b",
        "--cluster-id", "cluster-beta",
        "--health", "healthy",
    ]).await;

    let output = run_cli(&[
        "--server", &addr,
        "status",
        "--cluster-id", "cluster-alpha",
    ]).await;

    assert!(output.contains("cluster-alpha"),
        "Expected cluster-alpha in filtered output, got: {}", output);
    assert!(!output.contains("cluster-beta"),
        "Did not expect cluster-beta in filtered output, got: {}", output);
}

#[tokio::test]
async fn test_cli_unhealthy_heartbeat() {
    let port = find_free_port().await;
    let (addr, _guard) = start_server_binary(port).await;

    let output = run_cli(&[
        "--server", &addr,
        "heartbeat",
        "--node-id", "sick-node",
        "--cluster-id", "cluster-1",
        "--health", "unhealthy",
    ]).await;

    assert!(output.contains("UNASSIGNABLE"),
        "Expected UNASSIGNABLE status, got: {}", output);
}

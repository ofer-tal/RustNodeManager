# Rust Node Manager

A Rust-based Node Manager service for the control plane of a highly distributed data platform. The Node Manager monitors heartbeats from data nodes across hundreds or thousands of clusters and manages role assignments (metadata, data, storage) using leases and fencing tokens.

## Prerequisites

- Rust 1.94+ (edition 2021, latest stable recommended)
- Protobuf compiler (optional — handled by `tonic-prost-build`)

## Building

```bash
# Build all crates
cargo build --workspace

# Build in release mode
cargo build --workspace --release
```

## Running the Server

```bash
# Run with default configuration (listens on [::1]:50051)
cargo run -p node-manager-server

# Run with custom RUST_LOG level
RUST_LOG=node_manager_server=debug cargo run -p node-manager-server
```

The server reads configuration from `config/default.yaml`. See [architecture.md](architecture.md) for configuration details.

## CLI Tool (nm-cli)

The CLI tool interacts with a running Node Manager server via gRPC.

### Send a Heartbeat

```bash
# Send a healthy heartbeat
cargo run -p cli -- heartbeat \
  --node-id 550e8400-e29b-41d4-a716-446655440000 \
  --cluster-id 6ba7b810-9dad-11d1-80b4-00c04fd430c8 \
  --health healthy

# Send an unhealthy heartbeat
cargo run -p cli -- heartbeat \
  --node-id 550e8400-e29b-41d4-a716-446655440000 \
  --cluster-id 6ba7b810-9dad-11d1-80b4-00c04fd430c8 \
  --health unhealthy

# Specify a different server
cargo run -p cli -- heartbeat \
  --node-id 550e8400-e29b-41d4-a716-446655440000 \
  --cluster-id 6ba7b810-9dad-11d1-80b4-00c04fd430c8 \
  --server http://localhost:50051
```

### Query Status

```bash
# Get status of all clusters
cargo run -p cli -- status

# Filter by cluster
cargo run -p cli -- status --cluster-id 6ba7b810-9dad-11d1-80b4-00c04fd430c8
```

### Simulate a Cluster

```bash
# Simulate 5 nodes sending heartbeats every 10 seconds
cargo run -p cli -- simulate \
  --cluster-id 6ba7b810-9dad-11d1-80b4-00c04fd430c8 \
  --nodes 5 \
  --interval-secs 10

# Press Ctrl+C to stop the simulation
```

## Testing

```bash
# Run all tests (unit + integration)
cargo test --workspace

# Run only unit tests for a specific crate
cargo test -p node-manager-core

# Run integration tests
cargo test -p node-manager-server --test integration

# Run end-to-end tests
cargo test --test e2e

# Run with output
cargo test --workspace -- --nocapture

# Run clippy (zero warnings required)
cargo clippy --workspace -- -D warnings
```

## Configuration

Default configuration is in `config/default.yaml`:

```yaml
server:
  bind_address: "[::1]:50051"     # gRPC listen address

heartbeat:
  check_interval_secs: 15         # Seconds between heartbeat checks
  missed_threshold: 3             # Missed heartbeats before node is unassignable

roles:
  metadata_count: 1               # Metadata nodes per cluster (exactly N)
  storage_min_count: 1            # Minimum storage nodes
  storage_max_percent: 30         # Max storage nodes as % of assignable
  lease_duration_secs: 60         # Role lease duration

storage:
  backend: "json_file"            # Storage backend
  json_file_path: "data/state.json"
```

## Architecture

See [architecture.md](architecture.md) for the full architecture document covering:
- System overview and component interactions
- Data flow and heartbeat processing
- Node state machine (Assignable ↔ Suspect → Unassignable)
- Role assignment rules and fencing tokens
- Lease management
- Concurrency model
- Storage abstraction

## Project Structure

```
RustNodeManager/
├── Cargo.toml                    # Workspace root
├── proto/node_manager.proto      # gRPC service definition
├── config/default.yaml           # Default configuration
├── crates/
│   ├── proto-gen/                # Generated protobuf types
│   ├── node-manager-core/        # Domain model + business logic
│   ├── node-manager-server/      # gRPC service + server binary
│   └── cli/                      # CLI tool (nm-cli)
├── tests/e2e.rs                  # End-to-end tests
├── CLAUDE.md                     # Coding standards
├── architecture.md               # Architecture document
└── readme.md                     # This file
```

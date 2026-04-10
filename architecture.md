# Architecture — Rust Node Manager

## 1. System Overview

The Node Manager is a control-plane service responsible for monitoring data nodes across a distributed data platform and managing their role assignments. It operates within a larger system that includes a Cluster Manager (which provisions cloud resources) and the Data Plane (clusters of compute nodes).

```
┌─────────────────────────────────────────────────────────┐
│                    Control Plane                         │
│                                                          │
│  ┌──────────────────┐     ┌──────────────────────────┐  │
│  │  Cluster Manager  │     │     Node Manager         │  │
│  │  (cloud resources)│◄────│  (heartbeat + roles)     │  │
│  └──────────────────┘     └──────────┬───────────────┘  │
│                                      │                   │
└──────────────────────────────────────┼───────────────────┘
                                       │ gRPC
                    ┌──────────────────┼──────────────────┐
                    │                  │                   │
              ┌─────▼─────┐    ┌──────▼──────┐    ┌──────▼──────┐
              │ Cluster A  │    │ Cluster B   │    │ Cluster C   │
              │ ┌─Node─┐   │    │ ┌─Node─┐   │    │ ┌─Node─┐   │
              │ ┌─Node─┐   │    │ ┌─Node─┐   │    │ ┌─Node─┐   │
              │ ┌─Node─┐   │    │ ┌─Node─┐   │    │             │
              └────────────┘    └─────────────┘    └─────────────┘
```

**Scale:** The system is designed for hundreds to thousands of clusters, each with multiple data nodes.

## 2. Node Manager Responsibilities

1. **Heartbeat Monitoring:** Receive periodic heartbeat messages from all data nodes via gRPC
2. **Node Discovery:** Automatically discover and register new nodes when they send their first heartbeat
3. **Status Tracking:** Track each node's status (Assignable, Suspect, Unassignable) based on heartbeat regularity and self-reported health
4. **Role Assignment:** Assign and maintain roles (metadata, data, storage) for all nodes in each cluster based on configurable rules
5. **Lease Management:** Issue role leases with fencing tokens to prevent split-brain data corruption
6. **Failure Detection:** Detect missing heartbeats via a background timer and transition nodes through status states

## 3. Data Flow

### 3.1 Heartbeat Processing Flow

```
Data Node                         Node Manager
   │                                  │
   │── HeartbeatRequest ─────────────►│
   │   (node_id, cluster_id,          │  1. Register/update node
   │    health_status)                │  2. Update node status
   │                                  │  3. Compute role assignments
   │                                  │  4. Extend existing leases
   │                                  │  5. Persist state
   │◄── HeartbeatResponse ────────────│
   │   (node_status,                  │
   │    assigned_roles with leases,   │
   │    all cluster peers)            │
```

### 3.2 Failure Detection Flow

```
Background Timer (every N seconds)
   │
   ├── For each node:
   │     ├── Check elapsed time since last heartbeat
   │     ├── If missed 1 interval:     → set SUSPECT
   │     ├── If missed ≥ threshold:    → set UNASSIGNABLE
   │     └── If status changed:        → recompute role assignments
   │
   └── Persist state if any changes occurred
```

## 4. Node State Machine

```
                         Heartbeat (healthy)
    ┌──────────┐ ◄───────────────────────────── ┌──────────────┐
    │          │                                 │              │
    │ Assignable│                                │  Unassignable │
    │          │ ──── HealthStatus::Unhealthy ──►│              │
    │          │ ──── HealthStatus::Draining ───►│              │
    │          │ ──── Missed ≥ threshold ───────►│              │
    └────┬─────┘                                 └──────────────┘
         │                                             │
         │ Missed 1 heartbeat                          │ Heartbeat (healthy)
         │ (< threshold)                               │
         ▼                                             │
    ┌──────────┐                                       │
    │          │ ──── Missed ≥ threshold ──────────────►│
    │  Suspect  │                                      │
    │          │ ◄─── Heartbeat (healthy) ──────────────┘
    └──────────┘     (recovery to Assignable)
```

**Status meanings:**
- **Assignable:** Node is alive, healthy, and may receive new role assignments
- **Suspect:** Node missed at least one heartbeat — existing roles are preserved but no new roles are assigned
- **Unassignable:** Node is unhealthy, draining, or has exceeded the missed heartbeat threshold — all roles are revoked and may be reassigned to other nodes

## 5. Role Assignment Rules

Each cluster has its roles managed independently. The rules are configurable via YAML.

### 5.1 Metadata Role
- **Rule:** Exactly 1 node per cluster (configurable: `metadata_count`)
- **Assignment:** If no assignable node holds the metadata role, the assignable node with the lowest `node_id` (lexicographic) is selected — this ensures deterministic selection
- **Reassignment:** If the metadata holder becomes unassignable, the role is immediately reassigned to another assignable node
- **Fencing token:** A new, monotonically increasing fencing token is issued every time the metadata role is assigned to a node. This prevents split-brain scenarios where a previous holder might still believe it holds the role

### 5.2 Data Role
- **Rule:** 100% of assignable nodes in the cluster
- **Assignment:** Every assignable node receives the data role
- **Revocation:** Removed when a node becomes unassignable

### 5.3 Storage Role
- **Rule:** At least 1 node, at most N% of assignable nodes (default: 30%)
- **Assignment:** If fewer than `storage_min_count` nodes have the storage role, additional nodes are selected (sorted by `node_id` for determinism)
- **Revocation:** If more than `max(storage_min_count, assignable_count * storage_max_percent / 100)` nodes have storage, the role is revoked from nodes with the highest `node_id`
- **Cap formula:** `max(storage_min_count, floor(assignable_count * storage_max_percent / 100))`

### 5.4 Role Assignment Examples

| Nodes | Metadata | Data | Storage (30%) |
|-------|----------|------|---------------|
| 1     | 1        | 1    | 1             |
| 3     | 1        | 3    | 1             |
| 5     | 1        | 5    | 1             |
| 10    | 1        | 10   | 3             |
| 20    | 1        | 20   | 6             |
| 100   | 1        | 100  | 30            |

## 6. Lease and Fencing Token Lifecycle

### 6.1 Role Leases

Each role assignment comes with a lease:
- **Fencing token:** Monotonically increasing u64, unique per metadata role assignment
- **Granted at:** Timestamp when the lease was created or last extended
- **Lease duration:** Configurable (default: 60 seconds)
- **Expiration:** `granted_at + lease_duration`

### 6.2 Lease Extension

Leases are automatically extended every time a node sends a heartbeat:
- The `granted_at` timestamp is updated to the current time
- The fencing token remains the same (only changes on role transfer)
- If a node stops sending heartbeats, its leases will eventually expire

### 6.3 Fencing Token Guarantees

- Tokens are monotonically increasing (via `AtomicU64`)
- Only the metadata role uses fencing tokens (to prevent split-brain metadata writes)
- When metadata transfers from node A to node B, node B receives a higher token than node A had
- Data nodes use the fencing token to reject stale writes from nodes with lower tokens

## 7. gRPC API

### 7.1 Heartbeat RPC

```
rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse)
```

**Request:**
| Field | Type | Description |
|-------|------|-------------|
| node_id | string (UUID) | Unique identifier of the sending node |
| cluster_id | string (UUID) | Cluster this node belongs to |
| health_status | enum | Self-reported health: HEALTHY, UNHEALTHY, DRAINING |

**Response:**
| Field | Type | Description |
|-------|------|-------------|
| node_status | enum | Current assignability: ASSIGNABLE, SUSPECT, UNASSIGNABLE |
| assigned_roles | RoleLease[] | All roles assigned to this node, with fencing tokens and lease expiration |
| cluster_nodes | PeerNodeInfo[] | All nodes in the same cluster with their status and roles |

### 7.2 GetStatus RPC

```
rpc GetStatus(StatusRequest) returns (StatusResponse)
```

**Request:**
| Field | Type | Description |
|-------|------|-------------|
| cluster_id | string (optional) | Filter by cluster; omit for all clusters |

**Response:**
| Field | Type | Description |
|-------|------|-------------|
| clusters | ClusterStatus[] | One entry per cluster, each containing all nodes with status and roles |

## 8. State Storage

### 8.1 Abstraction Layer

```rust
trait StateStore: Send + Sync {
    async fn load_state(&self) -> Result<NodeManagerState>;
    async fn save_state(&self, state: &NodeManagerState) -> Result<()>;
}
```

### 8.2 Implementations

| Backend | Use Case | Description |
|---------|----------|-------------|
| `JsonFileStore` | Local development | Reads/writes a JSON file with atomic writes (temp + rename) |
| Future: `EtcdStore` | Production | Distributed key-value store with strong consistency |
| Future: `DynamoDbStore` | Production (AWS) | Managed NoSQL database |

### 8.3 JSON File Store Details

- **Path:** Configurable via `storage.json_file_path` (default: `data/state.json`)
- **Atomic writes:** State is written to a temporary file, then renamed to the target path
- **Missing file:** If the file doesn't exist, an empty state is returned
- **Not suitable for multi-instance deployment:** Use a distributed store for production

## 9. Concurrency Model

### 9.1 Shared State

```rust
Arc<RwLock<SharedState>>
```

- **SharedState** contains the `HeartbeatProcessor` (which holds `NodeManagerState` + `RoleAssignmentEngine`), `HeartbeatMonitor`, and `StateStore`
- The `RwLock` allows concurrent reads (status queries) with exclusive writes (heartbeats, monitor checks)

### 9.2 Lock Acquisition Patterns

| Operation | Lock Type | Hold Duration |
|-----------|-----------|---------------|
| Heartbeat RPC | Write | In-memory processing only (~microseconds) |
| Status RPC | Read | Snapshot creation only |
| Monitor timer | Write | In-memory check only |

### 9.3 Persistence Strategy

State is persisted **after** releasing the write lock to minimize contention:
1. Acquire write lock → process heartbeat → clone state snapshot → release lock
2. Spawn fire-and-forget task to persist the snapshot
3. Persistence failures are logged but do not block the response

This means in a crash, the most recent heartbeat may be lost. The system recovers from the last persisted snapshot on restart.

## 10. Configuration

Configuration is loaded from `config/default.yaml` with environment variable overrides.

```yaml
server:
  bind_address: "[::1]:50051"     # gRPC listen address

heartbeat:
  check_interval_secs: 15         # How often to check for missed heartbeats
  missed_threshold: 3             # Heartbeats missed before node is Unassignable

roles:
  metadata_count: 1               # Exact number of metadata nodes per cluster
  storage_min_count: 1            # Minimum storage nodes per cluster
  storage_max_percent: 30         # Maximum storage nodes as % of assignable nodes
  lease_duration_secs: 60         # Duration of role leases

storage:
  backend: "json_file"            # Storage backend type
  json_file_path: "data/state.json"  # Path for JSON file storage
```

## 11. CLI Tool (nm-cli)

The CLI tool (`crates/cli/`) provides three commands for testing and interaction:

| Command | Description |
|---------|-------------|
| `heartbeat` | Send a single heartbeat and print the response |
| `status` | Query current cluster/node status and print as JSON |
| `simulate` | Simulate N nodes sending periodic heartbeats |

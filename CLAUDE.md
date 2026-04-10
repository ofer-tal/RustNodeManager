# CLAUDE.md — Rust Node Manager Project

## Project Overview

The Node Manager is a Rust service that monitors heartbeats from data nodes across a distributed data platform and manages role assignments (metadata, data, storage) using leases and fencing tokens. It communicates via gRPC and uses an abstracted state storage layer (local JSON files for development, swappable to etcd/DynamoDB for production).

## Build & Run Commands

```bash
# Build everything
cargo build --workspace

# Run all tests
cargo test --workspace

# Run linter (zero warnings required)
cargo clippy --workspace -- -D warnings

# Run the server
cargo run -p node-manager-server

# CLI: Send a heartbeat
cargo run -p cli -- heartbeat --node-id <uuid> --cluster-id <uuid> --health healthy

# CLI: Query status
cargo run -p cli -- status

# CLI: Simulate a cluster of nodes
cargo run -p cli -- simulate --cluster-id <uuid> --nodes 5 --interval-secs 10
```

## Workspace Structure

```
crates/
  proto-gen/           — Generated protobuf types (tonic + prost)
  node-manager-core/   — Domain model, business logic (ZERO gRPC dependencies)
  node-manager-server/ — gRPC service implementation + server binary
  cli/                 — CLI tool (nm-cli) for testing and interaction
proto/
  node_manager.proto   — gRPC service definition
config/
  default.yaml         — Default configuration
tests/
  e2e.rs               — End-to-end tests
```

**Dependency flow:** `server -> core <- (no tonic deps)` | `cli -> proto-gen` | `server -> proto-gen`

## Coding Standards

### Readability & Clarity
- Use clear, descriptive names for variables, functions, structs, and enums
- Follow Rust naming conventions: `snake_case` for functions/variables, `PascalCase` for types
- Keep functions small and focused — prefer many small functions over long monolithic ones
- Avoid deep nesting; use early returns and guard clauses
- Write code that a human can read and understand without extensive comments

### Comments
- Add inline comments to explain **WHY** something is done, not **WHAT** the code does
- The code itself should be self-documenting through clear naming
- Add doc comments (`///`) on public APIs explaining purpose and behavior
- Do NOT over-comment — trust the reader to understand Rust syntax

### Error Handling
- All errors MUST be logged via `tracing` macros (`tracing::error!`, `tracing::warn!`)
- NEVER silently discard errors — every `Result` must be handled explicitly
- Use `thiserror` for domain error types with descriptive messages
- Use `?` operator for error propagation; do not use `unwrap()` or `expect()` in production code
- At system boundaries (gRPC handlers), convert domain errors to appropriate tonic::Status codes

### Code Reuse
- Extract common logic into shared functions — do not copy-paste code
- Prefer composition over duplication
- If you find yourself writing similar code in two places, extract it into a helper function
- Use Rust's trait system to enable polymorphism where appropriate

### Use Well-Maintained Crates
- Do NOT reinvent the wheel — use established, well-tested crates
- `tokio` for async runtime, `tonic` for gRPC, `serde` for serialization, `tracing` for logging
- `prost` for protobuf, `chrono` for timestamps, `uuid` for UUIDs, `clap` for CLI
- `thiserror` for error types, `async-trait` for async traits, `config` for YAML config

### Testing Requirements
- Every task MUST include unit tests — code is not deliverable until tests pass
- Unit tests go in `#[cfg(test)]` modules within the source file
- Integration tests go in `crates/node-manager-server/tests/`
- E2E tests go in `tests/e2e.rs`
- All tests must pass: `cargo test --workspace`
- Clippy must be clean: `cargo clippy --workspace -- -D warnings`

### Documentation Maintenance
- After finishing every task, update `architecture.md` with any spec/feature changes
- Keep `readme.md` updated with installation, usage, and testing procedures
- Keep this file (CLAUDE.md) updated with any new build commands or conventions

## Project Guardrails

- **Only edit files within** `C:\Projects\TempProjects\RustNodeManager\`
- Do not modify files outside the project directory
- Do not commit changes unless explicitly asked
- Do not push to any remote repository

## Architecture Reference

See `architecture.md` for the full architecture document covering data flow, state machines, role assignment rules, lease management, and concurrency model.

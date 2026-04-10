//! Heartbeat monitoring module.
//!
//! The heartbeat monitor runs on a timer to detect nodes that have stopped
//! sending heartbeats and updates their status accordingly.

pub mod heartbeat_monitor;

pub use heartbeat_monitor::HeartbeatMonitor;

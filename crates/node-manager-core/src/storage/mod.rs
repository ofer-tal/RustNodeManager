//! State persistence abstraction.
//!
//! Provides a trait-based storage layer that can be implemented for different backends.
//! The initial implementation uses JSON file storage with atomic writes.

pub mod json_file;
pub mod in_memory;
pub mod trait_;

pub use json_file::JsonFileStore;
pub use in_memory::InMemoryStore;
pub use trait_::{StateStore, EMPTY_STATE_MARKER};

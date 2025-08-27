//! # rskv: A High-Performance Key-Value Store in Rust
//!
//! `rskv` is a high-performance, concurrent, persistent key-value store inspired by
//! Microsoft's FASTER. It leverages modern Rust features for safety and performance.
//!
//! ## Core Features
//!
//! - **Hybrid Storage Engine**: Combines in-memory hot data with disk-backed log
//! - **Concurrent Hash Index**: Lock-free hash index for fast key lookups
//! - **Non-Blocking Checkpoints**: Consistent snapshots without pausing operations
//! - **Epoch-Based Garbage Collection**: Safe background space reclamation
//!
//! ## Example
//!
//! ```rust,ignore
//! use rskv::{RsKv, Config};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = Config::default();
//!     let kv_store = RsKv::new(config).await?;
//!     
//!     let key = b"hello".to_vec();
//!     let value = b"world".to_vec();
//!     
//!     kv_store.upsert(key.clone(), value).await?;
//!     let result = kv_store.read(&key).await?;
//!     
//!     println!("Value: {:?}", result);
//!     Ok(())
//! }
//! ```

#![allow(clippy::uninlined_format_args)]

pub mod background;
pub mod checkpoint;
pub mod common;
pub mod epoch;
pub mod gc;
pub mod hlog;
pub mod index;
pub mod metrics;
pub mod rskv;

// Re-export commonly used types
pub use background::{BackgroundTaskManager, BackgroundTaskStats};
pub use checkpoint::{CheckpointMetadata, CheckpointState, CheckpointStats};
pub use common::{Address, Config, Key, Result, RsKvError, Value};
pub use epoch::{EpochHandle, EpochManager, SharedEpochManager};
pub use gc::{GcConfig, GcEstimate, GcState, GcStats};
pub use metrics::{
    MetricsCollector, MetricsSnapshot, SharedMetricsCollector, new_shared_metrics_collector,
};
// Re-export main types
pub use rskv::{RsKv, RsKvStats};

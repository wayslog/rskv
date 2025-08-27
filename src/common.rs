//! Common types and error definitions for rskv
//! 
//! This module contains core data types and error handling used throughout the system.
//! Inspired by FASTER's address.h and common error handling patterns.

use thiserror::Error;
use serde::{Deserialize, Serialize};

/// Synchronization mode for durability vs performance trade-off
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncMode {
    /// No explicit sync - rely on OS page cache (fastest, least durable)
    None,
    /// Sync data to disk periodically (balanced)
    Periodic,
    /// Sync data after every write (slowest, most durable)
    Always,
    /// Sync only metadata, data can be cached (compromise)
    MetadataOnly,
}

/// Address type representing logical addresses in the hybrid log.
/// Follows FASTER's Address design with 48-bit addressing:
/// - 25 bits for offset within page (32MB page size)
/// - 23 bits for page index (supports ~8M pages)
/// - 16 bits reserved for hash table control bits
pub type Address = u64;

/// Key type for the key-value store.
/// Using Vec<u8> for maximum flexibility with different key types.
pub type Key = Vec<u8>;

/// Value type for the key-value store.
/// Using Vec<u8> for maximum flexibility with different value types.
pub type Value = Vec<u8>;

/// Page size constant - 32MB pages like FASTER
pub const PAGE_SIZE: u32 = 32 * 1024 * 1024; // 32MB

/// Address bit layout constants (matching FASTER's design)
pub const ADDRESS_BITS: u64 = 48;
pub const OFFSET_BITS: u64 = 25;
pub const PAGE_BITS: u64 = ADDRESS_BITS - OFFSET_BITS; // 23 bits
pub const MAX_OFFSET: u32 = ((1u32 << OFFSET_BITS) - 1) as u32;
pub const MAX_PAGE: u32 = ((1u32 << PAGE_BITS) - 1) as u32;
pub const INVALID_ADDRESS: Address = 1; // Matches FASTER's kInvalidAddress

/// Address utility functions
#[inline]
pub fn get_page(address: Address) -> u32 {
    ((address >> OFFSET_BITS) & ((1u64 << PAGE_BITS) - 1)) as u32
}

#[inline]
pub fn get_offset(address: Address) -> u32 {
    (address & ((1u64 << OFFSET_BITS) - 1)) as u32
}

#[inline]
pub fn make_address(page: u32, offset: u32) -> Address {
    ((page as u64) << OFFSET_BITS) | (offset as u64)
}

/// Error types for rskv operations
#[derive(Error, Debug)]
pub enum RsKvError {
    #[error("IO Error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Serialization Error: {0}")]
    Serialization(#[from] bincode::Error),
    
    /// Key not found in the store
    #[error("Key not found")]
    KeyNotFound,
    
    #[error("Address out of bounds: {address}")]
    AddressOutOfBounds { address: Address },
    
    #[error("Page not found: {page}")]
    PageNotFound { page: u32 },
    
    #[error("Allocation failed: size {size}")]
    AllocationFailed { size: u32 },
    
    #[error("Checkpoint operation failed: {message}")]
    CheckpointFailed { message: String },
    
    #[error("Recovery operation failed: {message}")]
    RecoveryFailed { message: String },
    
    #[error("Garbage collection failed: {message}")]
    GarbageCollectionFailed { message: String },
    
    #[error("Configuration error: {message}")]
    Configuration { message: String },
    
    /// Invalid configuration
    #[error("Invalid configuration: {message}")]
    InvalidConfig { message: String },
    
    /// Key is too large
    #[error("Key size {size} bytes exceeds maximum allowed size {max_size} bytes")]
    KeyTooLarge { size: usize, max_size: usize },
    
    /// Value is too large  
    #[error("Value size {size} bytes exceeds maximum allowed size {max_size} bytes")]
    ValueTooLarge { size: usize, max_size: usize },
    
    /// Storage device error
    #[error("Storage device error: {message}")]
    StorageError { message: String },
    
    /// Memory mapping error
    #[error("Memory mapping error: {message}")]
    MmapError { message: String },
    
    /// Data corruption detected
    #[error("Data corruption detected: {message}")]
    Corruption { message: String },
    
    /// Resource exhausted
    #[error("Resource exhausted: {resource}")]
    ResourceExhausted { resource: String },
    
    /// Operation timeout
    #[error("Operation timed out after {duration_ms} ms")]
    Timeout { duration_ms: u64 },
    
    /// Concurrent operation conflict
    #[error("Concurrent operation conflict: {message}")]
    Conflict { message: String },
    
    #[error("Internal error: {message}")]
    Internal { message: String },
}

impl RsKvError {
    /// Check if this error is recoverable
    pub fn is_recoverable(&self) -> bool {
        match self {
            RsKvError::Io(_) => true,
            RsKvError::Timeout { .. } => true,
            RsKvError::Conflict { .. } => true,
            RsKvError::ResourceExhausted { .. } => true,
            RsKvError::StorageError { .. } => true,
            RsKvError::MmapError { .. } => true,
            _ => false,
        }
    }
    
    /// Check if this error indicates data corruption
    pub fn is_corruption(&self) -> bool {
        matches!(self, RsKvError::Corruption { .. })
    }
    
    /// Check if this error is a user input error
    pub fn is_user_error(&self) -> bool {
        match self {
            RsKvError::KeyNotFound => true,
            RsKvError::KeyTooLarge { .. } => true,
            RsKvError::ValueTooLarge { .. } => true,
            RsKvError::InvalidConfig { .. } => true,
            RsKvError::Configuration { .. } => true,
            _ => false,
        }
    }
    
    /// Get error category for logging and metrics
    pub fn category(&self) -> &'static str {
        match self {
            RsKvError::Io(_) => "io",
            RsKvError::Serialization(_) => "serialization",
            RsKvError::AddressOutOfBounds { .. } => "addressing",
            RsKvError::PageNotFound { .. } => "addressing",
            RsKvError::AllocationFailed { .. } => "allocation",
            RsKvError::KeyNotFound => "not_found",
            RsKvError::KeyTooLarge { .. } | RsKvError::ValueTooLarge { .. } => "size_limit",
            RsKvError::CheckpointFailed { .. } => "checkpoint",
            RsKvError::RecoveryFailed { .. } => "recovery",
            RsKvError::GarbageCollectionFailed { .. } => "garbage_collection",
            RsKvError::Configuration { .. } | RsKvError::InvalidConfig { .. } => "configuration",
            RsKvError::StorageError { .. } => "storage",
            RsKvError::MmapError { .. } => "memory_mapping",
            RsKvError::Corruption { .. } => "corruption",
            RsKvError::ResourceExhausted { .. } => "resource_exhausted",
            RsKvError::Timeout { .. } => "timeout",
            RsKvError::Conflict { .. } => "conflict",
            RsKvError::Internal { .. } => "internal",
        }
    }
}

// Error conversion implementations
// Note: memmap2::Error is private, so we convert through std::io::Error

impl From<std::num::TryFromIntError> for RsKvError {
    fn from(err: std::num::TryFromIntError) -> Self {
        RsKvError::Internal {
            message: format!("Integer conversion error: {}", err),
        }
    }
}

/// Result type alias for rskv operations
pub type Result<T> = std::result::Result<T, RsKvError>;

/// Record header information (matches FASTER's RecordInfo)
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct RecordInfo {
    /// Previous address in the version chain
    pub previous_address: Address,
    /// Checkpoint version when this record was created
    pub checkpoint_version: u16,
    /// Whether this record is marked as invalid
    pub invalid: bool,
    /// Whether this is a tombstone (deleted) record
    pub tombstone: bool,
    /// Whether this is the final record in a version chain
    pub final_bit: bool,
}

impl RecordInfo {
    pub fn new(
        previous_address: Address,
        checkpoint_version: u16,
        final_bit: bool,
        tombstone: bool,
        invalid: bool,
    ) -> Self {
        Self {
            previous_address,
            checkpoint_version,
            invalid,
            tombstone,
            final_bit,
        }
    }

    pub fn is_null(&self) -> bool {
        self.previous_address == 0 && self.checkpoint_version == 0
    }
}

/// Configuration for rskv instance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Size of the hybrid log in memory (in bytes)
    pub memory_size: u64,
    /// Page size for the hybrid log
    pub page_size: u32,
    /// Directory for storing persistent data
    pub storage_dir: String,
    /// Whether to enable checkpointing
    pub enable_checkpointing: bool,
    /// Checkpoint interval in milliseconds
    pub checkpoint_interval_ms: u64,
    /// Whether to enable garbage collection
    pub enable_gc: bool,
    /// GC interval in milliseconds
    pub gc_interval_ms: u64,
    /// Maximum number of background threads
    pub max_background_threads: usize,
    /// Use memory mapping for storage devices
    pub use_mmap: bool,
    /// Enable read-ahead prefetching  
    pub enable_readahead: bool,
    /// Read-ahead buffer size in bytes
    pub readahead_size: usize,
    /// Enable write batching for better performance
    pub enable_write_batching: bool,
    /// Write batch size in bytes
    pub write_batch_size: usize,
    /// Enable compression for log data
    pub enable_compression: bool,
    /// Sync mode for durability vs performance trade-off
    pub sync_mode: SyncMode,
    /// Pre-allocate log file space
    pub preallocate_log: bool,
    /// Log file preallocation size in bytes
    pub log_prealloc_size: u64,
}

impl Config {
    /// Validate the configuration parameters
    pub fn validate(&self) -> Result<()> {
        // Memory size validation
        if self.memory_size < 1024 * 1024 {
            return Err(RsKvError::InvalidConfig {
                message: "Memory size must be at least 1MB".to_string(),
            });
        }
        
        if self.memory_size > 64 * 1024 * 1024 * 1024 {
            return Err(RsKvError::InvalidConfig {
                message: "Memory size cannot exceed 64GB".to_string(),
            });
        }
        
        // Page size validation
        if self.page_size < 4096 {
            return Err(RsKvError::InvalidConfig {
                message: "Page size must be at least 4KB".to_string(),
            });
        }
        
        if !self.page_size.is_power_of_two() {
            return Err(RsKvError::InvalidConfig {
                message: "Page size must be a power of 2".to_string(),
            });
        }
        
        if u64::from(self.page_size) > self.memory_size {
            return Err(RsKvError::InvalidConfig {
                message: "Page size cannot be larger than memory size".to_string(),
            });
        }
        
        // Storage directory validation
        if self.storage_dir.is_empty() {
            return Err(RsKvError::InvalidConfig {
                message: "Storage directory cannot be empty".to_string(),
            });
        }
        
        // Interval validation
        if self.checkpoint_interval_ms < 100 {
            return Err(RsKvError::InvalidConfig {
                message: "Checkpoint interval must be at least 100ms".to_string(),
            });
        }
        
        if self.gc_interval_ms < 1000 {
            return Err(RsKvError::InvalidConfig {
                message: "GC interval must be at least 1000ms".to_string(),
            });
        }
        
        // Thread count validation
        if self.max_background_threads == 0 {
            return Err(RsKvError::InvalidConfig {
                message: "Maximum background threads must be at least 1".to_string(),
            });
        }
        
        if self.max_background_threads > 32 {
            return Err(RsKvError::InvalidConfig {
                message: "Maximum background threads cannot exceed 32".to_string(),
            });
        }
        
        // Cross-parameter validation
        if self.checkpoint_interval_ms > self.gc_interval_ms {
            log::warn!("Checkpoint interval ({} ms) is longer than GC interval ({} ms), this might cause performance issues",
                      self.checkpoint_interval_ms, self.gc_interval_ms);
        }
        
        Ok(())
    }
    
    /// Create a configuration with memory size optimization
    pub fn with_memory_size(memory_size: u64) -> Result<Self> {
        let mut config = Self::default();
        config.memory_size = memory_size;
        
        // Adjust page size based on memory size for optimal performance
        if memory_size >= 8 * 1024 * 1024 * 1024 {
            // 8GB+: Use 64MB pages
            config.page_size = 64 * 1024 * 1024;
        } else if memory_size >= 1024 * 1024 * 1024 {
            // 1GB+: Use 32MB pages (default)
            config.page_size = 32 * 1024 * 1024;
        } else if memory_size >= 256 * 1024 * 1024 {
            // 256MB+: Use 16MB pages
            config.page_size = 16 * 1024 * 1024;
        } else {
            // <256MB: Use 8MB pages
            config.page_size = 8 * 1024 * 1024;
        }
        
        config.validate()?;
        Ok(config)
    }
    
    /// Create a configuration optimized for high-performance scenarios
    pub fn high_performance() -> Result<Self> {
        let mut config = Self::default();
        config.memory_size = 4 * 1024 * 1024 * 1024; // 4GB
        config.page_size = 64 * 1024 * 1024; // 64MB pages
        config.checkpoint_interval_ms = 30000; // 30 seconds
        config.gc_interval_ms = 60000; // 1 minute
        config.max_background_threads = 8;
        
        config.validate()?;
        Ok(config)
    }
    
    /// Create a configuration optimized for low-memory scenarios
    pub fn low_memory() -> Result<Self> {
        let mut config = Self::default();
        config.memory_size = 64 * 1024 * 1024; // 64MB
        config.page_size = 4 * 1024 * 1024; // 4MB pages
        config.checkpoint_interval_ms = 2000; // 2 seconds
        config.gc_interval_ms = 5000; // 5 seconds
        config.max_background_threads = 2;
        
        config.validate()?;
        Ok(config)
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            memory_size: 1024 * 1024 * 1024, // 1GB
            page_size: PAGE_SIZE,
            storage_dir: "./rskv_data".to_string(),
            enable_checkpointing: true,
            checkpoint_interval_ms: 5000, // 5 seconds
            enable_gc: true,
            gc_interval_ms: 10000, // 10 seconds
            max_background_threads: 4,
            use_mmap: true, // Enable mmap by default for better performance
            enable_readahead: true,
            readahead_size: 1024 * 1024, // 1MB
            enable_write_batching: true,
            write_batch_size: 64 * 1024, // 64KB
            enable_compression: false, // Disabled by default for simplicity
            sync_mode: SyncMode::Periodic,
            preallocate_log: true,
            log_prealloc_size: 100 * 1024 * 1024, // 100MB
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_address_utilities() {
        let page = 100;
        let offset = 1024;
        
        let address = make_address(page, offset);
        assert_eq!(get_page(address), page);
        assert_eq!(get_offset(address), offset);
    }

    #[test]
    fn test_record_info() {
        let record_info = RecordInfo::new(42, 1, true, false, false);
        assert_eq!(record_info.previous_address, 42);
        assert_eq!(record_info.checkpoint_version, 1);
        assert!(record_info.final_bit);
        assert!(!record_info.tombstone);
        assert!(!record_info.invalid);
        assert!(!record_info.is_null());
    }

    #[test]
    fn test_null_record_info() {
        let record_info = RecordInfo::new(0, 0, false, false, false);
        assert!(record_info.is_null());
    }
}

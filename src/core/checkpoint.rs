use crate::core::address::Address;
use crate::core::malloc_fixed_page_size::FixedPageAddress;

/// Checkpoint metadata for the index itself.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct IndexMetadata {
    pub version: u32,
    pub table_size: u64,
    pub num_ht_bytes: u64,
    pub num_ofb_bytes: u64,
    pub ofb_count: FixedPageAddress,
    /// Earliest address that is valid for the log.
    pub log_begin_address: Address,
    /// Address as of which this checkpoint was taken.
    pub checkpoint_start_address: Address,
}

impl Default for IndexMetadata {
    fn default() -> Self {
        Self {
            version: 0,
            table_size: 0,
            num_ht_bytes: 0,
            num_ofb_bytes: 0,
            ofb_count: FixedPageAddress::INVALID_ADDRESS,
            log_begin_address: Address::INVALID_ADDRESS,
            checkpoint_start_address: Address::INVALID_ADDRESS,
        }
    }
}

/// Checkpoint metadata, for the log.
#[derive(Debug, Clone, Copy, Default)]
#[repr(C)]
pub struct LogMetadata {
    pub version: u32,
    pub flushed_address: Address,
    pub final_address: Address,
    // In C++, this also contains thread-specific info like GUIDs and serial numbers.
    // We will simplify this for now.
}

/// Top-level checkpoint metadata.
#[derive(Debug, Clone, Copy, Default)]
#[repr(C)]
pub struct CheckpointMetadata {
    pub index_metadata: IndexMetadata,
    pub log_metadata: LogMetadata,
}

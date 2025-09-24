use crate::core::address::Address;
use crate::core::malloc_fixed_page_size::FixedPageAddress;
use crate::core::status::Status;
use std::time::{SystemTime, UNIX_EPOCH};

/// Types of checkpoints supported
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
#[derive(Default)]
pub enum CheckpointType {
    /// Full checkpoint containing all data
    #[default]
    Full = 0,
    /// Incremental checkpoint containing only changes since last checkpoint
    Incremental = 1,
}

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
    /// Checksum for integrity verification
    pub checksum: u64,
    /// Timestamp when checkpoint was created
    pub timestamp: u64,
    /// Checkpoint type (full vs incremental)
    pub checkpoint_type: CheckpointType,
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
            checksum: 0,
            timestamp: 0,
            checkpoint_type: CheckpointType::default(),
        }
    }
}

/// Checkpoint metadata, for the log.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct LogMetadata {
    pub version: u32,
    pub flushed_address: Address,
    pub final_address: Address,
    /// Checksum for integrity verification
    pub checksum: u64,
    /// Timestamp when checkpoint was created
    pub timestamp: u64,
    /// Number of records in the log
    pub record_count: u64,
    /// Size of the log data in bytes
    pub data_size: u64,
}

impl Default for LogMetadata {
    fn default() -> Self {
        Self {
            version: 0,
            flushed_address: Address::INVALID_ADDRESS,
            final_address: Address::INVALID_ADDRESS,
            checksum: 0,
            timestamp: 0,
            record_count: 0,
            data_size: 0,
        }
    }
}

/// Top-level checkpoint metadata.
#[derive(Debug, Clone, Copy, Default)]
#[repr(C)]
pub struct CheckpointMetadata {
    pub index_metadata: IndexMetadata,
    pub log_metadata: LogMetadata,
}

impl CheckpointMetadata {
    /// Creates a new checkpoint metadata with current timestamp
    pub fn new(index_metadata: IndexMetadata, log_metadata: LogMetadata) -> Self {
        Self {
            index_metadata,
            log_metadata,
        }
    }

    /// Validates the checkpoint metadata integrity
    pub fn validate(&self) -> Result<(), Status> {
        // Basic validation checks
        if self.index_metadata.version == 0 && self.log_metadata.version == 0 {
            return Err(Status::Corruption);
        }

        // Check timestamp consistency
        let time_diff = self
            .index_metadata
            .timestamp
            .abs_diff(self.log_metadata.timestamp);

        // Allow up to 10 seconds difference
        if time_diff > 10_000_000_000 {
            return Err(Status::Corruption);
        }

        Ok(())
    }

    /// Gets the minimum timestamp between index and log
    pub fn min_timestamp(&self) -> u64 {
        self.index_metadata
            .timestamp
            .min(self.log_metadata.timestamp)
    }

    /// Gets the maximum timestamp between index and log
    pub fn max_timestamp(&self) -> u64 {
        self.index_metadata
            .timestamp
            .max(self.log_metadata.timestamp)
    }

    /// Checks if this is an incremental checkpoint
    pub fn is_incremental(&self) -> bool {
        self.index_metadata.checkpoint_type == CheckpointType::Incremental
    }
}

impl IndexMetadata {
    /// Creates a new index metadata with current timestamp
    pub fn new(version: u32, table_size: u64, checkpoint_type: CheckpointType) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        Self {
            version,
            table_size,
            timestamp,
            checkpoint_type,
            ..Default::default()
        }
    }

    /// Calculates a simple checksum for the metadata
    pub fn calculate_checksum(&self) -> u64 {
        let mut checksum = 0u64;
        checksum ^= self.version as u64;
        checksum ^= self.table_size;
        checksum ^= self.num_ht_bytes;
        checksum ^= self.num_ofb_bytes;
        checksum ^= self.log_begin_address.control();
        checksum ^= self.checkpoint_start_address.control();
        checksum ^= self.timestamp;
        checksum
    }

    /// Updates the checksum field
    pub fn update_checksum(&mut self) {
        self.checksum = self.calculate_checksum();
    }

    /// Validates the checksum
    pub fn validate_checksum(&self) -> bool {
        self.checksum == self.calculate_checksum()
    }
}

impl LogMetadata {
    /// Creates a new log metadata with current timestamp
    pub fn new(
        version: u32,
        flushed_address: Address,
        final_address: Address,
        record_count: u64,
        data_size: u64,
    ) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        Self {
            version,
            flushed_address,
            final_address,
            timestamp,
            record_count,
            data_size,
            checksum: 0,
        }
    }

    /// Calculates a simple checksum for the metadata
    pub fn calculate_checksum(&self) -> u64 {
        let mut checksum = 0u64;
        checksum ^= self.version as u64;
        checksum ^= self.flushed_address.control();
        checksum ^= self.final_address.control();
        checksum ^= self.timestamp;
        checksum ^= self.record_count;
        checksum ^= self.data_size;
        checksum
    }

    /// Updates the checksum field
    pub fn update_checksum(&mut self) {
        self.checksum = self.calculate_checksum();
    }

    /// Validates the checksum
    pub fn validate_checksum(&self) -> bool {
        self.checksum == self.calculate_checksum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_metadata_checksum() {
        let mut metadata = IndexMetadata::new(1, 1024, CheckpointType::Full);

        // Initially checksum should be 0 since we just created it
        assert_eq!(metadata.checksum, 0);

        // Update checksum
        metadata.update_checksum();
        assert_ne!(metadata.checksum, 0);

        // Validation should pass
        assert!(metadata.validate_checksum());

        // Modify data and validation should fail
        metadata.table_size = 2048;
        assert!(!metadata.validate_checksum());

        // Update checksum again
        metadata.update_checksum();
        assert!(metadata.validate_checksum());
    }

    #[test]
    fn test_log_metadata_checksum() {
        let addr1 = Address::from_control(100);
        let addr2 = Address::from_control(200);

        let mut metadata = LogMetadata::new(1, addr1, addr2, 50, 1000);

        // Update checksum
        metadata.update_checksum();
        assert!(metadata.validate_checksum());

        // Modify data and validation should fail
        metadata.record_count = 100;
        assert!(!metadata.validate_checksum());
    }

    #[test]
    fn test_checkpoint_metadata_validation() {
        let index_meta = IndexMetadata::new(1, 1024, CheckpointType::Full);
        let log_meta = LogMetadata::new(
            1,
            Address::from_control(100),
            Address::from_control(200),
            50,
            1000,
        );

        let checkpoint = CheckpointMetadata::new(index_meta, log_meta);

        // Should pass validation
        assert!(checkpoint.validate().is_ok());

        // Test incremental checkpoint detection
        let mut index_meta_inc = index_meta;
        index_meta_inc.checkpoint_type = CheckpointType::Incremental;
        let checkpoint_inc = CheckpointMetadata::new(index_meta_inc, log_meta);
        assert!(checkpoint_inc.is_incremental());
    }

    #[test]
    fn test_checkpoint_metadata_timestamps() {
        let index_meta = IndexMetadata::new(1, 1024, CheckpointType::Full);
        let log_meta = LogMetadata::new(
            1,
            Address::from_control(100),
            Address::from_control(200),
            50,
            1000,
        );

        let checkpoint = CheckpointMetadata::new(index_meta, log_meta);

        // Timestamps should be reasonable (within current time)
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        assert!(checkpoint.min_timestamp() <= current_time);
        assert!(checkpoint.max_timestamp() <= current_time);
        assert!(checkpoint.min_timestamp() <= checkpoint.max_timestamp());
    }

    #[test]
    fn test_checkpoint_type_enum() {
        assert_eq!(CheckpointType::Full as u32, 0);
        assert_eq!(CheckpointType::Incremental as u32, 1);
        assert_eq!(CheckpointType::default(), CheckpointType::Full);
    }
}

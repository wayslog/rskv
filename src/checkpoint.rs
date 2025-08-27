//! Checkpoint and recovery implementation for rskv
//!
//! This module implements non-blocking checkpointing inspired by FASTER's design.
//! It provides consistent snapshots of the entire database state without pausing operations.

use crate::common::{Address, Key, Result, RsKvError};
use crate::hlog::HybridLog;
use crate::index::SharedMemHashIndex;

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::fs as async_fs;
use tokio::time::Instant;

/// Metadata for a checkpoint containing all necessary information for recovery
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointMetadata {
    /// Unique checkpoint ID
    pub checkpoint_id: u64,
    /// Timestamp when checkpoint was initiated
    pub timestamp: u64,
    /// Log addresses at checkpoint time
    pub log_metadata: LogMetadata,
    /// Index snapshot information
    pub index_metadata: IndexMetadata,
    /// Version of the checkpoint format
    pub format_version: u32,
    /// Size of the checkpoint in bytes
    pub total_size: u64,
}

/// Log-specific metadata in a checkpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogMetadata {
    /// Begin address of the log
    pub begin_address: Address,
    /// Head address at checkpoint time
    pub head_address: Address,
    /// Read-only address at checkpoint time
    pub read_only_address: Address,
    /// Tail address at checkpoint time
    pub tail_address: Address,
    /// Address up to which data has been flushed
    pub flushed_until_address: Address,
}

/// Index-specific metadata in a checkpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexMetadata {
    /// Number of entries in the index
    pub entry_count: usize,
    /// Total size of keys in bytes
    pub total_key_size: usize,
    /// Size of the index snapshot file
    pub snapshot_file_size: u64,
    /// Hash of the index snapshot for integrity checking
    pub snapshot_hash: u64,
}

/// State machine for checkpoint operations
pub struct CheckpointState {
    /// Unique ID for this checkpoint
    checkpoint_id: AtomicU64,
    
    /// Whether a checkpoint is currently in progress
    in_progress: AtomicBool,
    
    /// Directory where checkpoints are stored
    checkpoint_dir: PathBuf,
    
    /// Reference to the hybrid log
    hlog: Arc<HybridLog>,
    
    /// Reference to the hash index
    index: SharedMemHashIndex,
    
    /// Start time of current checkpoint
    start_time: parking_lot::Mutex<Option<Instant>>,
}

impl CheckpointState {
    /// Create a new checkpoint state manager
    pub fn new(
        checkpoint_dir: PathBuf,
        hlog: Arc<HybridLog>,
        index: SharedMemHashIndex,
    ) -> Result<Self> {
        // Ensure checkpoint directory exists
        std::fs::create_dir_all(&checkpoint_dir)?;
        
        Ok(Self {
            checkpoint_id: AtomicU64::new(1),
            in_progress: AtomicBool::new(false),
            checkpoint_dir,
            hlog,
            index,
            start_time: parking_lot::Mutex::new(None),
        })
    }
    
    /// Check if a checkpoint is currently in progress
    pub fn is_in_progress(&self) -> bool {
        self.in_progress.load(Ordering::Acquire)
    }
    
    /// Initiate a new checkpoint operation
    pub async fn initiate_checkpoint(&self) -> Result<CheckpointMetadata> {
        // Check if checkpoint is already in progress
        if self.in_progress.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire).is_err() {
            return Err(RsKvError::CheckpointFailed {
                message: "Checkpoint already in progress".to_string(),
            });
        }
        
        let checkpoint_id = self.checkpoint_id.fetch_add(1, Ordering::AcqRel);
        let start_time = Instant::now();
        *self.start_time.lock() = Some(start_time);
        
        log::info!("Initiating checkpoint {}", checkpoint_id);
        
        // Phase 1: Capture current log state and shift read-only address
        let tail_address_before = self.hlog.get_tail_address();
        let checkpoint_address = self.hlog.shift_read_only_address();
        
        log::debug!("Checkpoint {}: shifted read-only to address 0x{:x}", 
                   checkpoint_id, checkpoint_address);
        
        // Phase 2: Create log metadata
        let log_metadata = LogMetadata {
            begin_address: self.hlog.get_begin_address(),
            head_address: self.hlog.get_head_address(),
            read_only_address: checkpoint_address,
            tail_address: tail_address_before,
            flushed_until_address: checkpoint_address, // Will be updated after flush
        };
        
        // Phase 3: Create index snapshot
        let index_snapshot = self.create_index_snapshot(checkpoint_id).await?;
        let index_metadata = IndexMetadata {
            entry_count: index_snapshot.len(),
            total_key_size: index_snapshot.iter().map(|(k, _)| k.len()).sum(),
            snapshot_file_size: 0, // Will be updated after writing
            snapshot_hash: self.calculate_snapshot_hash(&index_snapshot),
        };
        
        // Phase 4: Flush log data to disk
        self.hlog.flush_to_disk(checkpoint_address).await?;
        
        // Phase 5: Write checkpoint files
        let metadata = CheckpointMetadata {
            checkpoint_id,
            timestamp: start_time.elapsed().as_millis() as u64,
            log_metadata,
            index_metadata,
            format_version: 1,
            total_size: 0, // Will be calculated
        };
        
        self.write_checkpoint_files(checkpoint_id, &metadata, index_snapshot).await?;
        
        log::info!("Checkpoint {} completed in {:?}", 
                  checkpoint_id, start_time.elapsed());
        
        // Mark checkpoint as complete
        self.in_progress.store(false, Ordering::Release);
        
        Ok(metadata)
    }
    
    /// Create a snapshot of the current index state
    async fn create_index_snapshot(&self, checkpoint_id: u64) -> Result<Vec<(Key, Address)>> {
        log::debug!("Creating index snapshot for checkpoint {}", checkpoint_id);
        
        let snapshot = self.index.snapshot();
        
        log::debug!("Index snapshot created with {} entries", snapshot.len());
        Ok(snapshot)
    }
    
    /// Calculate hash of index snapshot for integrity checking
    fn calculate_snapshot_hash(&self, snapshot: &[(Key, Address)]) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        
        // Sort snapshot by key for deterministic hashing
        let mut sorted_snapshot = snapshot.to_vec();
        sorted_snapshot.sort_by(|a, b| a.0.cmp(&b.0));
        
        for (key, address) in sorted_snapshot {
            key.hash(&mut hasher);
            address.hash(&mut hasher);
        }
        
        hasher.finish()
    }
    
    /// Write checkpoint files to disk
    async fn write_checkpoint_files(
        &self,
        checkpoint_id: u64,
        metadata: &CheckpointMetadata,
        index_snapshot: Vec<(Key, Address)>,
    ) -> Result<()> {
        let checkpoint_prefix = self.checkpoint_dir.join(format!("checkpoint_{}", checkpoint_id));
        
        // Write index snapshot
        let index_file_path = format!("{}.index", checkpoint_prefix.to_string_lossy());
        self.write_index_snapshot(&index_file_path, index_snapshot).await?;
        
        // Write metadata
        let metadata_file_path = format!("{}.meta", checkpoint_prefix.to_string_lossy());
        self.write_metadata(&metadata_file_path, metadata).await?;
        
        log::info!("Checkpoint {} files written to {}", 
                  checkpoint_id, checkpoint_prefix.to_string_lossy());
        
        Ok(())
    }
    
    /// Write index snapshot to file
    async fn write_index_snapshot(
        &self,
        file_path: &str,
        snapshot: Vec<(Key, Address)>,
    ) -> Result<()> {
        let data = bincode::serialize(&snapshot)?;
        async_fs::write(file_path, data).await?;
        
        log::debug!("Index snapshot written to {}", file_path);
        Ok(())
    }
    
    /// Write checkpoint metadata to file
    async fn write_metadata(
        &self,
        file_path: &str,
        metadata: &CheckpointMetadata,
    ) -> Result<()> {
        let data = bincode::serialize(metadata)?;
        async_fs::write(file_path, data).await?;
        
        log::debug!("Checkpoint metadata written to {}", file_path);
        Ok(())
    }
    
    /// Recover from the latest checkpoint
    pub async fn recover_from_latest_checkpoint(&self) -> Result<Option<CheckpointMetadata>> {
        let latest_checkpoint = self.find_latest_checkpoint().await?;
        
        if let Some(checkpoint_id) = latest_checkpoint {
            log::info!("Recovering from checkpoint {}", checkpoint_id);
            let metadata = self.load_checkpoint(checkpoint_id).await?;
            Ok(Some(metadata))
        } else {
            log::info!("No checkpoint found, starting fresh");
            Ok(None)
        }
    }
    
    /// Find the latest checkpoint ID
    async fn find_latest_checkpoint(&self) -> Result<Option<u64>> {
        let mut entries = async_fs::read_dir(&self.checkpoint_dir).await?;
        let mut latest_id = None;
        
        while let Some(entry) = entries.next_entry().await? {
            let file_name = entry.file_name();
            let file_str = file_name.to_string_lossy();
            
            if file_str.starts_with("checkpoint_") && file_str.ends_with(".meta") {
                if let Some(id_str) = file_str.strip_prefix("checkpoint_").and_then(|s| s.strip_suffix(".meta")) {
                    if let Ok(id) = id_str.parse::<u64>() {
                        latest_id = Some(latest_id.unwrap_or(0).max(id));
                    }
                }
            }
        }
        
        Ok(latest_id)
    }
    
    /// Load a specific checkpoint
    async fn load_checkpoint(&self, checkpoint_id: u64) -> Result<CheckpointMetadata> {
        let checkpoint_prefix = self.checkpoint_dir.join(format!("checkpoint_{}", checkpoint_id));
        
        // Load metadata
        let metadata_file_path = format!("{}.meta", checkpoint_prefix.to_string_lossy());
        let metadata_data = async_fs::read(&metadata_file_path).await?;
        let metadata: CheckpointMetadata = bincode::deserialize(&metadata_data)?;
        
        // Load and restore index snapshot
        let index_file_path = format!("{}.index", checkpoint_prefix.to_string_lossy());
        let index_data = async_fs::read(&index_file_path).await?;
        let index_snapshot: Vec<(Key, Address)> = bincode::deserialize(&index_data)?;
        
        // Verify snapshot integrity
        let calculated_hash = self.calculate_snapshot_hash(&index_snapshot);
        if calculated_hash != metadata.index_metadata.snapshot_hash {
            return Err(RsKvError::CheckpointFailed {
                message: format!("Index snapshot hash mismatch: expected {}, got {}", 
                               metadata.index_metadata.snapshot_hash, calculated_hash),
            });
        }
        
        // Restore index from snapshot
        self.index.restore_from_snapshot(index_snapshot);
        
        log::info!("Checkpoint {} loaded successfully", checkpoint_id);
        Ok(metadata)
    }
    
    /// List all available checkpoints
    pub async fn list_checkpoints(&self) -> Result<Vec<u64>> {
        let mut entries = async_fs::read_dir(&self.checkpoint_dir).await?;
        let mut checkpoint_ids = Vec::new();
        
        while let Some(entry) = entries.next_entry().await? {
            let file_name = entry.file_name();
            let file_str = file_name.to_string_lossy();
            
            if file_str.starts_with("checkpoint_") && file_str.ends_with(".meta") {
                if let Some(id_str) = file_str.strip_prefix("checkpoint_").and_then(|s| s.strip_suffix(".meta")) {
                    if let Ok(id) = id_str.parse::<u64>() {
                        checkpoint_ids.push(id);
                    }
                }
            }
        }
        
        checkpoint_ids.sort();
        Ok(checkpoint_ids)
    }
    
    /// Delete old checkpoints, keeping only the specified number
    pub async fn cleanup_old_checkpoints(&self, keep_count: usize) -> Result<()> {
        let mut checkpoint_ids = self.list_checkpoints().await?;
        checkpoint_ids.sort();
        
        if checkpoint_ids.len() <= keep_count {
            return Ok(()); // Nothing to cleanup
        }
        
        let to_delete = &checkpoint_ids[..checkpoint_ids.len() - keep_count];
        
        for &checkpoint_id in to_delete {
            self.delete_checkpoint(checkpoint_id).await?;
        }
        
        log::info!("Cleaned up {} old checkpoints", to_delete.len());
        Ok(())
    }
    
    /// Delete a specific checkpoint
    async fn delete_checkpoint(&self, checkpoint_id: u64) -> Result<()> {
        let checkpoint_prefix = self.checkpoint_dir.join(format!("checkpoint_{}", checkpoint_id));
        
        let metadata_file = format!("{}.meta", checkpoint_prefix.to_string_lossy());
        let index_file = format!("{}.index", checkpoint_prefix.to_string_lossy());
        
        if async_fs::metadata(&metadata_file).await.is_ok() {
            async_fs::remove_file(&metadata_file).await?;
        }
        
        if async_fs::metadata(&index_file).await.is_ok() {
            async_fs::remove_file(&index_file).await?;
        }
        
        log::debug!("Deleted checkpoint {}", checkpoint_id);
        Ok(())
    }
    
    /// Get checkpoint statistics
    pub async fn get_checkpoint_stats(&self) -> Result<CheckpointStats> {
        let checkpoint_ids = self.list_checkpoints().await?;
        let total_count = checkpoint_ids.len();
        
        let mut total_size = 0u64;
        for &checkpoint_id in &checkpoint_ids {
            let checkpoint_prefix = self.checkpoint_dir.join(format!("checkpoint_{}", checkpoint_id));
            
            let metadata_file = format!("{}.meta", checkpoint_prefix.to_string_lossy());
            let index_file = format!("{}.index", checkpoint_prefix.to_string_lossy());
            
            if let Ok(meta) = async_fs::metadata(&metadata_file).await {
                total_size += meta.len();
            }
            if let Ok(meta) = async_fs::metadata(&index_file).await {
                total_size += meta.len();
            }
        }
        
        Ok(CheckpointStats {
            total_checkpoints: total_count,
            total_size_bytes: total_size,
            latest_checkpoint_id: checkpoint_ids.last().copied(),
            in_progress: self.is_in_progress(),
        })
    }
}

/// Statistics about checkpoints
#[derive(Debug, Clone)]
pub struct CheckpointStats {
    /// Total number of checkpoints
    pub total_checkpoints: usize,
    /// Total size of all checkpoints in bytes
    pub total_size_bytes: u64,
    /// ID of the latest checkpoint
    pub latest_checkpoint_id: Option<u64>,
    /// Whether a checkpoint is currently in progress
    pub in_progress: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hlog::FileStorageDevice;
    use crate::index::new_shared_mem_hash_index;
    use crate::epoch::EpochManager;
    use tempfile::tempdir;

    async fn create_test_checkpoint_state() -> (CheckpointState, tempfile::TempDir) {
        let temp_dir = tempdir().unwrap();
        let checkpoint_dir = temp_dir.path().join("checkpoints");
        
        let epoch = Arc::new(EpochManager::new());
        let storage = Box::new(FileStorageDevice::new(temp_dir.path().join("test.log")).unwrap());
        let hlog = Arc::new(HybridLog::new(64 * 1024 * 1024, storage, epoch.clone()).unwrap());
        let index = new_shared_mem_hash_index(epoch);
        
        let checkpoint_state = CheckpointState::new(checkpoint_dir, hlog, index).unwrap();
        (checkpoint_state, temp_dir)
    }

    #[tokio::test]
    async fn test_checkpoint_creation() {
        let (checkpoint_state, _temp_dir) = create_test_checkpoint_state().await;
        
        // Add some data to index
        checkpoint_state.index.insert(b"key1".to_vec(), 100);
        checkpoint_state.index.insert(b"key2".to_vec(), 200);
        
        // Create checkpoint
        let metadata = checkpoint_state.initiate_checkpoint().await.unwrap();
        
        assert_eq!(metadata.checkpoint_id, 1);
        assert_eq!(metadata.index_metadata.entry_count, 2);
        assert!(!checkpoint_state.is_in_progress());
    }

    #[tokio::test]
    async fn test_checkpoint_recovery() {
        let (checkpoint_state, _temp_dir) = create_test_checkpoint_state().await;
        
        // Add data and create checkpoint
        checkpoint_state.index.insert(b"key1".to_vec(), 100);
        checkpoint_state.index.insert(b"key2".to_vec(), 200);
        
        let _metadata = checkpoint_state.initiate_checkpoint().await.unwrap();
        
        // Clear index
        checkpoint_state.index.clear();
        assert_eq!(checkpoint_state.index.len(), 0);
        
        // Recover from checkpoint
        let recovered_metadata = checkpoint_state.recover_from_latest_checkpoint().await.unwrap();
        
        assert!(recovered_metadata.is_some());
        assert_eq!(checkpoint_state.index.len(), 2);
        assert_eq!(checkpoint_state.index.find(&b"key1".to_vec()), Some(100));
        assert_eq!(checkpoint_state.index.find(&b"key2".to_vec()), Some(200));
    }

    #[tokio::test]
    async fn test_checkpoint_cleanup() {
        let (checkpoint_state, _temp_dir) = create_test_checkpoint_state().await;
        
        // Create multiple checkpoints
        for i in 0..5 {
            checkpoint_state.index.insert(format!("key{}", i).into_bytes(), i as u64);
            checkpoint_state.initiate_checkpoint().await.unwrap();
        }
        
        let checkpoints_before = checkpoint_state.list_checkpoints().await.unwrap();
        assert_eq!(checkpoints_before.len(), 5);
        
        // Cleanup, keeping only 2
        checkpoint_state.cleanup_old_checkpoints(2).await.unwrap();
        
        let checkpoints_after = checkpoint_state.list_checkpoints().await.unwrap();
        assert_eq!(checkpoints_after.len(), 2);
        assert_eq!(checkpoints_after, vec![4, 5]); // Should keep the latest 2
    }

    #[tokio::test]
    async fn test_checkpoint_stats() {
        let (checkpoint_state, _temp_dir) = create_test_checkpoint_state().await;
        
        let stats_before = checkpoint_state.get_checkpoint_stats().await.unwrap();
        assert_eq!(stats_before.total_checkpoints, 0);
        
        // Create a checkpoint
        checkpoint_state.index.insert(b"key1".to_vec(), 100);
        checkpoint_state.initiate_checkpoint().await.unwrap();
        
        let stats_after = checkpoint_state.get_checkpoint_stats().await.unwrap();
        assert_eq!(stats_after.total_checkpoints, 1);
        assert_eq!(stats_after.latest_checkpoint_id, Some(1));
        assert!(stats_after.total_size_bytes > 0);
    }
}


//! Background task management for rskv
//!
//! This module implements background tasks for automatic checkpointing,
//! garbage collection, and log maintenance operations.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::RwLock as AsyncRwLock;
use tokio::time::{Duration, MissedTickBehavior, interval};

use crate::checkpoint::CheckpointState;
use crate::common::{Config, Result, RsKvError};
use crate::gc::{GcConfig, GcState};
use crate::hlog::HybridLog;

/// Background task manager for automatic maintenance operations
pub struct BackgroundTaskManager {
    /// Whether background tasks are running
    running: Arc<AtomicBool>,

    /// Configuration
    config: Config,

    /// Reference to checkpoint state
    checkpoint_state: Arc<CheckpointState>,

    /// Reference to GC state
    gc_state: Arc<GcState>,

    /// Reference to hybrid log
    hlog: Arc<HybridLog>,

    /// Lock to coordinate with manual operations
    operation_lock: Arc<AsyncRwLock<()>>,

    /// Task handles for cleanup
    task_handles: parking_lot::Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl BackgroundTaskManager {
    /// Create a new background task manager
    pub fn new(
        config: Config,
        checkpoint_state: Arc<CheckpointState>,
        gc_state: Arc<GcState>,
        hlog: Arc<HybridLog>,
        operation_lock: Arc<AsyncRwLock<()>>,
    ) -> Self {
        Self {
            running: Arc::new(AtomicBool::new(false)),
            config,
            checkpoint_state,
            gc_state,
            hlog,
            operation_lock,
            task_handles: parking_lot::Mutex::new(Vec::new()),
        }
    }

    /// Start all background tasks
    pub fn start(&self) -> Result<()> {
        if self
            .running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(RsKvError::Internal {
                message: "Background tasks are already running".to_string(),
            });
        }

        log::info!("Starting background task manager");

        let mut handles = self.task_handles.lock();

        // Start checkpoint task if enabled
        if self.config.enable_checkpointing {
            let handle = self.start_checkpoint_task();
            handles.push(handle);
        }

        // Start GC task if enabled
        if self.config.enable_gc {
            let handle = self.start_gc_task();
            handles.push(handle);
        }

        // Start log maintenance task
        let handle = self.start_log_maintenance_task();
        handles.push(handle);

        log::info!("Started {} background tasks", handles.len());
        Ok(())
    }

    /// Stop all background tasks
    pub async fn stop(&self) -> Result<()> {
        if !self.running.swap(false, Ordering::AcqRel) {
            return Ok(()); // Already stopped
        }

        log::info!("Stopping background tasks");

        // Cancel all tasks
        let handles = {
            let mut handles = self.task_handles.lock();
            std::mem::take(&mut *handles)
        };

        for handle in handles {
            handle.abort();
            let _ = handle.await; // Ignore cancellation errors
        }

        log::info!("All background tasks stopped");
        Ok(())
    }

    /// Check if background tasks are running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    /// Start the checkpoint task
    fn start_checkpoint_task(&self) -> tokio::task::JoinHandle<()> {
        let running = self.running.clone();
        let checkpoint_state = self.checkpoint_state.clone();
        let operation_lock = self.operation_lock.clone();
        let interval_ms = self.config.checkpoint_interval_ms;

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(interval_ms));
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

            log::info!("Checkpoint task started with interval {interval_ms}ms");

            while running.load(Ordering::Acquire) {
                interval.tick().await;

                if !running.load(Ordering::Acquire) {
                    break;
                }

                // Try to acquire lock for checkpoint
                if let Ok(_lock) = operation_lock.try_write() {
                    match checkpoint_state.initiate_checkpoint().await {
                        Ok(metadata) => {
                            log::debug!(
                                "Background checkpoint {} completed",
                                metadata.checkpoint_id
                            );
                        }
                        Err(e) => {
                            log::warn!("Background checkpoint failed: {e}");
                        }
                    }
                } else {
                    log::debug!("Skipping checkpoint - manual operation in progress");
                }
            }

            log::info!("Checkpoint task stopped");
        })
    }

    /// Start the garbage collection task
    fn start_gc_task(&self) -> tokio::task::JoinHandle<()> {
        let running = self.running.clone();
        let gc_state = self.gc_state.clone();
        let operation_lock = self.operation_lock.clone();
        let interval_ms = self.config.gc_interval_ms;

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(interval_ms));
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

            log::info!("GC task started with interval {interval_ms}ms");

            while running.load(Ordering::Acquire) {
                interval.tick().await;

                if !running.load(Ordering::Acquire) {
                    break;
                }

                // Check if GC is needed
                let gc_config = GcConfig::default();
                match gc_state.should_run_gc(&gc_config) {
                    Ok(true) => {
                        // Try to acquire lock for GC
                        if let Ok(_lock) = operation_lock.try_read() {
                            match gc_state.initiate_gc(gc_config).await {
                                Ok(stats) => {
                                    log::debug!(
                                        "Background GC completed, reclaimed {} bytes",
                                        stats.bytes_reclaimed
                                    );
                                }
                                Err(e) => {
                                    log::warn!("Background GC failed: {e}");
                                }
                            }
                        } else {
                            log::debug!("Skipping GC - manual operation in progress");
                        }
                    }
                    Ok(false) => {
                        log::trace!("GC not needed");
                    }
                    Err(e) => {
                        log::warn!("Failed to check GC requirement: {e}");
                    }
                }
            }

            log::info!("GC task stopped");
        })
    }

    /// Start the log maintenance task
    fn start_log_maintenance_task(&self) -> tokio::task::JoinHandle<()> {
        let running = self.running.clone();
        let hlog = self.hlog.clone();
        let operation_lock = self.operation_lock.clone();

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(30)); // Run every 30 seconds
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

            log::info!("Log maintenance task started");

            while running.load(Ordering::Acquire) {
                interval.tick().await;

                if !running.load(Ordering::Acquire) {
                    break;
                }

                // Try to acquire read lock for maintenance
                if let Ok(_lock) = operation_lock.try_read() {
                    // Perform log maintenance operations
                    Self::perform_log_maintenance(&hlog).await;
                }
            }

            log::info!("Log maintenance task stopped");
        })
    }

    /// Perform log maintenance operations
    async fn perform_log_maintenance(hlog: &HybridLog) {
        // Check if we need to advance the read-only address
        let tail_address = hlog.get_tail_address();
        let read_only_address = hlog.get_read_only_address();
        let head_address = hlog.get_head_address();

        // If mutable region is getting large, advance read-only address
        let mutable_region_size = tail_address.saturating_sub(read_only_address);
        const MAX_MUTABLE_REGION_SIZE: u64 = 128 * 1024 * 1024; // 128MB

        if mutable_region_size > MAX_MUTABLE_REGION_SIZE {
            let new_read_only = hlog.shift_read_only_address();
            log::debug!("Advanced read-only address to 0x{new_read_only:x}");

            // Try to flush the newly read-only data
            if let Err(e) = hlog.flush_to_disk(new_read_only).await {
                log::warn!("Failed to flush during maintenance: {e}");
            }
        }

        // Check if we need to advance the head address
        let read_only_region_size = read_only_address.saturating_sub(head_address);
        const MAX_READ_ONLY_REGION_SIZE: u64 = 256 * 1024 * 1024; // 256MB

        if read_only_region_size > MAX_READ_ONLY_REGION_SIZE {
            // Move some data from memory to disk-only
            let new_head = head_address + (read_only_region_size / 2); // Move half
            if let Err(e) = hlog.shift_head_address(new_head) {
                log::warn!("Failed to shift head address during maintenance: {e}");
            } else {
                log::debug!("Advanced head address to 0x{new_head:x}");
            }
        }
    }

    /// Get statistics about background task performance
    pub fn get_stats(&self) -> BackgroundTaskStats {
        BackgroundTaskStats {
            is_running: self.is_running(),
            checkpoint_enabled: self.config.enable_checkpointing,
            gc_enabled: self.config.enable_gc,
            checkpoint_interval_ms: self.config.checkpoint_interval_ms,
            gc_interval_ms: self.config.gc_interval_ms,
            active_task_count: self.task_handles.lock().len(),
        }
    }
}

impl Drop for BackgroundTaskManager {
    fn drop(&mut self) {
        // Stop background tasks when dropping
        let running = self.running.clone();
        let handles = {
            let mut handles = self.task_handles.lock();
            std::mem::take(&mut *handles)
        };

        if running.swap(false, Ordering::AcqRel) {
            // Cancel all tasks
            for handle in handles {
                handle.abort();
            }
        }
    }
}

/// Statistics about background task performance
#[derive(Debug, Clone)]
pub struct BackgroundTaskStats {
    /// Whether background tasks are currently running
    pub is_running: bool,
    /// Whether checkpointing is enabled
    pub checkpoint_enabled: bool,
    /// Whether garbage collection is enabled
    pub gc_enabled: bool,
    /// Checkpoint interval in milliseconds
    pub checkpoint_interval_ms: u64,
    /// GC interval in milliseconds
    pub gc_interval_ms: u64,
    /// Number of active background tasks
    pub active_task_count: usize,
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::checkpoint::CheckpointState;
    use crate::epoch::EpochManager;
    use crate::hlog::FileStorageDevice;
    use crate::index::new_shared_mem_hash_index;

    async fn create_test_background_manager() -> (BackgroundTaskManager, tempfile::TempDir) {
        let temp_dir = tempdir().unwrap();

        let config = Config {
            storage_dir: temp_dir.path().to_string_lossy().to_string(),
            memory_size: 32 * 1024 * 1024, // 32MB for testing
            enable_checkpointing: true,
            checkpoint_interval_ms: 100, // Very short for testing
            enable_gc: true,
            gc_interval_ms: 200, // Very short for testing
            ..Default::default()
        };

        let epoch = Arc::new(EpochManager::new());
        let storage = Box::new(FileStorageDevice::new(temp_dir.path().join("test.log")).unwrap());
        let hlog = Arc::new(HybridLog::new(config.memory_size, storage, epoch.clone()).unwrap());
        let index = new_shared_mem_hash_index(epoch);

        let checkpoint_dir = temp_dir.path().join("checkpoints");
        let checkpoint_state =
            Arc::new(CheckpointState::new(checkpoint_dir, hlog.clone(), index.clone()).unwrap());
        let gc_state = Arc::new(GcState::new(hlog.clone(), index));
        let operation_lock = Arc::new(AsyncRwLock::new(()));

        let manager =
            BackgroundTaskManager::new(config, checkpoint_state, gc_state, hlog, operation_lock);

        (manager, temp_dir)
    }

    #[tokio::test]
    async fn test_background_manager_start_stop() {
        let (manager, _temp_dir) = create_test_background_manager().await;

        assert!(!manager.is_running());

        // Start background tasks
        manager.start().unwrap();
        assert!(manager.is_running());

        // Stop background tasks
        manager.stop().await.unwrap();
        assert!(!manager.is_running());
    }

    #[tokio::test]
    async fn test_background_manager_double_start() {
        let (manager, _temp_dir) = create_test_background_manager().await;

        // First start should succeed
        manager.start().unwrap();

        // Second start should fail
        let result = manager.start();
        assert!(result.is_err());

        manager.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_background_tasks_run() {
        let (manager, _temp_dir) = create_test_background_manager().await;

        manager.start().unwrap();

        // Let tasks run for a short time
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Tasks should still be running
        assert!(manager.is_running());

        manager.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_background_manager_stats() {
        let (manager, _temp_dir) = create_test_background_manager().await;

        let stats_before = manager.get_stats();
        assert!(!stats_before.is_running);
        assert_eq!(stats_before.active_task_count, 0);

        manager.start().unwrap();

        let stats_after = manager.get_stats();
        assert!(stats_after.is_running);
        assert!(stats_after.checkpoint_enabled);
        assert!(stats_after.gc_enabled);
        assert!(stats_after.active_task_count > 0);

        manager.stop().await.unwrap();
    }

    #[test]
    fn test_background_manager_drop() {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let (manager, _temp_dir) = create_test_background_manager().await;

            manager.start().unwrap();
            assert!(manager.is_running());

            // Drop should stop background tasks
            drop(manager);

            // Give some time for cleanup
            tokio::time::sleep(Duration::from_millis(50)).await;
        });
    }
}

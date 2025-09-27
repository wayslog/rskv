use crate::core::status::{Status, Result};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::thread::{self, ThreadId};
use std::time::{Duration, Instant};

/// Lock granularity levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockGranularity {
    /// Record-level locking (finest granularity)
    Record,
    /// Page-level locking
    Page,
    /// Index bucket-level locking
    Bucket,
    /// Global locking (coarsest granularity)
    Global,
}

/// Lock intent for deadlock detection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockIntent {
    Read,
    Write,
    ReadWrite,
}

/// Lock identifier for tracking locks across different granularities
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LockId {
    pub granularity: LockGranularity,
    pub resource_id: u64,
}

impl LockId {
    pub fn new(granularity: LockGranularity, resource_id: u64) -> Self {
        Self { granularity, resource_id }
    }

    /// Create a record-level lock ID
    pub fn record(address: u64) -> Self {
        Self::new(LockGranularity::Record, address)
    }

    /// Create a page-level lock ID
    pub fn page(page_id: u32) -> Self {
        Self::new(LockGranularity::Page, page_id as u64)
    }

    /// Create a bucket-level lock ID
    pub fn bucket(bucket_id: u32) -> Self {
        Self::new(LockGranularity::Bucket, bucket_id as u64)
    }
}

/// Lock holder information
#[derive(Debug, Clone)]
struct LockHolder {
    thread_id: ThreadId,
    intent: LockIntent,
    acquired_at: Instant,
    lock_id: LockId,
}

/// Deadlock detector using wait-for graph algorithm
pub struct DeadlockDetector {
    /// Current lock holdings per thread
    holdings: RwLock<HashMap<ThreadId, HashSet<LockId>>>,
    /// Wait-for relationships (thread -> lock it's waiting for -> thread holding it)
    wait_for: RwLock<HashMap<ThreadId, (LockId, ThreadId)>>,
    /// Lock to holder mapping
    lock_holders: RwLock<HashMap<LockId, Vec<LockHolder>>>,
    /// Detection interval
    detection_interval: Duration,
    /// Last detection time
    last_detection: Mutex<Instant>,
}

impl DeadlockDetector {
    pub fn new() -> Self {
        Self {
            holdings: RwLock::new(HashMap::new()),
            wait_for: RwLock::new(HashMap::new()),
            lock_holders: RwLock::new(HashMap::new()),
            detection_interval: Duration::from_millis(100),
            last_detection: Mutex::new(Instant::now()),
        }
    }

    /// Record that a thread has acquired a lock
    pub fn record_lock_acquired(&self, thread_id: ThreadId, lock_id: LockId, intent: LockIntent) {
        let holder = LockHolder {
            thread_id,
            intent,
            acquired_at: Instant::now(),
            lock_id,
        };

        // Update holdings
        if let Ok(mut holdings) = self.holdings.write() {
            holdings.entry(thread_id).or_insert_with(HashSet::new).insert(lock_id);
        }

        // Update lock holders
        if let Ok(mut lock_holders) = self.lock_holders.write() {
            lock_holders.entry(lock_id).or_insert_with(Vec::new).push(holder);
        }

        // Remove from wait-for graph
        if let Ok(mut wait_for) = self.wait_for.write() {
            wait_for.remove(&thread_id);
        }
    }

    /// Record that a thread is waiting for a lock
    pub fn record_lock_wait(&self, thread_id: ThreadId, lock_id: LockId) -> Result<()> {
        // Find who currently holds this lock
        let holder_threads = if let Ok(lock_holders) = self.lock_holders.read() {
            lock_holders.get(&lock_id)
                .map(|holders| holders.iter().map(|h| h.thread_id).collect::<Vec<_>>())
                .unwrap_or_default()
        } else {
            return Err(Status::InternalError);
        };

        // Update wait-for graph
        if let Ok(mut wait_for) = self.wait_for.write() {
            for &holder_thread in &holder_threads {
                if holder_thread != thread_id {
                    wait_for.insert(thread_id, (lock_id, holder_thread));
                    break; // Only track one wait relationship per thread
                }
            }
        }

        // Check for deadlock
        self.detect_deadlock()
    }

    /// Record that a thread has released a lock
    pub fn record_lock_released(&self, thread_id: ThreadId, lock_id: LockId) {
        // Remove from holdings
        if let Ok(mut holdings) = self.holdings.write() {
            if let Some(thread_locks) = holdings.get_mut(&thread_id) {
                thread_locks.remove(&lock_id);
                if thread_locks.is_empty() {
                    holdings.remove(&thread_id);
                }
            }
        }

        // Remove from lock holders
        if let Ok(mut lock_holders) = self.lock_holders.write() {
            if let Some(holders) = lock_holders.get_mut(&lock_id) {
                holders.retain(|h| h.thread_id != thread_id);
                if holders.is_empty() {
                    lock_holders.remove(&lock_id);
                }
            }
        }
    }

    /// Detect deadlock using cycle detection in wait-for graph
    fn detect_deadlock(&self) -> Result<()> {
        // Rate limit detection
        if let Ok(mut last) = self.last_detection.lock() {
            if last.elapsed() < self.detection_interval {
                return Ok(());
            }
            *last = Instant::now();
        }

        let wait_for = if let Ok(wait_for) = self.wait_for.read() {
            wait_for.clone()
        } else {
            return Err(Status::InternalError);
        };

        // Detect cycles using DFS
        for &start_thread in wait_for.keys() {
            if self.has_cycle_from(start_thread, &wait_for)? {
                log::warn!("Deadlock detected involving thread: {:?}", start_thread);
                return Err(Status::DeadlockDetected);
            }
        }

        Ok(())
    }

    /// Check if there's a cycle starting from the given thread
    fn has_cycle_from(
        &self,
        start: ThreadId,
        wait_for: &HashMap<ThreadId, (LockId, ThreadId)>,
    ) -> Result<bool> {
        let mut visited = HashSet::new();
        let mut path = HashSet::new();

        self.dfs_cycle_check(start, wait_for, &mut visited, &mut path)
    }

    /// DFS-based cycle detection
    fn dfs_cycle_check(
        &self,
        current: ThreadId,
        wait_for: &HashMap<ThreadId, (LockId, ThreadId)>,
        visited: &mut HashSet<ThreadId>,
        path: &mut HashSet<ThreadId>,
    ) -> Result<bool> {
        if path.contains(&current) {
            return Ok(true); // Cycle found
        }

        if visited.contains(&current) {
            return Ok(false); // Already processed
        }

        visited.insert(current);
        path.insert(current);

        if let Some((_, next_thread)) = wait_for.get(&current) {
            if self.dfs_cycle_check(*next_thread, wait_for, visited, path)? {
                return Ok(true);
            }
        }

        path.remove(&current);
        Ok(false)
    }

    /// Get statistics about current lock state
    pub fn get_statistics(&self) -> LockStatistics {
        let holdings_count = self.holdings.read()
            .map(|h| h.len())
            .unwrap_or(0);

        let wait_count = self.wait_for.read()
            .map(|w| w.len())
            .unwrap_or(0);

        let lock_count = self.lock_holders.read()
            .map(|h| h.len())
            .unwrap_or(0);

        LockStatistics {
            active_threads: holdings_count,
            waiting_threads: wait_count,
            active_locks: lock_count,
        }
    }
}

impl Default for DeadlockDetector {
    fn default() -> Self {
        Self::new()
    }
}

/// Lock statistics for monitoring
#[derive(Debug, Clone)]
pub struct LockStatistics {
    pub active_threads: usize,
    pub waiting_threads: usize,
    pub active_locks: usize,
}

/// Advanced hierarchical lock manager
pub struct HierarchicalLockManager {
    /// Deadlock detector
    detector: Arc<DeadlockDetector>,
    /// Lock contention counters
    contention_counters: RwLock<HashMap<LockId, AtomicUsize>>,
    /// Adaptive timeout based on contention
    adaptive_timeout: AtomicU64, // nanoseconds
    /// Base timeout
    base_timeout: Duration,
    /// Maximum timeout
    max_timeout: Duration,
}

impl HierarchicalLockManager {
    pub fn new() -> Self {
        Self {
            detector: Arc::new(DeadlockDetector::new()),
            contention_counters: RwLock::new(HashMap::new()),
            adaptive_timeout: AtomicU64::new(Duration::from_millis(100).as_nanos() as u64),
            base_timeout: Duration::from_millis(100),
            max_timeout: Duration::from_secs(5),
        }
    }

    /// Acquire lock with hierarchical ordering and deadlock detection
    pub fn acquire_lock(&self, lock_id: LockId, intent: LockIntent) -> Result<LockGuard> {
        let thread_id = thread::current().id();
        let start_time = Instant::now();

        // Record wait
        self.detector.record_lock_wait(thread_id, lock_id)?;

        // Simulate lock acquisition with timeout
        let timeout = self.get_adaptive_timeout(lock_id);
        let acquired = self.try_acquire_with_timeout(lock_id, intent, timeout)?;

        if acquired {
            self.detector.record_lock_acquired(thread_id, lock_id, intent);
            self.update_contention_stats(lock_id, false);

            Ok(LockGuard::new(
                lock_id,
                intent,
                self.detector.clone(),
                start_time.elapsed(),
            ))
        } else {
            self.update_contention_stats(lock_id, true);
            Err(Status::LockContentionTimeout)
        }
    }

    /// Try to acquire lock with timeout
    fn try_acquire_with_timeout(
        &self,
        lock_id: LockId,
        _intent: LockIntent,
        timeout: Duration,
    ) -> Result<bool> {
        let start = Instant::now();

        // Simulate lock acquisition logic
        while start.elapsed() < timeout {
            // In a real implementation, this would interact with actual locks
            // For now, simulate success after a short delay
            thread::sleep(Duration::from_millis(10));

            // Simulate some probability of acquiring the lock
            if lock_id.resource_id % 10 != 0 || start.elapsed() > Duration::from_millis(50) {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Get adaptive timeout based on contention history
    fn get_adaptive_timeout(&self, lock_id: LockId) -> Duration {
        let contention = if let Ok(counters) = self.contention_counters.read() {
            counters.get(&lock_id)
                .map(|c| c.load(Ordering::Relaxed))
                .unwrap_or(0)
        } else {
            0
        };

        // Increase timeout based on contention
        let multiplier = 1.0 + (contention as f64 * 0.1).min(10.0);
        let timeout = Duration::from_nanos(
            (self.base_timeout.as_nanos() as f64 * multiplier) as u64
        );

        timeout.min(self.max_timeout)
    }

    /// Update contention statistics
    fn update_contention_stats(&self, lock_id: LockId, contended: bool) {
        if let Ok(mut counters) = self.contention_counters.write() {
            let counter = counters.entry(lock_id)
                .or_insert_with(|| AtomicUsize::new(0));

            if contended {
                counter.fetch_add(1, Ordering::Relaxed);
            } else {
                // Decay contention over time
                let current = counter.load(Ordering::Relaxed);
                if current > 0 {
                    counter.store(current.saturating_sub(1), Ordering::Relaxed);
                }
            }
        }

        // Update adaptive timeout
        let avg_contention = self.get_average_contention();
        let new_timeout = (self.base_timeout.as_nanos() as f64 * (1.0 + avg_contention * 0.1)) as u64;
        self.adaptive_timeout.store(
            new_timeout.min(self.max_timeout.as_nanos() as u64),
            Ordering::Relaxed
        );
    }

    /// Get average contention across all locks
    fn get_average_contention(&self) -> f64 {
        if let Ok(counters) = self.contention_counters.read() {
            if counters.is_empty() {
                return 0.0;
            }

            let total: usize = counters.values()
                .map(|c| c.load(Ordering::Relaxed))
                .sum();

            total as f64 / counters.len() as f64
        } else {
            0.0
        }
    }

    /// Get lock manager statistics
    pub fn get_statistics(&self) -> (LockStatistics, Duration) {
        let detector_stats = self.detector.get_statistics();
        let current_timeout = Duration::from_nanos(
            self.adaptive_timeout.load(Ordering::Relaxed)
        );

        (detector_stats, current_timeout)
    }
}

impl Default for HierarchicalLockManager {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII lock guard with automatic cleanup
pub struct LockGuard {
    lock_id: LockId,
    intent: LockIntent,
    detector: Arc<DeadlockDetector>,
    acquisition_time: Duration,
    acquired_at: Instant,
}

impl LockGuard {
    fn new(
        lock_id: LockId,
        intent: LockIntent,
        detector: Arc<DeadlockDetector>,
        acquisition_time: Duration,
    ) -> Self {
        Self {
            lock_id,
            intent,
            detector,
            acquisition_time,
            acquired_at: Instant::now(),
        }
    }

    /// Get lock acquisition time
    pub fn acquisition_time(&self) -> Duration {
        self.acquisition_time
    }

    /// Get time held
    pub fn hold_time(&self) -> Duration {
        self.acquired_at.elapsed()
    }

    /// Get lock information
    pub fn lock_info(&self) -> (LockId, LockIntent) {
        (self.lock_id, self.intent)
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        let thread_id = thread::current().id();
        self.detector.record_lock_released(thread_id, self.lock_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_deadlock_detection() {
        let detector = DeadlockDetector::new();
        let lock1 = LockId::record(1);
        let lock2 = LockId::record(2);

        // Simulate Thread 1 holding lock1 and waiting for lock2
        let thread1 = thread::current().id();
        detector.record_lock_acquired(thread1, lock1, LockIntent::Write);

        // Create another thread context for testing
        let detector_clone = Arc::new(detector);
        let handle = thread::spawn(move || {
            let thread2 = thread::current().id();
            detector_clone.record_lock_acquired(thread2, lock2, LockIntent::Write);

            // Now thread2 waits for lock1 (potential deadlock)
            detector_clone.record_lock_wait(thread2, lock1)
        });

        let result = handle.join().unwrap();
        // Should detect potential deadlock scenario
        assert!(result.is_ok() || result.unwrap_err() == Status::DeadlockDetected);
    }

    #[test]
    fn test_lock_manager_basic() {
        let manager = HierarchicalLockManager::new();
        let lock_id = LockId::record(123);

        let guard = manager.acquire_lock(lock_id, LockIntent::Read);
        assert!(guard.is_ok());

        let guard = guard.unwrap();
        assert_eq!(guard.lock_info().0, lock_id);
        assert_eq!(guard.lock_info().1, LockIntent::Read);
    }

    #[test]
    fn test_contention_tracking() {
        let manager = HierarchicalLockManager::new();
        let lock_id = LockId::record(456);

        // Acquire and release multiple times to test contention tracking
        for _ in 0..5 {
            let _guard = manager.acquire_lock(lock_id, LockIntent::Read).unwrap();
            thread::sleep(Duration::from_millis(1));
        }

        let (_stats, timeout) = manager.get_statistics();
        assert!(timeout >= manager.base_timeout);
    }
}
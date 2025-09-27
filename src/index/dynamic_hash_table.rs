use crate::core::status::{Status, Result, ContextResult, ErrorContext, ResultExt};
use crate::core::advanced_locking::{HierarchicalLockManager, LockId, LockIntent, LockGranularity};
use crate::core::light_epoch::{LightEpoch, Guard};
use std::sync::atomic::{AtomicU64, AtomicUsize, AtomicPtr, Ordering};
use std::sync::{Arc, RwLock};
// std::collections::HashMap removed as it's not used
use std::hash::{Hash, Hasher};
use std::ptr;

/// Hash table bucket with overflow chaining
#[repr(align(64))] // Cache line alignment
pub struct HashBucket<K, V> {
    /// Number of entries in this bucket
    entry_count: AtomicUsize,
    /// Entries in the bucket (fixed size for cache efficiency)
    entries: [AtomicPtr<HashEntry<K, V>>; 7],
    /// Pointer to overflow bucket
    overflow: AtomicPtr<HashBucket<K, V>>,
    /// Statistics for load balancing
    access_count: AtomicU64,
    last_access: AtomicU64,
}

impl<K, V> HashBucket<K, V> {
    pub const ENTRIES_PER_BUCKET: usize = 7; // Fits in cache line with metadata

    pub fn new() -> Self {
        Self {
            entry_count: AtomicUsize::new(0),
            entries: Default::default(),
            overflow: AtomicPtr::new(ptr::null_mut()),
            access_count: AtomicU64::new(0),
            last_access: AtomicU64::new(0),
        }
    }

    /// Get load factor for this bucket
    fn load_factor(&self) -> f32 {
        let count = self.entry_count.load(Ordering::Relaxed);
        count as f32 / Self::ENTRIES_PER_BUCKET as f32
    }

    /// Get access statistics
    fn access_stats(&self) -> (u64, u64) {
        (
            self.access_count.load(Ordering::Relaxed),
            self.last_access.load(Ordering::Relaxed),
        )
    }

    /// Record access for statistics
    fn record_access(&self) {
        self.access_count.fetch_add(1, Ordering::Relaxed);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.last_access.store(now, Ordering::Relaxed);
    }

    /// Get current entry count
    pub fn entry_count(&self) -> usize {
        self.entry_count.load(Ordering::Relaxed)
    }
}

/// Hash table entry
pub struct HashEntry<K, V> {
    pub key: K,
    pub value: V,
    pub hash: u64,
    pub next: AtomicPtr<HashEntry<K, V>>,
}

impl<K, V> HashEntry<K, V> {
    fn new(key: K, value: V, hash: u64) -> Self {
        Self {
            key,
            value,
            hash,
            next: AtomicPtr::new(ptr::null_mut()),
        }
    }
}

/// Dynamic resize strategy
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ResizeStrategy {
    /// Never resize automatically
    None,
    /// Resize when load factor exceeds threshold
    LoadFactor { threshold: f32 },
    /// Resize when overflow buckets exceed threshold
    OverflowThreshold { max_overflow_ratio: f32 },
    /// Adaptive strategy based on access patterns
    Adaptive {
        load_threshold: f32,
        overflow_threshold: f32,
        min_resize_interval: u64, // seconds
    },
}

impl Default for ResizeStrategy {
    fn default() -> Self {
        ResizeStrategy::Adaptive {
            load_threshold: 0.75,
            overflow_threshold: 0.25,
            min_resize_interval: 60,
        }
    }
}

/// Hash table resize statistics
#[derive(Debug, Clone, Default)]
pub struct ResizeStatistics {
    pub resize_count: u64,
    pub total_resize_time_ms: u64,
    pub max_resize_time_ms: u64,
    pub elements_rehashed: u64,
    pub last_resize_timestamp: u64,
    pub current_bucket_count: usize,
    pub overflow_bucket_count: usize,
    pub total_entries: usize,
}

impl ResizeStatistics {
    /// Get current load factor
    pub fn load_factor(&self) -> f32 {
        if self.current_bucket_count > 0 {
            self.total_entries as f32 / self.current_bucket_count as f32
        } else {
            0.0
        }
    }

    /// Get overflow ratio
    pub fn overflow_ratio(&self) -> f32 {
        if self.current_bucket_count > 0 {
            self.overflow_bucket_count as f32 / self.current_bucket_count as f32
        } else {
            0.0
        }
    }
}

/// Dynamic hash table with automatic resizing
pub struct DynamicHashTable<K: Hash + Eq + Clone, V: Clone> {
    /// Array of hash buckets
    buckets: RwLock<Vec<Box<HashBucket<K, V>>>>,
    /// Current number of buckets (must be power of 2)
    bucket_count: AtomicUsize,
    /// Total number of entries
    entry_count: AtomicUsize,
    /// Resize strategy
    resize_strategy: RwLock<ResizeStrategy>,
    /// Lock manager for coordination
    lock_manager: Arc<HierarchicalLockManager>,
    /// Epoch for memory management
    epoch: Arc<LightEpoch>,
    /// Resize statistics
    statistics: RwLock<ResizeStatistics>,
    /// Resize in progress flag
    resize_in_progress: AtomicUsize, // Acts as a counter for concurrent resizes
    /// Hash function state
    hash_seed: u64,
}

impl<K: Hash + Eq + Clone, V: Clone> DynamicHashTable<K, V> {
    const INITIAL_BUCKET_COUNT: usize = 16;
    const MAX_BUCKET_COUNT: usize = 1 << 24; // 16M buckets
    const DEFAULT_HASH_SEED: u64 = 0x51_7c_c1_b7_27_22_0a_95;

    /// Create a new dynamic hash table
    pub fn new(epoch: Arc<LightEpoch>) -> Self {
        let initial_buckets: Vec<Box<HashBucket<K, V>>> = (0..Self::INITIAL_BUCKET_COUNT)
            .map(|_| Box::new(HashBucket::new()))
            .collect();

        Self {
            buckets: RwLock::new(initial_buckets),
            bucket_count: AtomicUsize::new(Self::INITIAL_BUCKET_COUNT),
            entry_count: AtomicUsize::new(0),
            resize_strategy: RwLock::new(ResizeStrategy::default()),
            lock_manager: Arc::new(HierarchicalLockManager::new()),
            epoch,
            statistics: RwLock::new(ResizeStatistics {
                current_bucket_count: Self::INITIAL_BUCKET_COUNT,
                ..Default::default()
            }),
            resize_in_progress: AtomicUsize::new(0),
            hash_seed: Self::DEFAULT_HASH_SEED,
        }
    }

    /// Insert or update a key-value pair
    pub fn upsert(&self, key: K, value: V, _guard: &Guard) -> ContextResult<Option<V>> {
        let hash = self.calculate_hash(&key);
        let bucket_idx = self.get_bucket_index(hash);

        // Acquire bucket lock
        let lock_id = LockId::new(LockGranularity::Bucket, bucket_idx as u64);
        let _lock_guard = self.lock_manager
            .acquire_lock(lock_id, LockIntent::Write)
            .map_err(|s| ErrorContext::new(s))?;

        // Check if resize is needed before insertion
        self.check_and_trigger_resize()?;

        // Perform the actual insertion
        let result = self.upsert_internal(hash, key, value);

        if result.is_ok() {
            self.entry_count.fetch_add(1, Ordering::Relaxed);
        }

        result
    }

    /// Get a value by key
    pub fn get(&self, key: &K, _guard: &Guard) -> ContextResult<Option<V>> {
        let hash = self.calculate_hash(key);
        let bucket_idx = self.get_bucket_index(hash);

        // Acquire bucket lock for reading
        let lock_id = LockId::new(LockGranularity::Bucket, bucket_idx as u64);
        let _lock_guard = self.lock_manager
            .acquire_lock(lock_id, LockIntent::Read)
            .map_err(|s| ErrorContext::new(s))?;

        self.get_internal(hash, key)
    }

    /// Remove a key-value pair
    pub fn remove(&self, key: &K, _guard: &Guard) -> ContextResult<Option<V>> {
        let hash = self.calculate_hash(key);
        let bucket_idx = self.get_bucket_index(hash);

        // Acquire bucket lock
        let lock_id = LockId::new(LockGranularity::Bucket, bucket_idx as u64);
        let _lock_guard = self.lock_manager
            .acquire_lock(lock_id, LockIntent::Write)
            .map_err(|s| ErrorContext::new(s))?;

        let result = self.remove_internal(hash, key);

        if result.as_ref().map(|r| r.is_some()).unwrap_or(false) {
            self.entry_count.fetch_sub(1, Ordering::Relaxed);
        }

        result
    }

    /// Update resize strategy
    pub fn set_resize_strategy(&self, strategy: ResizeStrategy) -> Result<()> {
        if let Ok(mut current_strategy) = self.resize_strategy.write() {
            *current_strategy = strategy;
            Ok(())
        } else {
            Err(Status::InternalError)
        }
    }

    /// Get current statistics
    pub fn get_statistics(&self) -> ResizeStatistics {
        if let Ok(stats) = self.statistics.read() {
            let mut updated_stats = stats.clone();
            updated_stats.current_bucket_count = self.bucket_count.load(Ordering::Relaxed);
            updated_stats.total_entries = self.entry_count.load(Ordering::Relaxed);
            updated_stats
        } else {
            ResizeStatistics::default()
        }
    }

    /// Manually trigger resize
    pub fn resize(&self) -> ContextResult<()> {
        self.resize_table(self.bucket_count.load(Ordering::Relaxed) * 2)
    }

    // Private implementation methods

    fn calculate_hash(&self, key: &K) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        hasher.write_u64(self.hash_seed);
        key.hash(&mut hasher);
        hasher.finish()
    }

    fn get_bucket_index(&self, hash: u64) -> usize {
        let bucket_count = self.bucket_count.load(Ordering::Relaxed);
        (hash as usize) & (bucket_count - 1) // Assumes power of 2
    }

    fn upsert_internal(&self, hash: u64, key: K, value: V) -> ContextResult<Option<V>> {
        let buckets = self.buckets.read()
            .map_err(|_| ErrorContext::new(Status::InternalError))?;

        let bucket_idx = self.get_bucket_index(hash);
        let bucket = &buckets[bucket_idx];
        bucket.record_access();

        // First check for existing key
        for i in 0..HashBucket::<K, V>::ENTRIES_PER_BUCKET {
            let entry_ptr = bucket.entries[i].load(Ordering::Acquire);
            if !entry_ptr.is_null() {
                unsafe {
                    if (*entry_ptr).hash == hash && (*entry_ptr).key == key {
                        // Update existing entry
                        let old_value = (*entry_ptr).value.clone();
                        (*entry_ptr).value = value;
                        return Ok(Some(old_value));
                    }
                }
            }
        }

        // No existing key found, try to insert new entry
        for i in 0..HashBucket::<K, V>::ENTRIES_PER_BUCKET {
            let entry_ptr = bucket.entries[i].load(Ordering::Acquire);
            if entry_ptr.is_null() {
                // Found empty slot - insert new entry
                let new_entry = Box::into_raw(Box::new(HashEntry::new(key, value, hash)));

                match bucket.entries[i].compare_exchange(
                    entry_ptr,
                    new_entry,
                    Ordering::Release,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        bucket.entry_count.fetch_add(1, Ordering::Relaxed);
                        return Ok(None);
                    }
                    Err(_) => {
                        // Another thread inserted, clean up and return error since we can't retry
                        // (key and value have been moved)
                        unsafe { drop(Box::from_raw(new_entry)); }
                        return Err(ErrorContext::new(Status::InternalError)
                            .with_context("Concurrent insertion conflict"));
                    }
                }
            }
        }

        // No space in main bucket, would need overflow handling here
        Err(ErrorContext::new(Status::OutOfMemory)
            .with_context("Bucket is full and overflow not implemented"))
    }

    fn get_internal(&self, hash: u64, key: &K) -> ContextResult<Option<V>> {
        let buckets = self.buckets.read()
            .map_err(|_| ErrorContext::new(Status::InternalError))?;

        let bucket_idx = self.get_bucket_index(hash);
        let bucket = &buckets[bucket_idx];
        bucket.record_access();

        // Search for key in bucket
        for i in 0..HashBucket::<K, V>::ENTRIES_PER_BUCKET {
            let entry_ptr = bucket.entries[i].load(Ordering::Acquire);
            if entry_ptr.is_null() {
                continue;
            }

            unsafe {
                if (*entry_ptr).hash == hash && (*entry_ptr).key == *key {
                    return Ok(Some((*entry_ptr).value.clone()));
                }
            }
        }

        Ok(None)
    }

    fn remove_internal(&self, hash: u64, key: &K) -> ContextResult<Option<V>> {
        let buckets = self.buckets.read()
            .map_err(|_| ErrorContext::new(Status::InternalError))?;

        let bucket_idx = self.get_bucket_index(hash);
        let bucket = &buckets[bucket_idx];
        bucket.record_access();

        // Search for key in bucket
        for i in 0..HashBucket::<K, V>::ENTRIES_PER_BUCKET {
            let entry_ptr = bucket.entries[i].load(Ordering::Acquire);
            if entry_ptr.is_null() {
                continue;
            }

            unsafe {
                if (*entry_ptr).hash == hash && (*entry_ptr).key == *key {
                    let old_value = (*entry_ptr).value.clone();

                    // Remove entry by setting to null
                    bucket.entries[i].store(ptr::null_mut(), Ordering::Release);
                    bucket.entry_count.fetch_sub(1, Ordering::Relaxed);

                    // Clean up memory (in real implementation, defer this)
                    drop(Box::from_raw(entry_ptr));

                    return Ok(Some(old_value));
                }
            }
        }

        Ok(None)
    }

    fn check_and_trigger_resize(&self) -> ContextResult<()> {
        let strategy = if let Ok(strategy) = self.resize_strategy.read() {
            *strategy
        } else {
            return Err(ErrorContext::new(Status::InternalError));
        };

        let should_resize = match strategy {
            ResizeStrategy::None => false,
            ResizeStrategy::LoadFactor { threshold } => {
                let stats = self.get_statistics();
                stats.load_factor() > threshold
            }
            ResizeStrategy::OverflowThreshold { max_overflow_ratio } => {
                let stats = self.get_statistics();
                stats.overflow_ratio() > max_overflow_ratio
            }
            ResizeStrategy::Adaptive {
                load_threshold,
                overflow_threshold,
                min_resize_interval,
            } => {
                let stats = self.get_statistics();
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();

                stats.load_factor() > load_threshold
                    || stats.overflow_ratio() > overflow_threshold
                    && (now - stats.last_resize_timestamp) > min_resize_interval
            }
        };

        if should_resize {
            let current_size = self.bucket_count.load(Ordering::Relaxed);
            self.resize_table(current_size * 2)?;
        }

        Ok(())
    }

    fn resize_table(&self, new_bucket_count: usize) -> ContextResult<()> {
        // Check if resize is already in progress
        if self.resize_in_progress.fetch_add(1, Ordering::AcqRel) > 0 {
            // Another resize is in progress, just wait
            self.resize_in_progress.fetch_sub(1, Ordering::AcqRel);
            return Ok(());
        }

        let start_time = std::time::Instant::now();

        let result = self.resize_table_internal(new_bucket_count, start_time);

        // Update statistics
        let resize_time_ms = start_time.elapsed().as_millis() as u64;
        if let Ok(mut stats) = self.statistics.write() {
            stats.resize_count += 1;
            stats.total_resize_time_ms += resize_time_ms;
            if resize_time_ms > stats.max_resize_time_ms {
                stats.max_resize_time_ms = resize_time_ms;
            }
            stats.last_resize_timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
        }

        // Mark resize as complete
        self.resize_in_progress.fetch_sub(1, Ordering::AcqRel);

        result.with_context("Hash table resize failed")
    }

    fn resize_table_internal(&self, new_bucket_count: usize, start_time: std::time::Instant) -> ContextResult<()> {
        if new_bucket_count > Self::MAX_BUCKET_COUNT {
            return Err(ErrorContext::new(Status::OutOfMemory)
                .with_context("Hash table size limit exceeded"));
        }

        // Create new bucket array
        let new_buckets: Vec<Box<HashBucket<K, V>>> = (0..new_bucket_count)
            .map(|_| Box::new(HashBucket::new()))
            .collect();

        let mut rehashed_count = 0u64;

        // Rehash all existing entries
        {
            let old_buckets = self.buckets.read()
                .map_err(|_| ErrorContext::new(Status::InternalError))?;

            for bucket in old_buckets.iter() {
                for i in 0..HashBucket::<K, V>::ENTRIES_PER_BUCKET {
                    let entry_ptr = bucket.entries[i].load(Ordering::Acquire);
                    if entry_ptr.is_null() {
                        continue;
                    }

                    unsafe {
                        let entry = &*entry_ptr;
                        let new_bucket_idx = (entry.hash as usize) & (new_bucket_count - 1);
                        let new_bucket = &new_buckets[new_bucket_idx];

                        // Find empty slot in new bucket
                        for j in 0..HashBucket::<K, V>::ENTRIES_PER_BUCKET {
                            if new_bucket.entries[j].load(Ordering::Relaxed).is_null() {
                                let new_entry = Box::into_raw(Box::new(HashEntry::new(
                                    entry.key.clone(),
                                    entry.value.clone(),
                                    entry.hash,
                                )));

                                new_bucket.entries[j].store(new_entry, Ordering::Release);
                                new_bucket.entry_count.fetch_add(1, Ordering::Relaxed);
                                rehashed_count += 1;
                                break;
                            }
                        }
                    }
                }
            }
        }

        // Replace old buckets with new ones
        {
            let mut buckets_guard = self.buckets.write()
                .map_err(|_| ErrorContext::new(Status::InternalError))?;
            *buckets_guard = new_buckets;
        }

        // Update bucket count
        self.bucket_count.store(new_bucket_count, Ordering::Release);

        // Update statistics
        if let Ok(mut stats) = self.statistics.write() {
            stats.elements_rehashed += rehashed_count;
            stats.current_bucket_count = new_bucket_count;
        }

        log::info!(
            "Hash table resized from {} to {} buckets, rehashed {} entries in {:?}",
            new_bucket_count / 2,
            new_bucket_count,
            rehashed_count,
            start_time.elapsed()
        );

        Ok(())
    }
}

impl<K: Hash + Eq + Clone, V: Clone> Drop for DynamicHashTable<K, V> {
    fn drop(&mut self) {
        // Clean up all entries
        if let Ok(buckets) = self.buckets.read() {
            for bucket in buckets.iter() {
                for i in 0..HashBucket::<K, V>::ENTRIES_PER_BUCKET {
                    let entry_ptr = bucket.entries[i].load(Ordering::Relaxed);
                    if !entry_ptr.is_null() {
                        unsafe {
                            drop(Box::from_raw(entry_ptr));
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_hash_table_creation() {
        let epoch = Arc::new(LightEpoch::new());
        let table: DynamicHashTable<u64, String> = DynamicHashTable::new(epoch);

        let stats = table.get_statistics();
        assert_eq!(stats.current_bucket_count, DynamicHashTable::<u64, String>::INITIAL_BUCKET_COUNT);
        assert_eq!(stats.total_entries, 0);
    }

    #[test]
    fn test_basic_operations() {
        let epoch = Arc::new(LightEpoch::new());
        let table = DynamicHashTable::new(epoch.clone());
        let guard = epoch.protect();

        // Insert
        let result = table.upsert(1u64, "value1".to_string(), &guard);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);

        // Get
        let result = table.get(&1u64, &guard);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some("value1".to_string()));

        // Update
        let result = table.upsert(1u64, "value2".to_string(), &guard);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some("value1".to_string()));

        // Remove
        let result = table.remove(&1u64, &guard);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some("value2".to_string()));

        // Get after remove
        let result = table.get(&1u64, &guard);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn test_resize_strategy() {
        let epoch = Arc::new(LightEpoch::new());
        let table: DynamicHashTable<i32, String> = DynamicHashTable::new(epoch);

        let strategy = ResizeStrategy::LoadFactor { threshold: 0.5 };
        assert!(table.set_resize_strategy(strategy).is_ok());

        // Verify strategy was set
        let stats = table.get_statistics();
        assert_eq!(stats.current_bucket_count, DynamicHashTable::<u64, String>::INITIAL_BUCKET_COUNT);
    }
}
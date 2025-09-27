use crate::core::status::{Status, Result, ContextResult, ErrorContext};
use crate::core::malloc_fixed_page_size::{FixedPageAddress, MallocFixedPageSize};
use crate::core::light_epoch::{LightEpoch, Guard};
use crate::index::hash_bucket::HashBucketEntry;
use crate::index::dynamic_hash_table::HashBucket;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::RwLock;
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Overflow bucket management strategies
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OverflowStrategy {
    /// Linear probing within bucket groups
    LinearProbing { max_distance: u8 },
    /// Chaining with separate overflow areas
    Chaining { max_chain_length: u8 },
    /// Hybrid approach combining both strategies
    Hybrid {
        probing_distance: u8,
        max_chain_length: u8,
    },
    /// Adaptive strategy that changes based on load patterns
    Adaptive,
}

impl Default for OverflowStrategy {
    fn default() -> Self {
        OverflowStrategy::Hybrid {
            probing_distance: 3,
            max_chain_length: 4,
        }
    }
}

/// Overflow bucket statistics for monitoring and optimization
#[derive(Debug, Clone, Default)]
pub struct OverflowStatistics {
    /// Total number of overflow buckets allocated
    pub total_overflow_buckets: u64,
    /// Number of overflow buckets currently in use
    pub active_overflow_buckets: u64,
    /// Average chain length for overflow buckets
    pub average_chain_length: f32,
    /// Maximum chain length observed
    pub max_chain_length: u32,
    /// Number of entries that required overflow
    pub overflow_entries: u64,
    /// Number of successful lookups in overflow areas
    pub overflow_hits: u64,
    /// Number of failed lookups in overflow areas
    pub overflow_misses: u64,
    /// Distribution of chain lengths
    pub chain_length_distribution: HashMap<u32, u32>,
    /// Time spent in overflow operations
    pub total_overflow_time_ns: u64,
    /// Number of overflow bucket consolidations performed
    pub consolidations_performed: u64,
}

impl OverflowStatistics {
    /// Calculate overflow ratio
    pub fn overflow_ratio(&self) -> f32 {
        if self.overflow_entries > 0 {
            self.total_overflow_buckets as f32 / self.overflow_entries as f32
        } else {
            0.0
        }
    }

    /// Calculate overflow hit ratio
    pub fn hit_ratio(&self) -> f32 {
        let total_accesses = self.overflow_hits + self.overflow_misses;
        if total_accesses > 0 {
            self.overflow_hits as f32 / total_accesses as f32
        } else {
            0.0
        }
    }

    /// Get most common chain length
    pub fn most_common_chain_length(&self) -> Option<(u32, u32)> {
        self.chain_length_distribution
            .iter()
            .max_by_key(|(_, count)| *count)
            .map(|(length, count)| (*length, *count))
    }
}

/// Enhanced overflow bucket with metadata
pub struct EnhancedOverflowBucket<K, V> {
    /// Base hash bucket functionality
    pub base: HashBucket<K, V>,
    /// Access frequency for hot/cold detection
    access_frequency: AtomicU64,
    /// Last access timestamp
    last_access_time: AtomicU64,
    /// Chain position (0 for primary bucket)
    chain_position: AtomicU32,
    /// Next bucket in chain
    next_bucket: AtomicU64, // Stores FixedPageAddress as u64
    /// Load factor of this specific bucket
    load_factor: AtomicU32, // Stored as fixed-point (factor * 1000)
    /// Bucket health score (0-100)
    health_score: AtomicU32,
}

impl<K, V> EnhancedOverflowBucket<K, V> {
    pub fn new() -> Self {
        Self {
            base: HashBucket::new(),
            access_frequency: AtomicU64::new(0),
            last_access_time: AtomicU64::new(0),
            chain_position: AtomicU32::new(0),
            next_bucket: AtomicU64::new(0),
            load_factor: AtomicU32::new(0),
            health_score: AtomicU32::new(100),
        }
    }

    /// Record an access to this bucket
    pub fn record_access(&self) {
        self.access_frequency.fetch_add(1, Ordering::Relaxed);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.last_access_time.store(now, Ordering::Relaxed);
        self.update_health_score();
    }

    /// Update the health score based on various factors
    fn update_health_score(&self) {
        let load_factor = self.load_factor.load(Ordering::Relaxed) as f32 / 1000.0;
        let chain_pos = self.chain_position.load(Ordering::Relaxed);

        // Calculate health score (0-100)
        let mut score = 100.0;

        // Penalize high load factor
        if load_factor > 0.8 {
            score -= (load_factor - 0.8) * 100.0;
        }

        // Penalize deep chain position
        score -= (chain_pos as f32) * 5.0;

        // Consider access recency
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let last_access = self.last_access_time.load(Ordering::Relaxed);
        let time_since_access = now.saturating_sub(last_access);

        if time_since_access > 3600 { // 1 hour
            score -= 20.0;
        } else if time_since_access > 300 { // 5 minutes
            score -= 10.0;
        }

        let final_score = score.max(0.0).min(100.0) as u32;
        self.health_score.store(final_score, Ordering::Relaxed);
    }

    /// Get current health metrics
    pub fn get_health_metrics(&self) -> (u32, u64, u64, u32) {
        (
            self.health_score.load(Ordering::Relaxed),
            self.access_frequency.load(Ordering::Relaxed),
            self.last_access_time.load(Ordering::Relaxed),
            self.chain_position.load(Ordering::Relaxed),
        )
    }
}

impl<K, V> Default for EnhancedOverflowBucket<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

/// Enhanced overflow bucket manager
pub struct EnhancedOverflowManager<'epoch, K, V> {
    /// Strategy for handling overflows
    strategy: RwLock<OverflowStrategy>,
    /// Fixed-size allocator for overflow buckets
    allocator: MallocFixedPageSize<'epoch, EnhancedOverflowBucket<K, V>>,
    /// Statistics tracking
    statistics: RwLock<OverflowStatistics>,
    /// Epoch for memory management
    epoch: &'epoch LightEpoch,
    /// Consolidation threshold (number of operations before consolidation)
    consolidation_threshold: AtomicUsize,
    /// Operations since last consolidation
    operations_since_consolidation: AtomicUsize,
    /// Background consolidation enabled
    auto_consolidation: AtomicUsize, // 0 = disabled, 1 = enabled
}

impl<'epoch, K, V> EnhancedOverflowManager<'epoch, K, V> {
    const DEFAULT_CONSOLIDATION_THRESHOLD: usize = 10000;

    pub fn new(epoch: &'epoch LightEpoch) -> Self {
        let mut allocator = MallocFixedPageSize::new();
        allocator.initialize(64, epoch); // 64-byte alignment for cache efficiency

        Self {
            strategy: RwLock::new(OverflowStrategy::default()),
            allocator,
            statistics: RwLock::new(OverflowStatistics::default()),
            epoch,
            consolidation_threshold: AtomicUsize::new(Self::DEFAULT_CONSOLIDATION_THRESHOLD),
            operations_since_consolidation: AtomicUsize::new(0),
            auto_consolidation: AtomicUsize::new(1),
        }
    }

    /// Set overflow handling strategy
    pub fn set_strategy(&self, strategy: OverflowStrategy) -> Result<()> {
        if let Ok(mut current_strategy) = self.strategy.write() {
            *current_strategy = strategy;
            Ok(())
        } else {
            Err(Status::InternalError)
        }
    }

    /// Allocate a new overflow bucket
    pub fn allocate_overflow_bucket(&self, _guard: &Guard) -> ContextResult<FixedPageAddress> {
        let start_time = Instant::now();
        let address = self.allocator.allocate();

        // Initialize the bucket
        unsafe {
            let bucket = self.allocator.get_unchecked(address);
            *bucket = EnhancedOverflowBucket::new();
        }

        // Update statistics
        self.update_allocation_statistics(start_time.elapsed());
        self.check_consolidation_trigger();

        log::debug!("Allocated overflow bucket at {:?}", address);
        Ok(address)
    }

    /// Insert entry with overflow handling
    pub fn insert_with_overflow(
        &self,
        primary_bucket: &mut HashBucket<K, V>,
        entry: HashBucketEntry,
        guard: &Guard,
    ) -> ContextResult<bool> {
        let strategy = if let Ok(strategy) = self.strategy.read() {
            *strategy
        } else {
            return Err(ErrorContext::new(Status::InternalError));
        };

        match strategy {
            OverflowStrategy::LinearProbing { max_distance } => {
                self.insert_with_probing(primary_bucket, entry, max_distance, guard)
            }
            OverflowStrategy::Chaining { max_chain_length } => {
                self.insert_with_chaining(primary_bucket, entry, max_chain_length, guard)
            }
            OverflowStrategy::Hybrid { probing_distance, max_chain_length } => {
                // Try probing first, then chaining
                match self.insert_with_probing(primary_bucket, entry, probing_distance, guard) {
                    Ok(true) => Ok(true),
                    Ok(false) | Err(_) => {
                        self.insert_with_chaining(primary_bucket, entry, max_chain_length, guard)
                    }
                }
            }
            OverflowStrategy::Adaptive => {
                self.insert_with_adaptive_strategy(primary_bucket, entry, guard)
            }
        }
    }

    /// Search for entry in overflow areas
    pub fn search_overflow(
        &self,
        primary_bucket: &HashBucket<K, V>,
        tag: u16,
        guard: &Guard,
    ) -> ContextResult<Option<HashBucketEntry>> {
        let start_time = Instant::now();
        let mut search_depth = 0;

        let result = self.search_overflow_internal(primary_bucket, tag, guard, &mut search_depth);

        // Update search statistics
        self.update_search_statistics(start_time.elapsed(), search_depth, result.is_ok());

        result
    }

    /// Consolidate overflow buckets to improve performance
    pub fn consolidate_overflow_buckets(&self, guard: &Guard) -> ContextResult<u32> {
        let start_time = Instant::now();
        // This would implement logic to:
        // 1. Identify underutilized overflow buckets
        // 2. Merge entries into primary buckets where possible
        // 3. Deallocate empty overflow buckets
        // 4. Reorganize chain structures for better performance

        // For now, return a placeholder
        let consolidated_count = self.consolidate_unhealthy_buckets(guard)?;

        // Update statistics
        if let Ok(mut stats) = self.statistics.write() {
            stats.consolidations_performed += 1;
            stats.total_overflow_time_ns += start_time.elapsed().as_nanos() as u64;
        }

        // Reset consolidation counter
        self.operations_since_consolidation.store(0, Ordering::Relaxed);

        log::info!("Consolidated {} overflow buckets in {:?}",
                  consolidated_count, start_time.elapsed());

        Ok(consolidated_count)
    }

    /// Get current overflow statistics
    pub fn get_statistics(&self) -> OverflowStatistics {
        self.statistics.read()
            .map(|s| s.clone())
            .unwrap_or_default()
    }

    /// Configure consolidation parameters
    pub fn configure_consolidation(&self, threshold: usize, auto_enabled: bool) {
        self.consolidation_threshold.store(threshold, Ordering::Relaxed);
        self.auto_consolidation.store(if auto_enabled { 1 } else { 0 }, Ordering::Relaxed);
    }

    // Private implementation methods

    fn insert_with_probing(
        &self,
        _primary_bucket: &mut HashBucket<K, V>,
        _entry: HashBucketEntry,
        _max_distance: u8,
        _guard: &Guard,
    ) -> ContextResult<bool> {
        // Placeholder for linear probing implementation
        Ok(false)
    }

    fn insert_with_chaining(
        &self,
        _primary_bucket: &mut HashBucket<K, V>,
        _entry: HashBucketEntry,
        _max_chain_length: u8,
        _guard: &Guard,
    ) -> ContextResult<bool> {
        // Placeholder for chaining implementation
        Ok(false)
    }

    fn insert_with_adaptive_strategy(
        &self,
        primary_bucket: &mut HashBucket<K, V>,
        entry: HashBucketEntry,
        guard: &Guard,
    ) -> ContextResult<bool> {
        // Adaptive strategy based on current load and access patterns
        let stats = self.get_statistics();

        if stats.average_chain_length < 2.0 && stats.hit_ratio() > 0.8 {
            // Good performance with chaining, continue using it
            self.insert_with_chaining(primary_bucket, entry, 4, guard)
        } else if stats.overflow_ratio() < 0.3 {
            // Low overflow, try probing first
            match self.insert_with_probing(primary_bucket, entry, 3, guard) {
                Ok(true) => Ok(true),
                _ => self.insert_with_chaining(primary_bucket, entry, 2, guard),
            }
        } else {
            // High overflow, be conservative
            self.insert_with_probing(primary_bucket, entry, 2, guard)
        }
    }

    fn search_overflow_internal(
        &self,
        _primary_bucket: &HashBucket<K, V>,
        _tag: u16,
        _guard: &Guard,
        search_depth: &mut u32,
    ) -> ContextResult<Option<HashBucketEntry>> {
        // Placeholder for overflow search implementation
        *search_depth = 1;
        Ok(None)
    }

    fn consolidate_unhealthy_buckets(&self, _guard: &Guard) -> ContextResult<u32> {
        // Identify buckets with low health scores and consolidate them
        // This would implement the actual consolidation logic
        // For now, return a simulated result
        let consolidated = 0;

        Ok(consolidated)
    }

    fn update_allocation_statistics(&self, duration: Duration) {
        if let Ok(mut stats) = self.statistics.write() {
            stats.total_overflow_buckets += 1;
            stats.active_overflow_buckets += 1;
            stats.total_overflow_time_ns += duration.as_nanos() as u64;
        }
    }

    fn update_search_statistics(&self, duration: Duration, depth: u32, found: bool) {
        if let Ok(mut stats) = self.statistics.write() {
            if found {
                stats.overflow_hits += 1;
            } else {
                stats.overflow_misses += 1;
            }

            stats.total_overflow_time_ns += duration.as_nanos() as u64;

            // Update chain length distribution
            *stats.chain_length_distribution.entry(depth).or_insert(0) += 1;

            if depth > stats.max_chain_length {
                stats.max_chain_length = depth;
            }

            // Recalculate average chain length
            let total_searches = stats.overflow_hits + stats.overflow_misses;
            if total_searches > 0 {
                let weighted_sum: u32 = stats.chain_length_distribution
                    .iter()
                    .map(|(length, count)| length * count)
                    .sum();
                stats.average_chain_length = weighted_sum as f32 / total_searches as f32;
            }
        }
    }

    fn check_consolidation_trigger(&self) {
        let ops = self.operations_since_consolidation.fetch_add(1, Ordering::Relaxed);
        let threshold = self.consolidation_threshold.load(Ordering::Relaxed);
        let auto_enabled = self.auto_consolidation.load(Ordering::Relaxed) == 1;

        if auto_enabled && ops >= threshold {
            // Trigger background consolidation (in a real implementation)
            log::debug!("Overflow consolidation trigger reached ({} operations)", ops);
        }
    }
}

/// Trait for enhanced overflow bucket operations
pub trait OverflowBucketOps {
    /// Check if bucket needs overflow handling
    fn needs_overflow(&self) -> bool;

    /// Get overflow utilization ratio
    fn overflow_utilization(&self) -> f32;

    /// Optimize bucket layout
    fn optimize_layout(&mut self) -> Result<()>;
}

impl<K, V> OverflowBucketOps for HashBucket<K, V> {
    fn needs_overflow(&self) -> bool {
        // Simple heuristic - bucket is considered full at 85% capacity
        self.entry_count() as f32 / HashBucket::<K, V>::ENTRIES_PER_BUCKET as f32 > 0.85
    }

    fn overflow_utilization(&self) -> f32 {
        self.entry_count() as f32 / HashBucket::<K, V>::ENTRIES_PER_BUCKET as f32
    }

    fn optimize_layout(&mut self) -> Result<()> {
        // Placeholder for bucket optimization logic
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_overflow_manager_creation() {
        let epoch = LightEpoch::new();
        let manager: EnhancedOverflowManager<i32, String> = EnhancedOverflowManager::new(&epoch);

        let stats = manager.get_statistics();
        assert_eq!(stats.total_overflow_buckets, 0);
        assert_eq!(stats.active_overflow_buckets, 0);
    }

    #[test]
    fn test_strategy_configuration() {
        let epoch = LightEpoch::new();
        let manager: EnhancedOverflowManager<i32, String> = EnhancedOverflowManager::new(&epoch);

        let strategy = OverflowStrategy::LinearProbing { max_distance: 5 };
        assert!(manager.set_strategy(strategy).is_ok());
    }

    #[test]
    fn test_consolidation_configuration() {
        let epoch = LightEpoch::new();
        let manager: EnhancedOverflowManager<i32, String> = EnhancedOverflowManager::new(&epoch);

        manager.configure_consolidation(5000, false);

        assert_eq!(manager.consolidation_threshold.load(Ordering::Relaxed), 5000);
        assert_eq!(manager.auto_consolidation.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_enhanced_overflow_bucket() {
        let bucket: EnhancedOverflowBucket<i32, String> = EnhancedOverflowBucket::new();

        // Test initial state
        let (health, frequency, _, position) = bucket.get_health_metrics();
        assert_eq!(health, 100);
        assert_eq!(frequency, 0);
        assert_eq!(position, 0);

        // Test access recording
        bucket.record_access();
        let (_, frequency_after, _, _) = bucket.get_health_metrics();
        assert_eq!(frequency_after, 1);
    }
}
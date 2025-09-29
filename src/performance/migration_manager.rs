use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

/// Migration strategy determines when and how to migrate data between hot and cold storage
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MigrationStrategy {
    /// Migrate based on access frequency
    AccessFrequency,
    /// Migrate based on least recently used (LRU)
    LeastRecentlyUsed,
    /// Migrate based on access recency and frequency combined
    AdaptiveRecencyFrequency,
    /// Migrate based on cost-benefit analysis
    CostBenefit,
}

/// Statistics for a single key
#[derive(Debug, Default)]
pub struct KeyStats {
    /// Total number of accesses
    pub access_count: AtomicU64,
    /// Last access timestamp (in milliseconds since epoch)
    pub last_access_time: AtomicU64,
    /// Whether this key is currently in hot storage
    pub in_hot: std::sync::atomic::AtomicBool,
    /// Size of the key-value pair in bytes
    pub size_bytes: AtomicUsize,
}

impl KeyStats {
    pub fn new(size_bytes: usize) -> Self {
        Self {
            access_count: AtomicU64::new(0),
            last_access_time: AtomicU64::new(0),
            in_hot: std::sync::atomic::AtomicBool::new(false),
            size_bytes: AtomicUsize::new(size_bytes),
        }
    }

    pub fn record_access(&self, timestamp_ms: u64) {
        self.access_count.fetch_add(1, Ordering::Relaxed);
        self.last_access_time.store(timestamp_ms, Ordering::Relaxed);
    }

    pub fn get_access_count(&self) -> u64 {
        self.access_count.load(Ordering::Relaxed)
    }

    pub fn get_last_access_time(&self) -> u64 {
        self.last_access_time.load(Ordering::Relaxed)
    }

    pub fn is_in_hot(&self) -> bool {
        self.in_hot.load(Ordering::Relaxed)
    }

    pub fn set_in_hot(&self, value: bool) {
        self.in_hot.store(value, Ordering::Relaxed);
    }

    pub fn get_size(&self) -> usize {
        self.size_bytes.load(Ordering::Relaxed)
    }
}

/// Migration policy configuration
#[derive(Debug, Clone)]
pub struct MigrationConfig {
    /// Strategy to use for migration decisions
    pub strategy: MigrationStrategy,
    /// Minimum access count before considering for hot storage
    pub min_access_threshold: u64,
    /// Time window for access frequency calculation (in seconds)
    pub time_window_secs: u64,
    /// Maximum size of hot storage (in bytes)
    pub max_hot_size_bytes: usize,
    /// Target hot storage utilization (0.0 to 1.0)
    pub target_hot_utilization: f64,
    /// Batch size for migration operations
    pub migration_batch_size: usize,
    /// Enable adaptive threshold adjustment
    pub adaptive_threshold: bool,
}

impl Default for MigrationConfig {
    fn default() -> Self {
        Self {
            strategy: MigrationStrategy::AdaptiveRecencyFrequency,
            min_access_threshold: 2,
            time_window_secs: 300, // 5 minutes
            max_hot_size_bytes: 1 << 30, // 1GB
            target_hot_utilization: 0.8,
            migration_batch_size: 128,
            adaptive_threshold: true,
        }
    }
}

/// Migration manager for intelligent hot/cold data migration
pub struct MigrationManager {
    config: MigrationConfig,
    current_hot_size: AtomicUsize,
    total_migrations: AtomicU64,
    successful_migrations: AtomicU64,
    start_time: Instant,
}

impl MigrationManager {
    pub fn new(config: MigrationConfig) -> Self {
        Self {
            config,
            current_hot_size: AtomicUsize::new(0),
            total_migrations: AtomicU64::new(0),
            successful_migrations: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }

    /// Determine if a key should be migrated to hot storage
    pub fn should_migrate_to_hot(&self, stats: &KeyStats, current_time_ms: u64) -> bool {
        let access_count = stats.get_access_count();
        let last_access_time = stats.get_last_access_time();
        let size = stats.get_size();

        // Check if key is already in hot storage
        if stats.is_in_hot() {
            return false;
        }

        // Check basic threshold
        if access_count < self.config.min_access_threshold {
            return false;
        }

        // Check if we have space in hot storage
        let current_size = self.current_hot_size.load(Ordering::Relaxed);
        if current_size + size > self.config.max_hot_size_bytes {
            return false;
        }

        // Apply strategy-specific logic
        match self.config.strategy {
            MigrationStrategy::AccessFrequency => {
                self.check_access_frequency(access_count, current_time_ms, last_access_time)
            }
            MigrationStrategy::LeastRecentlyUsed => {
                self.check_lru(current_time_ms, last_access_time)
            }
            MigrationStrategy::AdaptiveRecencyFrequency => {
                self.check_adaptive(access_count, current_time_ms, last_access_time)
            }
            MigrationStrategy::CostBenefit => {
                self.check_cost_benefit(access_count, size, current_time_ms, last_access_time)
            }
        }
    }

    /// Determine if a key should be evicted from hot storage
    pub fn should_evict_from_hot(&self, stats: &KeyStats, current_time_ms: u64) -> bool {
        // Check if key is in hot storage
        if !stats.is_in_hot() {
            return false;
        }

        // Check if hot storage is over capacity
        let current_size = self.current_hot_size.load(Ordering::Relaxed);
        let target_size = (self.config.max_hot_size_bytes as f64
            * self.config.target_hot_utilization) as usize;

        if current_size <= target_size {
            return false;
        }

        // Use LRU for eviction
        let last_access_time = stats.get_last_access_time();
        let age_ms = current_time_ms.saturating_sub(last_access_time);
        let age_secs = age_ms / 1000;

        // Evict if not accessed in the time window
        age_secs >= self.config.time_window_secs
    }

    /// Record a successful migration to hot storage
    pub fn record_migration_to_hot(&self, size: usize) {
        self.current_hot_size.fetch_add(size, Ordering::Relaxed);
        self.total_migrations.fetch_add(1, Ordering::Relaxed);
        self.successful_migrations
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successful eviction from hot storage
    pub fn record_eviction_from_hot(&self, size: usize) {
        self.current_hot_size.fetch_sub(size, Ordering::Relaxed);
    }

    /// Get current statistics
    pub fn get_stats(&self) -> MigrationStats {
        MigrationStats {
            current_hot_size: self.current_hot_size.load(Ordering::Relaxed),
            max_hot_size: self.config.max_hot_size_bytes,
            total_migrations: self.total_migrations.load(Ordering::Relaxed),
            successful_migrations: self.successful_migrations.load(Ordering::Relaxed),
            uptime_secs: self.start_time.elapsed().as_secs(),
        }
    }

    // Helper methods for different strategies

    fn check_access_frequency(
        &self,
        access_count: u64,
        current_time_ms: u64,
        last_access_time: u64,
    ) -> bool {
        let age_ms = current_time_ms.saturating_sub(last_access_time);
        let age_secs = age_ms / 1000;

        if age_secs == 0 {
            return true; // Very recent access
        }

        // Calculate access frequency (accesses per second)
        let frequency = access_count as f64 / age_secs.max(1) as f64;

        // Dynamic threshold based on adaptive mode
        let threshold = if self.config.adaptive_threshold {
            self.calculate_adaptive_threshold()
        } else {
            1.0 // Default: 1 access per second
        };

        frequency >= threshold
    }

    fn check_lru(&self, current_time_ms: u64, last_access_time: u64) -> bool {
        let age_ms = current_time_ms.saturating_sub(last_access_time);
        let age_secs = age_ms / 1000;

        // Migrate if accessed within the time window
        age_secs <= self.config.time_window_secs / 2
    }

    fn check_adaptive(
        &self,
        access_count: u64,
        current_time_ms: u64,
        last_access_time: u64,
    ) -> bool {
        let age_ms = current_time_ms.saturating_sub(last_access_time);
        let age_secs = age_ms / 1000;

        // Combine recency and frequency
        let recency_score = if age_secs <= self.config.time_window_secs / 4 {
            1.0
        } else if age_secs <= self.config.time_window_secs / 2 {
            0.5
        } else {
            0.1
        };

        let frequency_score = if age_secs == 0 {
            access_count as f64
        } else {
            access_count as f64 / age_secs.max(1) as f64
        };

        // Combined score
        let score = recency_score * 0.6 + frequency_score.min(10.0) * 0.4;

        score >= 1.0
    }

    fn check_cost_benefit(
        &self,
        access_count: u64,
        size: usize,
        current_time_ms: u64,
        last_access_time: u64,
    ) -> bool {
        let age_ms = current_time_ms.saturating_sub(last_access_time);
        let age_secs = age_ms / 1000;

        // Calculate benefit (expected future accesses)
        let frequency = if age_secs == 0 {
            access_count as f64
        } else {
            access_count as f64 / age_secs.max(1) as f64
        };

        // Calculate cost (storage overhead)
        let cost = size as f64 / 1024.0; // Cost in KB

        // Benefit-to-cost ratio
        let ratio = frequency / cost.max(1.0);

        ratio >= 0.1 // Threshold for migration
    }

    fn calculate_adaptive_threshold(&self) -> f64 {
        let current_size = self.current_hot_size.load(Ordering::Relaxed);
        let utilization =
            current_size as f64 / self.config.max_hot_size_bytes.max(1) as f64;

        // Increase threshold as utilization increases
        if utilization < 0.5 {
            0.5 // Low threshold when plenty of space
        } else if utilization < 0.8 {
            1.0 // Medium threshold
        } else {
            2.0 // High threshold when space is limited
        }
    }
}

/// Statistics about migration operations
#[derive(Debug, Clone)]
pub struct MigrationStats {
    pub current_hot_size: usize,
    pub max_hot_size: usize,
    pub total_migrations: u64,
    pub successful_migrations: u64,
    pub uptime_secs: u64,
}

impl MigrationStats {
    pub fn hot_utilization(&self) -> f64 {
        self.current_hot_size as f64 / self.max_hot_size.max(1) as f64
    }

    pub fn success_rate(&self) -> f64 {
        if self.total_migrations == 0 {
            0.0
        } else {
            self.successful_migrations as f64 / self.total_migrations as f64
        }
    }

    pub fn migrations_per_second(&self) -> f64 {
        if self.uptime_secs == 0 {
            0.0
        } else {
            self.total_migrations as f64 / self.uptime_secs as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_stats() {
        let stats = KeyStats::new(1024);
        assert_eq!(stats.get_access_count(), 0);
        assert_eq!(stats.get_size(), 1024);
        assert!(!stats.is_in_hot());

        stats.record_access(1000);
        assert_eq!(stats.get_access_count(), 1);
        assert_eq!(stats.get_last_access_time(), 1000);

        stats.set_in_hot(true);
        assert!(stats.is_in_hot());
    }

    #[test]
    fn test_migration_manager_threshold() {
        let config = MigrationConfig {
            min_access_threshold: 5,
            ..Default::default()
        };
        let manager = MigrationManager::new(config);

        let stats = KeyStats::new(1024);
        stats.record_access(1000);

        // Should not migrate with only 1 access
        assert!(!manager.should_migrate_to_hot(&stats, 2000));

        // Add more accesses
        for _ in 0..5 {
            stats.record_access(2000);
        }

        // Should migrate now
        assert!(manager.should_migrate_to_hot(&stats, 3000));
    }

    #[test]
    fn test_migration_stats() {
        let config = MigrationConfig::default();
        let manager = MigrationManager::new(config);

        manager.record_migration_to_hot(1024);
        manager.record_migration_to_hot(2048);

        let stats = manager.get_stats();
        assert_eq!(stats.current_hot_size, 3072);
        assert_eq!(stats.total_migrations, 2);
    }

    #[test]
    fn test_eviction_logic() {
        let config = MigrationConfig {
            time_window_secs: 100,
            max_hot_size_bytes: 1000,
            target_hot_utilization: 0.8,
            ..Default::default()
        };
        let manager = MigrationManager::new(config);

        let stats = KeyStats::new(500);
        stats.set_in_hot(true);
        stats.record_access(1000);

        // Set hot storage size to trigger eviction check
        manager.record_migration_to_hot(900);

        // Should evict old entries when over capacity
        assert!(manager.should_evict_from_hot(&stats, 200000));
    }
}
//! Performance metrics collection for rskv
//!
//! This module provides comprehensive performance monitoring capabilities
//! including operation counters, latency tracking, and resource utilization.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Global metrics collector for the rskv system
#[derive(Debug)]
pub struct MetricsCollector {
    /// Operation counters
    operations: OperationMetrics,
    /// Latency tracking
    latency: LatencyMetrics,
    /// Storage metrics
    storage: StorageMetrics,
    /// Memory metrics
    memory: MemoryMetrics,
    /// Background task metrics
    background: BackgroundMetrics,
    /// Error metrics
    errors: ErrorMetrics,
    /// Start time for uptime calculation
    start_time: Instant,
}

/// Operation-specific metrics
#[derive(Debug, Default)]
pub struct OperationMetrics {
    /// Total read operations
    pub reads_total: AtomicU64,
    /// Total write operations  
    pub writes_total: AtomicU64,
    /// Total delete operations
    pub deletes_total: AtomicU64,
    /// Total scan operations
    pub scans_total: AtomicU64,
    /// Read cache hits
    pub read_cache_hits: AtomicU64,
    /// Read cache misses
    pub read_cache_misses: AtomicU64,
    /// Bytes read
    pub bytes_read: AtomicU64,
    /// Bytes written
    pub bytes_written: AtomicU64,
}

/// Latency tracking metrics
#[derive(Debug)]
pub struct LatencyMetrics {
    /// Read operation latencies (in microseconds)
    pub read_latencies: RwLock<LatencyHistogram>,
    /// Write operation latencies
    pub write_latencies: RwLock<LatencyHistogram>,
    /// Delete operation latencies
    pub delete_latencies: RwLock<LatencyHistogram>,
    /// Scan operation latencies
    pub scan_latencies: RwLock<LatencyHistogram>,
}

/// Storage-related metrics
#[derive(Debug, Default)]
pub struct StorageMetrics {
    /// Disk read operations
    pub disk_reads: AtomicU64,
    /// Disk write operations
    pub disk_writes: AtomicU64,
    /// Disk bytes read
    pub disk_bytes_read: AtomicU64,
    /// Disk bytes written
    pub disk_bytes_written: AtomicU64,
    /// Disk flush operations
    pub disk_flushes: AtomicU64,
    /// Disk sync operations
    pub disk_syncs: AtomicU64,
}

/// Memory-related metrics
#[derive(Debug, Default)]
pub struct MemoryMetrics {
    /// Current memory usage in bytes
    pub current_memory_usage: AtomicU64,
    /// Peak memory usage in bytes
    pub peak_memory_usage: AtomicU64,
    /// Number of pages allocated
    pub pages_allocated: AtomicUsize,
    /// Number of pages evicted
    pub pages_evicted: AtomicUsize,
    /// Number of memory mappings
    pub mmap_count: AtomicUsize,
    /// Total memory mapped size
    pub mmap_size: AtomicU64,
}

/// Background task metrics
#[derive(Debug, Default)]
pub struct BackgroundMetrics {
    /// Number of checkpoints completed
    pub checkpoints_completed: AtomicU64,
    /// Number of checkpoint failures
    pub checkpoint_failures: AtomicU64,
    /// Total checkpoint duration (in milliseconds)
    pub checkpoint_duration_ms: AtomicU64,
    /// Number of GC cycles completed
    pub gc_cycles_completed: AtomicU64,
    /// Number of GC failures
    pub gc_failures: AtomicU64,
    /// Total GC duration (in milliseconds)
    pub gc_duration_ms: AtomicU64,
    /// Bytes reclaimed by GC
    pub gc_bytes_reclaimed: AtomicU64,
}

/// Error tracking metrics
#[derive(Debug, Default)]
pub struct ErrorMetrics {
    /// Total number of errors
    pub total_errors: AtomicU64,
    /// IO errors
    pub io_errors: AtomicU64,
    /// Serialization errors
    pub serialization_errors: AtomicU64,
    /// Corruption errors
    pub corruption_errors: AtomicU64,
    /// Configuration errors
    pub config_errors: AtomicU64,
    /// Timeout errors
    pub timeout_errors: AtomicU64,
    /// Resource exhaustion errors
    pub resource_exhausted_errors: AtomicU64,
}

/// Latency histogram for tracking operation latencies
#[derive(Debug)]
pub struct LatencyHistogram {
    /// Bucket boundaries in microseconds
    buckets: Vec<u64>,
    /// Count of operations in each bucket
    counts: Vec<AtomicU64>,
    /// Total count of operations
    total_count: AtomicU64,
    /// Sum of all latencies for average calculation
    total_sum: AtomicU64,
    /// Minimum latency observed
    min_latency: AtomicU64,
    /// Maximum latency observed
    max_latency: AtomicU64,
}

/// Snapshot of metrics at a point in time
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    /// Timestamp when snapshot was taken
    pub timestamp: u64,
    /// System uptime in seconds
    pub uptime_seconds: u64,
    /// Operation metrics
    pub operations: OperationMetricsSnapshot,
    /// Latency metrics
    pub latency: LatencyMetricsSnapshot,
    /// Storage metrics
    pub storage: StorageMetricsSnapshot,
    /// Memory metrics
    pub memory: MemoryMetricsSnapshot,
    /// Background task metrics
    pub background: BackgroundMetricsSnapshot,
    /// Error metrics
    pub errors: ErrorMetricsSnapshot,
}

/// Snapshot of operation metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationMetricsSnapshot {
    pub reads_total: u64,
    pub writes_total: u64,
    pub deletes_total: u64,
    pub scans_total: u64,
    pub read_cache_hits: u64,
    pub read_cache_misses: u64,
    pub cache_hit_rate: f64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub ops_per_second: f64,
}

/// Snapshot of latency metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyMetricsSnapshot {
    pub read_p50_us: f64,
    pub read_p95_us: f64,
    pub read_p99_us: f64,
    pub write_p50_us: f64,
    pub write_p95_us: f64,
    pub write_p99_us: f64,
    pub delete_p50_us: f64,
    pub delete_p95_us: f64,
    pub delete_p99_us: f64,
}

/// Snapshot of storage metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageMetricsSnapshot {
    pub disk_reads: u64,
    pub disk_writes: u64,
    pub disk_bytes_read: u64,
    pub disk_bytes_written: u64,
    pub disk_flushes: u64,
    pub disk_syncs: u64,
    pub disk_read_bandwidth_mbps: f64,
    pub disk_write_bandwidth_mbps: f64,
}

/// Snapshot of memory metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryMetricsSnapshot {
    pub current_memory_usage: u64,
    pub peak_memory_usage: u64,
    pub pages_allocated: usize,
    pub pages_evicted: usize,
    pub mmap_count: usize,
    pub mmap_size: u64,
    pub memory_utilization: f64,
}

/// Snapshot of background metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackgroundMetricsSnapshot {
    pub checkpoints_completed: u64,
    pub checkpoint_failures: u64,
    pub avg_checkpoint_duration_ms: f64,
    pub gc_cycles_completed: u64,
    pub gc_failures: u64,
    pub avg_gc_duration_ms: f64,
    pub gc_bytes_reclaimed: u64,
}

/// Snapshot of error metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorMetricsSnapshot {
    pub total_errors: u64,
    pub io_errors: u64,
    pub serialization_errors: u64,
    pub corruption_errors: u64,
    pub config_errors: u64,
    pub timeout_errors: u64,
    pub resource_exhausted_errors: u64,
    pub error_rate: f64,
}

impl MetricsCollector {
    /// Create a new metrics collector
    pub fn new() -> Self {
        Self {
            operations: OperationMetrics::default(),
            latency: LatencyMetrics::new(),
            storage: StorageMetrics::default(),
            memory: MemoryMetrics::default(),
            background: BackgroundMetrics::default(),
            errors: ErrorMetrics::default(),
            start_time: Instant::now(),
        }
    }

    /// Record a read operation
    pub fn record_read(&self, latency: Duration, bytes: u64, cache_hit: bool) {
        self.operations.reads_total.fetch_add(1, Ordering::Relaxed);
        self.operations
            .bytes_read
            .fetch_add(bytes, Ordering::Relaxed);

        if cache_hit {
            self.operations
                .read_cache_hits
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.operations
                .read_cache_misses
                .fetch_add(1, Ordering::Relaxed);
        }

        self.latency.read_latencies.write().record(latency);
    }

    /// Record a write operation
    pub fn record_write(&self, latency: Duration, bytes: u64) {
        self.operations.writes_total.fetch_add(1, Ordering::Relaxed);
        self.operations
            .bytes_written
            .fetch_add(bytes, Ordering::Relaxed);
        self.latency.write_latencies.write().record(latency);
    }

    /// Record a delete operation
    pub fn record_delete(&self, latency: Duration) {
        self.operations
            .deletes_total
            .fetch_add(1, Ordering::Relaxed);
        self.latency.delete_latencies.write().record(latency);
    }

    /// Record a scan operation
    pub fn record_scan(&self, latency: Duration) {
        self.operations.scans_total.fetch_add(1, Ordering::Relaxed);
        self.latency.scan_latencies.write().record(latency);
    }

    /// Record storage operation
    pub fn record_storage_op(&self, is_read: bool, bytes: u64) {
        if is_read {
            self.storage.disk_reads.fetch_add(1, Ordering::Relaxed);
            self.storage
                .disk_bytes_read
                .fetch_add(bytes, Ordering::Relaxed);
        } else {
            self.storage.disk_writes.fetch_add(1, Ordering::Relaxed);
            self.storage
                .disk_bytes_written
                .fetch_add(bytes, Ordering::Relaxed);
        }
    }

    /// Record memory usage
    pub fn record_memory_usage(&self, current: u64) {
        self.memory
            .current_memory_usage
            .store(current, Ordering::Relaxed);

        // Update peak if necessary
        let mut peak = self.memory.peak_memory_usage.load(Ordering::Relaxed);
        while current > peak {
            match self.memory.peak_memory_usage.compare_exchange_weak(
                peak,
                current,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(new_peak) => peak = new_peak,
            }
        }
    }

    /// Record an error
    pub fn record_error(&self, error_category: &str) {
        self.errors.total_errors.fetch_add(1, Ordering::Relaxed);

        match error_category {
            "io" => {
                self.errors.io_errors.fetch_add(1, Ordering::Relaxed);
            }
            "serialization" => {
                self.errors
                    .serialization_errors
                    .fetch_add(1, Ordering::Relaxed);
            }
            "corruption" => {
                self.errors
                    .corruption_errors
                    .fetch_add(1, Ordering::Relaxed);
            }
            "configuration" => {
                self.errors.config_errors.fetch_add(1, Ordering::Relaxed);
            }
            "timeout" => {
                self.errors.timeout_errors.fetch_add(1, Ordering::Relaxed);
            }
            "resource_exhausted" => {
                self.errors
                    .resource_exhausted_errors
                    .fetch_add(1, Ordering::Relaxed);
            }
            _ => {} // Unknown error category
        }
    }

    /// Get a snapshot of current metrics
    pub fn snapshot(&self) -> MetricsSnapshot {
        let uptime = self.start_time.elapsed();
        let uptime_seconds = uptime.as_secs();

        // Operation metrics
        let reads = self.operations.reads_total.load(Ordering::Relaxed);
        let writes = self.operations.writes_total.load(Ordering::Relaxed);
        let deletes = self.operations.deletes_total.load(Ordering::Relaxed);
        let scans = self.operations.scans_total.load(Ordering::Relaxed);
        let cache_hits = self.operations.read_cache_hits.load(Ordering::Relaxed);
        let cache_misses = self.operations.read_cache_misses.load(Ordering::Relaxed);

        let total_ops = reads + writes + deletes + scans;
        let ops_per_second = if uptime_seconds > 0 {
            total_ops as f64 / uptime_seconds as f64
        } else {
            0.0
        };

        let cache_hit_rate = if cache_hits + cache_misses > 0 {
            cache_hits as f64 / (cache_hits + cache_misses) as f64
        } else {
            0.0
        };

        MetricsSnapshot {
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            uptime_seconds,
            operations: OperationMetricsSnapshot {
                reads_total: reads,
                writes_total: writes,
                deletes_total: deletes,
                scans_total: scans,
                read_cache_hits: cache_hits,
                read_cache_misses: cache_misses,
                cache_hit_rate,
                bytes_read: self.operations.bytes_read.load(Ordering::Relaxed),
                bytes_written: self.operations.bytes_written.load(Ordering::Relaxed),
                ops_per_second,
            },
            latency: LatencyMetricsSnapshot {
                read_p50_us: self.latency.read_latencies.read().percentile(50.0),
                read_p95_us: self.latency.read_latencies.read().percentile(95.0),
                read_p99_us: self.latency.read_latencies.read().percentile(99.0),
                write_p50_us: self.latency.write_latencies.read().percentile(50.0),
                write_p95_us: self.latency.write_latencies.read().percentile(95.0),
                write_p99_us: self.latency.write_latencies.read().percentile(99.0),
                delete_p50_us: self.latency.delete_latencies.read().percentile(50.0),
                delete_p95_us: self.latency.delete_latencies.read().percentile(95.0),
                delete_p99_us: self.latency.delete_latencies.read().percentile(99.0),
            },
            storage: StorageMetricsSnapshot {
                disk_reads: self.storage.disk_reads.load(Ordering::Relaxed),
                disk_writes: self.storage.disk_writes.load(Ordering::Relaxed),
                disk_bytes_read: self.storage.disk_bytes_read.load(Ordering::Relaxed),
                disk_bytes_written: self.storage.disk_bytes_written.load(Ordering::Relaxed),
                disk_flushes: self.storage.disk_flushes.load(Ordering::Relaxed),
                disk_syncs: self.storage.disk_syncs.load(Ordering::Relaxed),
                disk_read_bandwidth_mbps: if uptime_seconds > 0 {
                    (self.storage.disk_bytes_read.load(Ordering::Relaxed) as f64)
                        / (uptime_seconds as f64 * 1024.0 * 1024.0)
                } else {
                    0.0
                },
                disk_write_bandwidth_mbps: if uptime_seconds > 0 {
                    (self.storage.disk_bytes_written.load(Ordering::Relaxed) as f64)
                        / (uptime_seconds as f64 * 1024.0 * 1024.0)
                } else {
                    0.0
                },
            },
            memory: MemoryMetricsSnapshot {
                current_memory_usage: self.memory.current_memory_usage.load(Ordering::Relaxed),
                peak_memory_usage: self.memory.peak_memory_usage.load(Ordering::Relaxed),
                pages_allocated: self.memory.pages_allocated.load(Ordering::Relaxed),
                pages_evicted: self.memory.pages_evicted.load(Ordering::Relaxed),
                mmap_count: self.memory.mmap_count.load(Ordering::Relaxed),
                mmap_size: self.memory.mmap_size.load(Ordering::Relaxed),
                memory_utilization: 0.0, // TODO: Calculate based on system memory
            },
            background: BackgroundMetricsSnapshot {
                checkpoints_completed: self
                    .background
                    .checkpoints_completed
                    .load(Ordering::Relaxed),
                checkpoint_failures: self.background.checkpoint_failures.load(Ordering::Relaxed),
                avg_checkpoint_duration_ms: {
                    let completed = self
                        .background
                        .checkpoints_completed
                        .load(Ordering::Relaxed);
                    if completed > 0 {
                        self.background
                            .checkpoint_duration_ms
                            .load(Ordering::Relaxed) as f64
                            / completed as f64
                    } else {
                        0.0
                    }
                },
                gc_cycles_completed: self.background.gc_cycles_completed.load(Ordering::Relaxed),
                gc_failures: self.background.gc_failures.load(Ordering::Relaxed),
                avg_gc_duration_ms: {
                    let completed = self.background.gc_cycles_completed.load(Ordering::Relaxed);
                    if completed > 0 {
                        self.background.gc_duration_ms.load(Ordering::Relaxed) as f64
                            / completed as f64
                    } else {
                        0.0
                    }
                },
                gc_bytes_reclaimed: self.background.gc_bytes_reclaimed.load(Ordering::Relaxed),
            },
            errors: ErrorMetricsSnapshot {
                total_errors: self.errors.total_errors.load(Ordering::Relaxed),
                io_errors: self.errors.io_errors.load(Ordering::Relaxed),
                serialization_errors: self.errors.serialization_errors.load(Ordering::Relaxed),
                corruption_errors: self.errors.corruption_errors.load(Ordering::Relaxed),
                config_errors: self.errors.config_errors.load(Ordering::Relaxed),
                timeout_errors: self.errors.timeout_errors.load(Ordering::Relaxed),
                resource_exhausted_errors: self
                    .errors
                    .resource_exhausted_errors
                    .load(Ordering::Relaxed),
                error_rate: if total_ops > 0 {
                    self.errors.total_errors.load(Ordering::Relaxed) as f64 / total_ops as f64
                } else {
                    0.0
                },
            },
        }
    }

    /// Reset all metrics (useful for testing)
    pub fn reset(&self) {
        // Reset operation metrics
        self.operations.reads_total.store(0, Ordering::Relaxed);
        self.operations.writes_total.store(0, Ordering::Relaxed);
        self.operations.deletes_total.store(0, Ordering::Relaxed);
        self.operations.scans_total.store(0, Ordering::Relaxed);
        self.operations.read_cache_hits.store(0, Ordering::Relaxed);
        self.operations
            .read_cache_misses
            .store(0, Ordering::Relaxed);
        self.operations.bytes_read.store(0, Ordering::Relaxed);
        self.operations.bytes_written.store(0, Ordering::Relaxed);

        // Reset latency histograms
        self.latency.read_latencies.write().reset();
        self.latency.write_latencies.write().reset();
        self.latency.delete_latencies.write().reset();
        self.latency.scan_latencies.write().reset();

        // Reset other metrics...
        // (Implementation truncated for brevity)
    }
}

impl LatencyMetrics {
    fn new() -> Self {
        Self {
            read_latencies: RwLock::new(LatencyHistogram::new()),
            write_latencies: RwLock::new(LatencyHistogram::new()),
            delete_latencies: RwLock::new(LatencyHistogram::new()),
            scan_latencies: RwLock::new(LatencyHistogram::new()),
        }
    }
}

impl LatencyHistogram {
    fn new() -> Self {
        // Bucket boundaries: 10us, 50us, 100us, 500us, 1ms, 5ms, 10ms, 50ms, 100ms, 500ms, 1s, 5s
        let buckets = vec![
            10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000, 5000000,
        ];
        let counts = buckets.iter().map(|_| AtomicU64::new(0)).collect();

        Self {
            buckets,
            counts,
            total_count: AtomicU64::new(0),
            total_sum: AtomicU64::new(0),
            min_latency: AtomicU64::new(u64::MAX),
            max_latency: AtomicU64::new(0),
        }
    }

    fn record(&self, latency: Duration) {
        let latency_us = latency.as_micros() as u64;

        // Update min/max
        let mut current_min = self.min_latency.load(Ordering::Relaxed);
        while latency_us < current_min {
            match self.min_latency.compare_exchange_weak(
                current_min,
                latency_us,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(new_min) => current_min = new_min,
            }
        }

        let mut current_max = self.max_latency.load(Ordering::Relaxed);
        while latency_us > current_max {
            match self.max_latency.compare_exchange_weak(
                current_max,
                latency_us,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(new_max) => current_max = new_max,
            }
        }

        // Find appropriate bucket and increment
        for (i, &bucket_limit) in self.buckets.iter().enumerate() {
            if latency_us <= bucket_limit {
                self.counts[i].fetch_add(1, Ordering::Relaxed);
                break;
            }
        }

        // Update totals
        self.total_count.fetch_add(1, Ordering::Relaxed);
        self.total_sum.fetch_add(latency_us, Ordering::Relaxed);
    }

    fn percentile(&self, p: f64) -> f64 {
        let total = self.total_count.load(Ordering::Relaxed);
        if total == 0 {
            return 0.0;
        }

        let target_count = (total as f64 * p / 100.0) as u64;
        let mut cumulative = 0;

        for (i, count) in self.counts.iter().enumerate() {
            cumulative += count.load(Ordering::Relaxed);
            if cumulative >= target_count {
                return self.buckets[i] as f64;
            }
        }

        *self.buckets.last().unwrap_or(&0) as f64
    }

    fn reset(&self) {
        for count in &self.counts {
            count.store(0, Ordering::Relaxed);
        }
        self.total_count.store(0, Ordering::Relaxed);
        self.total_sum.store(0, Ordering::Relaxed);
        self.min_latency.store(u64::MAX, Ordering::Relaxed);
        self.max_latency.store(0, Ordering::Relaxed);
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared metrics collector type
pub type SharedMetricsCollector = Arc<MetricsCollector>;

/// Create a new shared metrics collector
pub fn new_shared_metrics_collector() -> SharedMetricsCollector {
    Arc::new(MetricsCollector::new())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_metrics_collection() {
        let metrics = MetricsCollector::new();

        // Record some operations
        metrics.record_read(Duration::from_micros(100), 1024, true);
        metrics.record_write(Duration::from_micros(200), 2048);
        metrics.record_delete(Duration::from_micros(50));

        // Get snapshot
        let snapshot = metrics.snapshot();

        assert_eq!(snapshot.operations.reads_total, 1);
        assert_eq!(snapshot.operations.writes_total, 1);
        assert_eq!(snapshot.operations.deletes_total, 1);
        assert_eq!(snapshot.operations.bytes_read, 1024);
        assert_eq!(snapshot.operations.bytes_written, 2048);
        assert_eq!(snapshot.operations.cache_hit_rate, 1.0);

        // Test latency percentiles
        assert!(snapshot.latency.read_p50_us > 0.0);
        assert!(snapshot.latency.write_p50_us > 0.0);
        assert!(snapshot.latency.delete_p50_us > 0.0);
    }

    #[test]
    fn test_latency_histogram() {
        let histogram = LatencyHistogram::new();

        // Record some latencies
        histogram.record(Duration::from_micros(25)); // Should go to 50us bucket
        histogram.record(Duration::from_micros(75)); // Should go to 100us bucket
        histogram.record(Duration::from_micros(150)); // Should go to 500us bucket

        assert_eq!(histogram.total_count.load(Ordering::Relaxed), 3);
        assert!(histogram.percentile(50.0) > 0.0);
    }
}

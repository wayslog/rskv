use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Batch operation type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BatchOpType {
    Read,
    Write,
    Update,
    Delete,
}

/// A batch operation request
#[derive(Debug, Clone)]
pub struct BatchOp<K, V> {
    pub op_type: BatchOpType,
    pub key: K,
    pub key_hash: u64,
    pub value: Option<V>,
}

/// Result of a batch operation
#[derive(Debug, Clone, PartialEq)]
pub enum BatchOpResult<V> {
    Ok(Option<V>),
    NotFound,
    Error(String),
}

/// Batch optimizer configuration
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Maximum batch size
    pub max_batch_size: usize,
    /// Minimum batch size before processing
    pub min_batch_size: usize,
    /// Maximum wait time before processing partial batch (in milliseconds)
    pub max_wait_ms: u64,
    /// Enable operation reordering for optimization
    pub enable_reordering: bool,
    /// Enable duplicate detection and elimination
    pub enable_deduplication: bool,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 1000,
            min_batch_size: 10,
            max_wait_ms: 10,
            enable_reordering: true,
            enable_deduplication: true,
        }
    }
}

/// Batch operation optimizer
pub struct BatchOptimizer<K, V> {
    config: BatchConfig,
    pending_ops: Arc<Mutex<Vec<BatchOp<K, V>>>>,
    total_batches: AtomicU64,
    total_ops: AtomicU64,
    deduplicated_ops: AtomicU64,
}

impl<K, V> BatchOptimizer<K, V>
where
    K: Clone + Eq + std::hash::Hash + std::fmt::Debug,
    V: Clone,
{
    pub fn new(config: BatchConfig) -> Self {
        Self {
            config,
            pending_ops: Arc::new(Mutex::new(Vec::new())),
            total_batches: AtomicU64::new(0),
            total_ops: AtomicU64::new(0),
            deduplicated_ops: AtomicU64::new(0),
        }
    }

    /// Add operation to batch
    pub fn add_operation(&self, op: BatchOp<K, V>) -> bool {
        if let Ok(mut ops) = self.pending_ops.lock() {
            ops.push(op);
            ops.len() >= self.config.min_batch_size
        } else {
            false
        }
    }

    /// Get current batch and clear pending operations
    pub fn take_batch(&self) -> Vec<BatchOp<K, V>> {
        if let Ok(mut ops) = self.pending_ops.lock() {
            let batch = std::mem::take(&mut *ops);
            if !batch.is_empty() {
                self.total_batches.fetch_add(1, Ordering::Relaxed);
                self.total_ops
                    .fetch_add(batch.len() as u64, Ordering::Relaxed);
            }
            batch
        } else {
            Vec::new()
        }
    }

    /// Optimize a batch of operations
    pub fn optimize_batch(&self, mut batch: Vec<BatchOp<K, V>>) -> Vec<BatchOp<K, V>> {
        if batch.is_empty() {
            return batch;
        }

        // Apply deduplication if enabled
        if self.config.enable_deduplication {
            batch = self.deduplicate_operations(batch);
        }

        // Apply reordering if enabled
        if self.config.enable_reordering {
            batch = self.reorder_operations(batch);
        }

        batch
    }

    /// Remove duplicate operations (keep only the last operation for each key)
    fn deduplicate_operations(&self, batch: Vec<BatchOp<K, V>>) -> Vec<BatchOp<K, V>> {
        let original_len = batch.len();
        let mut key_to_op: HashMap<K, BatchOp<K, V>> = HashMap::new();

        for op in batch {
            key_to_op.insert(op.key.clone(), op);
        }

        let deduplicated: Vec<_> = key_to_op.into_values().collect();
        let removed = original_len.saturating_sub(deduplicated.len());

        if removed > 0 {
            self.deduplicated_ops
                .fetch_add(removed as u64, Ordering::Relaxed);
        }

        deduplicated
    }

    /// Reorder operations for better performance
    /// Strategy: Group by operation type, then sort by key hash for better cache locality
    fn reorder_operations(&self, mut batch: Vec<BatchOp<K, V>>) -> Vec<BatchOp<K, V>> {
        // First, separate by operation type
        let mut reads = Vec::new();
        let mut writes = Vec::new();
        let mut updates = Vec::new();
        let mut deletes = Vec::new();

        for op in batch.drain(..) {
            match op.op_type {
                BatchOpType::Read => reads.push(op),
                BatchOpType::Write => writes.push(op),
                BatchOpType::Update => updates.push(op),
                BatchOpType::Delete => deletes.push(op),
            }
        }

        // Sort each group by key hash for cache locality
        reads.sort_by_key(|op| op.key_hash);
        writes.sort_by_key(|op| op.key_hash);
        updates.sort_by_key(|op| op.key_hash);
        deletes.sort_by_key(|op| op.key_hash);

        // Combine back in optimal order: Reads first (don't modify), then Writes/Updates, then Deletes
        let mut result = Vec::with_capacity(
            reads.len() + writes.len() + updates.len() + deletes.len(),
        );
        result.extend(reads);
        result.extend(writes);
        result.extend(updates);
        result.extend(deletes);

        result
    }

    /// Check if batch is ready for processing
    pub fn is_batch_ready(&self) -> bool {
        if let Ok(ops) = self.pending_ops.lock() {
            ops.len() >= self.config.min_batch_size || ops.len() >= self.config.max_batch_size
        } else {
            false
        }
    }

    /// Get number of pending operations
    pub fn pending_count(&self) -> usize {
        self.pending_ops.lock().map(|ops| ops.len()).unwrap_or(0)
    }

    /// Get statistics
    pub fn get_stats(&self) -> BatchStats {
        BatchStats {
            total_batches: self.total_batches.load(Ordering::Relaxed),
            total_ops: self.total_ops.load(Ordering::Relaxed),
            deduplicated_ops: self.deduplicated_ops.load(Ordering::Relaxed),
            pending_ops: self.pending_count(),
        }
    }

    /// Clear all pending operations
    pub fn clear(&self) {
        if let Ok(mut ops) = self.pending_ops.lock() {
            ops.clear();
        }
    }
}

/// Batch operation statistics
#[derive(Debug, Clone)]
pub struct BatchStats {
    pub total_batches: u64,
    pub total_ops: u64,
    pub deduplicated_ops: u64,
    pub pending_ops: usize,
}

impl BatchStats {
    pub fn avg_batch_size(&self) -> f64 {
        if self.total_batches == 0 {
            0.0
        } else {
            self.total_ops as f64 / self.total_batches as f64
        }
    }

    pub fn deduplication_rate(&self) -> f64 {
        if self.total_ops == 0 {
            0.0
        } else {
            self.deduplicated_ops as f64 / self.total_ops as f64
        }
    }
}

/// Batch executor trait for executing optimized batches
pub trait BatchExecutor<K, V> {
    fn execute_batch(&self, batch: Vec<BatchOp<K, V>>) -> Vec<BatchOpResult<V>>;
}

/// Prefetch hint for batch operations
#[derive(Debug, Clone)]
pub struct PrefetchHint {
    pub key_hashes: Vec<u64>,
    pub estimated_size: usize,
}

impl PrefetchHint {
    pub fn new(batch: &[BatchOp<impl Clone, impl Clone>]) -> Self {
        let key_hashes: Vec<u64> = batch.iter().map(|op| op.key_hash).collect();
        let estimated_size = batch.len() * 64; // Estimate 64 bytes per operation

        Self {
            key_hashes,
            estimated_size,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_operations() {
        let config = BatchConfig {
            min_batch_size: 3,
            ..Default::default()
        };
        let optimizer = BatchOptimizer::<u64, u64>::new(config);

        let op1 = BatchOp {
            op_type: BatchOpType::Write,
            key: 1,
            key_hash: 1,
            value: Some(100),
        };

        let op2 = BatchOp {
            op_type: BatchOpType::Read,
            key: 2,
            key_hash: 2,
            value: None,
        };

        let op3 = BatchOp {
            op_type: BatchOpType::Update,
            key: 3,
            key_hash: 3,
            value: Some(300),
        };

        assert!(!optimizer.add_operation(op1));
        assert!(!optimizer.add_operation(op2));
        assert!(optimizer.add_operation(op3)); // Should be ready now

        let batch = optimizer.take_batch();
        assert_eq!(batch.len(), 3);
    }

    #[test]
    fn test_deduplication() {
        let config = BatchConfig {
            enable_deduplication: true,
            ..Default::default()
        };
        let optimizer = BatchOptimizer::<u64, u64>::new(config);

        let ops = vec![
            BatchOp {
                op_type: BatchOpType::Write,
                key: 1,
                key_hash: 1,
                value: Some(100),
            },
            BatchOp {
                op_type: BatchOpType::Write,
                key: 1,
                key_hash: 1,
                value: Some(200),
            },
            BatchOp {
                op_type: BatchOpType::Write,
                key: 2,
                key_hash: 2,
                value: Some(300),
            },
        ];

        let optimized = optimizer.optimize_batch(ops);
        assert_eq!(optimized.len(), 2); // Only 2 unique keys
    }

    #[test]
    fn test_reordering() {
        let config = BatchConfig {
            enable_reordering: true,
            enable_deduplication: false,
            ..Default::default()
        };
        let optimizer = BatchOptimizer::<u64, u64>::new(config);

        let ops = vec![
            BatchOp {
                op_type: BatchOpType::Write,
                key: 1,
                key_hash: 100,
                value: Some(100),
            },
            BatchOp {
                op_type: BatchOpType::Read,
                key: 2,
                key_hash: 50,
                value: None,
            },
            BatchOp {
                op_type: BatchOpType::Delete,
                key: 3,
                key_hash: 75,
                value: None,
            },
        ];

        let optimized = optimizer.optimize_batch(ops);
        assert_eq!(optimized.len(), 3);
        // Reads should come first
        assert_eq!(optimized[0].op_type, BatchOpType::Read);
    }

    #[test]
    fn test_batch_stats() {
        let optimizer = BatchOptimizer::<u64, u64>::new(BatchConfig::default());

        for i in 0..10 {
            optimizer.add_operation(BatchOp {
                op_type: BatchOpType::Write,
                key: i,
                key_hash: i,
                value: Some(i * 100),
            });
        }

        let batch = optimizer.take_batch();
        assert!(!batch.is_empty());

        let stats = optimizer.get_stats();
        assert_eq!(stats.total_batches, 1);
        assert_eq!(stats.total_ops, 10);
    }

    #[test]
    fn test_prefetch_hint() {
        let ops: Vec<BatchOp<u64, u64>> = vec![
            BatchOp {
                op_type: BatchOpType::Read,
                key: 1,
                key_hash: 100,
                value: None,
            },
            BatchOp {
                op_type: BatchOpType::Read,
                key: 2,
                key_hash: 200,
                value: None,
            },
        ];

        let hint = PrefetchHint::new(&ops);
        assert_eq!(hint.key_hashes.len(), 2);
        assert_eq!(hint.key_hashes[0], 100);
        assert_eq!(hint.key_hashes[1], 200);
    }
}
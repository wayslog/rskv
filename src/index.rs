//! Concurrent hash index implementation for rskv
//!
//! This module provides a thread-safe hash index for mapping keys to their
//! addresses in the hybrid log. It's inspired by FASTER's MemHashIndex design.

use std::hash::Hasher;
use std::sync::Arc;

use ahash::AHasher;
use dashmap::DashMap;

use crate::common::{Address, Key};
use crate::epoch::SharedEpochManager;

/// Custom hasher for better performance with binary keys
pub struct KeyHasher {
    #[allow(dead_code)]
    hasher: AHasher,
}

impl KeyHasher {
    pub fn new() -> Self {
        Self {
            hasher: AHasher::default(),
        }
    }

    pub fn hash_key(key: &[u8]) -> u64 {
        let mut hasher = AHasher::default();
        hasher.write(key);
        hasher.finish()
    }
}

impl Default for KeyHasher {
    fn default() -> Self {
        Self::new()
    }
}

/// Hash bucket entry containing the key-address mapping
#[derive(Debug, Clone)]
pub struct HashBucketEntry {
    /// The key
    pub key: Key,
    /// Address pointing to the latest version of the value in the log
    pub address: Address,
    /// Hash of the key for quick comparison
    pub key_hash: u64,
}

impl HashBucketEntry {
    pub fn new(key: Key, address: Address) -> Self {
        let key_hash = KeyHasher::hash_key(&key);
        Self {
            key,
            address,
            key_hash,
        }
    }

    /// Check if this entry matches the given key
    pub fn matches_key(&self, key: &[u8]) -> bool {
        // First check hash for quick rejection
        let other_hash = KeyHasher::hash_key(key);
        if self.key_hash != other_hash {
            return false;
        }

        // Then check actual key content
        self.key == key
    }
}

/// Memory-based concurrent hash index
///
/// This is the main index structure that maps keys to their latest addresses
/// in the hybrid log. It uses DashMap for thread-safe concurrent access.
pub struct MemHashIndex {
    /// Internal hash map using DashMap for lock-free concurrent access
    map: DashMap<Key, Address, ahash::RandomState>,

    /// Epoch manager for safe memory reclamation (currently unused but kept for future optimization)
    #[allow(dead_code)]
    epoch: SharedEpochManager,
}

impl MemHashIndex {
    /// Create a new memory hash index
    pub fn new(epoch: SharedEpochManager) -> Self {
        Self {
            map: DashMap::with_hasher(ahash::RandomState::new()),
            epoch,
        }
    }

    /// Create a new memory hash index with specified capacity
    pub fn with_capacity(capacity: usize, epoch: SharedEpochManager) -> Self {
        Self {
            map: DashMap::with_capacity_and_hasher(capacity, ahash::RandomState::new()),
            epoch,
        }
    }

    /// Find the address for a given key
    /// Returns None if the key is not found
    pub fn find(&self, key: &Key) -> Option<Address> {
        self.map.get(key).map(|entry| *entry.value())
    }

    /// Insert or update a key-address mapping
    /// This will overwrite any existing mapping for the key
    pub fn insert(&self, key: Key, address: Address) {
        self.map.insert(key, address);
    }

    /// Insert a key-address mapping only if the key doesn't exist
    /// Returns true if the insertion was successful, false if key already exists
    pub fn insert_if_not_exists(&self, key: Key, address: Address) -> bool {
        // Use entry API to check and insert atomically
        use dashmap::mapref::entry::Entry;

        match self.map.entry(key) {
            Entry::Occupied(_) => false, // Key already exists
            Entry::Vacant(entry) => {
                entry.insert(address);
                true // Insertion successful
            }
        }
    }

    /// Update an existing key-address mapping using compare-and-swap
    /// Returns true if the update was successful
    pub fn update_if_exists(&self, key: &Key, old_address: Address, new_address: Address) -> bool {
        if let Some(mut entry) = self.map.get_mut(key)
            && *entry.value() == old_address
        {
            *entry.value_mut() = new_address;
            return true;
        }
        false
    }

    /// Remove a key from the index
    /// Returns the old address if the key was found and removed
    pub fn remove(&self, key: &Key) -> Option<Address> {
        self.map.remove(key).map(|(_, address)| address)
    }

    /// Remove a key only if it currently maps to the specified address
    /// This is useful for conditional removals during garbage collection
    pub fn remove_if_address(&self, key: &Key, expected_address: Address) -> bool {
        if let Some(entry) = self.map.get(key) {
            if *entry.value() == expected_address {
                drop(entry);
                self.map.remove(key).is_some()
            } else {
                false
            }
        } else {
            false
        }
    }

    /// Get the number of entries in the index
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Check if the index is empty
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Clear all entries from the index
    pub fn clear(&self) {
        self.map.clear();
    }

    /// Iterate over all key-address pairs
    /// The provided closure will be called for each entry
    pub fn for_each<F>(&self, mut f: F)
    where
        F: FnMut(&Key, Address),
    {
        for entry in &self.map {
            f(entry.key(), *entry.value());
        }
    }

    /// Iterate over entries and collect those that match a predicate
    /// This is useful for operations like garbage collection
    pub fn collect_matching<F>(&self, predicate: F) -> Vec<(Key, Address)>
    where
        F: Fn(&Key, Address) -> bool,
    {
        let mut result = Vec::new();
        for entry in &self.map {
            let key = entry.key();
            let address = *entry.value();
            if predicate(key, address) {
                result.push((key.clone(), address));
            }
        }
        result
    }

    /// Remove entries that match a predicate
    /// Returns the number of entries removed
    pub fn remove_matching<F>(&self, predicate: F) -> usize
    where
        F: Fn(&Key, Address) -> bool,
    {
        let mut removed_count = 0;

        // Collect keys to remove first to avoid holding locks during iteration
        let keys_to_remove: Vec<Key> = self
            .map
            .iter()
            .filter_map(|entry| {
                let key = entry.key();
                let address = *entry.value();
                if predicate(key, address) {
                    Some(key.clone())
                } else {
                    None
                }
            })
            .collect();

        // Remove the collected keys
        for key in keys_to_remove {
            if self.map.remove(&key).is_some() {
                removed_count += 1;
            }
        }

        removed_count
    }

    /// Create a snapshot of the current index state
    /// This is useful for checkpointing
    pub fn snapshot(&self) -> Vec<(Key, Address)> {
        self.map
            .iter()
            .map(|entry| (entry.key().clone(), *entry.value()))
            .collect()
    }

    /// Restore the index from a snapshot
    /// This will clear the current index and load the snapshot data
    pub fn restore_from_snapshot(&self, snapshot: Vec<(Key, Address)>) {
        self.clear();
        for (key, address) in snapshot {
            self.insert(key, address);
        }
    }

    /// Get memory usage statistics
    pub fn memory_usage(&self) -> IndexMemoryStats {
        let entry_count = self.len();

        // Estimate memory usage
        // DashMap overhead + (Key + Address + metadata) per entry
        let dashmap_overhead = std::mem::size_of::<DashMap<Key, Address>>();

        let mut total_key_size = 0;
        for entry in &self.map {
            total_key_size += entry.key().capacity();
        }

        let address_size = entry_count * std::mem::size_of::<Address>();
        let estimated_overhead = entry_count * 64; // Rough estimate for DashMap overhead per entry

        IndexMemoryStats {
            entry_count,
            total_key_size,
            address_size,
            estimated_overhead: dashmap_overhead + estimated_overhead,
            total_estimated_size: dashmap_overhead
                + total_key_size
                + address_size
                + estimated_overhead,
        }
    }
}

/// Memory usage statistics for the hash index
#[derive(Debug, Clone)]
pub struct IndexMemoryStats {
    /// Number of entries in the index
    pub entry_count: usize,
    /// Total size of all keys in bytes
    pub total_key_size: usize,
    /// Total size of all addresses in bytes
    pub address_size: usize,
    /// Estimated overhead from the hash map structure
    pub estimated_overhead: usize,
    /// Total estimated memory usage in bytes
    pub total_estimated_size: usize,
}

/// Shared reference to a memory hash index
pub type SharedMemHashIndex = Arc<MemHashIndex>;

/// Create a new shared memory hash index
pub fn new_shared_mem_hash_index(epoch: SharedEpochManager) -> SharedMemHashIndex {
    Arc::new(MemHashIndex::new(epoch))
}

/// Create a new shared memory hash index with specified capacity
pub fn new_shared_mem_hash_index_with_capacity(
    capacity: usize,
    epoch: SharedEpochManager,
) -> SharedMemHashIndex {
    Arc::new(MemHashIndex::with_capacity(capacity, epoch))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::epoch::EpochManager;

    #[test]
    fn test_key_hasher() {
        let key1 = b"hello";
        let key2 = b"world";
        let key3 = b"hello";

        let hash1 = KeyHasher::hash_key(key1);
        let hash2 = KeyHasher::hash_key(key2);
        let hash3 = KeyHasher::hash_key(key3);

        assert_eq!(hash1, hash3);
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_hash_bucket_entry() {
        let key = b"test_key".to_vec();
        let address = 12345u64;

        let entry = HashBucketEntry::new(key.clone(), address);

        assert!(entry.matches_key(&key));
        assert!(!entry.matches_key(b"other_key"));
        assert_eq!(entry.address, address);
    }

    #[test]
    fn test_mem_hash_index_basic_operations() {
        let epoch = Arc::new(EpochManager::new());
        let index = MemHashIndex::new(epoch);

        let key1 = b"key1".to_vec();
        let key2 = b"key2".to_vec();
        let addr1 = 100u64;
        let addr2 = 200u64;

        // Test insertion
        index.insert(key1.clone(), addr1);
        index.insert(key2.clone(), addr2);

        // Test finding
        assert_eq!(index.find(&key1), Some(addr1));
        assert_eq!(index.find(&key2), Some(addr2));
        assert_eq!(index.find(&b"nonexistent".to_vec()), None);

        // Test length
        assert_eq!(index.len(), 2);
        assert!(!index.is_empty());

        // Test removal
        assert_eq!(index.remove(&key1), Some(addr1));
        assert_eq!(index.find(&key1), None);
        assert_eq!(index.len(), 1);

        // Test clear
        index.clear();
        assert_eq!(index.len(), 0);
        assert!(index.is_empty());
    }

    #[test]
    fn test_mem_hash_index_conditional_operations() {
        let epoch = Arc::new(EpochManager::new());
        let index = MemHashIndex::new(epoch);

        let key = b"test_key".to_vec();
        let addr1 = 100u64;
        let addr2 = 200u64;

        // Test insert_if_not_exists
        assert!(index.insert_if_not_exists(key.clone(), addr1));
        assert!(!index.insert_if_not_exists(key.clone(), addr2)); // Should fail
        assert_eq!(index.find(&key), Some(addr1));

        // Test update_if_exists
        assert!(index.update_if_exists(&key, addr1, addr2));
        assert_eq!(index.find(&key), Some(addr2));
        assert!(!index.update_if_exists(&key, addr1, 300u64)); // Should fail

        // Test remove_if_address
        assert!(!index.remove_if_address(&key, addr1)); // Should fail
        assert!(index.remove_if_address(&key, addr2)); // Should succeed
        assert_eq!(index.find(&key), None);
    }

    #[test]
    fn test_mem_hash_index_iteration() {
        let epoch = Arc::new(EpochManager::new());
        let index = MemHashIndex::new(epoch);

        let entries = vec![
            (b"key1".to_vec(), 100u64),
            (b"key2".to_vec(), 200u64),
            (b"key3".to_vec(), 300u64),
        ];

        // Insert test data
        for (key, addr) in &entries {
            index.insert(key.clone(), *addr);
        }

        // Test for_each
        let mut collected = Vec::new();
        index.for_each(|key, addr| {
            collected.push((key.clone(), addr));
        });
        assert_eq!(collected.len(), 3);

        // Test collect_matching
        let filtered = index.collect_matching(|_key, addr| addr > 150u64);
        assert_eq!(filtered.len(), 2);

        // Test remove_matching
        let removed_count = index.remove_matching(|_key, addr| addr > 150u64);
        assert_eq!(removed_count, 2);
        assert_eq!(index.len(), 1);
    }

    #[test]
    fn test_mem_hash_index_snapshot() {
        let epoch = Arc::new(EpochManager::new());
        let index = MemHashIndex::new(epoch);

        let entries = vec![(b"key1".to_vec(), 100u64), (b"key2".to_vec(), 200u64)];

        // Insert test data
        for (key, addr) in &entries {
            index.insert(key.clone(), *addr);
        }

        // Create snapshot
        let snapshot = index.snapshot();
        assert_eq!(snapshot.len(), 2);

        // Clear and restore
        index.clear();
        assert!(index.is_empty());

        index.restore_from_snapshot(snapshot);
        assert_eq!(index.len(), 2);

        // Verify data is restored correctly
        for (key, addr) in &entries {
            assert_eq!(index.find(key), Some(*addr));
        }
    }

    #[test]
    fn test_memory_stats() {
        let epoch = Arc::new(EpochManager::new());
        let index = MemHashIndex::new(epoch);

        // Insert some test data
        for i in 0..100 {
            let key = format!("key_{}", i).into_bytes();
            index.insert(key, i as u64);
        }

        let stats = index.memory_usage();
        assert_eq!(stats.entry_count, 100);
        assert!(stats.total_key_size > 0);
        assert!(stats.address_size > 0);
        assert!(stats.total_estimated_size > 0);
    }

    #[test]
    fn test_shared_index() {
        let epoch = Arc::new(EpochManager::new());
        let index: SharedMemHashIndex = new_shared_mem_hash_index(epoch);

        let key = b"test".to_vec();
        let addr = 42u64;

        index.insert(key.clone(), addr);
        assert_eq!(index.find(&key), Some(addr));
        assert_eq!(index.len(), 1);

        index.remove(&key);
        assert_eq!(index.find(&key), None);
        assert!(index.is_empty());
    }
}

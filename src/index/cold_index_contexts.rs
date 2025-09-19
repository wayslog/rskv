use crate::index::hash_bucket::{ColdLogIndexHashBucket, HashBucketEntry};

/// The key for a chunk in the two-level hash index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HashIndexChunkKey {
    pub chunk_id: u64,
    pub tag: u16, // Not strictly part of the key, but used for hashing
}

impl HashIndexChunkKey {
    pub fn get_hash(&self) -> u64 {
        // A simple hash combining chunk_id and tag.
        self.chunk_id ^ (self.tag as u64)
    }
}

/// The value stored in the internal KV for ColdIndex, representing a chunk of buckets.
#[derive(Clone)]
#[repr(C)]
pub struct HashIndexChunkValue {
    // For now, let's define a chunk as a single bucket.
    // The C++ version has this templated (kNumBuckets).
    pub bucket: ColdLogIndexHashBucket,
}

/// The RmwContext used to implement ColdIndex writes.
pub struct ColdIndexRmwContext {
    pub key: HashIndexChunkKey,
    pub index_in_chunk: u8,
    pub tag_in_chunk: u8,
    pub new_entry: HashBucketEntry,
    pub expected_entry: HashBucketEntry,
}

impl Default for HashIndexChunkValue {
    fn default() -> Self {
        Self {
            bucket: Default::default(),
        }
    }
}

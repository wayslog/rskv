/// Size of cache line in bytes
pub const K_CACHE_LINE_BYTES: usize = 64;

/// We issue 256 writes to disk, to checkpoint the hash table.
pub const K_NUM_MERGE_CHUNKS: usize = 256;

use crate::core::utility;

/// A raw 64-bit key hash.
#[derive(Clone, Copy, Debug, Default)]
#[repr(transparent)]
pub struct KeyHash(u64);

impl KeyHash {
    pub fn new(hash: u64) -> Self {
        Self(hash)
    }
    pub fn control(&self) -> u64 {
        self.0
    }
}

/// A key hash specialized for the `HotLog` index, defining how the raw 64 bits
/// are interpreted for table indexing and in-bucket tag matching.
#[derive(Clone, Copy, Debug)]
pub struct HotLogKeyHash(u64);

impl HotLogKeyHash {
    const TAG_BITS: u32 = 14;
    const TAG_MASK: u64 = (1 << Self::TAG_BITS) - 1;

    pub fn new(hash: u64) -> Self {
        Self(hash)
    }

    /// Truncates the hash to get the index into a hash table of a given size.
    #[inline]
    pub fn table_index(&self, table_size: u64) -> u64 {
        debug_assert!(utility::is_power_of_two(table_size));
        self.0 & (table_size - 1)
    }

    /// The tag serves as a discriminator inside a hash bucket.
    #[inline]
    pub fn tag(&self) -> u16 {
        // This needs to be consistent with HashBucketEntry's layout.
        // We shift by 48 to get to the tag bits, as defined in hash_bucket.rs
        (self.0 >> 48) as u16 & Self::TAG_MASK as u16
    }
}

/// A key hash specialized for the `ColdIndex`, which uses a two-level scheme.
#[derive(Clone, Copy, Debug)]
pub struct ColdLogKeyHash(u64);

impl ColdLogKeyHash {
    // These bit widths are from ColdLogIndexBucketEntryDef in C++
    const TAG_BITS: u32 = 3;
    const IN_CHUNK_INDEX_BITS: u32 = 5; // 32 entries per chunk -> 5 bits

    const TAG_MASK: u64 = (1 << Self::TAG_BITS) - 1;
    const IN_CHUNK_INDEX_MASK: u64 = (1 << Self::IN_CHUNK_INDEX_BITS) - 1;

    const TAG_SHIFT: u32 = 48;
    const IN_CHUNK_INDEX_SHIFT: u32 = Self::TAG_SHIFT + Self::TAG_BITS;

    pub fn new(hash: u64) -> Self {
        Self(hash)
    }

    pub fn chunk_id(&self, table_size: u64) -> u64 {
        self.0 & (table_size - 1)
    }

    pub fn index_in_chunk(&self) -> u8 {
        ((self.0 >> Self::IN_CHUNK_INDEX_SHIFT) & Self::IN_CHUNK_INDEX_MASK) as u8
    }

    pub fn tag_in_chunk(&self) -> u8 {
        ((self.0 >> Self::TAG_SHIFT) & Self::TAG_MASK) as u8
    }
}

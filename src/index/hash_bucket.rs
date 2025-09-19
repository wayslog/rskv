use crate::core::address::Address;
use crate::core::constants::K_CACHE_LINE_BYTES;
use crate::core::malloc_fixed_page_size::FixedPageAddress;
use std::sync::atomic::{AtomicU64, Ordering};

/// Entry stored in a hash bucket. Packed into 8 bytes.
/// Corresponds to `HashBucketEntry` and `HotLogIndexBucketEntryDef` in C++.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[repr(transparent)]
pub struct HashBucketEntry(u64);

impl HashBucketEntry {
    pub const INVALID_ENTRY: u64 = 0;

    // Bit layout from HotLogIndexBucketEntryDef
    const ADDRESS_BITS: u32 = 47;
    const READCACHE_BIT: u32 = 1;
    const TAG_BITS: u32 = 14;
    const TENTATIVE_BIT: u32 = 1;

    const ADDRESS_MASK: u64 = (1 << Self::ADDRESS_BITS) - 1;
    const READCACHE_SHIFT: u32 = Self::ADDRESS_BITS;
    const TAG_SHIFT: u32 = Self::READCACHE_SHIFT + Self::READCACHE_BIT;
    const TENTATIVE_SHIFT: u32 = Self::TAG_SHIFT + Self::TAG_BITS;

    pub fn new(address: Address, tag: u16, tentative: bool, readcache: bool) -> Self {
        let mut control = address.control() & Self::ADDRESS_MASK;
        if readcache {
            control |= 1 << Self::READCACHE_SHIFT;
        }
        control |= (tag as u64) << Self::TAG_SHIFT;
        if tentative {
            control |= 1 << Self::TENTATIVE_SHIFT;
        }
        HashBucketEntry(control)
    }

    #[inline]
    pub fn from_control(control: u64) -> Self {
        HashBucketEntry(control)
    }

    #[inline]
    pub fn control(&self) -> u64 {
        self.0
    }

    #[inline]
    pub fn unused(&self) -> bool {
        self.0 == Self::INVALID_ENTRY
    }

    #[inline]
    pub fn address(&self) -> Address {
        Address::from_control(self.0 & Self::ADDRESS_MASK)
    }

    #[inline]
    pub fn in_readcache(&self) -> bool {
        (self.0 >> Self::READCACHE_SHIFT) & 1 != 0
    }

    #[inline]
    pub fn tag(&self) -> u16 {
        ((self.0 >> Self::TAG_SHIFT) & ((1 << Self::TAG_BITS) - 1)) as u16
    }

    #[inline]
    pub fn tentative(&self) -> bool {
        (self.0 >> Self::TENTATIVE_SHIFT) & 1 != 0
    }
}

/// Atomic hash-bucket entry.
#[derive(Default)]
#[repr(transparent)]
pub struct AtomicHashBucketEntry(AtomicU64);

impl AtomicHashBucketEntry {
    #[inline]
    pub fn load(&self) -> HashBucketEntry {
        HashBucketEntry(self.0.load(Ordering::Relaxed))
    }

    #[inline]
    pub fn store(&self, desired: HashBucketEntry) {
        self.0.store(desired.control(), Ordering::Relaxed)
    }

    #[inline]
    pub fn compare_exchange(
        &self,
        current: HashBucketEntry,
        new: HashBucketEntry,
    ) -> Result<HashBucketEntry, HashBucketEntry> {
        match self.0.compare_exchange(
            current.control(),
            new.control(),
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(v) => Ok(HashBucketEntry(v)),
            Err(v) => Err(HashBucketEntry(v)),
        }
    }
}

/// Entry stored in a hash bucket that points to the next overflow bucket.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[repr(transparent)]
pub struct HashBucketOverflowEntry(u64);

impl HashBucketOverflowEntry {
    pub const INVALID_ENTRY: u64 = 0;

    pub fn new(address: FixedPageAddress) -> Self {
        HashBucketOverflowEntry(address.control())
    }

    #[inline]
    pub fn unused(&self) -> bool {
        self.0 == Self::INVALID_ENTRY
    }

    #[inline]
    pub fn address(&self) -> FixedPageAddress {
        FixedPageAddress::from_control(self.0)
    }
}

/// Atomic hash-bucket overflow entry.
#[derive(Default)]
#[repr(transparent)]
pub struct AtomicHashBucketOverflowEntry(AtomicU64);

impl AtomicHashBucketOverflowEntry {
    #[inline]
    pub fn load(&self) -> HashBucketOverflowEntry {
        HashBucketOverflowEntry(self.0.load(Ordering::Relaxed))
    }

    #[inline]
    pub fn store(&self, desired: HashBucketOverflowEntry) {
        self.0.store(desired.0, Ordering::Relaxed)
    }

    #[inline]
    pub fn compare_exchange(
        &self,
        current: HashBucketOverflowEntry,
        new: HashBucketOverflowEntry,
    ) -> Result<HashBucketOverflowEntry, HashBucketOverflowEntry> {
        match self
            .0
            .compare_exchange(current.0, new.0, Ordering::SeqCst, Ordering::SeqCst)
        {
            Ok(v) => Ok(HashBucketOverflowEntry(v)),
            Err(v) => Err(HashBucketOverflowEntry(v)),
        }
    }
}

/// A bucket consisting of 7 hash bucket entries, plus one hash bucket overflow entry.
/// Fits in a cache line.
#[derive(Default)]
#[repr(align(64))]
pub struct HotLogIndexHashBucket {
    pub entries: [AtomicHashBucketEntry; 7],
    pub overflow_entry: AtomicHashBucketOverflowEntry,
}

static_assertions::assert_eq_size!(HotLogIndexHashBucket, [u8; K_CACHE_LINE_BYTES]);

/// A bucket consisting of 8 hash bucket entries (no overflow buckets)
/// Fits in a cache line.
#[derive(Default)]
#[repr(align(64))]
pub struct ColdLogIndexHashBucket {
    pub entries: [AtomicHashBucketEntry; 8],
}

impl Clone for ColdLogIndexHashBucket {
    fn clone(&self) -> Self {
        let mut new_bucket = Self::default();
        for (i, entry) in self.entries.iter().enumerate() {
            new_bucket.entries[i] = AtomicHashBucketEntry(AtomicU64::new(entry.load().control()));
        }
        new_bucket
    }
}

static_assertions::assert_eq_size!(ColdLogIndexHashBucket, [u8; K_CACHE_LINE_BYTES]);

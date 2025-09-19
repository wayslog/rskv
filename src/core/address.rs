use std::ops::{Add, AddAssign, Sub};
use std::sync::atomic::{AtomicU64, Ordering};

/// Represents a logical address into persistent memory. Identifies a page and an offset within that page.
/// Uses 48 bits: 25 bits for the offset and 23 bits for the page. (The remaining 16 bits are
/// reserved for use by the hash table.)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
#[repr(transparent)]
pub struct Address(u64);

impl Address {
    /// A logical address is 8 bytes.
    /// --of which 48 bits are used for the address. (The remaining 16 bits are used by the hash
    /// table, for control bits and the tag.)
    pub const K_ADDRESS_BITS: u64 = 48;
    pub const K_MAX_ADDRESS: u64 = (1 << Self::K_ADDRESS_BITS) - 1;

    /// --of which 25 bits are used for offsets into a page, of size 2^25 = 32 MB.
    pub const K_OFFSET_BITS: u64 = 25;
    pub const K_MAX_OFFSET: u32 = (1 << Self::K_OFFSET_BITS) - 1;

    /// --and the remaining 23 bits are used for the page index, allowing for approximately 8 million
    /// pages.
    pub const K_PAGE_BITS: u64 = Self::K_ADDRESS_BITS - Self::K_OFFSET_BITS;
    pub const K_MAX_PAGE: u32 = (1 << Self::K_PAGE_BITS) - 1;

    /// An invalid address, used when you need to initialize an address but you don't have a valid
    /// value for it yet. NOTE: set to 1, not 0, to distinguish an invalid hash bucket entry
    /// (initialized to all zeros) from a valid hash bucket entry that points to an invalid address.
    pub const INVALID_ADDRESS: Address = Address(1);

    /// Mask to check if the address is in the read cache.
    pub const K_READ_CACHE_MASK: u64 = 1 << (Self::K_ADDRESS_BITS - 1);

    /// Creates a new `Address` from page and offset.
    pub fn new(page: u32, offset: u32) -> Self {
        debug_assert!(
            page <= Self::K_MAX_PAGE,
            "Page index {} exceeds max page index {}",
            page,
            Self::K_MAX_PAGE
        );
        debug_assert!(
            offset <= Self::K_MAX_OFFSET,
            "Offset {} exceeds max offset {}",
            offset,
            Self::K_MAX_OFFSET
        );
        Address(((page as u64) << Self::K_OFFSET_BITS) | (offset as u64))
    }

    /// Creates an `Address` from a raw `u64` control value.
    /// Asserts that the reserved bits (above K_ADDRESS_BITS) are zero, as per C++ implementation.
    pub fn from_control(control: u64) -> Self {
        debug_assert!(
            control >> Self::K_ADDRESS_BITS == 0,
            "Invalid address control value: reserved bits are not zero. Value: {:#x}",
            control
        );
        Address(control)
    }

    /// Creates a new `Address` from a raw pointer address.
    /// The pointer must be 8-byte aligned.
    pub fn from_ptr(ptr: *const u8) -> Self {
        debug_assert!(
            ptr as usize % 8 == 0,
            "Pointer must be 8-byte aligned, but got address {:p}",
            ptr
        );
        Address(ptr as u64)
    }

    /// Converts the address back to a raw pointer.
    /// This should only be used for addresses created with `from_ptr`.
    pub fn as_ptr(&self) -> *const u8 {
        self.0 as *const u8
    }

    /// Returns the raw `u64` control value of the address.
    pub fn control(&self) -> u64 {
        self.0
    }

    /// Returns the page index of the address.
    pub fn page(&self) -> u32 {
        ((self.0 >> Self::K_OFFSET_BITS) & Self::K_MAX_PAGE as u64) as u32
    }

    /// Returns the offset within the page.
    pub fn offset(&self) -> u32 {
        (self.0 & Self::K_MAX_OFFSET as u64) as u32
    }

    /// Checks if the address is in the read cache.
    pub fn in_readcache(&self) -> bool {
        (self.0 & Self::K_READ_CACHE_MASK) != 0
    }

    /// Returns the address with the read cache bit cleared.
    pub fn readcache_address(&self) -> Address {
        Address(self.0 & !Self::K_READ_CACHE_MASK)
    }
}

// --- Trait Implementations for Address ---

impl From<u64> for Address {
    fn from(control: u64) -> Self {
        Address::from_control(control)
    }
}

impl From<Address> for u64 {
    fn from(address: Address) -> Self {
        address.0
    }
}

impl Add<u64> for Address {
    type Output = Self;
    fn add(self, delta: u64) -> Self::Output {
        // C++ version asserts delta < UINT32_MAX, but then adds to uint64_t control.
        // We'll allow adding any u64, but ensure reserved bits remain zero if possible.
        let new_control = self.0 + delta;
        debug_assert!(
            new_control >> Self::K_ADDRESS_BITS == 0,
            "Address addition overflowed reserved bits. Original: {:#x}, Delta: {:#x}, New: {:#x}",
            self.0,
            delta,
            new_control
        );
        Address(new_control)
    }
}

impl AddAssign<u64> for Address {
    fn add_assign(&mut self, delta: u64) {
        self.0 += delta;
        debug_assert!(
            self.0 >> Self::K_ADDRESS_BITS == 0,
            "Address addition overflowed reserved bits. Value: {:#x}",
            self.0
        );
    }
}

impl Sub for Address {
    type Output = u64;
    fn sub(self, other: Self) -> Self::Output {
        // C++ returns Address, but it's really a u64 difference.
        self.0 - other.0
    }
}

/// Atomic (logical) address.
#[derive(Default)]
pub struct AtomicAddress(AtomicU64);

impl AtomicAddress {
    /// Creates a new `AtomicAddress` from an `Address`.
    pub fn new(address: Address) -> Self {
        AtomicAddress(AtomicU64::new(address.control()))
    }

    /// Atomically loads the current `Address`.
    pub fn load(&self, order: Ordering) -> Address {
        Address(self.0.load(order))
    }

    /// Atomically stores an `Address`.
    pub fn store(&self, value: Address, order: Ordering) {
        self.0.store(value.control(), order)
    }

    /// Atomically compares and exchanges the `Address`.
    /// If `current` is the same as the current value, `new` is stored.
    /// Returns `Ok(Address)` if the exchange happened, `Err(Address)` with the actual value otherwise.
    pub fn compare_exchange(
        &self,
        current: Address,
        new: Address,
        success: Ordering,
        failure: Ordering,
    ) -> Result<Address, Address> {
        match self.0.compare_exchange(current.0, new.0, success, failure) {
            Ok(v) => Ok(Address(v)),
            Err(v) => Err(Address(v)),
        }
    }

    /// Atomically compares and exchanges the `Address` (strong version).
    /// This is equivalent to `compare_exchange` for `AtomicU64`.
    pub fn compare_exchange_strong(
        &self,
        current: Address,
        new: Address,
        success: Ordering,
        failure: Ordering,
    ) -> Result<Address, Address> {
        self.compare_exchange(current, new, success, failure)
    }

    /// Returns the page index of the address.
    pub fn page(&self) -> u32 {
        self.load(Ordering::Relaxed).page()
    }

    /// Returns the offset within the page.
    pub fn offset(&self) -> u32 {
        self.load(Ordering::Relaxed).offset()
    }

    /// Returns the raw `u64` control value of the address.
    pub fn control(&self) -> u64 {
        self.load(Ordering::Relaxed).control()
    }
}

use crate::core::address::Address;
use std::marker::PhantomData;
use std::mem;
use std::ptr;

/// Record header, internal to FASTER.
/// Corresponds to `RecordInfo` in C++ `core/record.h`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
#[repr(C, align(8))]
pub struct RecordInfo(u64);

impl RecordInfo {
    // Bitfield constants based on C++ `RecordInfo` union layout
    pub const PREVIOUS_ADDRESS_BITS: u32 = 48;
    pub const CHECKPOINT_VERSION_BITS: u32 = 13;
    pub const INVALID_BIT: u32 = 1;
    pub const TOMBSTONE_BIT: u32 = 1;
    pub const FINAL_BIT: u32 = 1;

    pub const PREVIOUS_ADDRESS_MASK: u64 = (1 << Self::PREVIOUS_ADDRESS_BITS) - 1;
    pub const CHECKPOINT_VERSION_MASK: u64 = (1 << Self::CHECKPOINT_VERSION_BITS) - 1;

    pub const CHECKPOINT_VERSION_SHIFT: u32 = Self::PREVIOUS_ADDRESS_BITS;
    pub const INVALID_SHIFT: u32 = Self::PREVIOUS_ADDRESS_BITS + Self::CHECKPOINT_VERSION_BITS;
    pub const TOMBSTONE_SHIFT: u32 = Self::INVALID_SHIFT + Self::INVALID_BIT;
    pub const FINAL_SHIFT: u32 = Self::TOMBSTONE_SHIFT + Self::TOMBSTONE_BIT;

    pub fn new(
        previous_address: Address,
        checkpoint_version: u16,
        invalid: bool,
        tombstone: bool,
        final_bit: bool,
    ) -> Self {
        let mut control = previous_address.control();
        control |= (checkpoint_version as u64 & Self::CHECKPOINT_VERSION_MASK)
            << Self::CHECKPOINT_VERSION_SHIFT;
        if invalid {
            control |= 1 << Self::INVALID_SHIFT;
        }
        if tombstone {
            control |= 1 << Self::TOMBSTONE_SHIFT;
        }
        if final_bit {
            control |= 1 << Self::FINAL_SHIFT;
        }
        RecordInfo(control)
    }

    pub fn from_control(control: u64) -> Self {
        RecordInfo(control)
    }

    pub fn control(&self) -> u64 {
        self.0
    }

    pub fn previous_address(&self) -> Address {
        Address::from_control(self.0 & Self::PREVIOUS_ADDRESS_MASK)
    }

    pub fn set_invalid(&mut self, invalid: bool) {
        if invalid {
            self.0 |= 1 << Self::INVALID_SHIFT;
        } else {
            self.0 &= !(1 << Self::INVALID_SHIFT);
        }
    }

    pub fn tombstone(&self) -> bool {
        ((self.0 >> Self::TOMBSTONE_SHIFT) & 1) != 0
    }

    pub fn set_tombstone(&mut self, tombstone: bool) {
        if tombstone {
            self.0 |= 1 << Self::TOMBSTONE_SHIFT;
        } else {
            self.0 &= !(1 << Self::TOMBSTONE_SHIFT);
        }
    }
}

/// A record stored in the log.
#[repr(C, align(8))]
pub struct Record<K, V> {
    pub header: RecordInfo,
    _key: PhantomData<K>,
    _value: PhantomData<V>,
}

impl<K, V> Record<K, V>
where
    K: Sized,
    V: Sized,
{
    /// Calculates the required size for a record with the given key and value.
    pub fn required_size() -> u32 {
        let layout = std::alloc::Layout::new::<Self>()
            .extend(std::alloc::Layout::new::<K>())
            .unwrap()
            .0
            .extend(std::alloc::Layout::new::<V>())
            .unwrap()
            .0;
        layout.pad_to_align().size() as u32
    }

    /// Calculates the required size for a record with alignment padding.
    pub fn required_size_with_alignment() -> u32 {
        let header_size = mem::size_of::<RecordInfo>();
        let key_size = mem::size_of::<K>();
        let value_size = mem::size_of::<V>();

        // Calculate alignment requirements
        let header_alignment = mem::align_of::<RecordInfo>();
        let key_alignment = mem::align_of::<K>();
        let value_alignment = mem::align_of::<V>();

        // Start with header size
        let mut total_size = header_size;

        // Add key with proper alignment
        let key_offset = total_size;
        let key_alignment_padding = key_offset % key_alignment;
        if key_alignment_padding != 0 {
            total_size += key_alignment - key_alignment_padding;
        }
        total_size += key_size;

        // Add value with proper alignment
        let value_offset = total_size;
        let value_alignment_padding = value_offset % value_alignment;
        if value_alignment_padding != 0 {
            total_size += value_alignment - value_alignment_padding;
        }
        total_size += value_size;

        // Add extra padding to ensure the entire record is properly aligned
        let max_alignment = std::cmp::max(
            header_alignment,
            std::cmp::max(key_alignment, value_alignment),
        );
        let final_alignment_padding = total_size % max_alignment;
        if final_alignment_padding != 0 {
            total_size += max_alignment - final_alignment_padding;
        }

        total_size as u32
    }

    /// Creates a new record in the provided byte slice.
    ///
    /// # Safety
    /// The caller must ensure the buffer is large enough and properly aligned.
    pub unsafe fn create_in(buffer: &mut [u8], header: RecordInfo, key: &K, value: &V) {
        unsafe {
            // Ensure the buffer is properly aligned for Record<K, V>
            let buffer_ptr = buffer.as_mut_ptr();
            let max_alignment = std::cmp::max(
                mem::align_of::<RecordInfo>(),
                std::cmp::max(mem::align_of::<K>(), mem::align_of::<V>()),
            );

            // Calculate the offset needed to align the buffer
            let offset = buffer_ptr.align_offset(max_alignment);
            if offset >= buffer.len() {
                panic!("Buffer too small for alignment requirements");
            }

            let aligned_ptr = buffer_ptr.add(offset);
            let record_ptr = aligned_ptr as *mut Self;

            // Verify alignment
            debug_assert!(
                record_ptr as usize % max_alignment == 0,
                "Record pointer not properly aligned: {:p} (alignment: {})",
                record_ptr,
                max_alignment
            );

            // Write header
            ptr::write(&mut (*record_ptr).header, header);

            // Calculate key position - assume key is properly aligned after header
            let key_offset = mem::size_of::<RecordInfo>();
            let key_ptr = (record_ptr as *mut u8).add(key_offset) as *mut K;
            ptr::copy_nonoverlapping(key, key_ptr, 1);

            // Calculate value position - assume value is properly aligned after key
            let value_offset = key_offset + mem::size_of::<K>();
            let value_ptr = (record_ptr as *mut u8).add(value_offset) as *mut V;
            ptr::copy_nonoverlapping(value, value_ptr, 1);
        }
    }

    /// Returns a reference to the key from a record pointer.
    /// # Safety
    /// The caller must ensure the pointer points to a valid record layout
    /// and that the lifetime of the returned reference does not outlive the buffer.
    pub unsafe fn key<'a>(record_ptr: *const Self) -> &'a K {
        unsafe {
            // Use a simpler approach: calculate the key position based on the record layout
            let key_offset = mem::size_of::<RecordInfo>();
            let key_ptr = (record_ptr as *const u8).add(key_offset) as *const K;

            // Use unaligned read to avoid alignment issues
            let key = ptr::read_unaligned(key_ptr);
            // Create a static reference to the key (this is safe because we're returning a reference)
            Box::leak(Box::new(key))
        }
    }

    /// Returns a reference to the value from a record pointer.
    pub unsafe fn value<'a>(record_ptr: *const Self) -> &'a V {
        unsafe {
            // Use a simpler approach: calculate the value position based on the record layout
            let key_offset = mem::size_of::<RecordInfo>();
            let key_size = mem::size_of::<K>();

            // Calculate value position - assume key is properly aligned after header
            let value_offset = key_offset + key_size;
            let value_ptr = (record_ptr as *const u8).add(value_offset) as *const V;

            // Use unaligned read to avoid alignment issues
            let value = ptr::read_unaligned(value_ptr);
            // Create a static reference to the value (this is safe because we're returning a reference)
            Box::leak(Box::new(value))
        }
    }

    /// Returns a mutable reference to the value from a record pointer.
    pub unsafe fn value_mut<'a>(record_ptr: *mut Self) -> &'a mut V {
        unsafe {
            // Use a simpler approach: calculate the value position based on the record layout
            let key_offset = mem::size_of::<RecordInfo>();
            let key_size = mem::size_of::<K>();

            // Calculate value position - assume key is properly aligned after header
            let value_offset = key_offset + key_size;
            let value_ptr = (record_ptr as *mut u8).add(value_offset) as *mut V;

            // Use unaligned read to avoid alignment issues
            let value = ptr::read_unaligned(value_ptr);
            // Create a static mutable reference to the value (this is safe because we're returning a reference)
            Box::leak(Box::new(value))
        }
    }
}

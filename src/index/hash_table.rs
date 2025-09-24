use crate::core::alloc::{aligned_alloc, aligned_free};
use crate::core::constants::K_CACHE_LINE_BYTES;
use crate::core::status::Status;
use crate::core::utility;
use crate::environment::file::File;
use crate::index::definitions::HashIndexDefinition;
use std::alloc::Layout;
use std::marker::PhantomData;
use std::ptr::null_mut;

/// The hash table itself: a sized array of HashBuckets.
pub struct InternalHashTable<D: HashIndexDefinition> {
    buckets: *mut D::HashBucket,
    size: u64,
    _marker: PhantomData<D>,
}

// The hash table is Send + Sync because it owns the memory, and access to the buckets
// is done via atomic operations within AtomicHashBucketEntry.
unsafe impl<D: HashIndexDefinition> Send for InternalHashTable<D> {}
unsafe impl<D: HashIndexDefinition> Sync for InternalHashTable<D> {}

impl<D: HashIndexDefinition> Default for InternalHashTable<D> {
    fn default() -> Self {
        Self::new()
    }
}

impl<D: HashIndexDefinition> InternalHashTable<D> {
    /// Creates a new, uninitialized hash table.
    pub fn new() -> Self {
        Self {
            buckets: null_mut(),
            size: 0,
            _marker: PhantomData,
        }
    }

    /// Initializes the hash table with a given size and alignment.
    ///
    /// # Safety
    /// This is unsafe because it involves raw pointer allocation and manipulation.
    /// The caller must ensure that this method is called only on an uninitialized
    /// or properly deinitialized table.
    pub unsafe fn initialize(&mut self, new_size: u64, alignment: usize) {
        assert!(new_size > 0);
        assert!(utility::is_power_of_two(new_size));
        assert!(alignment >= K_CACHE_LINE_BYTES);
        assert!(utility::is_power_of_two(alignment as u64));

        if self.size != new_size {
            if !self.buckets.is_null() {
                unsafe {
                    self.uninitialize();
                }
            }
            self.size = new_size;
            let layout = Layout::from_size_align(
                (self.size as usize) * std::mem::size_of::<D::HashBucket>(),
                alignment,
            )
            .expect("Failed to create layout for hash table");
            self.buckets = unsafe { aligned_alloc(layout) as *mut D::HashBucket };
        }

        // Zero out the memory
        unsafe {
            std::ptr::write_bytes(self.buckets, 0, self.size as usize);
        }
    }

    /// Deinitializes the hash table, freeing its memory.
    ///
    /// # Safety
    /// This is unsafe because it deallocates raw memory. The caller must ensure
    /// this is not called more than once for a given allocation.
    pub unsafe fn uninitialize(&mut self) {
        if !self.buckets.is_null() {
            let layout = unsafe {
                Layout::from_size_align_unchecked(
                    (self.size as usize) * std::mem::size_of::<D::HashBucket>(),
                    K_CACHE_LINE_BYTES, // Assuming at least cache line alignment
                )
            };
            unsafe {
                aligned_free(self.buckets as *mut u8, layout);
            }
            self.buckets = null_mut();
            self.size = 0;
        }
    }

    /// Returns the number of buckets in the hash table.
    #[inline]
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Gets a reference to the bucket at the specified index.
    ///
    /// # Safety
    /// The caller must ensure the index is within bounds.
    #[inline]
    pub unsafe fn get_bucket(&self, index: u64) -> &D::HashBucket {
        debug_assert!(index < self.size);
        unsafe { &*self.buckets.add(index as usize) }
    }

    pub fn checkpoint(&mut self, file: &mut File) -> Result<(), Status> {
        let bytes = unsafe {
            std::slice::from_raw_parts(
                self.buckets as *const u8,
                self.size as usize * std::mem::size_of::<D::HashBucket>(),
            )
        };
        file.write(0, bytes)?;
        Ok(())
    }

    pub fn recover(
        &mut self,
        file: &mut File,
        table_size: u64,
        num_ht_bytes: u64,
    ) -> Result<(), Status> {
        let expected_bytes = table_size * std::mem::size_of::<D::HashBucket>() as u64;
        if expected_bytes != num_ht_bytes {
            return Err(Status::Corruption);
        }
        unsafe {
            self.initialize(table_size, K_CACHE_LINE_BYTES);
            let buffer =
                std::slice::from_raw_parts_mut(self.buckets as *mut u8, num_ht_bytes as usize);
            file.read(0, buffer)?;
        }
        Ok(())
    }
}

impl<D: HashIndexDefinition> Drop for InternalHashTable<D> {
    fn drop(&mut self) {
        unsafe {
            self.uninitialize();
        }
    }
}

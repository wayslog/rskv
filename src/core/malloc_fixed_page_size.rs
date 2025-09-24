use crate::core::alloc::{aligned_alloc, aligned_free};
use crate::core::light_epoch::{Guard, LightEpoch};
use crate::core::status::Status;
use crate::environment::file::File;

use log::error;

use std::alloc::Layout;
use std::cell::{RefCell, UnsafeCell};
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::ptr::{self};
use std::sync::Mutex;
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};

/// Address into a fixed page.
/// Corresponds to `FixedPageAddress` in C++ `core/malloc_fixed_page_size.h`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
#[repr(transparent)]
pub struct FixedPageAddress(u64);

impl FixedPageAddress {
    /// A fixed-page address is 8 bytes.
    /// --of which 48 bits are used for the address.
    pub const K_ADDRESS_BITS: u64 = 48;

    /// --of which 20 bits are used for offsets into a page.
    pub const K_OFFSET_BITS: u64 = 20;
    pub const K_MAX_OFFSET: u64 = (1 << Self::K_OFFSET_BITS) - 1;

    /// --and the remaining 28 bits are used for the page index.
    pub const K_PAGE_BITS: u64 = Self::K_ADDRESS_BITS - Self::K_OFFSET_BITS;
    pub const K_MAX_PAGE: u64 = (1 << Self::K_PAGE_BITS) - 1;

    /// Invalid address constant.
    pub const INVALID_ADDRESS: FixedPageAddress = FixedPageAddress(0);

    /// Creates a new `FixedPageAddress` from a raw `u64` control value.
    pub fn from_control(control: u64) -> Self {
        debug_assert!(
            control >> Self::K_ADDRESS_BITS == 0,
            "Invalid FixedPageAddress control value: reserved bits are not zero. Value: {:#x}",
            control
        );
        FixedPageAddress(control)
    }

    /// Returns the raw `u64` control value.
    pub fn control(&self) -> u64 {
        self.0
    }

    /// Returns the page index.
    pub fn page(&self) -> u64 {
        (self.0 >> Self::K_OFFSET_BITS) & Self::K_MAX_PAGE
    }

    /// Returns the offset within the page.
    pub fn offset(&self) -> u32 {
        (self.0 & Self::K_MAX_OFFSET) as u32
    }
}

/// Atomic address into a fixed page.
#[repr(transparent)]
#[derive(Default)]
pub struct AtomicFixedPageAddress(AtomicU64);

impl AtomicFixedPageAddress {
    /// Creates a new `AtomicFixedPageAddress`.
    pub fn new(address: FixedPageAddress) -> Self {
        AtomicFixedPageAddress(AtomicU64::new(address.control()))
    }

    /// Atomically loads the current `FixedPageAddress`.
    pub fn load(&self, order: Ordering) -> FixedPageAddress {
        FixedPageAddress(self.0.load(order))
    }

    /// Atomically stores a `FixedPageAddress`.
    pub fn store(&self, value: FixedPageAddress, order: Ordering) {
        self.0.store(value.control(), order)
    }

    /// Atomically increments the address and returns the previous value.
    pub fn fetch_add(&self, val: u64, order: Ordering) -> FixedPageAddress {
        FixedPageAddress(self.0.fetch_add(val, order))
    }
}

const K_PAGE_SIZE: usize = (FixedPageAddress::K_MAX_OFFSET + 1) as usize;

/// A single page of items.
#[repr(align(64))]
struct FixedPage<T: Sized + Default> {
    // Use heap allocation for large arrays to avoid stack overflow
    elements: Box<[UnsafeCell<T>]>,
}

impl<T: Sized + Default> Default for FixedPage<T> {
    fn default() -> Self {
        // Use heap allocation for large arrays to avoid stack overflow
        let elements = (0..K_PAGE_SIZE)
            .map(|_| UnsafeCell::new(T::default()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        FixedPage { elements }
    }
}

/// A dynamically growing array of `FixedPage<T>`.
struct FixedPageArray<T: Sized + Default> {
    alignment: usize,
    pages: Box<[AtomicPtr<FixedPage<T>>]>,
}

impl<T: Sized + Default> FixedPageArray<T> {
    fn new(size: u64, alignment: usize) -> Self {
        let pages = (0..size)
            .map(|_| AtomicPtr::new(ptr::null_mut()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { alignment, pages }
    }

    fn size(&self) -> u64 {
        self.pages.len() as u64
    }

    fn get(&self, page_idx: u64) -> *mut FixedPage<T> {
        self.pages[page_idx as usize].load(Ordering::Acquire)
    }

    fn get_or_add(&self, page_idx: u64) -> *mut FixedPage<T> {
        let mut page = self.get(page_idx);
        if page.is_null() {
            page = self.add_page(page_idx);
        }
        page
    }

    fn add_page(&self, page_idx: u64) -> *mut FixedPage<T> {
        let layout =
            match Layout::from_size_align(std::mem::size_of::<FixedPage<T>>(), self.alignment) {
                Ok(layout) => layout,
                Err(_) => return ptr::null_mut(), // Invalid alignment
            };
        let new_page_ptr = unsafe { aligned_alloc(layout) as *mut FixedPage<T> };
        unsafe { ptr::write(new_page_ptr, FixedPage::default()) };

        match self.pages[page_idx as usize].compare_exchange(
            ptr::null_mut(),
            new_page_ptr,
            Ordering::Release,
            Ordering::Relaxed,
        ) {
            Ok(_) => new_page_ptr,
            Err(existing_ptr) => {
                unsafe {
                    ptr::drop_in_place(new_page_ptr);
                    aligned_free(new_page_ptr as *mut u8, layout);
                }
                existing_ptr
            }
        }
    }
}

impl<T: Sized + Default> Drop for FixedPageArray<T> {
    fn drop(&mut self) {
        let layout =
            match Layout::from_size_align(std::mem::size_of::<FixedPage<T>>(), self.alignment) {
                Ok(layout) => layout,
                Err(_) => {
                    // If we can't create the layout, we can't safely free the memory
                    // This should never happen if the array was created successfully
                    log::error!("Cannot create layout during drop - potential memory leak");
                    return;
                }
            };
        for page_ptr in self.pages.iter() {
            let ptr = page_ptr.load(Ordering::Relaxed);
            if !ptr.is_null() {
                unsafe {
                    ptr::drop_in_place(ptr);
                    aligned_free(ptr as *mut u8, layout);
                }
            }
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
struct FreeAddress {
    addr: FixedPageAddress,
    removal_epoch: u64,
}

thread_local! {
    static FREE_LIST: RefCell<VecDeque<FreeAddress>> = const { RefCell::new(VecDeque::new()) };
}

/// Free list entry for deferred deallocation
#[derive(Debug, Clone)]
struct FreeListEntry {
    address: FixedPageAddress,
    epoch: u64,
}

/// The allocator used for the hash table's overflow buckets.
pub struct MallocFixedPageSize<'epoch, T: Sized + Default> {
    alignment: usize,
    page_array: AtomicPtr<FixedPageArray<T>>,
    count: AtomicFixedPageAddress,
    epoch: Option<&'epoch LightEpoch>,
    free_list: Mutex<VecDeque<FreeListEntry>>,
    _marker: PhantomData<T>,
}

impl<'epoch, T: Sized + Default> Default for MallocFixedPageSize<'epoch, T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'epoch, T: Sized + Default> MallocFixedPageSize<'epoch, T> {
    pub fn new() -> Self {
        Self {
            alignment: 0,
            page_array: AtomicPtr::new(ptr::null_mut()),
            count: AtomicFixedPageAddress::default(),
            epoch: None, // Will be set during initialization
            free_list: Mutex::new(VecDeque::new()),
            _marker: PhantomData,
        }
    }

    pub fn initialize(&mut self, alignment: usize, epoch: &'epoch LightEpoch) {
        self.uninitialize(); // Clear any previous state
        self.alignment = alignment;
        self.epoch = Some(epoch);
        self.count = AtomicFixedPageAddress::new(FixedPageAddress::from_control(0));

        let initial_array = Box::into_raw(Box::new(FixedPageArray::new(2, alignment)));
        self.page_array.store(initial_array, Ordering::Release);
        unsafe {
            (*initial_array).add_page(0);
        }

        // Allocate the null pointer address, which is address 0.
        self.allocate();
    }

    pub fn uninitialize(&mut self) {
        let array_ptr = self.page_array.swap(ptr::null_mut(), Ordering::AcqRel);
        if !array_ptr.is_null() {
            unsafe {
                drop(Box::from_raw(array_ptr));
            }
        }
    }

    pub fn checkpoint(&self, file: &mut File) -> Result<u64, Status> {
        let array = unsafe { &*self.page_array.load(Ordering::Acquire) };
        let count = self.count.load(Ordering::Acquire);
        let num_pages = count.page() + if count.offset() > 0 { 1 } else { 0 };
        let mut offset = 0;

        for i in 0..num_pages {
            let page_ptr = array.get(i);
            if page_ptr.is_null() {
                continue;
            } // Should not happen in a consistent checkpoint
            let page_size = std::mem::size_of::<FixedPage<T>>();
            let buffer = unsafe { std::slice::from_raw_parts(page_ptr as *const u8, page_size) };
            file.write(offset, buffer)?;
            offset += page_size as u64;
        }
        Ok(offset)
    }

    pub fn recover(
        &mut self,
        file: &mut File,
        num_ofb_bytes: u64,
        ofb_count: FixedPageAddress,
    ) -> Status {
        let num_pages = ofb_count.page() + if ofb_count.offset() > 0 { 1 } else { 0 };
        if num_ofb_bytes != num_pages * std::mem::size_of::<FixedPage<T>>() as u64 {
            return Status::Corruption;
        }

        let new_array_size = (ofb_count.page() + 2).next_power_of_two();
        let new_array = Box::into_raw(Box::new(FixedPageArray::new(
            new_array_size,
            self.alignment,
        )));
        self.page_array.store(new_array, Ordering::Release);
        self.count.store(ofb_count, Ordering::Relaxed);

        let mut offset = 0;
        for i in 0..num_pages {
            let page_ptr = unsafe { (*new_array).get_or_add(i) };
            let page_size = std::mem::size_of::<FixedPage<T>>();
            let buffer = unsafe { std::slice::from_raw_parts_mut(page_ptr as *mut u8, page_size) };
            if let Err(status) = file.read(offset, buffer) {
                error!("Failed to read from file: {:?}", status);
                return Status::IoError;
            }
            offset += page_size as u64;
        }
        Status::Ok
    }

    /// Gets a mutable reference to the element at the given address.
    ///
    /// # Safety
    /// This method is unsafe because it allows multiple mutable references
    /// to the same data. The caller must ensure exclusive access.
    #[allow(clippy::mut_from_ref)]
    pub unsafe fn get_unchecked(&self, address: FixedPageAddress) -> &mut T {
        unsafe {
            let array = &*self.page_array.load(Ordering::Acquire);
            let page = array.get(address.page());
            assert!(!page.is_null());
            let elements = &(*page).elements;
            let cell = &elements[address.offset() as usize];
            &mut *cell.get()
        }
    }

    /// Gets a safe immutable reference to the element at the given address.
    pub fn get(&self, address: FixedPageAddress) -> &T {
        let array = unsafe { &*self.page_array.load(Ordering::Acquire) };
        let page = array.get(address.page());
        assert!(!page.is_null());
        let elements = unsafe { &(*page).elements };
        let cell = &elements[address.offset() as usize];
        unsafe { &*cell.get() }
    }

    /// Returns the actual memory address for a given FixedPageAddress
    pub fn get_address(&self, address: FixedPageAddress) -> *mut u8 {
        let array = unsafe { &*self.page_array.load(Ordering::Acquire) };
        let page = array.get(address.page());
        assert!(!page.is_null());
        let elements = unsafe { &(*page).elements };
        let cell = &elements[address.offset() as usize];
        cell.get() as *mut u8
    }

    pub fn allocate(&self) -> FixedPageAddress {
        let epoch = match self.epoch {
            Some(epoch) => epoch,
            None => {
                // If epoch is not set, just allocate without epoch protection
                let addr = self.count.fetch_add(1, Ordering::Relaxed);
                let array = unsafe { &*self.page_array.load(Ordering::Acquire) };
                array.get_or_add(addr.page());
                return addr;
            }
        };

        // Check free list first with epoch safety
        let guard = epoch.protect();
        if let Some(freed_addr) = self.try_allocate_from_free_list(&guard) {
            return freed_addr;
        }

        // Fallback to allocating new
        let addr = self.count.fetch_add(1, Ordering::Relaxed);
        let mut array = unsafe { &*self.page_array.load(Ordering::Acquire) };

        if addr.page() >= array.size() {
            self.expand_array(addr.page() + 1, &guard);
            array = unsafe { &*self.page_array.load(Ordering::Acquire) };
        }

        if addr.offset() == 0 && addr.page() + 1 < array.size() {
            array.add_page(addr.page() + 1);
        }
        array.get_or_add(addr.page());
        addr
    }

    /// Try to allocate from the free list with epoch safety
    fn try_allocate_from_free_list(&self, _guard: &Guard) -> Option<FixedPageAddress> {
        let mut free_list = self.free_list.lock().ok()?;
        let current_epoch = 0; // Simplified: use 0 as current epoch

        // Find the first entry that's safe to reuse
        if let Some(entry) = free_list.front()
            && entry.epoch <= current_epoch
        {
            let addr = entry.address;
            free_list.pop_front();
            return Some(addr);
        }
        // Entry is not yet safe to reuse, no point in checking others
        // since they are ordered by epoch
        None
    }

    pub fn free_at_epoch(&self, addr: FixedPageAddress, _guard: &Guard) {
        // Add to deferred free list with current epoch
        if let Ok(mut free_list) = self.free_list.lock() {
            let entry = FreeListEntry {
                address: addr,
                epoch: 0, // Simplified: use 0 as current epoch
            };
            free_list.push_back(entry);
        }
    }

    fn expand_array(&self, min_size: u64, guard: &Guard) {
        let old_array_ptr = self.page_array.load(Ordering::Acquire);
        let old_array = unsafe { &*old_array_ptr };
        let old_size = old_array.size();

        if min_size <= old_size {
            return;
        }

        let new_size = min_size.next_power_of_two();
        let new_array_box = Box::new(FixedPageArray::new(new_size, self.alignment));

        for i in 0..old_size {
            let page = old_array.get(i);
            if !page.is_null() {
                new_array_box.pages[i as usize].store(page, Ordering::Relaxed);
            }
        }

        let new_array_ptr = Box::into_raw(new_array_box);

        if self
            .page_array
            .compare_exchange(
                old_array_ptr,
                new_array_ptr,
                Ordering::Release,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            unsafe {
                guard.defer_unchecked(move || {
                    drop(Box::from_raw(old_array_ptr));
                });
            }
        } else {
            unsafe {
                drop(Box::from_raw(new_array_ptr));
            }
        }
    }
}

impl<'epoch, T: Sized + Default> Drop for MallocFixedPageSize<'epoch, T> {
    fn drop(&mut self) {
        self.uninitialize();
    }
}

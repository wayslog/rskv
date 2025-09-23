use crate::core::address::{Address, AtomicAddress};
use crate::core::alloc::aligned_alloc;
use crate::core::checkpoint::LogMetadata;
use crate::core::light_epoch::LightEpoch;
use crate::core::record::Record;
use crate::core::status::Status;
use std::alloc::Layout;
use std::ptr;
use std::sync::Mutex;
use std::sync::atomic::{AtomicPtr, AtomicU16, AtomicU64, Ordering};

// --- Page Status Enums and Structs ---

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FlushStatus {
    Flushed,
    InProgress,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CloseStatus {
    Closed,
    Open,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct FlushCloseStatus {
    pub flush: FlushStatus,
    pub close: CloseStatus,
}

impl From<u16> for FlushCloseStatus {
    fn from(control: u16) -> Self {
        unsafe { std::mem::transmute(control) }
    }
}

impl From<FlushCloseStatus> for u16 {
    fn from(status: FlushCloseStatus) -> Self {
        unsafe { std::mem::transmute(status) }
    }
}

#[derive(Default)]
#[repr(transparent)]
pub struct AtomicFlushCloseStatus(AtomicU16);

impl AtomicFlushCloseStatus {
    pub fn load(&self) -> FlushCloseStatus {
        self.0.load(Ordering::Acquire).into()
    }

    pub fn store(&self, flush: FlushStatus, close: CloseStatus) {
        self.0
            .store(FlushCloseStatus { flush, close }.into(), Ordering::Release);
    }

    pub fn compare_exchange(
        &self,
        current: FlushCloseStatus,
        new: FlushCloseStatus,
    ) -> Result<FlushCloseStatus, FlushCloseStatus> {
        match self.0.compare_exchange(
            current.into(),
            new.into(),
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(v) => Ok(v.into()),
            Err(v) => Err(v.into()),
        }
    }
}

#[derive(Default)]
pub struct FullPageStatus {
    pub last_flushed_until_address: AtomicAddress,
    pub status: AtomicFlushCloseStatus,
}

// --- PageOffset for Allocation ---

#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
struct PageOffset(u64);

impl PageOffset {
    fn new(page: u32, offset: u64) -> Self {
        Self(((page as u64) << Address::K_OFFSET_BITS) | offset)
    }

    fn page(&self) -> u32 {
        (self.0 >> Address::K_OFFSET_BITS) as u32
    }

    fn offset(&self) -> u64 {
        self.0 & ((1 << Address::K_OFFSET_BITS) - 1)
    }
}

#[repr(transparent)]
pub struct AtomicPageOffset(AtomicU64);

impl AtomicPageOffset {
    fn new(address: Address) -> Self {
        Self(AtomicU64::new(
            PageOffset::new(address.page(), address.offset() as u64).0,
        ))
    }

    fn load(&self) -> PageOffset {
        PageOffset(self.0.load(Ordering::Acquire))
    }

    fn reserve(&self, num_slots: u32) -> PageOffset {
        // Convert slots to bytes (each slot is 8 bytes)
        let bytes = (num_slots as u64) * 8;
        let current = self.0.load(Ordering::Acquire);
        let current_offset = PageOffset(current);

        // Check if we need to advance to next page
        if current_offset.offset() + bytes > (1 << Address::K_OFFSET_BITS) {
            // Need to advance to next page
            let next_page = current_offset.page() + 1;
            let new_offset = PageOffset::new(next_page, 0);
            self.0.store(new_offset.0, Ordering::Release);
            new_offset
        } else {
            // Can fit in current page
            PageOffset(self.0.fetch_add(bytes, Ordering::Relaxed))
        }
    }

    #[allow(dead_code)]
    fn new_page(&self, old_page: u32) -> bool {
        let expected = self.load();
        if old_page != expected.page() {
            return true; // Another thread already advanced the page
        }
        let new_page_offset = PageOffset::new(old_page + 1, 0);
        self.0
            .compare_exchange(
                expected.0,
                new_page_offset.0,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_ok()
    }
}

// --- Disk Trait ---
pub trait Disk {
    fn write_async(
        &mut self,
        offset: u64,
        data: &[u8],
        callback: Box<dyn FnOnce(Status) + Send>,
    ) -> Status;
    fn index_checkpoint_path(&self, token: &str) -> String;
}

pub struct NullDisk;
impl Disk for NullDisk {
    fn write_async(
        &mut self,
        _offset: u64,
        _data: &[u8],
        callback: Box<dyn FnOnce(Status) + Send>,
    ) -> Status {
        callback(Status::Ok);
        Status::Ok
    }

    fn index_checkpoint_path(&self, _token: &str) -> String {
        String::new()
    }
}

// --- Main Allocator Struct ---
pub struct PersistentMemoryMalloc<'epoch, D: Disk> {
    pub pages: Box<[AtomicPtr<u8>]>,
    pub page_status: Box<[FullPageStatus]>,
    pub buffer_size_in_pages: u32,
    pub page_size: u64,
    pub tail_page_offset: AtomicPageOffset,
    pub read_only_address: AtomicAddress,
    pub safe_read_only_address: AtomicAddress,
    pub head_address: AtomicAddress,
    pub safe_head_address: AtomicAddress,
    pub begin_address: AtomicAddress,
    pub flushed_until_address: AtomicAddress,
    pub epoch: Option<&'epoch LightEpoch>,
    pub disk: Option<Mutex<D>>,
}

impl<'epoch, D: Disk> PersistentMemoryMalloc<'epoch, D> {
    pub const K_PAGE_SIZE: u64 = (Address::K_MAX_OFFSET + 1) as u64;

    pub fn new() -> Self {
        Self {
            pages: Box::new([]),
            page_status: Box::new([]),
            buffer_size_in_pages: 0,
            page_size: Self::K_PAGE_SIZE,
            tail_page_offset: AtomicPageOffset::new(Address::from_control(0)),
            read_only_address: AtomicAddress::new(Address::from_control(0)),
            safe_read_only_address: AtomicAddress::new(Address::from_control(0)),
            head_address: AtomicAddress::new(Address::from_control(0)),
            safe_head_address: AtomicAddress::new(Address::from_control(0)),
            begin_address: AtomicAddress::new(Address::from_control(0)),
            flushed_until_address: AtomicAddress::new(Address::from_control(0)),
            epoch: None,
            disk: None,
        }
    }

    pub fn initialize(&mut self, log_size: u64, epoch: &'epoch LightEpoch, disk: D) {
        self.epoch = Some(epoch);
        self.disk = Some(Mutex::new(disk));
        // Ensure we have at least 1 page, even if log_size is smaller than page_size
        self.buffer_size_in_pages = std::cmp::max(1, (log_size / self.page_size) as u32);

        // Initialize pages array
        let mut pages_vec = Vec::with_capacity(self.buffer_size_in_pages as usize);
        for _ in 0..self.buffer_size_in_pages as usize {
            pages_vec.push(AtomicPtr::new(ptr::null_mut()));
        }
        self.pages = pages_vec.into_boxed_slice();

        let mut status_vec = Vec::with_capacity(self.buffer_size_in_pages as usize);
        for _ in 0..self.buffer_size_in_pages as usize {
            status_vec.push(FullPageStatus::default());
        }
        self.page_status = status_vec.into_boxed_slice();

        // Initialize the first page immediately
        self.new_page(Address::from_control(0));

        // Set initial addresses
        self.begin_address
            .store(Address::from_control(0), Ordering::Release);
        self.head_address
            .store(Address::from_control(0), Ordering::Release);
        self.flushed_until_address
            .store(Address::from_control(0), Ordering::Release);
    }

    pub fn get_tail_address(&self) -> Address {
        let page_offset = self.tail_page_offset.load();
        Address::new(page_offset.page(), page_offset.offset() as u32)
    }

    pub fn get_head_address(&self) -> Address {
        self.head_address.load(Ordering::Acquire)
    }

    pub fn get_read_only_address(&self) -> Address {
        self.read_only_address.load(Ordering::Acquire)
    }

    pub fn get_safe_read_only_address(&self) -> Address {
        self.safe_read_only_address.load(Ordering::Acquire)
    }

    pub fn get_slice(&self, address: Address, size: usize) -> &[u8] {
        let page_idx = address.page() as usize;
        if page_idx >= self.pages.len() {
            return &[];
        }

        let page_ptr = self.pages[page_idx].load(Ordering::Acquire);
        if page_ptr.is_null() {
            return &[];
        }

        let offset = address.offset() as usize;
        unsafe { std::slice::from_raw_parts(page_ptr.add(offset), size) }
    }

    pub fn get_mut_slice(&self, address: Address, size: usize) -> &mut [u8] {
        let page_idx = address.page() as usize;
        if page_idx >= self.pages.len() {
            return &mut [];
        }

        let page_ptr = self.pages[page_idx].load(Ordering::Acquire);
        if page_ptr.is_null() {
            return &mut [];
        }

        let offset = address.offset() as usize;
        unsafe { std::slice::from_raw_parts_mut(page_ptr.add(offset), size) }
    }

    /// Get a record at the given address
    pub fn get<K, V>(&self, address: Address) -> Option<&Record<K, V>> {
        let record_size = std::mem::size_of::<Record<K, V>>();
        let slice = self.get_slice(address, record_size);
        if slice.is_empty() {
            return None;
        }

        unsafe {
            // Ensure proper alignment before dereferencing
            let ptr = slice.as_ptr();
            if ptr as usize % std::mem::align_of::<Record<K, V>>() != 0 {
                return None;
            }
            let record_ptr = ptr as *const Record<K, V>;
            Some(&*record_ptr)
        }
    }

    pub fn allocate(&self, size: u64) -> Result<Address, Address> {
        // Ensure size is aligned to 8-byte boundary for proper alignment
        let aligned_size = ((size + 7) / 8) * 8;
        let num_slots = (aligned_size / 8) as u32;
        let page_offset = self.tail_page_offset.reserve(num_slots);

        // Calculate actual memory address
        let page_idx = page_offset.page() as usize;

        if page_idx < self.pages.len() {
            let page_ptr = self.pages[page_idx].load(Ordering::Acquire);

            if page_ptr.is_null() {
                // Page not allocated yet, try to allocate it
                self.new_page(Address::new(page_offset.page(), 0));
                let page_ptr = self.pages[page_idx].load(Ordering::Acquire);
                if page_ptr.is_null() {
                    return Err(Address::new(page_offset.page(), 0));
                }
            }

            let offset = page_offset.offset() as usize;

            // Check if offset is within page bounds
            if offset + aligned_size as usize > self.page_size as usize {
                return Err(Address::new(page_offset.page(), 0));
            }

            // offset is already 8-byte aligned since we reserve in 8-byte slots
            let actual_address = unsafe { page_ptr.add(offset) };

            // Verify alignment
            debug_assert!(
                actual_address as usize % 8 == 0,
                "Allocated address not 8-byte aligned: {:p}",
                actual_address
            );

            // Ensure the logical address is also 8-byte aligned
            let logical_offset = page_offset.offset() as u32;
            debug_assert!(
                logical_offset % 8 == 0,
                "Logical offset not 8-byte aligned: {}",
                logical_offset
            );

            // Return logical address, not physical pointer
            Ok(Address::new(page_offset.page(), logical_offset))
        } else {
            Err(Address::new(page_offset.page(), 0))
        }
    }

    pub fn new_page(&self, closed_page: Address) {
        let page_idx = closed_page.page() as usize;
        if page_idx < self.pages.len() {
            // Check if page is already allocated
            let current_page = self.pages[page_idx].load(Ordering::Acquire);
            if !current_page.is_null() {
                return; // Page already allocated
            }

            // Allocate new page
            let layout = match Layout::from_size_align(self.page_size as usize, 64) {
                Ok(layout) => layout,
                Err(_) => {
                    // This should never happen with valid page_size and alignment
                    log::error!("Invalid layout parameters: size={}, align=64", self.page_size);
                    return;
                }
            };
            let new_page = unsafe { aligned_alloc(layout) };
            if !new_page.is_null() {
                // Zero out the page
                unsafe {
                    std::ptr::write_bytes(new_page, 0, self.page_size as usize);
                }
                self.pages[page_idx].store(new_page, Ordering::Release);

                // Update the tail page offset to point to the new page
                self.tail_page_offset
                    .0
                    .store(PageOffset::new(page_idx as u32, 0).0, Ordering::Release);
            }
        }
    }

    pub fn checkpoint(&mut self, _disk: &mut D, _token: &str) -> Result<LogMetadata, Status> {
        // Get current addresses
        let flushed_address = self.flushed_until_address.load(Ordering::Acquire);
        let final_address = self.head_address.load(Ordering::Acquire);

        // Calculate approximate record count and data size
        // This is a simplified estimation - in a real implementation,
        // we would track these metrics more precisely
        let data_size = final_address.control() - flushed_address.control();
        let estimated_record_count = data_size / 64; // Rough estimate assuming average record size

        let mut metadata = LogMetadata::new(
            1,
            flushed_address,
            final_address,
            estimated_record_count,
            data_size,
        );

        // Update checksum
        metadata.update_checksum();

        Ok(metadata)
    }

    pub fn recover(
        &mut self,
        _disk: &mut D,
        _token: &str,
        metadata: &LogMetadata,
    ) -> Result<(), Status> {
        // Simplified recover implementation
        self.begin_address
            .store(Address::from_control(0), Ordering::Release);
        self.head_address
            .store(metadata.final_address, Ordering::Release);
        self.flushed_until_address
            .store(metadata.flushed_address, Ordering::Release);
        Ok(())
    }
}

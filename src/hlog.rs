//! Hybrid Log (HLog) implementation for rskv
//!
//! This module implements the core storage engine inspired by FASTER's
//! PersistentMemoryMalloc. It provides a large, in-memory, circular buffer
//! with persistent storage support.

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use memmap2::{MmapMut, MmapOptions};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};

use crate::common::{
    Address, Key, PAGE_SIZE, RecordInfo, Result, RsKvError, Value, get_offset, get_page,
    make_address,
};
use crate::epoch::SharedEpochManager;

/// Storage device trait for abstracting disk I/O operations
pub trait StorageDevice {
    /// Write data to storage at the specified offset
    fn write(&mut self, offset: u64, data: &[u8]) -> Result<()>;

    /// Read data from storage at the specified offset
    fn read(&self, offset: u64, buf: &mut [u8]) -> Result<usize>;

    /// Flush pending writes to storage
    fn flush(&mut self) -> Result<()>;

    /// Get the size of the storage device
    fn size(&self) -> u64;

    /// Truncate the storage to the specified size
    fn truncate(&mut self, size: u64) -> Result<()>;

    /// Check if the storage device supports memory mapping
    fn supports_mmap(&self) -> bool {
        false
    }

    /// Get memory mapped access to the storage (if supported)
    fn get_mmap(&mut self, offset: u64, len: usize) -> Result<Option<&mut [u8]>> {
        let _ = (offset, len);
        Ok(None)
    }
}

/// File-based storage device implementation
pub struct FileStorageDevice {
    file: File,
    #[allow(dead_code)]
    path: PathBuf,
}

impl FileStorageDevice {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;

        Ok(Self { file, path })
    }
}

impl StorageDevice for FileStorageDevice {
    fn write(&mut self, offset: u64, data: &[u8]) -> Result<()> {
        use std::io::{Seek, SeekFrom, Write};

        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(data)?;
        Ok(())
    }

    fn read(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        use std::io::{Read, Seek, SeekFrom};

        let mut file = &self.file;
        file.seek(SeekFrom::Start(offset))?;
        Ok(file.read(buf)?)
    }

    fn flush(&mut self) -> Result<()> {
        use std::io::Write;
        self.file.flush()?;
        Ok(())
    }

    fn size(&self) -> u64 {
        self.file.metadata().map(|m| m.len()).unwrap_or(0)
    }

    fn truncate(&mut self, size: u64) -> Result<()> {
        self.file.set_len(size)?;
        Ok(())
    }
}

/// Atomic page offset structure (matches FASTER's PageOffset)
#[derive(Debug)]
pub struct AtomicPageOffset {
    value: AtomicU64,
}

impl AtomicPageOffset {
    pub fn new(page: u32, offset: u32) -> Self {
        let value = make_address(page, offset);
        Self {
            value: AtomicU64::new(value),
        }
    }

    pub fn load(&self) -> (u32, u32) {
        let addr = self.value.load(Ordering::Acquire);
        (get_page(addr), get_offset(addr))
    }

    pub fn store(&self, page: u32, offset: u32) {
        let addr = make_address(page, offset);
        self.value.store(addr, Ordering::Release);
    }

    /// Reserve space for allocation (atomic fetch_add operation)
    /// Returns the old page and offset values
    pub fn reserve(&self, size: u32) -> (u32, u32) {
        let old_value = self.value.fetch_add(size as u64, Ordering::AcqRel);
        (get_page(old_value), get_offset(old_value))
    }

    /// Compare and exchange operation for page boundary crossing
    pub fn compare_exchange(
        &self,
        expected_page: u32,
        expected_offset: u32,
        new_page: u32,
        new_offset: u32,
    ) -> std::result::Result<(), (u32, u32)> {
        let expected = make_address(expected_page, expected_offset);
        let new_value = make_address(new_page, new_offset);

        match self
            .value
            .compare_exchange(expected, new_value, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => Ok(()),
            Err(actual) => Err((get_page(actual), get_offset(actual))),
        }
    }
}

/// Status of a page in the hybrid log
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageStatus {
    /// Page is not allocated
    NotAllocated,
    /// Page is in memory and mutable
    InMemory,
    /// Page is being flushed to disk
    Flushing,
    /// Page has been flushed to disk
    OnDisk,
}

/// Record stored in the hybrid log
/// This is the serialized form that gets written to the log
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogRecord {
    /// Record header with metadata
    pub header: RecordInfo,
    /// The key (variable length)
    pub key: Key,
    /// The value (variable length)
    pub value: Value,
}

impl LogRecord {
    pub fn new(key: Key, value: Value, previous_address: Address) -> Self {
        Self {
            header: RecordInfo::new(previous_address, 0, true, false, false),
            key,
            value,
        }
    }

    /// Calculate the serialized size of this record
    pub fn serialized_size(&self) -> u32 {
        // Use bincode to estimate size
        bincode::serialized_size(self).unwrap_or(0) as u32
    }

    /// Create a tombstone record for deletion
    pub fn tombstone(key: Key, previous_address: Address) -> Self {
        Self {
            header: RecordInfo::new(previous_address, 0, true, true, false),
            key,
            value: Vec::new(),
        }
    }
}

/// The Hybrid Log - core storage engine inspired by FASTER
pub struct HybridLog {
    /// In-memory circular buffer of pages
    pages: Vec<RwLock<Option<Box<[u8]>>>>,

    /// Page status tracking
    page_status: Vec<RwLock<PageStatus>>,

    /// Size of the circular buffer (number of pages)
    buffer_size: u32,

    /// Four atomic pointers defining log regions (matching FASTER design)
    ///
    /// Logical address space regions:
    /// [begin_address, head_address): on disk only, can be garbage collected
    /// [head_address, read_only_address): in memory, read-only, can be flushed
    /// [read_only_address, tail_address): in memory, mutable (hot data)

    /// Beginning of the log (data before this is truncated)
    begin_address: AtomicU64,

    /// Start of the in-memory portion
    head_address: AtomicU64,

    /// Boundary between read-only and mutable regions
    read_only_address: AtomicU64,

    /// End of the log where new data is appended
    tail_page_offset: AtomicPageOffset,

    /// Epoch manager for safe memory reclamation
    #[allow(dead_code)]
    epoch: SharedEpochManager,

    /// Storage device for persistence
    #[allow(dead_code)]
    storage: Arc<Mutex<Box<dyn StorageDevice + Send + Sync>>>,

    /// Address that has been flushed to disk
    flushed_until_address: AtomicU64,
}

impl HybridLog {
    /// Create a new hybrid log instance
    pub fn new(
        memory_size: u64,
        storage_device: Box<dyn StorageDevice + Send + Sync>,
        epoch: SharedEpochManager,
    ) -> Result<Self> {
        let buffer_size = (memory_size / PAGE_SIZE as u64) as u32;
        if buffer_size == 0 {
            return Err(RsKvError::Configuration {
                message: "Memory size too small for at least one page".to_string(),
            });
        }

        let mut pages = Vec::with_capacity(buffer_size as usize);
        let mut page_status = Vec::with_capacity(buffer_size as usize);

        for _ in 0..buffer_size {
            pages.push(RwLock::new(None));
            page_status.push(RwLock::new(PageStatus::NotAllocated));
        }

        // Initialize the first page
        let start_address = u64_to_address(PAGE_SIZE as u64); // Skip the invalid page

        let hlog = Self {
            pages,
            page_status,
            buffer_size,
            begin_address: AtomicU64::new(address_to_u64(start_address)),
            head_address: AtomicU64::new(address_to_u64(start_address)),
            read_only_address: AtomicU64::new(address_to_u64(start_address)),
            tail_page_offset: AtomicPageOffset::new(
                get_page(start_address),
                get_offset(start_address),
            ),
            epoch,
            storage: Arc::new(Mutex::new(storage_device)),
            flushed_until_address: AtomicU64::new(address_to_u64(start_address)),
        };

        // Allocate the first page
        hlog.allocate_page(get_page(start_address))?;

        Ok(hlog)
    }

    /// Allocate space in the log for a record of given size
    /// Returns the address where the record can be written, or None if allocation fails
    pub fn allocate(&self, size: u32) -> Option<Address> {
        if size == 0 || size > PAGE_SIZE {
            return None;
        }

        loop {
            let (old_page, old_offset) = self.tail_page_offset.reserve(size);
            let new_offset = old_offset + size;

            if new_offset <= PAGE_SIZE {
                // Allocation fits in current page
                let address = make_address(old_page, old_offset);

                // Ensure the page is allocated
                if self.allocate_page(old_page).is_err() {
                    return None;
                }

                return Some(address);
            } else {
                // Need to move to next page
                let new_page = old_page + 1;
                if new_page > u32::MAX - 1 {
                    return None; // Address space exhausted
                }

                // Try to advance to the next page
                if self
                    .tail_page_offset
                    .compare_exchange(old_page, new_offset, new_page, size)
                    .is_ok()
                {
                    // Successfully moved to new page
                    if self.allocate_page(new_page).is_err() {
                        return None;
                    }

                    return Some(make_address(new_page, 0));
                }
                // If CAS failed, retry the allocation
            }
        }
    }

    /// Get a pointer to data at the specified address
    /// Returns a slice of the requested data if available in memory
    pub fn get(&self, address: Address) -> Option<&[u8]> {
        let page = get_page(address);
        let offset = get_offset(address);

        let page_index = (page % self.buffer_size) as usize;
        let page_guard = self.pages[page_index].read();

        if let Some(ref page_data) = *page_guard
            && (offset as usize) < page_data.len()
        {
            // SAFETY: We've verified the bounds above
            unsafe {
                let ptr = page_data.as_ptr().add(offset as usize);
                return Some(std::slice::from_raw_parts(
                    ptr,
                    page_data.len() - offset as usize,
                ));
            }
        }

        None
    }

    /// Write data to the log at the specified address
    pub fn write(&self, address: Address, data: &[u8]) -> Result<()> {
        let page = get_page(address);
        let offset = get_offset(address);

        if offset as usize + data.len() > PAGE_SIZE as usize {
            return Err(RsKvError::AllocationFailed {
                size: data.len() as u32,
            });
        }

        let page_index = (page % self.buffer_size) as usize;
        let mut page_guard = self.pages[page_index].write();

        if let Some(ref mut page_data) = *page_guard {
            let start = offset as usize;
            let end = start + data.len();

            if end <= page_data.len() {
                page_data[start..end].copy_from_slice(data);
                return Ok(());
            }
        }

        Err(RsKvError::AddressOutOfBounds { address })
    }

    /// Insert a record into the log
    pub fn insert_record(&self, record: LogRecord) -> Result<Address> {
        // Serialize the record
        let serialized = bincode::serialize(&record)?;
        let size = serialized.len() as u32;

        // Allocate space
        let address = self
            .allocate(size)
            .ok_or(RsKvError::AllocationFailed { size })?;

        // Write the serialized record
        self.write(address, &serialized)?;

        Ok(address)
    }

    /// Read a record from the log
    pub fn read_record(&self, address: Address) -> Result<LogRecord> {
        // First, try to read from memory
        if let Some(data) = self.get(address) {
            // Try to deserialize the record from memory
            match bincode::deserialize(data) {
                Ok(record) => return Ok(record),
                Err(_) => {
                    // Data might be truncated in memory buffer, try disk
                }
            }
        }

        // If not in memory or incomplete, read from disk
        self.read_record_from_disk(address)
    }

    /// Read a record from disk storage
    fn read_record_from_disk(&self, address: Address) -> Result<LogRecord> {
        // For this implementation, we'll read a fixed buffer size and try to deserialize
        const INITIAL_READ_SIZE: usize = 1024; // Start with 1KB
        const MAX_RECORD_SIZE: usize = 64 * 1024; // Max 64KB per record

        let storage = self.storage.lock();
        let mut buffer = vec![0u8; INITIAL_READ_SIZE];

        // Read initial chunk
        let bytes_read = storage.read(address, &mut buffer)?;
        if bytes_read == 0 {
            return Err(RsKvError::AddressOutOfBounds { address });
        }

        // Try to deserialize with initial buffer
        match bincode::deserialize::<LogRecord>(&buffer[..bytes_read]) {
            Ok(record) => Ok(record),
            Err(_) => {
                // Buffer might be too small, try with larger buffer
                let mut large_buffer = vec![0u8; MAX_RECORD_SIZE];
                let large_bytes_read = storage.read(address, &mut large_buffer)?;

                if large_bytes_read == 0 {
                    return Err(RsKvError::AddressOutOfBounds { address });
                }

                bincode::deserialize(&large_buffer[..large_bytes_read])
                    .map_err(RsKvError::Serialization)
            }
        }
    }

    /// Allocate a page in the buffer
    fn allocate_page(&self, page: u32) -> Result<()> {
        let page_index = (page % self.buffer_size) as usize;

        let mut page_guard = self.pages[page_index].write();
        if page_guard.is_none() {
            // Allocate the page
            let page_data = vec![0u8; PAGE_SIZE as usize].into_boxed_slice();
            *page_guard = Some(page_data);

            // Update status
            let mut status_guard = self.page_status[page_index].write();
            *status_guard = PageStatus::InMemory;
        }

        Ok(())
    }

    /// Shift the read-only address to the current tail
    /// This makes all current mutable data read-only
    pub fn shift_read_only_address(&self) -> Address {
        let tail_address = self.get_tail_address();
        let old_read_only = self
            .read_only_address
            .swap(address_to_u64(tail_address), Ordering::AcqRel);
        u64_to_address(old_read_only)
    }

    /// Shift the head address forward
    /// This removes pages from memory and makes them disk-only
    pub fn shift_head_address(&self, new_head_address: Address) -> Result<()> {
        let old_head = self
            .head_address
            .swap(address_to_u64(new_head_address), Ordering::AcqRel);
        let old_head_address = u64_to_address(old_head);

        // Evict pages that are now below the head address
        self.evict_pages_below_head(old_head_address, new_head_address)?;

        log::debug!(
            "Shifted head address from 0x{:x} to 0x{:x}",
            old_head_address,
            new_head_address
        );

        Ok(())
    }

    /// Evict pages from memory that are now below the head address
    fn evict_pages_below_head(&self, old_head: Address, new_head: Address) -> Result<()> {
        let old_head_page = get_page(old_head);
        let new_head_page = get_page(new_head);

        // Evict all pages between old_head and new_head
        for page in old_head_page..new_head_page {
            self.evict_page(page)?;
        }

        Ok(())
    }

    /// Evict a specific page from memory
    fn evict_page(&self, page: u32) -> Result<()> {
        let page_index = (page % self.buffer_size) as usize;

        // Lock the page and set status to OnDisk
        {
            let mut page_guard = self.pages[page_index].write();
            let mut status_guard = self.page_status[page_index].write();

            if *status_guard == PageStatus::InMemory {
                // Free the page memory
                *page_guard = None;
                *status_guard = PageStatus::OnDisk;

                log::trace!("Evicted page {page} from memory");
            }
        }

        Ok(())
    }

    /// Get current tail address
    pub fn get_tail_address(&self) -> Address {
        let (page, offset) = self.tail_page_offset.load();
        make_address(page, offset)
    }

    /// Get current head address
    pub fn get_head_address(&self) -> Address {
        u64_to_address(self.head_address.load(Ordering::Acquire))
    }

    /// Get current read-only address
    pub fn get_read_only_address(&self) -> Address {
        u64_to_address(self.read_only_address.load(Ordering::Acquire))
    }

    /// Get current begin address
    pub fn get_begin_address(&self) -> Address {
        u64_to_address(self.begin_address.load(Ordering::Acquire))
    }

    /// Advance the begin address and truncate the log
    /// This permanently removes data from storage and reclaims space
    pub fn advance_begin_address(&self, new_begin_address: Address) -> Result<u64> {
        let old_begin = self
            .begin_address
            .swap(address_to_u64(new_begin_address), Ordering::AcqRel);
        let old_begin_address = u64_to_address(old_begin);

        if new_begin_address <= old_begin_address {
            // Nothing to truncate
            return Ok(0);
        }

        // Calculate how many bytes we're reclaiming
        let bytes_reclaimed = new_begin_address.saturating_sub(old_begin_address);

        // Perform actual storage truncation
        self.truncate_storage(old_begin_address, new_begin_address)?;

        log::info!(
            "Advanced begin address from 0x{:x} to 0x{:x}, reclaimed {} bytes",
            old_begin_address,
            new_begin_address,
            bytes_reclaimed
        );

        Ok(bytes_reclaimed)
    }

    /// Truncate storage by removing data before the new begin address
    fn truncate_storage(&self, old_begin: Address, new_begin: Address) -> Result<()> {
        let mut storage = self.storage.lock();

        // For memory-mapped files, we can't actually truncate from the beginning
        // Instead, we mark the space as invalid and potentially compact later
        if storage.supports_mmap() {
            // For mmap devices, we use a different strategy
            self.mark_space_invalid(old_begin, new_begin)?;
        } else {
            // For regular file devices, we can perform actual truncation
            // by copying remaining data to the beginning of the file
            self.compact_storage(&mut **storage, old_begin, new_begin)?;
        }

        Ok(())
    }

    /// Mark space as invalid for memory-mapped storage
    fn mark_space_invalid(&self, _old_begin: Address, _new_begin: Address) -> Result<()> {
        // For now, we just update the begin address
        // In a production system, this might involve:
        // 1. Marking pages as free in a free list
        // 2. Scheduling background compaction
        // 3. Using file hole punching (fallocate) on supported filesystems

        log::debug!("Marked address range as invalid (mmap storage)");
        Ok(())
    }

    /// Compact storage by moving data and truncating the file
    fn compact_storage(
        &self,
        storage: &mut dyn StorageDevice,
        old_begin: Address,
        new_begin: Address,
    ) -> Result<()> {
        const BUFFER_SIZE: usize = 1024 * 1024; // 1MB buffer
        let mut buffer = vec![0u8; BUFFER_SIZE];

        let total_size = storage.size();
        let truncate_amount = new_begin - old_begin;

        if new_begin >= total_size {
            // Truncating everything
            storage.truncate(0)?;
            return Ok(());
        }

        // Read data from new_begin onwards and write it to the beginning
        let mut read_offset = new_begin;
        let mut write_offset = 0u64;

        while read_offset < total_size {
            let bytes_to_read = BUFFER_SIZE.min((total_size - read_offset) as usize);
            let bytes_read = storage.read(read_offset, &mut buffer[..bytes_to_read])?;

            if bytes_read == 0 {
                break;
            }

            storage.write(write_offset, &buffer[..bytes_read])?;

            read_offset += bytes_read as u64;
            write_offset += bytes_read as u64;
        }

        // Truncate file to new size
        let new_size = total_size - truncate_amount;
        storage.truncate(new_size)?;
        storage.flush()?;

        log::debug!(
            "Compacted storage: removed {} bytes, new size: {} bytes",
            truncate_amount,
            new_size
        );

        Ok(())
    }

    /// Flush data to storage device
    pub async fn flush_to_disk(&self, until_address: Address) -> Result<()> {
        let current_flushed = u64_to_address(self.flushed_until_address.load(Ordering::Acquire));

        if until_address <= current_flushed {
            // Already flushed
            return Ok(());
        }

        log::debug!(
            "Flushing data from 0x{:x} to 0x{:x}",
            current_flushed,
            until_address
        );

        // Flush page by page
        let start_page = get_page(current_flushed);
        let end_page = get_page(until_address);

        for page in start_page..=end_page {
            self.flush_page_to_disk(page).await?;
        }

        // Update flushed address
        self.flushed_until_address
            .store(address_to_u64(until_address), Ordering::Release);

        // Ensure storage device commits the data
        {
            let mut storage = self.storage.lock();
            storage.flush()?;
        }

        log::debug!("Flush completed to address 0x{until_address:x}");
        Ok(())
    }

    /// Flush a specific page to disk
    async fn flush_page_to_disk(&self, page: u32) -> Result<()> {
        let page_index = (page % self.buffer_size) as usize;

        // Get page data under lock
        let page_data = {
            let page_guard = self.pages[page_index].read();
            let status_guard = self.page_status[page_index].read();

            if *status_guard != PageStatus::InMemory {
                // Page not in memory or already flushed
                return Ok(());
            }

            if let Some(ref data) = *page_guard {
                data.clone()
            } else {
                return Ok(()); // No data to flush
            }
        };

        // Calculate disk offset for this page
        let disk_offset = (page as u64) * (PAGE_SIZE as u64);

        // Write to storage device (this is the potentially slow operation)
        {
            let mut storage = self.storage.lock();
            storage.write(disk_offset, &page_data)?;
        }

        // Update page status to indicate it's been flushed
        {
            let mut status_guard = self.page_status[page_index].write();
            if *status_guard == PageStatus::InMemory {
                *status_guard = PageStatus::Flushing; // Mark as flushing
            }
        }

        log::trace!(
            "Flushed page {} to disk at offset 0x{:x}",
            page,
            disk_offset
        );
        Ok(())
    }
}

/// Memory-mapped storage device for high-performance large file access
pub struct MmapStorageDevice {
    file: File,
    mmap: Option<MmapMut>,
    #[allow(dead_code)]
    path: PathBuf,
    size: u64,
    dirty: bool,
}

impl MmapStorageDevice {
    /// Create a new memory-mapped storage device
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)?;

        let metadata = file.metadata()?;
        let size = metadata.len();

        let mut device = Self {
            file,
            mmap: None,
            path,
            size,
            dirty: false,
        };

        // Initialize memory mapping if file is not empty
        if size > 0 {
            device.init_mmap()?;
        }

        Ok(device)
    }

    /// Initialize memory mapping for the current file size
    fn init_mmap(&mut self) -> Result<()> {
        if self.size > 0 {
            let mmap = unsafe {
                MmapOptions::new()
                    .len(self.size as usize)
                    .map_mut(&self.file)?
            };
            self.mmap = Some(mmap);
        }
        Ok(())
    }

    /// Resize the file and remmap if necessary
    fn resize_and_remap(&mut self, new_size: u64) -> Result<()> {
        if new_size != self.size {
            // Drop old mapping
            self.mmap = None;

            // Resize file
            self.file.set_len(new_size)?;
            self.size = new_size;

            // Create new mapping
            if new_size > 0 {
                self.init_mmap()?;
            }
        }
        Ok(())
    }

    /// Ensure the file is large enough for the given offset + length
    fn ensure_capacity(&mut self, offset: u64, len: usize) -> Result<()> {
        let required_size = offset + len as u64;
        if required_size > self.size {
            // Grow file by at least 64MB chunks for efficiency
            const GROWTH_CHUNK: u64 = 64 * 1024 * 1024;
            let new_size = required_size.div_ceil(GROWTH_CHUNK) * GROWTH_CHUNK;
            self.resize_and_remap(new_size)?;
        }
        Ok(())
    }
}

impl StorageDevice for MmapStorageDevice {
    fn write(&mut self, offset: u64, data: &[u8]) -> Result<()> {
        self.ensure_capacity(offset, data.len())?;

        if let Some(ref mut mmap) = self.mmap {
            let start = offset as usize;
            let end = start + data.len();

            if end <= mmap.len() {
                mmap[start..end].copy_from_slice(data);
                self.dirty = true;
                return Ok(());
            }
        }

        // Fallback to file I/O if mmap is not available or out of bounds
        use std::io::{Seek, SeekFrom, Write};
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(data)?;
        Ok(())
    }

    fn read(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        if let Some(ref mmap) = self.mmap {
            let start = offset as usize;
            let len = buf.len().min(mmap.len().saturating_sub(start));

            if len > 0 {
                buf[..len].copy_from_slice(&mmap[start..start + len]);
                return Ok(len);
            }
        }

        // Fallback to file I/O if mmap is not available
        use std::io::{Read, Seek, SeekFrom};
        let mut file = &self.file;
        file.seek(SeekFrom::Start(offset))?;
        Ok(file.read(buf)?)
    }

    fn flush(&mut self) -> Result<()> {
        if self.dirty {
            if let Some(ref mut mmap) = self.mmap {
                mmap.flush()?;
            }
            self.file.sync_all()?;
            self.dirty = false;
        }
        Ok(())
    }

    fn size(&self) -> u64 {
        self.size
    }

    fn truncate(&mut self, size: u64) -> Result<()> {
        self.resize_and_remap(size)?;
        Ok(())
    }

    fn supports_mmap(&self) -> bool {
        true
    }

    fn get_mmap(&mut self, offset: u64, len: usize) -> Result<Option<&mut [u8]>> {
        self.ensure_capacity(offset, len)?;

        if let Some(ref mut mmap) = self.mmap {
            let start = offset as usize;
            let end = start + len;

            if end <= mmap.len() {
                return Ok(Some(&mut mmap[start..end]));
            }
        }

        Ok(None)
    }
}

impl Drop for MmapStorageDevice {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

// Address conversion utilities
/// Convert Address to u64
#[inline]
pub fn address_to_u64(addr: Address) -> u64 {
    addr
}

/// Convert u64 to Address
#[inline]
pub fn u64_to_address(val: u64) -> Address {
    val
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    /// Mock storage device for testing
    struct MockStorageDevice {
        data: Vec<u8>,
    }

    impl MockStorageDevice {
        fn new() -> Self {
            Self { data: Vec::new() }
        }
    }

    impl StorageDevice for MockStorageDevice {
        fn write(&mut self, offset: u64, data: &[u8]) -> Result<()> {
            let end = offset as usize + data.len();
            if self.data.len() < end {
                self.data.resize(end, 0);
            }
            self.data[offset as usize..end].copy_from_slice(data);
            Ok(())
        }

        fn read(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
            let start = offset as usize;
            let end = std::cmp::min(start + buf.len(), self.data.len());
            if start < self.data.len() {
                let copy_len = end - start;
                buf[..copy_len].copy_from_slice(&self.data[start..end]);
                Ok(copy_len)
            } else {
                Ok(0)
            }
        }

        fn flush(&mut self) -> Result<()> {
            Ok(())
        }

        fn size(&self) -> u64 {
            self.data.len() as u64
        }

        fn truncate(&mut self, size: u64) -> Result<()> {
            self.data.truncate(size as usize);
            Ok(())
        }
    }

    #[test]
    fn test_atomic_page_offset() {
        let offset = AtomicPageOffset::new(0, 100);
        let (page, offset_val) = offset.load();
        assert_eq!(page, 0);
        assert_eq!(offset_val, 100);

        let (old_page, old_offset) = offset.reserve(50);
        assert_eq!(old_page, 0);
        assert_eq!(old_offset, 100);

        let (page, offset_val) = offset.load();
        assert_eq!(page, 0);
        assert_eq!(offset_val, 150);
    }

    #[test]
    fn test_hybrid_log_creation() {
        let storage = Box::new(MockStorageDevice::new());
        let epoch = Arc::new(crate::epoch::EpochManager::new());
        let memory_size = 64 * 1024 * 1024; // 64MB

        let hlog = HybridLog::new(memory_size, storage, epoch).unwrap();
        assert_eq!(hlog.buffer_size, 2); // 64MB / 32MB = 2 pages
    }

    #[test]
    fn test_allocation() {
        let storage = Box::new(MockStorageDevice::new());
        let epoch = Arc::new(crate::epoch::EpochManager::new());
        let memory_size = 64 * 1024 * 1024;

        let hlog = HybridLog::new(memory_size, storage, epoch).unwrap();

        // Allocate some space
        let addr1 = hlog.allocate(1024).unwrap();
        let addr2 = hlog.allocate(2048).unwrap();

        assert_ne!(addr1, addr2);
        assert!(get_offset(addr2) > get_offset(addr1));
    }

    #[test]
    fn test_record_operations() {
        let storage = Box::new(MockStorageDevice::new());
        let epoch = Arc::new(crate::epoch::EpochManager::new());
        let memory_size = 64 * 1024 * 1024;

        let hlog = HybridLog::new(memory_size, storage, epoch).unwrap();

        // Create and insert a record
        let key = b"test_key".to_vec();
        let value = b"test_value".to_vec();
        let record = LogRecord::new(key.clone(), value.clone(), crate::common::INVALID_ADDRESS);

        let address = hlog.insert_record(record).unwrap();

        // Read the record back
        let read_record = hlog.read_record(address).unwrap();
        assert_eq!(read_record.key, key);
        assert_eq!(read_record.value, value);
    }

    #[test]
    fn test_file_storage_device() {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test.log");

        let mut storage = FileStorageDevice::new(&file_path).unwrap();

        let test_data = b"Hello, World!";
        storage.write(0, test_data).unwrap();
        storage.flush().unwrap();

        let mut read_buffer = vec![0u8; test_data.len()];
        let bytes_read = storage.read(0, &mut read_buffer).unwrap();

        assert_eq!(bytes_read, test_data.len());
        assert_eq!(&read_buffer, test_data);
    }
}

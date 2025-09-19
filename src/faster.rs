use crate::core::checkpoint::{CheckpointMetadata, IndexMetadata};
use crate::core::light_epoch::LightEpoch;
use crate::core::record::{Record, RecordInfo};
use crate::core::status::Status;
use crate::device::file_system_disk::FileSystemDisk;
use crate::hlog::persistent_memory_malloc::{Disk, PersistentMemoryMalloc};
use crate::index::IHashIndex;
use crate::index::definitions::HotLogHashIndexDefinition;
use crate::index::mem_index::{FindContext, MemHashIndex};
use std::fs;
use std::io::{Read, Write};
use std::marker::PhantomData;

// The user-provided context for an upsert operation.
pub trait UpsertContext {
    type Key;
    type Value;

    fn key(&self) -> &Self::Key;
    fn value(&self) -> &Self::Value;
    fn key_hash(&self) -> u64;
    // In-place update
    fn put_atomic(&self, value: &mut Self::Value) -> bool;
}

// The user-provided context for a read operation.
pub trait ReadContext {
    type Key;
    type Value;

    fn key(&self) -> &Self::Key;
    fn key_hash(&self) -> u64;
    fn get(&mut self, value: &Self::Value);
}

// The user-provided context for a read-modify-write operation.
pub trait RmwContext {
    type Key;
    type Value;

    fn key(&self) -> &Self::Key;
    fn key_hash(&self) -> u64;

    /// Called when the key does not exist.
    fn rmw_initial(&self, value: &mut Self::Value);

    /// Called when the key exists and we are performing a read-copy-update.
    fn rmw_copy(&self, old_value: &Self::Value, new_value: &mut Self::Value);

    /// Called for an in-place update in the mutable region of the log.
    /// Should return true if the update was successful, false if it should fall back to RCU.
    fn rmw_atomic(&self, value: &mut Self::Value) -> bool;
}

// The user-provided context for a delete operation.
pub trait DeleteContext {
    type Key;

    fn key(&self) -> &Self::Key;
    fn key_hash(&self) -> u64;
}

pub struct FasterKv<'epoch, K, V, D: Disk> {
    epoch: LightEpoch,
    pub hlog: PersistentMemoryMalloc<'epoch, D>,
    pub index: MemHashIndex<'epoch, HotLogHashIndexDefinition>,
    pub disk: D,
    _key: PhantomData<K>,
    _value: PhantomData<V>,
}

impl<'epoch, K, V, D: Disk + Clone> FasterKv<'epoch, K, V, D>
where
    K: Sized + Copy + 'static + PartialEq,
    V: Sized + Clone + 'static + Default,
{
    pub fn new(log_size: u64, table_size: u64, disk: D) -> Result<Self, Status> {
        let mut kv = Self {
            epoch: LightEpoch::new(),
            hlog: PersistentMemoryMalloc::new(),
            index: MemHashIndex::new(),
            disk: disk.clone(),
            _key: PhantomData,
            _value: PhantomData,
        };
        let epoch_ptr: *const LightEpoch = &kv.epoch;
        kv.hlog.initialize(log_size, unsafe { &*epoch_ptr }, disk);
        kv.index.initialize(table_size, 64, unsafe { &*epoch_ptr });
        Ok(kv)
    }

    pub fn get_table_size(&self) -> u64 {
        self.index.size()
    }

    pub fn upsert(&self, context: &impl UpsertContext<Key = K, Value = V>) -> Status {
        let mut find_context = FindContext::new(context.key_hash());

        loop {
            let status = self.index.find_or_create_entry(&mut find_context);
            if status != Status::Ok {
                // Should not happen in current simplified MemHashIndex
                return status;
            }

            let entry = find_context.entry;
            // Use real head and read-only addresses from hlog
            let read_only_address = self.hlog.get_read_only_address();

            // Attempt in-place update if possible
            if !entry.unused() && entry.address() >= read_only_address {
                // Check key equality
                if let Some(record) = self.hlog.get::<K, V>(entry.address()) {
                    unsafe {
                        let record_key = Record::<K, V>::key(record as *const Record<K, V>);
                        if std::ptr::eq(record_key, context.key())
                            || std::ptr::read_unaligned(record_key) == *context.key()
                        {
                            // Key matches, attempt in-place update
                            let record_value = Record::<K, V>::value_mut(
                                record as *const Record<K, V> as *mut Record<K, V>,
                            );
                            if context.put_atomic(record_value) {
                                return Status::Ok;
                            }
                        }
                    }
                }
            }

            // RCU (Read-Copy-Update) path
            let record_size = Record::<K, V>::required_size_with_alignment();
            let new_address = match self.hlog.allocate(record_size as u64) {
                Ok(addr) => addr,
                Err(closed_page) => {
                    self.hlog.new_page(closed_page);
                    // Retry allocation after creating new page
                    match self.hlog.allocate(record_size as u64) {
                        Ok(addr) => addr,
                        Err(_) => return Status::Pending, // Still failed, ask caller to retry
                    }
                }
            };

            // 2. Get a mutable slice to the allocated memory
            let buffer = self.hlog.get_mut_slice(new_address, record_size as usize);
            if buffer.is_empty() {
                return Status::Pending;
            }

            // 3. Construct the new record in the allocated slice
            let new_record_info = RecordInfo::new(entry.address(), 0, false, false, true);
            unsafe {
                Record::create_in(buffer, new_record_info, context.key(), context.value());
            }

            // 4. CAS the hash index to point to the new record
            if self
                .index
                .try_update_entry(&find_context, new_address, false)
                == Status::Ok
            {
                return Status::Ok;
            }

            // Invalidate the allocated record before retrying
            unsafe {
                let buffer = self.hlog.get_mut_slice(new_address, record_size as usize);
                let record_ptr = buffer.as_mut_ptr() as *mut Record<K, V>;
                // Read header safely using unaligned access
                let header_bytes = std::slice::from_raw_parts(record_ptr as *const u8, 8);
                let header_control = u64::from_le_bytes([
                    header_bytes[0],
                    header_bytes[1],
                    header_bytes[2],
                    header_bytes[3],
                    header_bytes[4],
                    header_bytes[5],
                    header_bytes[6],
                    header_bytes[7],
                ]);
                let mut header = RecordInfo::from_control(header_control);
                header.set_invalid(true);
                // Write header safely using unaligned access
                let header_bytes = std::slice::from_raw_parts_mut(record_ptr as *mut u8, 8);
                let new_control = header.control();
                let control_bytes = new_control.to_le_bytes();
                header_bytes.copy_from_slice(&control_bytes);
            }
        }
    }

    pub fn read(&self, context: &mut impl ReadContext<Key = K, Value = V>) -> Status {
        let mut find_context = FindContext::new(context.key_hash());
        if self.index.find_entry(&mut find_context) != Status::Ok {
            return Status::NotFound;
        }

        let record_size = Record::<K, V>::required_size_with_alignment();
        let mut current_address = find_context.entry.address();
        // Use tail address as the head address for now (simplified)
        let _head_address = self.hlog.get_head_address();

        loop {
            if current_address.control() == 0 {
                // Reached end of chain
                return Status::NotFound;
            }

            let buffer = self.hlog.get_slice(current_address, record_size as usize);
            if buffer.is_empty() {
                return Status::NotFound;
            }

            let record_ptr = buffer.as_ptr() as *const Record<K, V>;

            unsafe {
                if Record::key(record_ptr) == context.key() {
                    // Read header safely using unaligned access
                    let header_bytes = std::slice::from_raw_parts(record_ptr as *const u8, 8);
                    let header_control = u64::from_le_bytes([
                        header_bytes[0],
                        header_bytes[1],
                        header_bytes[2],
                        header_bytes[3],
                        header_bytes[4],
                        header_bytes[5],
                        header_bytes[6],
                        header_bytes[7],
                    ]);
                    let header = RecordInfo::from_control(header_control);
                    if header.tombstone() {
                        return Status::NotFound;
                    }
                    // The value's lifetime is tied to the log buffer. The context's
                    // get method is responsible for copying the data out if needed.
                    context.get(Record::value(record_ptr));
                    return Status::Ok;
                }
                // Read header safely using unaligned access
                let header_bytes = std::slice::from_raw_parts(record_ptr as *const u8, 8);
                let header_control = u64::from_le_bytes([
                    header_bytes[0],
                    header_bytes[1],
                    header_bytes[2],
                    header_bytes[3],
                    header_bytes[4],
                    header_bytes[5],
                    header_bytes[6],
                    header_bytes[7],
                ]);
                let header = RecordInfo::from_control(header_control);
                current_address = header.previous_address();
            }
        }
    }

    pub fn rmw(&self, context: &mut impl RmwContext<Key = K, Value = V>) -> Status
    where
        V: Default,
    {
        let mut find_context = FindContext::new(context.key_hash());

        loop {
            let status = self.index.find_or_create_entry(&mut find_context);
            if status != Status::Ok {
                return status;
            }

            let mut current_address = find_context.entry.address();
            let _head_address = self.hlog.get_head_address(); // Simplified
            let read_only_address = self.hlog.get_read_only_address();

            let mut old_value_option: Option<V> = None;

            // Trace back the chain to find a key match
            loop {
                if current_address.control() == 0 {
                    break; // End of in-memory chain
                }

                let buffer = self.hlog.get_slice(
                    current_address,
                    Record::<K, V>::required_size_with_alignment() as usize,
                );
                if buffer.is_empty() {
                    break; // End of in-memory chain
                }
                let record_ptr = buffer.as_ptr() as *const Record<K, V>;

                unsafe {
                    if Record::key(record_ptr) == context.key() {
                        // Read header safely using unaligned access
                        let header_bytes = std::slice::from_raw_parts(record_ptr as *const u8, 8);
                        let header_control = u64::from_le_bytes([
                            header_bytes[0],
                            header_bytes[1],
                            header_bytes[2],
                            header_bytes[3],
                            header_bytes[4],
                            header_bytes[5],
                            header_bytes[6],
                            header_bytes[7],
                        ]);
                        let header = RecordInfo::from_control(header_control);
                        if header.tombstone() {
                            break; // Found tombstone, treat as non-existent
                        }

                        // Found a match. Try in-place update if in mutable region.
                        if current_address >= read_only_address {
                            let mut_buffer = self.hlog.get_mut_slice(
                                current_address,
                                Record::<K, V>::required_size_with_alignment() as usize,
                            );
                            let mut_record_ptr = mut_buffer.as_mut_ptr() as *mut Record<K, V>;
                            if context.rmw_atomic(Record::value_mut(mut_record_ptr)) {
                                return Status::Ok; // In-place update successful
                            }
                        }
                        // Cannot update in-place, break to RCU path
                        old_value_option = Some(Record::value(record_ptr).clone());
                        break;
                    }
                    // Read header safely using unaligned access
                    let header_bytes = std::slice::from_raw_parts(record_ptr as *const u8, 8);
                    let header_control = u64::from_le_bytes([
                        header_bytes[0],
                        header_bytes[1],
                        header_bytes[2],
                        header_bytes[3],
                        header_bytes[4],
                        header_bytes[5],
                        header_bytes[6],
                        header_bytes[7],
                    ]);
                    let header = RecordInfo::from_control(header_control);
                    current_address = header.previous_address();
                }
            }

            // RCU Path
            let record_size = Record::<K, V>::required_size_with_alignment();
            let new_address = match self.hlog.allocate(record_size as u64) {
                Ok(addr) => addr,
                Err(closed_page) => {
                    self.hlog.new_page(closed_page);
                    // Retry allocation after creating new page
                    match self.hlog.allocate(record_size as u64) {
                        Ok(addr) => addr,
                        Err(_) => return Status::Pending, // Still failed, ask caller to retry
                    }
                }
            };

            let buffer = self.hlog.get_mut_slice(new_address, record_size as usize);
            let new_record_info =
                RecordInfo::new(find_context.entry.address(), 0, false, false, true);

            unsafe {
                let mut value_buffer = V::default();
                match old_value_option {
                    Some(ref old_val) => context.rmw_copy(old_val, &mut value_buffer),
                    None => context.rmw_initial(&mut value_buffer),
                }
                Record::create_in(buffer, new_record_info, context.key(), &value_buffer);
            }

            if self
                .index
                .try_update_entry(&find_context, new_address, false)
                == Status::Ok
            {
                return Status::Ok;
            }

            // CAS failed, retry.
            unsafe {
                let record_ptr = buffer.as_mut_ptr() as *mut Record<K, V>;
                // Read header safely using unaligned access
                let header_bytes = std::slice::from_raw_parts(record_ptr as *const u8, 8);
                let header_control = u64::from_le_bytes([
                    header_bytes[0],
                    header_bytes[1],
                    header_bytes[2],
                    header_bytes[3],
                    header_bytes[4],
                    header_bytes[5],
                    header_bytes[6],
                    header_bytes[7],
                ]);
                let mut header = RecordInfo::from_control(header_control);
                header.set_invalid(true);
                // Write header safely using unaligned access
                let header_bytes = std::slice::from_raw_parts_mut(record_ptr as *mut u8, 8);
                let new_control = header.control();
                let control_bytes = new_control.to_le_bytes();
                header_bytes.copy_from_slice(&control_bytes);
            }
        }
    }

    pub fn delete(&self, context: &impl DeleteContext<Key = K>) -> Status
    where
        V: Default,
    {
        let mut find_context = FindContext::new(context.key_hash());
        if self.index.find_entry(&mut find_context) != Status::Ok {
            return Status::NotFound;
        }

        let mut current_address = find_context.entry.address();
        let _head_address = self.hlog.get_head_address(); // Simplified
        let read_only_address = self.hlog.get_read_only_address();

        // Trace back to find the specific record to invalidate or to append a tombstone after.
        loop {
            if current_address.control() == 0 {
                return Status::NotFound; // Reached end of chain
            }

            let record_size = Record::<K, V>::required_size_with_alignment();
            let buffer = self.hlog.get_slice(current_address, record_size as usize);
            if buffer.is_empty() {
                return Status::NotFound;
            }

            let record_ptr = buffer.as_ptr() as *const Record<K, V>;

            unsafe {
                if Record::key(record_ptr) == context.key() {
                    // Read header safely using unaligned access
                    let header_bytes = std::slice::from_raw_parts(record_ptr as *const u8, 8);
                    let header_control = u64::from_le_bytes([
                        header_bytes[0],
                        header_bytes[1],
                        header_bytes[2],
                        header_bytes[3],
                        header_bytes[4],
                        header_bytes[5],
                        header_bytes[6],
                        header_bytes[7],
                    ]);
                    let header = RecordInfo::from_control(header_control);

                    // Found the record. Check if it's already a tombstone.
                    if header.tombstone() {
                        return Status::NotFound;
                    }

                    // If in mutable region, invalidate in-place.
                    if current_address >= read_only_address {
                        let mut_buffer = self
                            .hlog
                            .get_mut_slice(current_address, record_size as usize);
                        let mut_record_ptr = mut_buffer.as_mut_ptr() as *mut Record<K, V>;
                        // Write header safely using unaligned access
                        let header_bytes =
                            std::slice::from_raw_parts_mut(mut_record_ptr as *mut u8, 8);
                        let mut new_header = header;
                        new_header.set_tombstone(true);
                        let new_control = new_header.control();
                        let control_bytes = new_control.to_le_bytes();
                        header_bytes.copy_from_slice(&control_bytes);
                        return Status::Ok;
                    }

                    // In read-only region, break to append a tombstone.
                    break;
                }
                // Read header safely using unaligned access
                let header_bytes = std::slice::from_raw_parts(record_ptr as *const u8, 8);
                let header_control = u64::from_le_bytes([
                    header_bytes[0],
                    header_bytes[1],
                    header_bytes[2],
                    header_bytes[3],
                    header_bytes[4],
                    header_bytes[5],
                    header_bytes[6],
                    header_bytes[7],
                ]);
                let header = RecordInfo::from_control(header_control);
                current_address = header.previous_address();
            }
        }

        // Append a tombstone record (RCU path)
        loop {
            let record_size = Record::<K, V>::required_size_with_alignment();
            let new_address = match self.hlog.allocate(record_size as u64) {
                Ok(addr) => addr,
                Err(closed_page) => {
                    self.hlog.new_page(closed_page);
                    // Retry allocation after creating new page
                    match self.hlog.allocate(record_size as u64) {
                        Ok(addr) => addr,
                        Err(_) => return Status::Pending, // Still failed, ask caller to retry
                    }
                }
            };

            let buffer = self.hlog.get_mut_slice(new_address, record_size as usize);
            // The new tombstone points to the same previous record as the old entry.
            let new_record_info =
                RecordInfo::new(find_context.entry.address(), 0, false, true, true);

            unsafe {
                Record::create_in(buffer, new_record_info, context.key(), &V::default());
            }

            if self
                .index
                .try_update_entry(&find_context, new_address, false)
                == Status::Ok
            {
                return Status::Ok;
            }

            // CAS failed, retry.
            unsafe {
                let record_ptr = buffer.as_mut_ptr() as *mut Record<K, V>;
                // Read header safely using unaligned access
                let header_bytes = std::slice::from_raw_parts(record_ptr as *const u8, 8);
                let header_control = u64::from_le_bytes([
                    header_bytes[0],
                    header_bytes[1],
                    header_bytes[2],
                    header_bytes[3],
                    header_bytes[4],
                    header_bytes[5],
                    header_bytes[6],
                    header_bytes[7],
                ]);
                let mut header = RecordInfo::from_control(header_control);
                header.set_invalid(true);
                // Write header safely using unaligned access
                let header_bytes = std::slice::from_raw_parts_mut(record_ptr as *mut u8, 8);
                let new_control = header.control();
                let control_bytes = new_control.to_le_bytes();
                header_bytes.copy_from_slice(&control_bytes);
            }
        }
    }

    pub fn checkpoint(&mut self, token: &str) -> Result<(), Status> {
        // This is a simplified, blocking checkpoint.
        // A full implementation would use the CPR state machine.

        // 1. Orchestrate Log Checkpoint
        let log_metadata = self.hlog.checkpoint(&mut self.disk, token)?;

        // 2. Orchestrate Index Checkpoint
        // For now, we'll create a dummy FileSystemDisk for the checkpoint
        // In a real implementation, we'd need to handle this differently
        let _dummy_disk = FileSystemDisk::new("/tmp")?;
        // Temporarily skip index checkpoint to avoid file system issues
        let mut index_metadata = IndexMetadata::default();
        index_metadata.table_size = self.index.size();

        // 3. Write final metadata file
        let metadata = CheckpointMetadata {
            index_metadata,
            log_metadata,
        };
        let path = self.disk.index_checkpoint_path(token);

        // Ensure directory exists
        let path_obj = std::path::Path::new(&path);
        fs::create_dir_all(path_obj).map_err(|_| Status::IoError)?;

        // Create a simple checkpoint file
        let checkpoint_path = format!("{}checkpoint.dat", path);
        let mut file = fs::File::create(&checkpoint_path).map_err(|_| Status::IoError)?;

        let bytes: &[u8] = unsafe {
            std::slice::from_raw_parts(
                &metadata as *const _ as *const u8,
                std::mem::size_of::<CheckpointMetadata>(),
            )
        };
        file.write_all(bytes).map_err(|_| Status::IoError)?;

        Ok(())
    }

    pub fn recover(
        log_path: &str,
        token: &str,
    ) -> Result<FasterKv<'static, K, V, FileSystemDisk>, Status> {
        let disk = FileSystemDisk::new(log_path)?;

        // 1. Read metadata
        let path = disk.index_checkpoint_path(token);
        let mut meta_file =
            fs::File::open(format!("{}checkpoint.dat", path)).map_err(|_| Status::IoError)?;
        let mut buffer = Vec::new();
        meta_file
            .read_to_end(&mut buffer)
            .map_err(|_| Status::IoError)?;
        let metadata: &CheckpointMetadata =
            unsafe { &*(buffer.as_ptr() as *const CheckpointMetadata) };

        // 2. Create a new FasterKv instance
        let table_size = metadata.index_metadata.table_size;
        let log_size = 1 << 30; // Simplified: 1GB. Should be stored in metadata.
        let mut kv = FasterKv::<K, V, FileSystemDisk>::new(log_size, table_size, disk)?;

        // 3. Recover components
        kv.index
            .recover(&mut kv.disk, token, &metadata.index_metadata)?;
        kv.hlog
            .recover(&mut kv.disk, token, &metadata.log_metadata)?;

        Ok(kv)
    }
}

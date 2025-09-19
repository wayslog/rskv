use crate::core::address::Address;
use crate::core::checkpoint::IndexMetadata;
use crate::core::light_epoch::LightEpoch;
use crate::core::malloc_fixed_page_size::MallocFixedPageSize;
use crate::core::status::Status;
use crate::device::file_system_disk::FileSystemDisk;
use crate::index::IHashIndex;
use crate::index::definitions::{HashIndexDefinition, HotLogHashIndexDefinition};
use crate::index::hash_bucket::{
    AtomicHashBucketEntry, HashBucketEntry, HashBucketOverflowEntry, HotLogIndexHashBucket,
};
use crate::index::hash_table::InternalHashTable;
use crate::index::key_hash::HotLogKeyHash;

/// The in-memory hash index for FASTER.
/// It consists of a main hash table and an allocator for overflow buckets.
pub struct MemHashIndex<'epoch, D: HashIndexDefinition> {
    // An array of size two, for the old and new versions of the hash-table during resizing.
    table: [InternalHashTable<D>; 2],
    // Allocator for the hash buckets that don't fit in the hash table.
    overflow_buckets_allocator: [MallocFixedPageSize<'epoch, D::HashBucket>; 2],
    // Current version of the hash table in use (0 or 1).
    version: u8,
    epoch: Option<&'epoch LightEpoch>,
}

// This is a simplified context for now. The C++ version is a template parameter `C`.
// We'll use this to pass necessary info into the index methods.
pub struct FindContext {
    pub key_hash: u64,
    // Output parameters
    pub entry: HashBucketEntry,
    pub atomic_entry: Option<*const AtomicHashBucketEntry>,
}

impl FindContext {
    pub fn new(key_hash: u64) -> Self {
        Self {
            key_hash,
            entry: HashBucketEntry::default(),
            atomic_entry: None,
        }
    }
}

// We implement methods specifically for the HotLog definition for now.
type HotLogMemHashIndex<'epoch> = MemHashIndex<'epoch, HotLogHashIndexDefinition>;

impl<'epoch> HotLogMemHashIndex<'epoch> {
    pub fn size(&self) -> u64 {
        self.table[self.version as usize].size()
    }

    pub fn new() -> Self {
        Self {
            table: [InternalHashTable::new(), InternalHashTable::new()],
            overflow_buckets_allocator: [MallocFixedPageSize::new(), MallocFixedPageSize::new()],
            version: 0,
            epoch: None,
        }
    }

    pub fn initialize(&mut self, table_size: u64, alignment: usize, epoch: &'epoch LightEpoch) {
        self.epoch = Some(epoch);
        unsafe {
            self.table[0].initialize(table_size, alignment);
        }
        self.overflow_buckets_allocator[0].initialize(alignment, self.epoch.unwrap());
        self.version = 0;
    }

    /// Finds an existing entry or a free slot for a new entry.
    /// If a free slot is found, it's tentatively marked.
    fn find_tentative_entry<'a>(&'a self, key_hash: HotLogKeyHash, context: &mut FindContext) {
        let version = self.version as usize;
        let table_size = self.table[version].size();
        let bucket_idx = key_hash.table_index(table_size) as usize;
        let tag = key_hash.tag();

        let mut bucket: &HotLogIndexHashBucket =
            unsafe { self.table[version].get_bucket(bucket_idx as u64) };
        let mut free_slot: Option<&'a AtomicHashBucketEntry> = None;

        loop {
            // Search for match or free slot
            for i in 0..7 {
                let entry = bucket.entries[i].load();
                if entry.unused() {
                    if free_slot.is_none() {
                        free_slot = Some(&bucket.entries[i]);
                    }
                    continue;
                }
                if entry.tag() == tag && !entry.tentative() {
                    context.entry = entry;
                    context.atomic_entry = Some(&bucket.entries[i] as *const AtomicHashBucketEntry);
                    return; // Found a match
                }
            }

            let overflow_entry = bucket.overflow_entry.load();
            if overflow_entry.unused() {
                // End of chain
                if let Some(slot) = free_slot {
                    context.entry = HashBucketEntry::default();
                    context.atomic_entry = Some(slot);
                } else {
                    // Chain is full, need to allocate a new bucket
                    let new_bucket_addr = self.overflow_buckets_allocator[version].allocate();
                    let new_bucket = self.overflow_buckets_allocator[version].get(new_bucket_addr);
                    let new_overflow_entry = HashBucketOverflowEntry::new(new_bucket_addr);

                    if bucket
                        .overflow_entry
                        .compare_exchange(HashBucketOverflowEntry::default(), new_overflow_entry)
                        .is_ok()
                    {
                        context.entry = HashBucketEntry::default();
                        context.atomic_entry = Some(&new_bucket.entries[0]);
                    } else {
                        let guard = self.epoch.as_ref().unwrap().protect();
                        self.overflow_buckets_allocator[version]
                            .free_at_epoch(new_bucket_addr, &guard);
                        context.atomic_entry = None;
                    }
                }
                return;
            }
            bucket = self.overflow_buckets_allocator[version].get(overflow_entry.address());
        }
    }

    pub fn checkpoint(
        &mut self,
        disk: &mut FileSystemDisk,
        token: &str,
    ) -> Result<IndexMetadata, Status> {
        let version = self.version as usize;
        let _dir = disk.create_index_checkpoint_directory(token)?;

        let mut ht_file = disk.new_file(&format!("index-checkpoints/{}/ht.dat", token));
        ht_file.open(
            crate::environment::file::FileCreateDisposition::CreateOrTruncate,
            Default::default(),
        )?;
        self.table[version].checkpoint(&mut ht_file)?;

        let mut ofb_file = disk.new_file(&format!("index-checkpoints/{}/ofb.dat", token));
        ofb_file.open(
            crate::environment::file::FileCreateDisposition::CreateOrTruncate,
            Default::default(),
        )?;
        let ofb_bytes = self.overflow_buckets_allocator[version].checkpoint(&mut ofb_file)?;

        let mut metadata = IndexMetadata::default();
        metadata.table_size = self.table[version].size();
        metadata.num_ht_bytes =
            metadata.table_size * std::mem::size_of::<HotLogIndexHashBucket>() as u64;
        metadata.num_ofb_bytes = ofb_bytes;

        Ok(metadata)
    }

    pub fn recover(
        &mut self,
        disk: &mut FileSystemDisk,
        token: &str,
        metadata: &IndexMetadata,
    ) -> Result<(), Status> {
        let version = self.version as usize;
        let _dir = disk.index_checkpoint_path(token);

        let mut ht_file = disk.new_file(&format!("index-checkpoints/{}/ht.dat", token));
        ht_file.open(
            crate::environment::file::FileCreateDisposition::OpenExisting,
            Default::default(),
        )?;
        self.table[version].recover(&mut ht_file, metadata.table_size, metadata.num_ht_bytes)?;

        let mut ofb_file = disk.new_file(&format!("index-checkpoints/{}/ofb.dat", token));
        ofb_file.open(
            crate::environment::file::FileCreateDisposition::OpenExisting,
            Default::default(),
        )?;
        let status = self.overflow_buckets_allocator[version].recover(
            &mut ofb_file,
            metadata.num_ofb_bytes,
            metadata.ofb_count,
        );
        if status != Status::Ok {
            return Err(status);
        }

        // Clear tentative entries
        self.clear_tentative_entries();
        Ok(())
    }

    /// Clear all tentative entries in the hash table
    fn clear_tentative_entries(&self) {
        let version = self.version as usize;
        let table_size = self.table[version].size();

        for bucket_idx in 0..table_size {
            let bucket: &HotLogIndexHashBucket =
                unsafe { self.table[version].get_bucket(bucket_idx) };

            // Clear tentative entries in the main bucket
            for slot_idx in 0..bucket.entries.len() {
                let atomic_entry = &bucket.entries[slot_idx];
                let entry = atomic_entry.load();

                if !entry.unused() && entry.tentative() {
                    // Clear the tentative bit
                    let new_entry = HashBucketEntry::new(
                        entry.address(),
                        entry.tag(),
                        false, // tentative = false
                        false, // readcache = false
                    );
                    atomic_entry.store(new_entry);
                }
            }

            // Clear tentative entries in overflow buckets
            // Note: Overflow bucket handling is simplified for now
        }
    }

    /// Check if there's a conflicting entry with the same tag in the bucket chain
    fn has_conflicting_entry(&self, bucket_idx: usize, tag: u16) -> bool {
        let version = self.version as usize;
        let bucket: &HotLogIndexHashBucket =
            unsafe { self.table[version].get_bucket(bucket_idx as u64) };

        // Check main bucket slots
        for slot in &bucket.entries {
            let entry = slot.load();
            if !entry.unused() && entry.tag() == tag && !entry.tentative() {
                return true;
            }
        }

        // Check overflow buckets
        // Note: Overflow bucket handling is simplified for now

        false
    }
}

impl<'epoch> IHashIndex<'epoch> for HotLogMemHashIndex<'epoch> {
    fn find_entry(&self, context: &mut FindContext) -> Status {
        let key_hash = HotLogKeyHash::new(context.key_hash);
        let version = self.version as usize;
        let table_size = self.table[version].size();

        let bucket_idx = key_hash.table_index(table_size) as usize;
        let tag = key_hash.tag();

        let mut bucket: &HotLogIndexHashBucket =
            unsafe { self.table[version].get_bucket(bucket_idx as u64) };

        loop {
            // Search through the bucket
            for i in 0..7 {
                // HotLogIndexHashBucket has 7 entries
                let entry = bucket.entries[i].load();
                if !entry.unused() && entry.tag() == tag && !entry.tentative() {
                    context.entry = entry;
                    context.atomic_entry = Some(&bucket.entries[i] as *const AtomicHashBucketEntry);
                    return Status::Ok;
                }
            }

            // Follow overflow chain
            let overflow_entry = bucket.overflow_entry.load();
            if overflow_entry.unused() {
                break; // End of chain
            }
            bucket = self.overflow_buckets_allocator[version].get(overflow_entry.address());
        }

        context.entry = HashBucketEntry::default();
        context.atomic_entry = None;
        Status::NotFound
    }

    fn find_or_create_entry(&self, context: &mut FindContext) -> Status {
        let key_hash = HotLogKeyHash::new(context.key_hash);
        let tag = key_hash.tag();
        let version = self.version as usize;
        let table_size = self.table[version].size();
        let bucket_idx = key_hash.table_index(table_size) as usize;

        loop {
            self.find_tentative_entry(key_hash, context);

            if let Some(atomic_entry_ptr) = context.atomic_entry {
                let atomic_entry = unsafe { &*atomic_entry_ptr };
                if !context.entry.unused() {
                    return Status::Ok;
                }

                let desired = HashBucketEntry::new(Address::INVALID_ADDRESS, tag, true, false);
                if atomic_entry
                    .compare_exchange(HashBucketEntry::default(), desired)
                    .is_ok()
                {
                    // Successfully claimed the slot. Now check for conflicts.
                    if self.has_conflicting_entry(bucket_idx, tag) {
                        // Conflict detected, release the tentative slot and retry
                        atomic_entry.store(HashBucketEntry::default());
                        continue;
                    }

                    // No conflict, finalize the entry.
                    let final_entry =
                        HashBucketEntry::new(Address::INVALID_ADDRESS, tag, false, false);
                    atomic_entry.store(final_entry);
                    context.entry = final_entry;
                    return Status::Ok;
                }
                // Lost the race, retry the whole process
            }
        }
    }

    fn try_update_entry(
        &self,
        context: &FindContext,
        new_address: Address,
        readcache: bool,
    ) -> Status {
        if let Some(atomic_entry_ptr) = context.atomic_entry {
            let atomic_entry = unsafe { &*atomic_entry_ptr };
            let key_hash = HotLogKeyHash::new(context.key_hash);
            let tag = key_hash.tag();
            let new_entry = HashBucketEntry::new(new_address, tag, false, readcache);

            if atomic_entry
                .compare_exchange(context.entry, new_entry)
                .is_ok()
            {
                return Status::Ok;
            }
        }
        Status::Aborted
    }
}

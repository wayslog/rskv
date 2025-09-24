use crate::core::address::Address;
use crate::core::status::Status;
use crate::device::file_system_disk::FileSystemDisk;
use crate::faster::{FasterKv, ReadContext, RmwContext};
use crate::index::IHashIndex;
use crate::index::cold_index_contexts::{
    ColdIndexRmwContext, HashIndexChunkKey, HashIndexChunkValue,
};
use crate::index::hash_bucket::HashBucketEntry;
use crate::index::key_hash::ColdLogKeyHash;
use crate::index::mem_index::FindContext;

struct ColdIndexRead<'a> {
    key: HashIndexChunkKey,
    original_context: &'a mut FindContext,
    #[allow(dead_code)]
    index_in_chunk: u8,
    tag_in_chunk: u8,
}

impl<'a> ReadContext for ColdIndexRead<'a> {
    type Key = HashIndexChunkKey;
    type Value = HashIndexChunkValue;

    fn key(&self) -> &Self::Key {
        &self.key
    }
    fn key_hash(&self) -> u64 {
        self.key.get_hash()
    }
    fn get(&mut self, value: &Self::Value) {
        let bucket = &value.bucket;
        let entry = bucket.entries[self.tag_in_chunk as usize].load();
        self.original_context.entry = entry;
    }
}

pub struct ColdIndex<'epoch> {
    internal_kv: FasterKv<'epoch, HashIndexChunkKey, HashIndexChunkValue, FileSystemDisk>,
}

impl RmwContext for ColdIndexRmwContext {
    type Key = HashIndexChunkKey;
    type Value = HashIndexChunkValue;

    fn key(&self) -> &Self::Key {
        &self.key
    }
    fn key_hash(&self) -> u64 {
        self.key.get_hash()
    }

    fn rmw_initial(&self, value: &mut Self::Value) {
        let bucket = &mut value.bucket;
        let entry_ref = &mut bucket.entries[self.index_in_chunk as usize];
        entry_ref.store(self.new_entry);
    }

    fn rmw_copy(&self, old_value: &Self::Value, new_value: &mut Self::Value) {
        *new_value = old_value.clone();
        let bucket = &mut new_value.bucket;
        let entry_ref = &mut bucket.entries[self.index_in_chunk as usize];
        let _ = entry_ref.compare_exchange(self.expected_entry, self.new_entry);
    }

    fn rmw_atomic(&self, value: &mut Self::Value) -> bool {
        let bucket = &mut value.bucket;
        let entry_ref = &mut bucket.entries[self.index_in_chunk as usize];
        entry_ref
            .compare_exchange(self.expected_entry, self.new_entry)
            .is_ok()
    }
}

impl<'epoch> IHashIndex<'epoch> for ColdIndex<'epoch> {
    fn find_entry(&self, context: &mut FindContext) -> Status {
        let key_hash = ColdLogKeyHash::new(context.key_hash);
        let table_size = self.internal_kv.get_table_size();
        let chunk_id = key_hash.chunk_id(table_size);
        let tag = key_hash.tag_in_chunk();

        let mut read_context = ColdIndexRead {
            key: HashIndexChunkKey {
                chunk_id,
                tag: tag as u16,
            },
            original_context: context,
            index_in_chunk: key_hash.index_in_chunk(),
            tag_in_chunk: tag,
        };

        let status = self.internal_kv.read(&mut read_context);
        if status == Status::Ok && read_context.original_context.entry.unused() {
            return Status::NotFound;
        }
        status
    }

    fn find_or_create_entry(&self, context: &mut FindContext) -> Status {
        self.rmw_entry(context, Address::INVALID_ADDRESS, true)
    }

    fn try_update_entry(
        &self,
        context: &FindContext,
        new_address: crate::core::address::Address,
        _readcache: bool,
    ) -> Status {
        self.rmw_entry(context, new_address, false)
    }
}

impl<'epoch> ColdIndex<'epoch> {
    pub fn new(log_path: &str) -> Result<Self, Status> {
        let disk = FileSystemDisk::new(&format!("{}/cold_index", log_path))?;
        let internal_kv = FasterKv::new(1 << 30, 1 << 20, disk)?;
        Ok(Self { internal_kv })
    }

    fn rmw_entry(&self, context: &FindContext, new_address: Address, is_create: bool) -> Status {
        let key_hash = ColdLogKeyHash::new(context.key_hash);
        let table_size = self.internal_kv.get_table_size();
        let chunk_id = key_hash.chunk_id(table_size);
        let tag = key_hash.tag_in_chunk();

        let new_entry = if is_create {
            HashBucketEntry::new(Address::INVALID_ADDRESS, tag as u16, false, false)
        } else {
            HashBucketEntry::new(new_address, tag as u16, false, false)
        };

        let mut rmw_context = ColdIndexRmwContext {
            key: HashIndexChunkKey {
                chunk_id,
                tag: tag as u16,
            },
            index_in_chunk: key_hash.index_in_chunk(),
            tag_in_chunk: tag,
            new_entry,
            expected_entry: context.entry,
        };

        self.internal_kv.rmw(&mut rmw_context)
    }
}

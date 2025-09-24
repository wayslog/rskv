use crate::core::record::{Record, RecordInfo};
use crate::core::status::Status;
use crate::device::file_system_disk::FileSystemDisk;
use crate::faster::{FasterKv, ReadContext, RmwContext, UpsertContext};
use crate::index::IHashIndex;
use crate::index::mem_index::FindContext;
use std::marker::PhantomData;

#[cfg(test)]
mod tests;

// Redefine FasterKv with a specific index type for clarity.
pub type HotStore<'a, K, V> = FasterKv<'a, K, V, FileSystemDisk>;
pub type ColdStore<'a, K, V> = FasterKv<'a, K, V, FileSystemDisk>; // This is conceptually the cold store.

pub struct F2Kv<'epoch, K, V> {
    hot_store: HotStore<'epoch, K, V>,
    cold_store: ColdStore<'epoch, K, V>,
    _v: PhantomData<V>,
}

impl<'epoch, K, V> F2Kv<'epoch, K, V>
where
    K: Sized + Copy + 'static + PartialEq,
    V: Sized + Copy + 'static + Default,
{
    pub fn new(hot_log_path: &str, cold_log_path: &str) -> Result<Self, Status> {
        let hot_disk = FileSystemDisk::new(hot_log_path)?;
        let cold_disk = FileSystemDisk::new(cold_log_path)?;

        let hot_store = FasterKv::new(1 << 28, 1 << 20, hot_disk)?;
        let cold_store = FasterKv::new(1 << 30, 1 << 24, cold_disk)?;

        Ok(Self {
            hot_store,
            cold_store,
            _v: PhantomData,
        })
    }

    pub fn upsert(&self, context: &impl UpsertContext<Key = K, Value = V>) -> Status {
        // All writes go to the hot store.
        self.hot_store.upsert(context)
    }

    pub fn read(&self, context: &mut impl ReadContext<Key = K, Value = V>) -> Status {
        let status = self.hot_store.read(context);
        if status == Status::NotFound {
            // If not found in hot store, check cold store.
            self.cold_store.read(context)
        } else {
            status
        }
    }

    pub fn rmw(&self, context: &mut impl RmwContext<Key = K, Value = V>) -> Status {
        loop {
            let status = self.hot_store.rmw(context);
            if status != Status::NotFound {
                return status;
            }

            // Key not found in hot store, try to read from cold store.
            struct F2RmwReadContext<'a, K, V> {
                key: &'a K,
                key_hash: u64,
                value: Option<V>,
            }

            impl<'a, K: PartialEq, V: Copy> ReadContext for F2RmwReadContext<'a, K, V> {
                type Key = K;
                type Value = V;
                fn key(&self) -> &K {
                    self.key
                }
                fn key_hash(&self) -> u64 {
                    self.key_hash
                }
                fn get(&mut self, value: &V) {
                    self.value = Some(*value);
                }
            }

            let mut read_context = F2RmwReadContext {
                key: context.key(),
                key_hash: context.key_hash(),
                value: None,
            };

            self.cold_store.read(&mut read_context);

            // Now, conditionally insert the modified value into the hot store.
            let mut find_context = FindContext::new(context.key_hash());
            self.hot_store.index.find_or_create_entry(&mut find_context);

            if !find_context.entry.unused() {
                // Another thread inserted a value while we were reading from cold store. Retry.
                continue;
            }

            let record_size = Record::<K, V>::required_size_with_alignment();
            let new_address = match self.hot_store.hlog.allocate(record_size as u64) {
                Ok(addr) => addr,
                Err(closed_page) => {
                    self.hot_store.hlog.new_page(closed_page);
                    return Status::Pending;
                }
            };

            let buffer = unsafe {
                self.hot_store
                    .hlog
                    .get_mut_slice_unchecked(new_address, record_size as usize)
            };
            let new_record_info =
                RecordInfo::new(find_context.entry.address(), 0, false, false, true);

            unsafe {
                let mut value_buffer = V::default();
                match read_context.value {
                    Some(ref old_val) => context.rmw_copy(old_val, &mut value_buffer),
                    None => context.rmw_initial(&mut value_buffer),
                }
                Record::create_in(buffer, new_record_info, context.key(), &value_buffer);
            }

            if self
                .hot_store
                .index
                .try_update_entry(&find_context, new_address, false)
                == Status::Ok
            {
                return Status::Ok;
            }

            // CAS failed, retry.
            unsafe {
                let record_ptr = buffer.as_mut_ptr() as *mut Record<K, V>;
                (*record_ptr).header.set_invalid(true);
            }
        }
    }
}

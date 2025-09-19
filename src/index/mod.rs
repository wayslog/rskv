pub mod cold_index;
pub mod cold_index_contexts;
pub mod definitions;
pub mod hash_bucket;
pub mod hash_table;
pub mod key_hash;
pub mod mem_index;

use crate::core::address::Address;
use crate::core::status::Status;
use crate::index::mem_index::FindContext;

/// A trait for hash index implementations.
pub trait IHashIndex<'a> {
    fn find_entry(&self, context: &mut FindContext) -> Status;
    fn find_or_create_entry(&self, context: &mut FindContext) -> Status;
    fn try_update_entry(
        &self,
        context: &FindContext,
        new_address: Address,
        readcache: bool,
    ) -> Status;
}

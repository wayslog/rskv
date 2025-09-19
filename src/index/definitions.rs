use crate::index::hash_bucket::{ColdLogIndexHashBucket, HotLogIndexHashBucket};

/// A trait that defines the components of a specific hash index implementation.
/// This allows `InternalHashTable` and other structures to be generic over
/// different index configurations (e.g., for the hot log vs. the cold log).
pub trait HashIndexDefinition {
    /// The type of the hash bucket used in this index.
    type HashBucket: Sized + Default + Send + Sync;
}

/// The definition for the in-memory ("hot") log's hash index.
#[derive(Default)]
pub struct HotLogHashIndexDefinition;

impl HashIndexDefinition for HotLogHashIndexDefinition {
    type HashBucket = HotLogIndexHashBucket;
}

/// The definition for the on-disk ("cold") log's hash index.
#[derive(Default)]
pub struct ColdLogHashIndexDefinition;

impl HashIndexDefinition for ColdLogHashIndexDefinition {
    type HashBucket = ColdLogIndexHashBucket;
}

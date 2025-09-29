use crate::core::record::{Record, RecordInfo};
use crate::core::status::Status;
use crate::device::file_system_disk::FileSystemDisk;
use crate::faster::{FasterKv, ReadContext, RmwContext, UpsertContext};
use crate::index::IHashIndex;
use crate::index::mem_index::FindContext;
use crate::performance::access_analyzer::{AccessAnalyzer, AnalyzerConfig, OperationType};
use crate::performance::migration_manager::{KeyStats, MigrationConfig, MigrationManager};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(test)]
mod tests;

// Redefine FasterKv with a specific index type for clarity.
pub type HotStore<'a, K, V> = FasterKv<'a, K, V, FileSystemDisk>;
pub type ColdStore<'a, K, V> = FasterKv<'a, K, V, FileSystemDisk>; // This is conceptually the cold store.

pub struct F2Kv<'epoch, K, V> {
    hot_store: HotStore<'epoch, K, V>,
    cold_store: ColdStore<'epoch, K, V>,
    migration_manager: Arc<MigrationManager>,
    access_analyzer: Arc<AccessAnalyzer>,
    key_stats: Arc<RwLock<HashMap<u64, Arc<KeyStats>>>>,
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

        let migration_config = MigrationConfig {
            max_hot_size_bytes: 1 << 28, // 256MB
            ..Default::default()
        };

        Ok(Self {
            hot_store,
            cold_store,
            migration_manager: Arc::new(MigrationManager::new(migration_config)),
            access_analyzer: Arc::new(AccessAnalyzer::new(AnalyzerConfig::default())),
            key_stats: Arc::new(RwLock::new(HashMap::new())),
            _v: PhantomData,
        })
    }

    pub fn new_with_config(
        hot_log_path: &str,
        cold_log_path: &str,
        migration_config: MigrationConfig,
        analyzer_config: AnalyzerConfig,
    ) -> Result<Self, Status> {
        let hot_disk = FileSystemDisk::new(hot_log_path)?;
        let cold_disk = FileSystemDisk::new(cold_log_path)?;

        let hot_store = FasterKv::new(1 << 28, 1 << 20, hot_disk)?;
        let cold_store = FasterKv::new(1 << 30, 1 << 24, cold_disk)?;

        Ok(Self {
            hot_store,
            cold_store,
            migration_manager: Arc::new(MigrationManager::new(migration_config)),
            access_analyzer: Arc::new(AccessAnalyzer::new(analyzer_config)),
            key_stats: Arc::new(RwLock::new(HashMap::new())),
            _v: PhantomData,
        })
    }

    fn get_current_time_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    fn get_or_create_key_stats(&self, key_hash: u64) -> Arc<KeyStats> {
        // Try read first
        if let Ok(stats_map) = self.key_stats.read() {
            if let Some(stats) = stats_map.get(&key_hash) {
                return Arc::clone(stats);
            }
        }

        // Need to create new stats
        let new_stats = Arc::new(KeyStats::new(std::mem::size_of::<K>() + std::mem::size_of::<V>()));

        if let Ok(mut stats_map) = self.key_stats.write() {
            stats_map.entry(key_hash).or_insert_with(|| Arc::clone(&new_stats));
        }

        new_stats
    }

    pub fn upsert(&self, context: &impl UpsertContext<Key = K, Value = V>) -> Status {
        let key_hash = context.key_hash();

        // Record access
        self.access_analyzer.record_access(key_hash, OperationType::Write);

        // Update key stats
        let stats = self.get_or_create_key_stats(key_hash);
        stats.record_access(Self::get_current_time_ms());
        stats.set_in_hot(true);

        // Record migration
        self.migration_manager.record_migration_to_hot(stats.get_size());

        // All writes go to the hot store.
        self.hot_store.upsert(context)
    }

    pub fn read(&self, context: &mut impl ReadContext<Key = K, Value = V>) -> Status {
        let key_hash = context.key_hash();

        // Record access
        self.access_analyzer.record_access(key_hash, OperationType::Read);

        // Update key stats
        let stats = self.get_or_create_key_stats(key_hash);
        let current_time = Self::get_current_time_ms();
        stats.record_access(current_time);

        let status = self.hot_store.read(context);
        if status == Status::NotFound {
            // If not found in hot store, check cold store.
            let cold_status = self.cold_store.read(context);

            // If found in cold store, check if we should migrate to hot
            if cold_status == Status::Ok {
                stats.set_in_hot(false);
                if self.migration_manager.should_migrate_to_hot(&stats, current_time) {
                    // Mark for future migration (actual migration happens in rmw)
                }
            }

            cold_status
        } else {
            stats.set_in_hot(true);
            status
        }
    }

    pub fn rmw(&self, context: &mut impl RmwContext<Key = K, Value = V>) -> Status {
        let key_hash = context.key_hash();

        // Record access
        self.access_analyzer.record_access(key_hash, OperationType::Update);

        // Update key stats
        let stats = self.get_or_create_key_stats(key_hash);
        stats.record_access(Self::get_current_time_ms());

        loop {
            let status = self.hot_store.rmw(context);
            if status != Status::NotFound {
                stats.set_in_hot(true);
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

    /// Get migration statistics
    pub fn get_migration_stats(&self) -> crate::performance::migration_manager::MigrationStats {
        self.migration_manager.get_stats()
    }

    /// Get access pattern statistics
    pub fn get_access_stats(&self) -> crate::performance::access_analyzer::AccessStats {
        self.access_analyzer.analyze_patterns()
    }

    /// Get access pattern recommendation
    pub fn get_access_recommendation(&self) -> crate::performance::access_analyzer::AccessRecommendation {
        self.access_analyzer.get_recommendation()
    }

    /// Get hot keys (most frequently accessed)
    pub fn get_hot_keys(&self, n: usize) -> Vec<(u64, u64)> {
        self.access_analyzer.get_hot_keys(n)
    }
}

/// Lockable record wrapper that provides fine-grained locking for FASTER-style operations
use crate::core::locking::{RecordLock, SharedLockGuard, ExclusiveLockGuard};
use crate::core::record::{Record, RecordInfo};
use std::sync::atomic::{AtomicPtr, Ordering};
use std::ptr;

/// A lockable wrapper around a Record that provides fine-grained concurrency control
pub struct LockableRecord<K, V>
where
    K: Sized,
    V: Sized,
{
    /// The actual record data
    record_ptr: AtomicPtr<Record<K, V>>,
    /// The lock for this record
    lock: RecordLock,
}

impl<K, V> LockableRecord<K, V>
where
    K: Sized + Copy,
    V: Sized + Copy,
{
    /// Creates a new empty lockable record
    pub fn new() -> Self {
        Self {
            record_ptr: AtomicPtr::new(ptr::null_mut()),
            lock: RecordLock::new(),
        }
    }

    /// Sets the record pointer (typically called by the allocator)
    pub fn set_record_ptr(&self, ptr: *mut Record<K, V>) {
        self.record_ptr.store(ptr, Ordering::Release);
    }

    /// Attempts to acquire a shared lock for reading
    pub fn try_read(&self) -> Option<LockableRecordReadGuard<'_, K, V>> {
        SharedLockGuard::new(&self.lock).map(|guard| {
            LockableRecordReadGuard {
                record_ptr: &self.record_ptr,
                _lock_guard: guard,
            }
        })
    }

    /// Attempts to acquire an exclusive lock for writing
    pub fn try_write(&self) -> Option<LockableRecordWriteGuard<'_, K, V>> {
        ExclusiveLockGuard::new(&self.lock).map(|guard| {
            LockableRecordWriteGuard {
                record_ptr: &self.record_ptr,
                _lock_guard: guard,
            }
        })
    }

    /// Checks if the record is currently locked
    pub fn is_locked(&self) -> bool {
        self.lock.is_locked()
    }

    /// Checks if the record has an exclusive lock
    pub fn is_exclusively_locked(&self) -> bool {
        self.lock.is_exclusively_locked()
    }

    /// Gets the current shared lock count
    pub fn shared_lock_count(&self) -> u32 {
        self.lock.shared_count()
    }

    /// Atomically replaces the record (requires exclusive lock)
    pub fn replace_record(&self, new_record: Record<K, V>) -> Option<Record<K, V>> {
        if let Some(_guard) = ExclusiveLockGuard::new(&self.lock) {
            let new_ptr = Box::into_raw(Box::new(new_record));
            let old_ptr = self.record_ptr.swap(new_ptr, Ordering::SeqCst);

            if old_ptr.is_null() {
                None
            } else {
                unsafe { Some(*Box::from_raw(old_ptr)) }
            }
        } else {
            None // Could not acquire exclusive lock
        }
    }

    /// Checks if the record exists (non-null)
    pub fn exists(&self) -> bool {
        !self.record_ptr.load(Ordering::Acquire).is_null()
    }
}

impl<K, V> Drop for LockableRecord<K, V>
where
    K: Sized,
    V: Sized,
{
    fn drop(&mut self) {
        let ptr = self.record_ptr.load(Ordering::Acquire);
        if !ptr.is_null() {
            unsafe {
                let _ = Box::from_raw(ptr);
            }
        }
    }
}

/// RAII guard for reading from a lockable record
pub struct LockableRecordReadGuard<'a, K, V>
where
    K: Sized,
    V: Sized,
{
    record_ptr: &'a AtomicPtr<Record<K, V>>,
    _lock_guard: SharedLockGuard<'a>,
}

impl<'a, K, V> LockableRecordReadGuard<'a, K, V>
where
    K: Sized + Copy,
    V: Sized + Copy,
{
    /// Gets a reference to the record (if it exists)
    pub fn record(&self) -> Option<&Record<K, V>> {
        let ptr = self.record_ptr.load(Ordering::Acquire);
        if ptr.is_null() {
            None
        } else {
            unsafe { Some(&*ptr) }
        }
    }

    /// Gets the key (if record exists)
    pub fn key(&self) -> Option<&K> {
        let ptr = self.record_ptr.load(Ordering::Acquire);
        if ptr.is_null() {
            None
        } else {
            unsafe { Some(Record::key(ptr)) }
        }
    }

    /// Gets the value (if record exists)
    pub fn value(&self) -> Option<&V> {
        let ptr = self.record_ptr.load(Ordering::Acquire);
        if ptr.is_null() {
            None
        } else {
            unsafe { Some(Record::value(ptr)) }
        }
    }

    /// Gets the record info (if record exists)
    pub fn record_info(&self) -> Option<&RecordInfo> {
        let ptr = self.record_ptr.load(Ordering::Acquire);
        if ptr.is_null() {
            None
        } else {
            unsafe { Some(&(*ptr).header) }
        }
    }
}

/// RAII guard for writing to a lockable record
pub struct LockableRecordWriteGuard<'a, K, V>
where
    K: Sized,
    V: Sized,
{
    record_ptr: &'a AtomicPtr<Record<K, V>>,
    _lock_guard: ExclusiveLockGuard<'a>,
}

impl<'a, K, V> LockableRecordWriteGuard<'a, K, V>
where
    K: Sized + Copy,
    V: Sized + Copy,
{
    /// Gets a mutable reference to the record (if it exists)
    pub fn record_mut(&mut self) -> Option<&mut Record<K, V>> {
        let ptr = self.record_ptr.load(Ordering::Acquire);
        if ptr.is_null() {
            None
        } else {
            unsafe { Some(&mut *ptr) }
        }
    }

    /// Gets a reference to the record (if it exists)
    pub fn record(&self) -> Option<&Record<K, V>> {
        let ptr = self.record_ptr.load(Ordering::Acquire);
        if ptr.is_null() {
            None
        } else {
            unsafe { Some(&*ptr) }
        }
    }

    /// Sets the record pointer
    pub fn set_record(&mut self, new_ptr: *mut Record<K, V>) -> *mut Record<K, V> {
        self.record_ptr.swap(new_ptr, Ordering::SeqCst)
    }

    /// Removes the record (sets to null)
    pub fn remove(&mut self) -> *mut Record<K, V> {
        self.record_ptr.swap(ptr::null_mut(), Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lockable_record_creation() {
        let lockable: LockableRecord<u64, u64> = LockableRecord::new();
        assert!(!lockable.exists());
        assert!(!lockable.is_locked());
    }

    #[test]
    fn test_concurrent_reads() {
        let lockable: LockableRecord<u64, u64> = LockableRecord::new();

        let _guard1 = lockable.try_read().unwrap();
        let _guard2 = lockable.try_read().unwrap(); // Multiple reads allowed

        assert_eq!(lockable.shared_lock_count(), 2);
    }

    #[test]
    fn test_exclusive_write() {
        let lockable: LockableRecord<u64, u64> = LockableRecord::new();

        let _write_guard = lockable.try_write().unwrap();
        assert!(lockable.is_exclusively_locked());

        // Should not be able to acquire another lock
        assert!(lockable.try_read().is_none());
        assert!(lockable.try_write().is_none());
    }

    #[test]
    fn test_lock_basic_functionality() {
        let lockable: LockableRecord<u64, u64> = LockableRecord::new();

        // Test basic locking
        assert!(!lockable.is_locked());

        {
            let _shared_guard = lockable.try_read().unwrap();
            assert!(lockable.is_locked());
            assert!(!lockable.is_exclusively_locked());
            assert_eq!(lockable.shared_lock_count(), 1);
        }

        // Lock should be released after guard drops
        assert!(!lockable.is_locked());
        assert_eq!(lockable.shared_lock_count(), 0);
    }
}
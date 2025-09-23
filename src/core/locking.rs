/// Record-level locking system for FASTER-style concurrency control
use std::sync::atomic::{AtomicU64, Ordering};

/// Lock types supported by the system
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LockType {
    None = 0,
    Shared = 1,
    Exclusive = 2,
}

/// Lock state packed into a single u64
/// Layout: [shared_count: 32][exclusive: 1][pending_exclusive: 1][reserved: 30]
#[derive(Debug)]
#[repr(transparent)]
pub struct RecordLock(AtomicU64);

impl RecordLock {
    const SHARED_COUNT_SHIFT: u32 = 32;
    #[allow(dead_code)]
    const SHARED_COUNT_MASK: u64 = 0xFFFF_FFFF;
    const EXCLUSIVE_BIT: u64 = 1 << 31;
    const PENDING_EXCLUSIVE_BIT: u64 = 1 << 30;

    /// Creates a new unlocked record lock
    pub fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    /// Attempts to acquire a shared lock
    /// Returns true if successful, false if blocked by exclusive lock
    pub fn try_lock_shared(&self) -> bool {
        let mut current = self.0.load(Ordering::Acquire);
        loop {
            // Cannot acquire shared lock if exclusive lock is held or pending
            if (current & (Self::EXCLUSIVE_BIT | Self::PENDING_EXCLUSIVE_BIT)) != 0 {
                return false;
            }

            let shared_count = (current >> Self::SHARED_COUNT_SHIFT) as u32;
            if shared_count == u32::MAX {
                // Overflow protection
                return false;
            }

            let new_value = current + (1u64 << Self::SHARED_COUNT_SHIFT);

            match self.0.compare_exchange_weak(
                current,
                new_value,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    /// Attempts to acquire an exclusive lock
    /// Returns true if successful, false if any locks are held
    pub fn try_lock_exclusive(&self) -> bool {
        let expected = 0u64;
        self.0.compare_exchange(
            expected,
            Self::EXCLUSIVE_BIT,
            Ordering::Acquire,
            Ordering::Relaxed,
        ).is_ok()
    }

    /// Releases a shared lock
    /// Returns true if successful, false if no shared lock was held
    pub fn unlock_shared(&self) -> bool {
        let mut current = self.0.load(Ordering::Acquire);
        loop {
            let shared_count = (current >> Self::SHARED_COUNT_SHIFT) as u32;
            if shared_count == 0 {
                return false; // No shared lock to release
            }

            let new_value = current - (1u64 << Self::SHARED_COUNT_SHIFT);

            match self.0.compare_exchange_weak(
                current,
                new_value,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    /// Releases an exclusive lock
    /// Returns true if successful, false if no exclusive lock was held
    pub fn unlock_exclusive(&self) -> bool {
        let expected = Self::EXCLUSIVE_BIT;
        self.0.compare_exchange(
            expected,
            0u64,
            Ordering::Release,
            Ordering::Relaxed,
        ).is_ok()
    }

    /// Checks if any lock is held
    pub fn is_locked(&self) -> bool {
        let current = self.0.load(Ordering::Acquire);
        current != 0
    }

    /// Checks if an exclusive lock is held
    pub fn is_exclusively_locked(&self) -> bool {
        let current = self.0.load(Ordering::Acquire);
        (current & Self::EXCLUSIVE_BIT) != 0
    }

    /// Gets the current shared lock count
    pub fn shared_count(&self) -> u32 {
        let current = self.0.load(Ordering::Acquire);
        (current >> Self::SHARED_COUNT_SHIFT) as u32
    }
}

impl Default for RecordLock {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII guard for shared locks
pub struct SharedLockGuard<'a> {
    lock: &'a RecordLock,
}

impl<'a> SharedLockGuard<'a> {
    pub fn new(lock: &'a RecordLock) -> Option<Self> {
        if lock.try_lock_shared() {
            Some(Self { lock })
        } else {
            None
        }
    }
}

impl<'a> Drop for SharedLockGuard<'a> {
    fn drop(&mut self) {
        self.lock.unlock_shared();
    }
}

/// RAII guard for exclusive locks
pub struct ExclusiveLockGuard<'a> {
    lock: &'a RecordLock,
}

impl<'a> ExclusiveLockGuard<'a> {
    pub fn new(lock: &'a RecordLock) -> Option<Self> {
        if lock.try_lock_exclusive() {
            Some(Self { lock })
        } else {
            None
        }
    }
}

impl<'a> Drop for ExclusiveLockGuard<'a> {
    fn drop(&mut self) {
        self.lock.unlock_exclusive();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_shared_lock() {
        let lock = RecordLock::new();
        assert!(lock.try_lock_shared());
        assert!(lock.try_lock_shared()); // Multiple shared locks allowed
        assert_eq!(lock.shared_count(), 2);
        assert!(lock.unlock_shared());
        assert!(lock.unlock_shared());
        assert!(!lock.is_locked());
    }

    #[test]
    fn test_exclusive_lock() {
        let lock = RecordLock::new();
        assert!(lock.try_lock_exclusive());
        assert!(lock.is_exclusively_locked());
        assert!(!lock.try_lock_shared()); // Blocked by exclusive lock
        assert!(!lock.try_lock_exclusive()); // Only one exclusive lock
        assert!(lock.unlock_exclusive());
        assert!(!lock.is_locked());
    }

    #[test]
    fn test_concurrent_shared_locks() {
        let lock = Arc::new(RecordLock::new());
        let mut handles = vec![];

        // Spawn multiple threads trying to acquire shared locks
        for _ in 0..10 {
            let lock_clone = Arc::clone(&lock);
            let handle = thread::spawn(move || {
                let _guard = SharedLockGuard::new(&lock_clone);
                thread::sleep(std::time::Duration::from_millis(10));
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert!(!lock.is_locked());
    }

    #[test]
    fn test_exclusive_blocks_shared() {
        let lock = RecordLock::new();
        let _exclusive_guard = ExclusiveLockGuard::new(&lock).unwrap();
        assert!(SharedLockGuard::new(&lock).is_none());
    }
}
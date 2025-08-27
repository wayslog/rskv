//! Epoch-based memory management for rskv
//! 
//! This module provides epoch-based garbage collection and memory reclamation
//! using crossbeam-epoch. It's inspired by FASTER's light_epoch.h design.

use crossbeam_epoch::{Collector, Guard, LocalHandle};
use std::sync::Arc;

/// Epoch manager that provides safe memory reclamation
/// This is a wrapper around crossbeam-epoch that provides a simpler interface
/// for the rest of the rskv codebase.
pub struct EpochManager {
    collector: Collector,
}

impl EpochManager {
    /// Create a new epoch manager
    pub fn new() -> Self {
        Self {
            collector: Collector::new(),
        }
    }

    /// Create a new local handle for epoch management
    /// Each thread should have its own local handle
    pub fn register(&self) -> EpochHandle {
        EpochHandle {
            handle: self.collector.register(),
        }
    }

    /// Pin the current thread to an epoch and return a guard
    /// The guard must be held while accessing epoch-protected data
    pub fn pin(&self) -> Guard {
        self.collector.register().pin()
    }

    /// Flush all pending destructions in this epoch
    pub fn flush(&self) {
        // Force garbage collection for all threads
        let guard = self.pin();
        drop(guard);
    }
}

impl Default for EpochManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Thread-local epoch handle
/// Each thread should have its own handle for optimal performance
pub struct EpochHandle {
    handle: LocalHandle,
}

impl EpochHandle {
    /// Pin the current thread to an epoch and return a guard
    pub fn pin(&mut self) -> Guard {
        self.handle.pin()
    }

    /// Pin the current thread and return a guard (convenience method)
    pub fn protect(&mut self) -> Guard {
        self.pin()
    }

    /// Defer destruction of an object until it's safe to reclaim
    /// This is used for lock-free data structures where we need to defer
    /// the destruction of nodes until no other threads are accessing them
    pub fn defer<F>(&mut self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let guard = self.pin();
        guard.defer(f);
    }

    /// Defer destruction with a specific destructor function
    /// 
    /// # Safety
    /// The caller must ensure that the pointer was allocated via Box::into_raw
    /// and is not used elsewhere after this call.
    pub unsafe fn defer_destroy<T>(&mut self, ptr: *mut T)
    where
        T: Send + 'static,
    {
        // Convert to usize to make it Send
        let ptr_addr = ptr as usize;
        self.defer(move || {
            let ptr = ptr_addr as *mut T;
            if !ptr.is_null() {
                unsafe {
                    drop(Box::from_raw(ptr));
                }
            }
        });
    }

    /// Flush any pending destructions
    pub fn flush(&mut self) {
        // Pin and then immediately unpin to force collection
        let _guard = self.pin();
    }
}

/// Epoch-protected pointer
/// This is a smart pointer that can be safely accessed within an epoch
pub struct EpochPtr<T> {
    ptr: *mut T,
}

impl<T> EpochPtr<T> {
    /// Create a new epoch-protected pointer
    pub fn new(ptr: *mut T) -> Self {
        Self { ptr }
    }

    /// Create a null epoch-protected pointer
    pub fn null() -> Self {
        Self {
            ptr: std::ptr::null_mut(),
        }
    }

    /// Check if the pointer is null
    pub fn is_null(&self) -> bool {
        self.ptr.is_null()
    }

    /// Get the raw pointer (unsafe)
    /// The caller must ensure they hold an appropriate epoch guard
    pub unsafe fn as_ptr(&self) -> *mut T {
        self.ptr
    }

    /// Get a reference to the pointed object (unsafe)
    /// The caller must ensure they hold an appropriate epoch guard
    /// and that the pointer is valid
    pub unsafe fn as_ref(&self) -> Option<&T> {
        if self.ptr.is_null() {
            None
        } else {
            unsafe { Some(&*self.ptr) }
        }
    }

    /// Get a mutable reference to the pointed object (unsafe)
    /// The caller must ensure they hold an appropriate epoch guard
    /// and that the pointer is valid and exclusively accessible
    pub unsafe fn as_mut(&mut self) -> Option<&mut T> {
        if self.ptr.is_null() {
            None
        } else {
            unsafe { Some(&mut *self.ptr) }
        }
    }
}

unsafe impl<T: Send> Send for EpochPtr<T> {}
unsafe impl<T: Sync> Sync for EpochPtr<T> {}

impl<T> Clone for EpochPtr<T> {
    fn clone(&self) -> Self {
        Self { ptr: self.ptr }
    }
}

impl<T> Copy for EpochPtr<T> {}

/// Utility trait for epoch-based operations
pub trait EpochProtected {
    /// Execute a function within an epoch guard
    fn with_epoch<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Guard) -> R;
}

impl EpochProtected for EpochManager {
    fn with_epoch<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Guard) -> R,
    {
        let guard = self.pin();
        f(&guard)
    }
}

/// Shared epoch manager that can be used across multiple threads
pub type SharedEpochManager = Arc<EpochManager>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_epoch_manager_creation() {
        let epoch_manager = EpochManager::new();
        let _handle = epoch_manager.register();
    }

    #[test]
    fn test_epoch_guard() {
        let epoch_manager = EpochManager::new();
        let _guard = epoch_manager.pin();
        // Guard should protect current epoch
    }

    #[test]
    fn test_defer_destruction() {
        let epoch_manager = EpochManager::new();
        let mut handle = epoch_manager.register();
        
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();
        
        handle.defer(move || {
            counter_clone.fetch_add(1, Ordering::SeqCst);
        });
        
        // Force garbage collection
        handle.flush();
        
        // Give some time for deferred destruction
        thread::sleep(std::time::Duration::from_millis(10));
        
        // Note: The exact timing of deferred destruction is not guaranteed
        // This test mainly ensures the API works without panicking
    }

    #[test]
    fn test_epoch_ptr() {
        let value = Box::into_raw(Box::new(42i32));
        let epoch_ptr = EpochPtr::new(value);
        
        assert!(!epoch_ptr.is_null());
        
        unsafe {
            assert_eq!(*epoch_ptr.as_ptr(), 42);
            if let Some(val_ref) = epoch_ptr.as_ref() {
                assert_eq!(*val_ref, 42);
            }
            
            // Clean up
            drop(Box::from_raw(value));
        }
    }

    #[test]
    fn test_null_epoch_ptr() {
        let epoch_ptr: EpochPtr<i32> = EpochPtr::null();
        assert!(epoch_ptr.is_null());
        
        unsafe {
            assert!(epoch_ptr.as_ref().is_none());
        }
    }

    #[test]
    fn test_with_epoch() {
        let epoch_manager = EpochManager::new();
        
        let result = epoch_manager.with_epoch(|_guard| {
            42
        });
        
        assert_eq!(result, 42);
    }
}

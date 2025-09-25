use std::alloc::{Layout, alloc, dealloc};
use std::collections::HashMap;
use std::ptr::null_mut;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Memory allocation tracker for leak detection
#[derive(Debug)]
struct AllocationInfo {
    layout: Layout,
    allocated_at: std::time::Instant,
}

struct AllocationTracker {
    allocations: Mutex<HashMap<usize, AllocationInfo>>,
    total_allocated: AtomicUsize,
    peak_allocated: AtomicUsize,
}

impl AllocationTracker {
    fn new() -> Self {
        Self {
            allocations: Mutex::new(HashMap::new()),
            total_allocated: AtomicUsize::new(0),
            peak_allocated: AtomicUsize::new(0),
        }
    }

    fn track_allocation(&self, ptr: *mut u8, layout: Layout) {
        if let Ok(mut allocations) = self.allocations.lock() {
            allocations.insert(
                ptr as usize,
                AllocationInfo {
                    layout,
                    allocated_at: std::time::Instant::now(),
                },
            );
            let current = self
                .total_allocated
                .fetch_add(layout.size(), Ordering::Relaxed)
                + layout.size();
            let mut peak = self.peak_allocated.load(Ordering::Relaxed);
            while current > peak {
                match self.peak_allocated.compare_exchange_weak(
                    peak,
                    current,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(new_peak) => peak = new_peak,
                }
            }
        }
    }

    fn track_deallocation(&self, ptr: *mut u8) -> Option<Layout> {
        if let Ok(mut allocations) = self.allocations.lock()
            && let Some(info) = allocations.remove(&(ptr as usize)) {
                self.total_allocated
                    .fetch_sub(info.layout.size(), Ordering::Relaxed);
                return Some(info.layout);
            }
        None
    }

    fn get_stats(&self) -> (usize, usize, usize) {
        let current = self.total_allocated.load(Ordering::Relaxed);
        let peak = self.peak_allocated.load(Ordering::Relaxed);
        let leak_count = self.allocations.lock().map(|a| a.len()).unwrap_or(0);
        (current, peak, leak_count)
    }

    fn report_leaks(&self) -> Vec<(usize, std::time::Duration)> {
        let mut leaks = Vec::new();
        if let Ok(allocations) = self.allocations.lock() {
            let now = std::time::Instant::now();
            for info in allocations.values() {
                leaks.push((info.layout.size(), now.duration_since(info.allocated_at)));
            }
        }
        leaks
    }
}

static ALLOCATION_TRACKER: std::sync::LazyLock<AllocationTracker> =
    std::sync::LazyLock::new(AllocationTracker::new);

/// Get memory allocation statistics
pub fn get_memory_stats() -> (usize, usize, usize) {
    ALLOCATION_TRACKER.get_stats()
}

/// Report potential memory leaks
pub fn report_memory_leaks() -> Vec<(usize, std::time::Duration)> {
    ALLOCATION_TRACKER.report_leaks()
}

/// Allocates memory with a specified alignment.
///
/// # Safety
///
/// The caller must ensure that the `layout` has a non-zero size.
pub unsafe fn aligned_alloc(layout: Layout) -> *mut u8 {
    if layout.size() == 0 {
        return null_mut();
    }
    let ptr = unsafe { alloc(layout) };
    if !ptr.is_null() {
        ALLOCATION_TRACKER.track_allocation(ptr, layout);
    }
    ptr
}

/// Frees memory that was allocated with `aligned_alloc`.
///
/// # Safety
///
/// `ptr` must have been allocated using `aligned_alloc` with the same `layout`.
pub unsafe fn aligned_free(ptr: *mut u8, layout: Layout) {
    if !ptr.is_null() {
        let tracked_layout = ALLOCATION_TRACKER.track_deallocation(ptr);
        unsafe {
            dealloc(ptr, layout);
        }

        // Verify the layout matches what we tracked
        if let Some(tracked) = tracked_layout {
            debug_assert_eq!(
                tracked.size(),
                layout.size(),
                "Layout size mismatch during deallocation"
            );
            debug_assert_eq!(
                tracked.align(),
                layout.align(),
                "Layout alignment mismatch during deallocation"
            );
        } else {
            log::warn!("Freeing untracked memory allocation at {:p}", ptr);
        }
    }
}

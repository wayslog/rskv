use std::alloc::{Layout, alloc, dealloc};
use std::ptr::null_mut;

/// Allocates memory with a specified alignment.
///
/// # Safety
///
/// The caller must ensure that the `layout` has a non-zero size.
pub unsafe fn aligned_alloc(layout: Layout) -> *mut u8 {
    if layout.size() == 0 {
        return null_mut();
    }
    unsafe { alloc(layout) }
}

/// Frees memory that was allocated with `aligned_alloc`.
///
/// # Safety
///
/// `ptr` must have been allocated using `aligned_alloc` with the same `layout`.
pub unsafe fn aligned_free(ptr: *mut u8, layout: Layout) {
    if !ptr.is_null() {
        unsafe {
            dealloc(ptr, layout);
        }
    }
}

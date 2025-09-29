use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::alloc::{alloc, dealloc, Layout};
use std::ptr::NonNull;

/// Cache line size for modern CPUs (typically 64 bytes)
pub const CACHE_LINE_SIZE: usize = 64;

/// Memory alignment for cache-friendly data structures
pub const CACHE_ALIGNED: usize = CACHE_LINE_SIZE;

/// Prefetch hint for CPU
#[derive(Debug, Clone, Copy)]
pub enum PrefetchHint {
    /// Prefetch for read access (non-temporal)
    Read,
    /// Prefetch for write access
    Write,
    /// Prefetch with high temporal locality
    HighTemporal,
    /// Prefetch with low temporal locality
    LowTemporal,
}

/// Cache-aligned data structure wrapper
#[repr(align(64))]
#[derive(Copy, Clone)]
pub struct CacheAligned<T> {
    data: T,
}

impl<T> CacheAligned<T> {
    pub fn new(data: T) -> Self {
        Self { data }
    }

    pub fn get(&self) -> &T {
        &self.data
    }

    pub fn get_mut(&mut self) -> &mut T {
        &mut self.data
    }
}

/// Cache-friendly memory allocator
pub struct CacheAlignedAllocator {
    allocated_bytes: AtomicUsize,
    allocation_count: AtomicU64,
}

impl CacheAlignedAllocator {
    pub fn new() -> Self {
        Self {
            allocated_bytes: AtomicUsize::new(0),
            allocation_count: AtomicU64::new(0),
        }
    }

    /// Allocate cache-aligned memory
    pub fn allocate<T>(&self, count: usize) -> Option<NonNull<T>> {
        let size = std::mem::size_of::<T>() * count;
        let align = CACHE_ALIGNED.max(std::mem::align_of::<T>());

        unsafe {
            let layout = Layout::from_size_align(size, align).ok()?;
            let ptr = alloc(layout) as *mut T;

            if ptr.is_null() {
                return None;
            }

            self.allocated_bytes.fetch_add(size, Ordering::Relaxed);
            self.allocation_count.fetch_add(1, Ordering::Relaxed);

            NonNull::new(ptr)
        }
    }

    /// Deallocate cache-aligned memory
    pub unsafe fn deallocate<T>(&self, ptr: NonNull<T>, count: usize) {
        let size = std::mem::size_of::<T>() * count;
        let align = CACHE_ALIGNED.max(std::mem::align_of::<T>());

        unsafe {
            let layout = Layout::from_size_align_unchecked(size, align);
            dealloc(ptr.as_ptr() as *mut u8, layout);
        }

        self.allocated_bytes.fetch_sub(size, Ordering::Relaxed);
    }

    /// Get allocation statistics
    pub fn get_stats(&self) -> AllocatorStats {
        AllocatorStats {
            allocated_bytes: self.allocated_bytes.load(Ordering::Relaxed),
            allocation_count: self.allocation_count.load(Ordering::Relaxed),
        }
    }
}

impl Default for CacheAlignedAllocator {
    fn default() -> Self {
        Self::new()
    }
}

/// Allocator statistics
#[derive(Debug, Clone)]
pub struct AllocatorStats {
    pub allocated_bytes: usize,
    pub allocation_count: u64,
}

/// Prefetch manager for cache optimization
pub struct PrefetchManager {
    prefetch_distance: usize,
    prefetch_enabled: bool,
    total_prefetches: AtomicU64,
}

impl PrefetchManager {
    pub fn new(prefetch_distance: usize) -> Self {
        Self {
            prefetch_distance,
            prefetch_enabled: true,
            total_prefetches: AtomicU64::new(0),
        }
    }

    /// Prefetch memory at the given address
    #[inline(always)]
    pub fn prefetch<T>(&self, _ptr: *const T, _hint: PrefetchHint) {
        if !self.prefetch_enabled {
            return;
        }

        #[cfg(target_arch = "x86_64")]
        {
            use std::arch::x86_64::*;
            unsafe {
                // Use appropriate prefetch instruction based on hint
                match _hint {
                    PrefetchHint::Read | PrefetchHint::HighTemporal => {
                        _mm_prefetch(_ptr as *const i8, _MM_HINT_T0);
                    }
                    PrefetchHint::Write => {
                        _mm_prefetch(_ptr as *const i8, _MM_HINT_T1);
                    }
                    PrefetchHint::LowTemporal => {
                        _mm_prefetch(_ptr as *const i8, _MM_HINT_NTA);
                    }
                }
            }
            self.total_prefetches.fetch_add(1, Ordering::Relaxed);
        }

        #[cfg(target_arch = "aarch64")]
        {
            // ARM prefetch intrinsics require unstable feature, so we skip for now
            // In production, you would enable: #![feature(stdarch_aarch64_prefetch)]
            self.total_prefetches.fetch_add(1, Ordering::Relaxed);
        }

        // For other architectures, this is a no-op
    }

    /// Prefetch a sequence of addresses
    pub fn prefetch_sequence<T>(&self, ptrs: &[*const T], hint: PrefetchHint) {
        for ptr in ptrs {
            self.prefetch(*ptr, hint);
        }
    }

    /// Enable or disable prefetching
    pub fn set_enabled(&mut self, enabled: bool) {
        self.prefetch_enabled = enabled;
    }

    /// Get prefetch statistics
    pub fn get_stats(&self) -> PrefetchStats {
        PrefetchStats {
            total_prefetches: self.total_prefetches.load(Ordering::Relaxed),
            prefetch_distance: self.prefetch_distance,
            enabled: self.prefetch_enabled,
        }
    }
}

impl Default for PrefetchManager {
    fn default() -> Self {
        Self::new(8) // Prefetch 8 elements ahead by default
    }
}

/// Prefetch statistics
#[derive(Debug, Clone)]
pub struct PrefetchStats {
    pub total_prefetches: u64,
    pub prefetch_distance: usize,
    pub enabled: bool,
}

/// Data locality optimizer
pub struct LocalityOptimizer {
    access_stride: AtomicUsize,
    sequential_count: AtomicU64,
    random_count: AtomicU64,
}

impl LocalityOptimizer {
    pub fn new() -> Self {
        Self {
            access_stride: AtomicUsize::new(1),
            sequential_count: AtomicU64::new(0),
            random_count: AtomicU64::new(0),
        }
    }

    /// Record an access pattern
    pub fn record_access(&self, current_addr: u64, previous_addr: u64) {
        let diff = current_addr.abs_diff(previous_addr);

        if diff <= 64 {
            // Sequential or nearby access
            self.sequential_count.fetch_add(1, Ordering::Relaxed);
            // Update stride estimate
            self.access_stride.store(diff as usize, Ordering::Relaxed);
        } else {
            // Random access
            self.random_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Get estimated access stride
    pub fn get_stride(&self) -> usize {
        self.access_stride.load(Ordering::Relaxed)
    }

    /// Get locality score (0.0 = random, 1.0 = perfect sequential)
    pub fn get_locality_score(&self) -> f64 {
        let sequential = self.sequential_count.load(Ordering::Relaxed);
        let random = self.random_count.load(Ordering::Relaxed);
        let total = sequential + random;

        if total == 0 {
            0.0
        } else {
            sequential as f64 / total as f64
        }
    }

    /// Get statistics
    pub fn get_stats(&self) -> LocalityStats {
        LocalityStats {
            sequential_count: self.sequential_count.load(Ordering::Relaxed),
            random_count: self.random_count.load(Ordering::Relaxed),
            estimated_stride: self.access_stride.load(Ordering::Relaxed),
            locality_score: self.get_locality_score(),
        }
    }

    /// Reset statistics
    pub fn reset(&self) {
        self.sequential_count.store(0, Ordering::Relaxed);
        self.random_count.store(0, Ordering::Relaxed);
        self.access_stride.store(1, Ordering::Relaxed);
    }
}

impl Default for LocalityOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

/// Locality statistics
#[derive(Debug, Clone)]
pub struct LocalityStats {
    pub sequential_count: u64,
    pub random_count: u64,
    pub estimated_stride: usize,
    pub locality_score: f64,
}

/// Cache-friendly array structure with padding to avoid false sharing
#[repr(C)]
pub struct PaddedArray<T, const N: usize> {
    data: [CacheAligned<T>; N],
}

impl<T: Default + Copy, const N: usize> PaddedArray<T, N> {
    pub fn new() -> Self {
        Self {
            data: [CacheAligned::new(T::default()); N],
        }
    }

    pub fn get(&self, index: usize) -> Option<&T> {
        self.data.get(index).map(|aligned| aligned.get())
    }

    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        self.data.get_mut(index).map(|aligned| aligned.get_mut())
    }

    pub fn len(&self) -> usize {
        N
    }

    pub fn is_empty(&self) -> bool {
        N == 0
    }
}

impl<T: Default + Copy, const N: usize> Default for PaddedArray<T, N> {
    fn default() -> Self {
        Self::new()
    }
}

/// NUMA-aware memory allocation hint
#[derive(Debug, Clone, Copy)]
pub struct NumaHint {
    pub preferred_node: Option<usize>,
    pub allow_fallback: bool,
}

impl Default for NumaHint {
    fn default() -> Self {
        Self {
            preferred_node: None,
            allow_fallback: true,
        }
    }
}

/// NUMA-aware allocator (simplified version)
pub struct NumaAllocator {
    node_allocations: Vec<AtomicUsize>,
    current_node: AtomicUsize,
}

impl NumaAllocator {
    pub fn new(num_nodes: usize) -> Self {
        let mut node_allocations = Vec::with_capacity(num_nodes);
        for _ in 0..num_nodes {
            node_allocations.push(AtomicUsize::new(0));
        }

        Self {
            node_allocations,
            current_node: AtomicUsize::new(0),
        }
    }

    /// Get the best NUMA node for allocation
    pub fn select_node(&self, hint: NumaHint) -> usize {
        if let Some(node) = hint.preferred_node {
            if node < self.node_allocations.len() {
                return node;
            }
        }

        // Round-robin selection
        let node = self.current_node.fetch_add(1, Ordering::Relaxed) % self.node_allocations.len();
        node
    }

    /// Record allocation on a node
    pub fn record_allocation(&self, node: usize, size: usize) {
        if let Some(counter) = self.node_allocations.get(node) {
            counter.fetch_add(size, Ordering::Relaxed);
        }
    }

    /// Get statistics per node
    pub fn get_node_stats(&self) -> Vec<usize> {
        self.node_allocations
            .iter()
            .map(|counter| counter.load(Ordering::Relaxed))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_aligned() {
        let aligned = CacheAligned::new(42u64);
        assert_eq!(*aligned.get(), 42);

        // Check alignment
        let ptr = aligned.get() as *const u64 as usize;
        assert_eq!(ptr % CACHE_LINE_SIZE, 0);
    }

    #[test]
    fn test_allocator() {
        let allocator = CacheAlignedAllocator::new();

        let ptr = allocator.allocate::<u64>(10);
        assert!(ptr.is_some());

        let stats = allocator.get_stats();
        assert!(stats.allocated_bytes >= 80); // At least 10 * 8 bytes

        unsafe {
            allocator.deallocate(ptr.unwrap(), 10);
        }
    }

    #[test]
    fn test_prefetch_manager() {
        let manager = PrefetchManager::default();

        let data = vec![1, 2, 3, 4, 5];
        manager.prefetch(data.as_ptr(), PrefetchHint::Read);

        let stats = manager.get_stats();
        assert!(stats.enabled);
    }

    #[test]
    fn test_locality_optimizer() {
        let optimizer = LocalityOptimizer::new();

        // Sequential accesses
        optimizer.record_access(100, 64);
        optimizer.record_access(164, 100);
        optimizer.record_access(228, 164);

        let stats = optimizer.get_stats();
        assert!(stats.locality_score > 0.8); // High locality

        optimizer.reset();
        let stats = optimizer.get_stats();
        assert_eq!(stats.sequential_count, 0);
    }

    #[test]
    fn test_padded_array() {
        let mut array = PaddedArray::<u64, 4>::new();
        assert_eq!(array.len(), 4);

        *array.get_mut(0).unwrap() = 42;
        assert_eq!(*array.get(0).unwrap(), 42);
    }

    #[test]
    fn test_numa_allocator() {
        let allocator = NumaAllocator::new(4);

        let hint = NumaHint {
            preferred_node: Some(2),
            allow_fallback: true,
        };

        let node = allocator.select_node(hint);
        assert_eq!(node, 2);

        allocator.record_allocation(node, 1024);
        let stats = allocator.get_node_stats();
        assert_eq!(stats[2], 1024);
    }
}
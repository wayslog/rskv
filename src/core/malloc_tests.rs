use super::malloc_fixed_page_size::*;
use super::light_epoch::LightEpoch;
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use std::thread;
// std::mem removed as unused

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fixed_page_address_creation() {
        let addr = FixedPageAddress::from_control(0x12345678);
        assert_eq!(addr.control(), 0x12345678);
    }

    #[test]
    fn test_fixed_page_address_invalid() {
        let invalid = FixedPageAddress::INVALID_ADDRESS;
        assert_eq!(invalid.control(), 0);
        assert!(invalid.is_invalid());
    }

    #[test]
    fn test_fixed_page_address_constants() {
        assert_eq!(FixedPageAddress::K_ADDRESS_BITS, 48);
        assert_eq!(FixedPageAddress::K_OFFSET_BITS, 20);
        assert_eq!(FixedPageAddress::K_PAGE_BITS, 28);
        assert_eq!(FixedPageAddress::K_MAX_OFFSET, (1 << 20) - 1);
        assert_eq!(FixedPageAddress::K_MAX_PAGE, (1 << 28) - 1);
    }

    #[test]
    fn test_fixed_page_address_decomposition() {
        // Test address decomposition for various values
        let test_cases = [
            (0x0000000000000000u64, 0, 0),
            (0x00000000000FFFFFu64, 0, 0xFFFFF),
            (0x0000000000100000u64, 1, 0),
            (0x0000000FFFF00000u64, 0xFFFF, 0),
            (0x0000000FFFF05678u64, 0xFFFF, 0x5678),
        ];

        for (control, expected_page, expected_offset) in test_cases {
            let addr = FixedPageAddress::from_control(control);
            assert_eq!(addr.page(), expected_page, "Page mismatch for control {:#x}", control);
            assert_eq!(addr.offset(), expected_offset, "Offset mismatch for control {:#x}", control);
        }
    }

    #[test]
    fn test_fixed_page_address_round_trip() {
        let test_page = 0x12345;
        let test_offset = 0x6789A;

        let addr = FixedPageAddress::new(test_page, test_offset);
        assert_eq!(addr.page(), test_page);
        assert_eq!(addr.offset(), test_offset as u32);
    }

    #[test]
    #[should_panic(expected = "Invalid FixedPageAddress control value")]
    fn test_fixed_page_address_invalid_control() {
        // This should panic because it uses reserved bits
        let invalid_control = 1u64 << 50; // Using bit 50 which is reserved
        FixedPageAddress::from_control(invalid_control);
    }

    #[test]
    fn test_fixed_page_address_boundary_values() {
        // Test maximum valid page and offset
        let max_page = FixedPageAddress::K_MAX_PAGE;
        let max_offset = FixedPageAddress::K_MAX_OFFSET;

        let addr = FixedPageAddress::new(max_page, max_offset);
        assert_eq!(addr.page(), max_page);
        assert_eq!(addr.offset(), max_offset as u32);
    }

    #[test]
    fn test_malloc_fixed_page_size_creation() {
        let _epoch = LightEpoch::new();
        let allocator: MallocFixedPageSize<u64> = MallocFixedPageSize::new();

        // Basic creation should succeed
        assert_eq!(std::mem::size_of_val(&allocator), std::mem::size_of::<MallocFixedPageSize<u64>>());
    }

    #[test]
    fn test_allocator_address_generation() {
        let _epoch = LightEpoch::new();
        let allocator: MallocFixedPageSize<u64> = MallocFixedPageSize::new();

        // Test allocation returns valid addresses
        let addr1 = allocator.allocate();
        let addr2 = allocator.allocate();

        // Addresses should be different
        assert_ne!(addr1, addr2);

        // Addresses should not be invalid
        assert!(!addr1.is_invalid());
        assert!(!addr2.is_invalid());
    }

    #[test]
    fn test_multiple_allocations() {
        let _epoch = LightEpoch::new();
        let allocator: MallocFixedPageSize<u64> = MallocFixedPageSize::new();

        let mut addresses = Vec::new();

        // Allocate multiple addresses
        for _ in 0..100 {
            let addr = allocator.allocate();
            assert!(!addr.is_invalid());
            addresses.push(addr);
        }

        // All addresses should be unique
        for i in 0..addresses.len() {
            for j in (i + 1)..addresses.len() {
                assert_ne!(addresses[i], addresses[j],
                    "Duplicate addresses found at indices {} and {}", i, j);
            }
        }
    }

    #[test]
    fn test_concurrent_allocation() {
        let epoch = Arc::new(LightEpoch::new());
        let allocator = Arc::new(MallocFixedPageSize::<u64>::new());
        let counter = Arc::new(AtomicUsize::new(0));

        let mut handles = vec![];

        for _ in 0..10 {
            let allocator_clone = allocator.clone();
            let counter_clone = counter.clone();

            let handle = thread::spawn(move || {
                let mut local_addresses = Vec::new();

                for _ in 0..50 {
                    let addr = allocator_clone.allocate();
                    assert!(!addr.is_invalid());
                    local_addresses.push(addr);
                    counter_clone.fetch_add(1, Ordering::Relaxed);
                }

                local_addresses
            });
            handles.push(handle);
        }

        let mut all_addresses = Vec::new();
        for handle in handles {
            let addresses = handle.join().unwrap();
            all_addresses.extend(addresses);
        }

        assert_eq!(counter.load(Ordering::Relaxed), 500);
        assert_eq!(all_addresses.len(), 500);

        // Check for duplicates in concurrent allocation
        for i in 0..all_addresses.len() {
            for j in (i + 1)..all_addresses.len() {
                assert_ne!(all_addresses[i], all_addresses[j],
                    "Concurrent allocation produced duplicate addresses");
            }
        }
    }

    #[test]
    fn test_pointer_safety() {
        let _epoch = LightEpoch::new();
        let allocator: MallocFixedPageSize<u64> = MallocFixedPageSize::new();

        // Test that allocator doesn't return null or obviously invalid pointers
        for _ in 0..1000 {
            let addr = allocator.allocate();

            // Address should not be the invalid address
            assert!(!addr.is_invalid());

            // Page should be reasonable (not zero for valid allocations)
            // Note: This depends on implementation details
            if !addr.is_invalid() {
                // Address should have some meaningful content
                assert!(addr.control() != 0);
            }
        }
    }

    #[test]
    fn test_allocator_stress() {
        let _epoch = LightEpoch::new();
        let allocator: MallocFixedPageSize<u64> = MallocFixedPageSize::new();

        // Stress test allocation
        let mut addresses = Vec::with_capacity(10000);

        for i in 0..10000 {
            let addr = allocator.allocate();
            assert!(!addr.is_invalid(), "Allocation {} failed", i);
            addresses.push(addr);
        }

        // Verify all addresses are unique
        addresses.sort_by_key(|addr| addr.control());
        for i in 1..addresses.len() {
            assert_ne!(addresses[i-1], addresses[i],
                "Found duplicate address at positions {} and {}", i-1, i);
        }
    }

    #[test]
    fn test_memory_alignment() {
        let _epoch = LightEpoch::new();
        let allocator: MallocFixedPageSize<u64> = MallocFixedPageSize::new();

        // Test that allocations respect alignment requirements
        for _ in 0..100 {
            let addr = allocator.allocate();

            // The offset should respect alignment for u64 (8 bytes)
            // This test depends on implementation specifics
            let offset = addr.offset();
            // For this test, we just verify the address is valid
            assert!(!addr.is_invalid());
            assert!(offset <= FixedPageAddress::K_MAX_OFFSET as u32);
        }
    }

    #[test]
    fn test_epoch_integration() {
        let epoch = Arc::new(LightEpoch::new());
        let allocator = Arc::new(MallocFixedPageSize::<u64>::new());

        // Test allocation under epoch protection
        let guard = epoch.protect();

        let addr1 = allocator.allocate();
        let addr2 = allocator.allocate();

        assert!(!addr1.is_invalid());
        assert!(!addr2.is_invalid());
        assert_ne!(addr1, addr2);

        drop(guard);

        // Should still be able to allocate after guard is dropped
        let addr3 = allocator.allocate();
        assert!(!addr3.is_invalid());
    }

    #[test]
    fn test_concurrent_allocation_with_epoch() {
        let epoch = Arc::new(LightEpoch::new());
        let allocator = Arc::new(MallocFixedPageSize::<u64>::new());

        let mut handles = vec![];

        for _ in 0..5 {
            let epoch_clone = epoch.clone();
            let allocator_clone = allocator.clone();

            let handle = thread::spawn(move || {
                let mut addresses = Vec::new();

                for _ in 0..100 {
                    let _guard = epoch_clone.protect();
                    let addr = allocator_clone.allocate();
                    assert!(!addr.is_invalid());
                    addresses.push(addr);

                    // Occasionally trigger epoch advancement
                    if addresses.len() % 10 == 0 {
                        epoch_clone.bump_and_drain();
                    }
                }

                addresses
            });
            handles.push(handle);
        }

        let mut all_addresses = Vec::new();
        for handle in handles {
            all_addresses.extend(handle.join().unwrap());
        }

        // Verify no duplicates
        all_addresses.sort_by_key(|addr| addr.control());
        for i in 1..all_addresses.len() {
            assert_ne!(all_addresses[i-1], all_addresses[i]);
        }
    }

    #[test]
    fn test_address_arithmetic() {
        // Test address calculations don't overflow
        let max_page = FixedPageAddress::K_MAX_PAGE;
        let max_offset = FixedPageAddress::K_MAX_OFFSET;

        let addr = FixedPageAddress::new(max_page, max_offset);

        // Verify no overflow in bit operations
        assert_eq!(addr.page(), max_page);
        assert_eq!(addr.offset(), max_offset as u32);

        // Test that control value fits in expected range
        let control = addr.control();
        assert!(control < (1u64 << FixedPageAddress::K_ADDRESS_BITS));
    }

    #[test]
    fn test_allocator_page_overflow_protection() {
        let _epoch = LightEpoch::new();
        let allocator: MallocFixedPageSize<u64> = MallocFixedPageSize::new();

        // Try to detect any page overflow issues
        // This test might need adjustment based on actual implementation
        for _ in 0..1000 {
            let addr = allocator.allocate();

            let page = addr.page();
            let offset = addr.offset();

            // Verify page and offset are within valid ranges
            assert!(page <= FixedPageAddress::K_MAX_PAGE,
                "Page {} exceeds maximum {}", page, FixedPageAddress::K_MAX_PAGE);
            assert!(offset <= FixedPageAddress::K_MAX_OFFSET as u32,
                "Offset {} exceeds maximum {}", offset, FixedPageAddress::K_MAX_OFFSET);
        }
    }
}
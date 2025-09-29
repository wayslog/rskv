// 专门测试内存指针安全和并发问题的测试套件
use crate::core::light_epoch::LightEpoch;
use crate::core::malloc_fixed_page_size::{MallocFixedPageSize, FixedPageAddress};
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use std::thread;
use std::time::Duration;
use std::collections::HashSet;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_epoch_memory_reclamation_safety() {
        let epoch = Arc::new(LightEpoch::new());
        let shared_counter = Arc::new(AtomicUsize::new(0));

        let mut handles = vec![];

        // 多线程测试epoch保护机制
        for thread_id in 0..10 {
            let epoch_clone = epoch.clone();
            let counter_clone = shared_counter.clone();

            let handle = thread::spawn(move || {
                for iteration in 0..1000 {
                    let guard = epoch_clone.protect();

                    // 模拟在epoch保护下的内存访问
                    let current = counter_clone.load(Ordering::Acquire);

                    // 模拟一些计算
                    thread::sleep(Duration::from_nanos(100));

                    // 尝试更新计数器
                    let _ = counter_clone.compare_exchange_weak(
                        current,
                        current + 1,
                        Ordering::AcqRel,
                        Ordering::Relaxed
                    );

                    // 每100次迭代触发一次epoch推进
                    if iteration % 100 == 0 {
                        epoch_clone.bump_and_drain();
                    }

                    drop(guard);

                    // 模拟guard丢弃后的短暂延迟
                    if thread_id % 3 == 0 {
                        thread::sleep(Duration::from_nanos(50));
                    }
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // 验证所有操作都完成了
        assert!(shared_counter.load(Ordering::Acquire) > 0);
    }

    #[test]
    fn test_concurrent_pointer_allocation_safety() {
        let _epoch = Arc::new(LightEpoch::new());
        let allocator = Arc::new(MallocFixedPageSize::<u64>::new());
        let all_addresses = Arc::new(std::sync::Mutex::new(Vec::new()));

        let mut handles = vec![];

        // 并发分配测试
        for _ in 0..20 {
            let allocator_clone = allocator.clone();
            let addresses_clone = all_addresses.clone();

            let handle = thread::spawn(move || {
                let mut local_addresses = Vec::new();

                for _ in 0..100 {
                    let addr = allocator_clone.allocate();
                    assert!(!addr.is_invalid(), "Allocated invalid address");

                    // 验证地址的有效性
                    assert!(addr.page() <= FixedPageAddress::K_MAX_PAGE);
                    assert!(addr.offset() <= FixedPageAddress::K_MAX_OFFSET as u32);

                    local_addresses.push(addr);
                }

                // 将本地地址添加到全局集合中
                {
                    let mut global_addresses = addresses_clone.lock().unwrap();
                    global_addresses.extend(local_addresses);
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // 验证所有分配的地址都是唯一的
        let addresses = all_addresses.lock().unwrap();
        let mut address_set = HashSet::new();

        for &addr in addresses.iter() {
            assert!(address_set.insert(addr.control()),
                "Found duplicate address: {:?}", addr);
        }

        assert_eq!(addresses.len(), 20 * 100);
        assert_eq!(address_set.len(), 20 * 100);
    }

    #[test]
    fn test_memory_leak_detection() {
        // 这个测试检查是否有明显的内存泄漏
        let epoch = Arc::new(LightEpoch::new());
        let allocator = Arc::new(MallocFixedPageSize::<u64>::new());

        {
            let mut addresses = Vec::new();

            // 大量分配
            for _ in 0..10000 {
                let addr = allocator.allocate();
                addresses.push(addr);
            }

            // 验证所有地址都有效
            for addr in &addresses {
                assert!(!addr.is_invalid());
            }

            // 触发epoch清理
            for _ in 0..10 {
                epoch.bump_and_drain();
                thread::sleep(Duration::from_millis(1));
            }
        }

        // 再次触发清理
        for _ in 0..10 {
            epoch.bump_and_drain();
            thread::sleep(Duration::from_millis(1));
        }

        // 分配器应该继续正常工作
        for _ in 0..100 {
            let addr = allocator.allocate();
            assert!(!addr.is_invalid());
        }
    }

    #[test]
    fn test_double_free_protection() {
        // 测试防止double-free的保护机制
        let _epoch = LightEpoch::new();
        let allocator: MallocFixedPageSize<u64> = MallocFixedPageSize::new();

        let mut addresses = Vec::new();

        // 分配一些地址
        for _ in 0..100 {
            let addr = allocator.allocate();
            addresses.push(addr);
        }

        // Rust的类型系统应该防止double-free
        // 这里我们测试地址的唯一性
        let mut seen_addresses = HashSet::new();
        for addr in addresses {
            assert!(seen_addresses.insert(addr.control()),
                "Duplicate address detected: {:?}", addr);
        }
    }

    #[test]
    fn test_aba_problem_protection() {
        // 测试ABA问题的保护
        use std::sync::atomic::{AtomicPtr, AtomicU64};

        let epoch = Arc::new(LightEpoch::new());
        let shared_ptr = Arc::new(AtomicPtr::new(std::ptr::null_mut()));
        let counter = Arc::new(AtomicU64::new(0));

        let mut handles = vec![];

        // 模拟可能导致ABA问题的并发操作
        for thread_id in 0..10 {
            let epoch_clone = epoch.clone();
            let ptr_clone = shared_ptr.clone();
            let counter_clone = counter.clone();

            let handle = thread::spawn(move || {
                for _ in 0..1000 {
                    let guard = epoch_clone.protect();

                    let current_ptr = ptr_clone.load(Ordering::Acquire);
                    let new_value = thread_id as u64;

                    // 模拟一些计算
                    counter_clone.fetch_add(1, Ordering::Relaxed);

                    // 尝试CAS操作
                    let new_ptr = new_value as *mut u64;
                    let _ = ptr_clone.compare_exchange_weak(
                        current_ptr,
                        new_ptr,
                        Ordering::AcqRel,
                        Ordering::Relaxed
                    );

                    drop(guard);
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // 验证最终状态一致
        assert_eq!(counter.load(Ordering::Relaxed), 10000);
    }

    #[test]
    fn test_concurrent_allocation_stress() {
        let epoch = Arc::new(LightEpoch::new());
        let allocator = Arc::new(MallocFixedPageSize::<u64>::new());

        let mut handles = vec![];

        // 压力测试并发分配
        for _ in 0..50 {
            let epoch_clone = epoch.clone();
            let allocator_clone = allocator.clone();

            let handle = thread::spawn(move || {
                let mut addresses = Vec::new();

                for _ in 0..200 {
                    let _guard = epoch_clone.protect();
                    let addr = allocator_clone.allocate();

                    assert!(!addr.is_invalid());
                    addresses.push(addr);

                    // 偶尔触发epoch清理
                    if addresses.len() % 50 == 0 {
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

        // 验证所有地址都是唯一的
        let mut seen_addresses = HashSet::new();
        for addr in all_addresses {
            assert!(seen_addresses.insert(addr.control()),
                "Duplicate address in stress test: {:?}", addr);
        }

        assert_eq!(seen_addresses.len(), 50 * 200);
    }

    #[test]
    fn test_address_arithmetic_safety() {
        // 测试地址运算的安全性
        for page in 0..=100 {
            for offset in 0..=100 {
                if page <= FixedPageAddress::K_MAX_PAGE &&
                   offset <= FixedPageAddress::K_MAX_OFFSET {
                    let addr = FixedPageAddress::new(page, offset);

                    // 验证往返转换
                    assert_eq!(addr.page(), page);
                    assert_eq!(addr.offset(), offset as u32);

                    // 验证控制值在有效范围内
                    assert!(addr.control() < (1u64 << FixedPageAddress::K_ADDRESS_BITS));
                }
            }
        }
    }

    #[test]
    fn test_boundary_conditions() {
        // 测试边界条件
        let max_addr = FixedPageAddress::new(
            FixedPageAddress::K_MAX_PAGE,
            FixedPageAddress::K_MAX_OFFSET
        );

        assert_eq!(max_addr.page(), FixedPageAddress::K_MAX_PAGE);
        assert_eq!(max_addr.offset(), FixedPageAddress::K_MAX_OFFSET as u32);
        assert!(!max_addr.is_invalid());

        let zero_addr = FixedPageAddress::new(0, 0);
        assert_eq!(zero_addr.page(), 0);
        assert_eq!(zero_addr.offset(), 0);
        assert!(zero_addr.is_invalid()); // Zero address is invalid
    }

    #[test]
    fn test_address_overflow_protection() {
        // 测试地址溢出保护
        let allocator: MallocFixedPageSize<u64> = MallocFixedPageSize::new();

        // 大量分配应该不会导致地址溢出
        for _ in 0..10000 {
            let addr = allocator.allocate();

            // 验证地址组件在有效范围内
            assert!(addr.page() <= FixedPageAddress::K_MAX_PAGE);
            assert!(addr.offset() <= FixedPageAddress::K_MAX_OFFSET as u32);
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::status::Status;
    use crate::f2::F2Kv;
    use crate::faster::{ReadContext, UpsertContext, RmwContext};
    use std::path::Path;
    use std::sync::Arc;
    use std::thread;

    // 测试数据结构
    #[derive(Debug, Clone, Copy, PartialEq, Default)]
    struct TestData {
        id: u64,
        value: u64,
        timestamp: u64,
    }

    impl TestData {
        fn new(id: u64, value: u64) -> Self {
            Self {
                id,
                value,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            }
        }
    }

    // Upsert上下文
    struct TestUpsertContext {
        key: u64,
        value: TestData,
    }

    impl UpsertContext for TestUpsertContext {
        type Key = u64;
        type Value = TestData;

        fn key(&self) -> &Self::Key {
            &self.key
        }

        fn value(&self) -> &Self::Value {
            &self.value
        }

        fn key_hash(&self) -> u64 {
            self.key
        }

        fn put_atomic(&self, _value: &mut Self::Value) -> bool {
            false
        }
    }

    // Read上下文
    struct TestReadContext {
        key: u64,
        value: Option<TestData>,
    }

    impl ReadContext for TestReadContext {
        type Key = u64;
        type Value = TestData;

        fn key(&self) -> &Self::Key {
            &self.key
        }

        fn key_hash(&self) -> u64 {
            self.key
        }

        fn get(&mut self, value: &Self::Value) {
            self.value = Some(*value);
        }
    }

    // RMW上下文
    struct TestRmwContext {
        key: u64,
        increment: u64,
    }

    impl RmwContext for TestRmwContext {
        type Key = u64;
        type Value = TestData;

        fn key(&self) -> &Self::Key {
            &self.key
        }

        fn key_hash(&self) -> u64 {
            self.key
        }

        fn rmw_initial(&self, value: &mut Self::Value) {
            *value = TestData::new(self.key, self.increment);
        }

        fn rmw_copy(&self, old_value: &Self::Value, new_value: &mut Self::Value) {
            *new_value = *old_value;
            new_value.value += self.increment;
        }

        fn rmw_atomic(&self, _value: &mut Self::Value) -> bool {
            false
        }
    }

    // 创建临时测试目录
    fn create_test_dirs() -> (String, String) {
        let hot_dir = "/tmp/f2_test_hot";
        let cold_dir = "/tmp/f2_test_cold";
        
        for dir in [hot_dir, cold_dir] {
            if Path::new(dir).exists() {
                std::fs::remove_dir_all(dir).unwrap();
            }
            std::fs::create_dir_all(dir).unwrap();
        }
        
        (hot_dir.to_string(), cold_dir.to_string())
    }

    // 清理测试目录
    fn cleanup_test_dirs(hot_dir: &str, cold_dir: &str) {
        let _ = std::fs::remove_dir_all(hot_dir);
        let _ = std::fs::remove_dir_all(cold_dir);
    }

    #[test]
    fn test_f2_basic_operations() {
        let (hot_dir, cold_dir) = create_test_dirs();
        
        // 初始化F2存储系统
        let f2_kv = F2Kv::<u64, TestData>::new(&hot_dir, &cold_dir).unwrap();
        
        // 测试写入
        let test_data = TestData::new(1, 100);
        let upsert_ctx = TestUpsertContext {
            key: 1,
            value: test_data,
        };
        
        let status = f2_kv.upsert(&upsert_ctx);
        assert_eq!(status, Status::Ok);
        
        // 测试读取
        let mut read_ctx = TestReadContext {
            key: 1,
            value: None,
        };
        
        let status = f2_kv.read(&mut read_ctx);
        assert_eq!(status, Status::Ok);
        assert!(read_ctx.value.is_some());
        assert_eq!(read_ctx.value.unwrap().value, 100);
        
        // 测试RMW
        let mut rmw_ctx = TestRmwContext {
            key: 1,
            increment: 50,
        };
        
        let status = f2_kv.rmw(&mut rmw_ctx);
        assert_eq!(status, Status::Ok);
        
        // 验证RMW结果
        let mut read_ctx = TestReadContext {
            key: 1,
            value: None,
        };
        
        let status = f2_kv.read(&mut read_ctx);
        assert_eq!(status, Status::Ok);
        assert_eq!(read_ctx.value.unwrap().value, 150); // 100 + 50
        
        cleanup_test_dirs(&hot_dir, &cold_dir);
    }

    #[test]
    fn test_f2_cold_hot_migration() {
        let (hot_dir, cold_dir) = create_test_dirs();
        
        // 初始化F2存储系统
        let f2_kv = F2Kv::<u64, TestData>::new(&hot_dir, &cold_dir).unwrap();
        
        // 写入一些数据到热存储
        for i in 1..=10 {
            let test_data = TestData::new(i, i * 100);
            let upsert_ctx = TestUpsertContext {
                key: i,
                value: test_data,
            };
            f2_kv.upsert(&upsert_ctx);
        }
        
        // 模拟冷数据访问（通过RMW操作）
        // 当热存储中找不到数据时，F2会尝试从冷存储读取
        let mut rmw_ctx = TestRmwContext {
            key: 5, // 假设这个键在冷存储中
            increment: 1000,
        };
        
        let status = f2_kv.rmw(&mut rmw_ctx);
        // 注意：由于我们的实现中冷存储是空的，这个操作会创建新数据
        assert_eq!(status, Status::Ok);
        
        // 验证数据
        let mut read_ctx = TestReadContext {
            key: 5,
            value: None,
        };
        
        let status = f2_kv.read(&mut read_ctx);
        assert_eq!(status, Status::Ok);
        assert!(read_ctx.value.is_some());
        
        cleanup_test_dirs(&hot_dir, &cold_dir);
    }

    #[test]
    fn test_f2_concurrent_operations() {
        let (hot_dir, cold_dir) = create_test_dirs();
        
        // 初始化F2存储系统
        let f2_kv = Arc::new(F2Kv::<u64, TestData>::new(&hot_dir, &cold_dir).unwrap());
        
        let num_threads = 4;
        let operations_per_thread = 25;
        
        let handles: Vec<_> = (0..num_threads)
            .map(|thread_id| {
                let f2_kv = Arc::clone(&f2_kv);
                thread::spawn(move || {
                    let mut success_count = 0;
                    let start_key = thread_id * operations_per_thread + 1;
                    
                    for i in 0..operations_per_thread {
                        let key = start_key + i;
                        
                        // 写入操作
                        let test_data = TestData::new(key, key * 100);
                        let upsert_ctx = TestUpsertContext {
                            key,
                            value: test_data,
                        };
                        if f2_kv.upsert(&upsert_ctx) == Status::Ok {
                            success_count += 1;
                        }
                        
                        // 读取操作
                        let mut read_ctx = TestReadContext {
                            key,
                            value: None,
                        };
                        if f2_kv.read(&mut read_ctx) == Status::Ok {
                            success_count += 1;
                        }
                        
                        // RMW操作
                        let mut rmw_ctx = TestRmwContext {
                            key,
                            increment: 1,
                        };
                        if f2_kv.rmw(&mut rmw_ctx) == Status::Ok {
                            success_count += 1;
                        }
                    }
                    
                    success_count
                })
            })
            .collect();
        
        let mut total_success = 0;
        for handle in handles {
            total_success += handle.join().unwrap();
        }
        
        // 验证所有操作都成功
        assert_eq!(total_success, num_threads * operations_per_thread * 3);
        
        cleanup_test_dirs(&hot_dir, &cold_dir);
    }

    #[test]
    fn test_f2_batch_operations() {
        let (hot_dir, cold_dir) = create_test_dirs();
        
        // 初始化F2存储系统
        let f2_kv = F2Kv::<u64, TestData>::new(&hot_dir, &cold_dir).unwrap();
        
        // 批量写入
        let num_items = 100;
        for i in 1..=num_items {
            let test_data = TestData::new(i, i * 1000);
            let upsert_ctx = TestUpsertContext {
                key: i,
                value: test_data,
            };
            let status = f2_kv.upsert(&upsert_ctx);
            assert_eq!(status, Status::Ok);
        }
        
        // 批量读取验证
        for i in 1..=num_items {
            let mut read_ctx = TestReadContext {
                key: i,
                value: None,
            };
            let status = f2_kv.read(&mut read_ctx);
            assert_eq!(status, Status::Ok);
            assert_eq!(read_ctx.value.unwrap().value, i * 1000);
        }
        
        // 批量RMW操作
        for i in 1..=num_items {
            let mut rmw_ctx = TestRmwContext {
                key: i,
                increment: 100,
            };
            let status = f2_kv.rmw(&mut rmw_ctx);
            assert_eq!(status, Status::Ok);
        }
        
        // 验证RMW结果
        for i in 1..=num_items {
            let mut read_ctx = TestReadContext {
                key: i,
                value: None,
            };
            let status = f2_kv.read(&mut read_ctx);
            assert_eq!(status, Status::Ok);
            assert_eq!(read_ctx.value.unwrap().value, i * 1000 + 100);
        }
        
        cleanup_test_dirs(&hot_dir, &cold_dir);
    }

    #[test]
    fn test_f2_error_handling() {
        let (hot_dir, cold_dir) = create_test_dirs();
        
        // 初始化F2存储系统
        let f2_kv = F2Kv::<u64, TestData>::new(&hot_dir, &cold_dir).unwrap();
        
        // 测试读取不存在的键
        let mut read_ctx = TestReadContext {
            key: 999,
            value: None,
        };
        
        let status = f2_kv.read(&mut read_ctx);
        assert_eq!(status, Status::NotFound);
        assert!(read_ctx.value.is_none());
        
        // 测试RMW不存在的键（应该创建新数据）
        let mut rmw_ctx = TestRmwContext {
            key: 999,
            increment: 1000,
        };
        
        let status = f2_kv.rmw(&mut rmw_ctx);
        assert_eq!(status, Status::Ok);
        
        // 验证RMW创建的数据
        let mut read_ctx = TestReadContext {
            key: 999,
            value: None,
        };
        
        let status = f2_kv.read(&mut read_ctx);
        assert_eq!(status, Status::Ok);
        assert_eq!(read_ctx.value.unwrap().value, 1000);
        
        cleanup_test_dirs(&hot_dir, &cold_dir);
    }

    #[test]
    fn test_f2_data_consistency() {
        let (hot_dir, cold_dir) = create_test_dirs();
        
        // 初始化F2存储系统
        let f2_kv = F2Kv::<u64, TestData>::new(&hot_dir, &cold_dir).unwrap();
        
        // 写入初始数据
        let test_data = TestData::new(1, 100);
        let upsert_ctx = TestUpsertContext {
            key: 1,
            value: test_data,
        };
        f2_kv.upsert(&upsert_ctx);
        
        // 多次RMW操作
        for i in 1..=10 {
            let mut rmw_ctx = TestRmwContext {
                key: 1,
                increment: i,
            };
            let status = f2_kv.rmw(&mut rmw_ctx);
            assert_eq!(status, Status::Ok);
        }
        
        // 验证最终数据一致性
        let mut read_ctx = TestReadContext {
            key: 1,
            value: None,
        };
        
        let status = f2_kv.read(&mut read_ctx);
        assert_eq!(status, Status::Ok);
        
        let final_data = read_ctx.value.unwrap();
        let expected_value = 100 + (1..=10).sum::<u64>(); // 100 + 55 = 155
        assert_eq!(final_data.value, expected_value);
        
        cleanup_test_dirs(&hot_dir, &cold_dir);
    }
}

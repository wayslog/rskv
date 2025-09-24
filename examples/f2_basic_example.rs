use rskv::core::status::Status;
use rskv::f2::F2Kv;
use rskv::faster::{ReadContext, RmwContext, UpsertContext};
use std::path::Path;

// 简单的测试数据结构
#[derive(Debug, Clone, Copy, PartialEq, Default)]
struct TestData {
    id: u64,
    value: u64,
    timestamp: u64,
}

// Upsert上下文实现
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
        false // 总是使用RCU路径
    }
}

// Read上下文实现
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

// RMW上下文实现
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
        *value = TestData {
            id: self.key,
            value: self.increment,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };
    }

    fn rmw_copy(&self, old_value: &Self::Value, new_value: &mut Self::Value) {
        *new_value = *old_value;
        new_value.value += self.increment;
        new_value.timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }

    fn rmw_atomic(&self, _value: &mut Self::Value) -> bool {
        false // 总是使用RCU路径
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("F2 冷热存储系统基础示例");
    println!("================================");

    // 创建临时目录
    let hot_dir = "/tmp/f2_hot_test";
    let cold_dir = "/tmp/f2_cold_test";

    for dir in [hot_dir, cold_dir] {
        if Path::new(dir).exists() {
            std::fs::remove_dir_all(dir)?;
        }
        std::fs::create_dir_all(dir)?;
    }

    // 初始化F2存储系统
    println!("初始化F2存储系统...");
    let f2_kv = F2Kv::<u64, TestData>::new(hot_dir, cold_dir)?;
    println!("F2存储系统初始化成功");
    println!("   - 热存储路径: {}", hot_dir);
    println!("   - 冷存储路径: {}", cold_dir);

    // 测试1: 基本写入操作（写入热存储）
    println!("\n 测试1: 热存储写入操作");
    let test_data = TestData {
        id: 1,
        value: 100,
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    };

    let upsert_ctx = TestUpsertContext {
        key: 1,
        value: test_data,
    };

    let status = f2_kv.upsert(&upsert_ctx);
    match status {
        Status::Ok => println!("   热存储写入成功: key=1, value={}", test_data.value),
        _ => println!("   热存储写入失败: {:?}", status),
    }

    // 测试2: 热存储读取操作
    println!("\n 测试2: 热存储读取操作");
    let mut read_ctx = TestReadContext {
        key: 1,
        value: None,
    };

    let status = f2_kv.read(&mut read_ctx);
    match status {
        Status::Ok => {
            if let Some(data) = read_ctx.value {
                println!("   热存储读取成功: {:?}", data);
            } else {
                println!("   读取成功但无数据");
            }
        }
        Status::NotFound => println!("   键未找到"),
        _ => println!("   读取失败: {:?}", status),
    }

    // 测试3: RMW操作（可能触发冷热数据迁移）
    println!("\n 测试3: RMW操作（冷热数据迁移测试）");
    let mut rmw_ctx = TestRmwContext {
        key: 1,
        increment: 50,
    };

    let status = f2_kv.rmw(&mut rmw_ctx);
    match status {
        Status::Ok => println!("   RMW操作成功，值增加50"),
        _ => println!("   RMW操作失败: {:?}", status),
    }

    // 测试4: 验证RMW后的数据
    println!("\n 测试4: 验证RMW后的数据");
    let mut read_ctx = TestReadContext {
        key: 1,
        value: None,
    };

    let status = f2_kv.read(&mut read_ctx);
    match status {
        Status::Ok => {
            if let Some(data) = read_ctx.value {
                println!("   RMW后数据: {:?}", data);
                println!("   预期值: 150, 实际值: {}", data.value);
            } else {
                println!("   读取成功但无数据");
            }
        }
        Status::NotFound => println!("   键未找到"),
        _ => println!("   读取失败: {:?}", status),
    }

    // 测试5: 写入冷存储数据（通过直接操作冷存储）
    println!("\n 测试5: 冷存储数据写入");
    // 注意：这里我们无法直接访问冷存储，但可以通过F2的机制来测试
    // 在实际场景中，冷存储数据通常是通过数据迁移或老化策略产生的

    // 测试6: 批量操作测试
    println!("\n 测试6: 批量操作测试");
    let mut success_count = 0;
    for i in 2..=10 {
        let test_data = TestData {
            id: i,
            value: i * 10,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        let upsert_ctx = TestUpsertContext {
            key: i,
            value: test_data,
        };

        let status = f2_kv.upsert(&upsert_ctx);
        if status == Status::Ok {
            success_count += 1;
        }
    }
    println!("   批量写入完成: {}/9 成功", success_count);

    // 测试7: 混合读写操作
    println!("\n 测试7: 混合读写操作");
    for i in 2..=5 {
        // 读取
        let mut read_ctx = TestReadContext {
            key: i,
            value: None,
        };
        let read_status = f2_kv.read(&mut read_ctx);

        // RMW
        let mut rmw_ctx = TestRmwContext {
            key: i,
            increment: 1,
        };
        let rmw_status = f2_kv.rmw(&mut rmw_ctx);

        println!("  Key {}: 读取={:?}, RMW={:?}", i, read_status, rmw_status);
    }

    // 清理
    for dir in [hot_dir, cold_dir] {
        std::fs::remove_dir_all(dir)?;
    }
    println!("\n 清理完成");
    println!("\n F2基础示例测试完成！");

    Ok(())
}

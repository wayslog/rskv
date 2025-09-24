use rskv::core::status::Status;
use rskv::f2::F2Kv;
use rskv::faster::{ReadContext, RmwContext, UpsertContext};
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

// 测试数据结构
#[derive(Debug, Clone, Copy, PartialEq, Default)]
struct MigrationTestData {
    id: u64,
    value: u64,
    access_count: u64,
    last_access: u64,
    is_hot: bool,
}

impl MigrationTestData {
    fn new(id: u64, value: u64) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        Self {
            id,
            value,
            access_count: 1,
            last_access: now,
            is_hot: true,
        }
    }

    fn access(&mut self) {
        self.access_count += 1;
        self.last_access = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }
}

// Upsert上下文
struct MigrationUpsertContext {
    key: u64,
    value: MigrationTestData,
}

impl UpsertContext for MigrationUpsertContext {
    type Key = u64;
    type Value = MigrationTestData;

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
struct MigrationReadContext {
    key: u64,
    value: Option<MigrationTestData>,
}

impl ReadContext for MigrationReadContext {
    type Key = u64;
    type Value = MigrationTestData;

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
struct MigrationRmwContext {
    key: u64,
    increment: u64,
}

impl RmwContext for MigrationRmwContext {
    type Key = u64;
    type Value = MigrationTestData;

    fn key(&self) -> &Self::Key {
        &self.key
    }

    fn key_hash(&self) -> u64 {
        self.key
    }

    fn rmw_initial(&self, value: &mut Self::Value) {
        *value = MigrationTestData::new(self.key, self.increment);
    }

    fn rmw_copy(&self, old_value: &Self::Value, new_value: &mut Self::Value) {
        *new_value = *old_value;
        new_value.access();
        new_value.value += self.increment;
    }

    fn rmw_atomic(&self, _value: &mut Self::Value) -> bool {
        false
    }
}

// 模拟数据访问模式
fn simulate_access_patterns(f2_kv: &F2Kv<u64, MigrationTestData>, num_keys: u64) {
    println!(" 模拟数据访问模式...");

    // 创建初始数据
    for i in 1..=num_keys {
        let data = MigrationTestData::new(i, i * 100);
        let upsert_ctx = MigrationUpsertContext {
            key: i,
            value: data,
        };
        f2_kv.upsert(&upsert_ctx);
    }
    println!("   创建了 {} 个初始数据项", num_keys);

    // 模拟热点数据访问（频繁访问前20%的数据）
    let hot_keys = (1..=num_keys / 5).collect::<Vec<_>>();
    println!(" 模拟热点数据访问（前20%的数据）...");

    for _ in 0..50 {
        for &key in &hot_keys {
            // 读取操作
            let mut read_ctx = MigrationReadContext { key, value: None };
            let _ = f2_kv.read(&mut read_ctx);

            // RMW操作
            let mut rmw_ctx = MigrationRmwContext { key, increment: 1 };
            let _ = f2_kv.rmw(&mut rmw_ctx);
        }
    }
    println!("   完成热点数据访问模拟");

    // 模拟冷数据访问（偶尔访问后80%的数据）
    let cold_keys = (num_keys / 5 + 1..=num_keys).collect::<Vec<_>>();
    println!(" 模拟冷数据访问（后80%的数据）...");

    for _ in 0..10 {
        for &key in &cold_keys {
            let mut read_ctx = MigrationReadContext { key, value: None };
            let _ = f2_kv.read(&mut read_ctx);
        }
    }
    println!("   完成冷数据访问模拟");
}

// 测试冷热数据迁移
fn test_cold_hot_migration(f2_kv: &F2Kv<u64, MigrationTestData>) {
    println!("\n 测试冷热数据迁移过程");

    // 测试1: 从冷存储读取数据
    println!("   测试从冷存储读取数据...");
    let mut read_ctx = MigrationReadContext {
        key: 100, // 假设这是一个冷数据键
        value: None,
    };

    let status = f2_kv.read(&mut read_ctx);
    match status {
        Status::Ok => {
            if let Some(data) = read_ctx.value {
                println!("     从冷存储成功读取: {:?}", data);
            }
        }
        Status::NotFound => println!("     键100未找到（可能尚未创建）"),
        _ => println!("     读取失败: {:?}", status),
    }

    // 测试2: RMW操作触发冷热数据迁移
    println!("   测试RMW操作触发冷热数据迁移...");
    let mut rmw_ctx = MigrationRmwContext {
        key: 100,
        increment: 1000,
    };

    let status = f2_kv.rmw(&mut rmw_ctx);
    match status {
        Status::Ok => {
            println!("     RMW操作成功，可能触发了冷热数据迁移");

            // 验证迁移后的数据
            let mut read_ctx = MigrationReadContext {
                key: 100,
                value: None,
            };
            let read_status = f2_kv.read(&mut read_ctx);
            if read_status == Status::Ok {
                if let Some(data) = read_ctx.value {
                    println!("     迁移后数据: {:?}", data);
                }
            }
        }
        _ => println!("     RMW操作失败: {:?}", status),
    }
}

// 并发访问测试
fn test_concurrent_access(f2_kv: &F2Kv<u64, MigrationTestData>) {
    println!("\n 测试并发访问和冷热数据迁移");

    // 由于生命周期限制，我们使用顺序操作来模拟并发场景
    let num_operations = 100;
    let mut success_count = 0;
    let mut total_operations = 0;

    for i in 1..=num_operations {
        let key = i;

        // 创建数据
        let data = MigrationTestData::new(key, key * 10);
        let upsert_ctx = MigrationUpsertContext { key, value: data };
        if f2_kv.upsert(&upsert_ctx) == Status::Ok {
            success_count += 1;
        }
        total_operations += 1;

        // 读取数据
        let mut read_ctx = MigrationReadContext { key, value: None };
        if f2_kv.read(&mut read_ctx) == Status::Ok {
            success_count += 1;
        }
        total_operations += 1;

        // RMW操作
        let mut rmw_ctx = MigrationRmwContext { key, increment: 1 };
        if f2_kv.rmw(&mut rmw_ctx) == Status::Ok {
            success_count += 1;
        }
        total_operations += 1;
    }

    println!(
        "   并发模拟测试完成: {}/{} 总操作成功",
        success_count, total_operations
    );
}

// 性能测试
fn performance_test(f2_kv: &F2Kv<u64, MigrationTestData>) {
    println!("\n 性能测试");

    let num_operations = 1000;
    let start_time = Instant::now();

    // 写入测试
    let write_start = Instant::now();
    for i in 1..=num_operations {
        let data = MigrationTestData::new(i, i * 100);
        let upsert_ctx = MigrationUpsertContext {
            key: i,
            value: data,
        };
        f2_kv.upsert(&upsert_ctx);
    }
    let write_duration = write_start.elapsed();

    // 读取测试
    let read_start = Instant::now();
    for i in 1..=num_operations {
        let mut read_ctx = MigrationReadContext {
            key: i,
            value: None,
        };
        f2_kv.read(&mut read_ctx);
    }
    let read_duration = read_start.elapsed();

    // RMW测试
    let rmw_start = Instant::now();
    for i in 1..=num_operations {
        let mut rmw_ctx = MigrationRmwContext {
            key: i,
            increment: 1,
        };
        f2_kv.rmw(&mut rmw_ctx);
    }
    let rmw_duration = rmw_start.elapsed();

    let total_duration = start_time.elapsed();

    println!(
        "   写入 {} 次操作耗时: {:?}",
        num_operations, write_duration
    );
    println!("   读取 {} 次操作耗时: {:?}", num_operations, read_duration);
    println!("   RMW {} 次操作耗时: {:?}", num_operations, rmw_duration);
    println!("   总耗时: {:?}", total_duration);
    println!(
        "   平均每操作耗时: {:?}",
        total_duration / (num_operations as u32 * 3)
    );
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!(" F2 冷热数据迁移测试");
    println!("================================");

    // 创建临时目录
    let hot_dir = "/tmp/f2_migration_hot";
    let cold_dir = "/tmp/f2_migration_cold";

    for dir in [hot_dir, cold_dir] {
        if Path::new(dir).exists() {
            std::fs::remove_dir_all(dir)?;
        }
        std::fs::create_dir_all(dir)?;
    }

    // 初始化F2存储系统
    println!(" 初始化F2存储系统...");
    let f2_kv = F2Kv::<u64, MigrationTestData>::new(hot_dir, cold_dir)?;
    println!(" F2存储系统初始化成功");

    // 模拟数据访问模式
    simulate_access_patterns(&f2_kv, 100);

    // 测试冷热数据迁移
    test_cold_hot_migration(&f2_kv);

    // 性能测试
    performance_test(&f2_kv);

    // 并发访问测试
    test_concurrent_access(&f2_kv);

    // 清理
    for dir in [hot_dir, cold_dir] {
        std::fs::remove_dir_all(dir)?;
    }
    println!("\n 清理完成");
    println!("\n F2冷热数据迁移测试完成！");

    Ok(())
}

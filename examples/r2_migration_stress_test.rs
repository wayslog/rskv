use rskv::core::status::Status;
use rskv::r2::R2Kv;
use rskv::rskv_core::{ReadContext, RmwContext, UpsertContext};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

// 迁移测试数据结构
#[derive(Debug, Clone, Copy, PartialEq, Default)]
struct MigrationData {
    id: u64,
    value: u64,
    access_count: u64,
    last_access: u64,
    migration_count: u64,
}

impl MigrationData {
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
            migration_count: 0,
        }
    }

    fn access(&mut self) {
        self.access_count += 1;
        self.last_access = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }

    fn migrate(&mut self) {
        self.migration_count += 1;
        self.access();
    }
}

// Upsert上下文
struct MigrationUpsertContext {
    key: u64,
    value: MigrationData,
}

impl UpsertContext for MigrationUpsertContext {
    type Key = u64;
    type Value = MigrationData;

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
    value: Option<MigrationData>,
}

impl ReadContext for MigrationReadContext {
    type Key = u64;
    type Value = MigrationData;

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
    type Value = MigrationData;

    fn key(&self) -> &Self::Key {
        &self.key
    }

    fn key_hash(&self) -> u64 {
        self.key
    }

    fn rmw_initial(&self, value: &mut Self::Value) {
        *value = MigrationData::new(self.key, self.increment);
    }

    fn rmw_copy(&self, old_value: &Self::Value, new_value: &mut Self::Value) {
        *new_value = *old_value;
        new_value.access();
        new_value.value += self.increment;
        // 模拟迁移检测
        if new_value.access_count % 10 == 0 {
            new_value.migrate();
        }
    }

    fn rmw_atomic(&self, _value: &mut Self::Value) -> bool {
        false
    }
}

// 模拟冷热数据分离
fn simulate_cold_hot_separation(r2_kv: &R2Kv<u64, MigrationData>) {
    println!(" 模拟冷热数据分离过程");

    // 阶段1: 创建大量初始数据
    println!("  📝 阶段1: 创建大量初始数据");
    let num_initial_data = 1000;
    for i in 1..=num_initial_data {
        let data = MigrationData::new(i, i * 100);
        let upsert_ctx = MigrationUpsertContext {
            key: i,
            value: data,
        };
        r2_kv.upsert(&upsert_ctx);
    }
    println!("     创建了 {} 个初始数据项", num_initial_data);

    // 阶段2: 模拟热点数据访问模式
    println!("   阶段2: 模拟热点数据访问模式");
    let hot_data_ratio = 0.2; // 20%的数据是热点
    let hot_data_count = (num_initial_data as f64 * hot_data_ratio) as u64;

    // 频繁访问热点数据
    for _ in 0..50 {
        for i in 1..=hot_data_count {
            // 读取操作
            let mut read_ctx = MigrationReadContext {
                key: i,
                value: None,
            };
            r2_kv.read(&mut read_ctx);

            // RMW操作
            let mut rmw_ctx = MigrationRmwContext {
                key: i,
                increment: 1,
            };
            r2_kv.rmw(&mut rmw_ctx);
        }
    }
    println!("     完成热点数据访问模拟 ({} 个热点数据)", hot_data_count);

    // 阶段3: 模拟冷数据访问
    println!("   阶段3: 模拟冷数据访问");
    let cold_data_start = hot_data_count + 1;
    let cold_data_count = num_initial_data - hot_data_count;

    // 偶尔访问冷数据
    for _ in 0..5 {
        for i in cold_data_start..=num_initial_data {
            let mut read_ctx = MigrationReadContext {
                key: i,
                value: None,
            };
            r2_kv.read(&mut read_ctx);
        }
    }
    println!("     完成冷数据访问模拟 ({} 个冷数据)", cold_data_count);
}

// 测试冷热数据迁移触发
fn test_migration_triggers(r2_kv: &R2Kv<u64, MigrationData>) {
    println!("\n 测试冷热数据迁移触发机制");

    // 测试1: 冷数据被访问时触发迁移
    println!("   测试1: 冷数据访问触发迁移");
    let cold_key = 500; // 假设这是一个冷数据键

    // 先读取冷数据
    let mut read_ctx = MigrationReadContext {
        key: cold_key,
        value: None,
    };
    let status = r2_kv.read(&mut read_ctx);
    match status {
        Status::Ok => {
            if let Some(data) = read_ctx.value {
                println!(
                    "     冷数据读取成功: 访问次数={}, 迁移次数={}",
                    data.access_count, data.migration_count
                );
            }
        }
        Status::NotFound => println!("     冷数据键 {} 未找到", cold_key),
        _ => println!("     冷数据读取失败: {:?}", status),
    }

    // 对冷数据执行RMW操作（可能触发迁移）
    let mut rmw_ctx = MigrationRmwContext {
        key: cold_key,
        increment: 1000,
    };
    let status = r2_kv.rmw(&mut rmw_ctx);
    match status {
        Status::Ok => {
            println!("     冷数据RMW操作成功，可能触发迁移");

            // 验证迁移后的数据
            let mut read_ctx = MigrationReadContext {
                key: cold_key,
                value: None,
            };
            let status = r2_kv.read(&mut read_ctx);
            if status == Status::Ok
                && let Some(data) = read_ctx.value
            {
                println!(
                    "     迁移后数据: 值={}, 访问次数={}, 迁移次数={}",
                    data.value, data.access_count, data.migration_count
                );
            }
        }
        _ => println!("     冷数据RMW操作失败: {:?}", status),
    }

    // 测试2: 批量冷数据迁移
    println!("   测试2: 批量冷数据迁移");
    let cold_keys = vec![600, 700, 800, 900, 1000];
    let mut migration_count = 0;

    for &key in &cold_keys {
        let mut rmw_ctx = MigrationRmwContext {
            key,
            increment: 100,
        };
        if r2_kv.rmw(&mut rmw_ctx) == Status::Ok {
            migration_count += 1;
        }
    }
    println!(
        "     批量迁移完成: {}/{} 个冷数据迁移成功",
        migration_count,
        cold_keys.len()
    );
}

// 并发迁移测试
fn test_concurrent_migration(r2_kv: &R2Kv<u64, MigrationData>) {
    println!("\n 测试并发冷热数据迁移");

    let num_operations = 800; // 8 * 100
    let mut total_success = 0;
    let mut total_migrations = 0;

    for i in 1..=num_operations {
        let key = i;

        // 随机选择操作类型
        match i % 4 {
            0 => {
                // 写入新数据
                let data = MigrationData::new(key, key * 100);
                let upsert_ctx = MigrationUpsertContext { key, value: data };
                if r2_kv.upsert(&upsert_ctx) == Status::Ok {
                    total_success += 1;
                }
            }
            1 => {
                // 读取数据
                let mut read_ctx = MigrationReadContext { key, value: None };
                if r2_kv.read(&mut read_ctx) == Status::Ok {
                    total_success += 1;
                }
            }
            2 => {
                // RMW操作（可能触发迁移）
                let mut rmw_ctx = MigrationRmwContext { key, increment: 1 };
                if r2_kv.rmw(&mut rmw_ctx) == Status::Ok {
                    total_success += 1;
                    total_migrations += 1;
                }
            }
            _ => {
                // 批量RMW操作
                for j in 0..5 {
                    let batch_key = key + j * 1000;
                    let mut rmw_ctx = MigrationRmwContext {
                        key: batch_key,
                        increment: 10,
                    };
                    if r2_kv.rmw(&mut rmw_ctx) == Status::Ok {
                        total_success += 1;
                        total_migrations += 1;
                    }
                }
            }
        }
    }

    println!("   并发迁移测试完成:");
    println!("    - 总操作数: {}", total_success);
    println!("    - 总迁移数: {}", total_migrations);
    println!(
        "    - 迁移率: {:.2}%",
        (total_migrations as f64 / total_success as f64) * 100.0
    );
}

// 迁移性能测试
fn test_migration_performance(r2_kv: &R2Kv<u64, MigrationData>) {
    println!("\n 迁移性能测试");

    let num_operations = 5000;
    let start_time = Instant::now();

    // 创建测试数据
    for i in 1..=num_operations {
        let data = MigrationData::new(i, i * 100);
        let upsert_ctx = MigrationUpsertContext {
            key: i,
            value: data,
        };
        r2_kv.upsert(&upsert_ctx);
    }

    let create_duration = start_time.elapsed();
    println!("   数据创建耗时: {:?}", create_duration);

    // 执行迁移操作
    let migration_start = Instant::now();
    let mut migration_count = 0;

    for i in 1..=num_operations {
        let mut rmw_ctx = MigrationRmwContext {
            key: i,
            increment: 1,
        };
        if r2_kv.rmw(&mut rmw_ctx) == Status::Ok {
            migration_count += 1;
        }
    }

    let migration_duration = migration_start.elapsed();
    let total_duration = start_time.elapsed();

    println!("   迁移操作耗时: {:?}", migration_duration);
    println!("   总耗时: {:?}", total_duration);
    println!("   迁移操作数: {}", migration_count);
    println!(
        "   迁移吞吐量: {:.2} 操作/秒",
        migration_count as f64 / migration_duration.as_secs_f64()
    );
}

// 迁移一致性测试
fn test_migration_consistency(r2_kv: &R2Kv<u64, MigrationData>) {
    println!("\n 迁移一致性测试");

    // 创建测试数据
    let test_key = 1;
    let initial_data = MigrationData::new(test_key, 1000);
    let upsert_ctx = MigrationUpsertContext {
        key: test_key,
        value: initial_data,
    };
    r2_kv.upsert(&upsert_ctx);

    // 执行多次RMW操作
    let mut expected_value = 1000;
    for i in 1..=100 {
        let mut rmw_ctx = MigrationRmwContext {
            key: test_key,
            increment: i,
        };
        let status = r2_kv.rmw(&mut rmw_ctx);
        assert_eq!(status, Status::Ok);
        expected_value += i;

        // 每10次操作验证一次数据一致性
        if i % 10 == 0 {
            let mut read_ctx = MigrationReadContext {
                key: test_key,
                value: None,
            };
            let status = r2_kv.read(&mut read_ctx);
            assert_eq!(status, Status::Ok);

            if let Some(data) = read_ctx.value {
                assert_eq!(data.value, expected_value);
                println!(
                    "     第 {} 次验证: 值={}, 访问次数={}",
                    i, data.value, data.access_count
                );
            }
        }
    }

    // 最终验证
    let mut read_ctx = MigrationReadContext {
        key: test_key,
        value: None,
    };
    let status = r2_kv.read(&mut read_ctx);
    assert_eq!(status, Status::Ok);

    if let Some(data) = read_ctx.value {
        assert_eq!(data.value, expected_value);
        println!(
            "   最终验证成功: 值={}, 访问次数={}, 迁移次数={}",
            data.value, data.access_count, data.migration_count
        );
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!(" F2 冷热数据迁移压力测试");
    println!("=====================================");

    // 创建临时目录
    let hot_dir = "/tmp/r2_migration_stress_hot";
    let cold_dir = "/tmp/r2_migration_stress_cold";

    for dir in [hot_dir, cold_dir] {
        if Path::new(dir).exists() {
            std::fs::remove_dir_all(dir)?;
        }
        std::fs::create_dir_all(dir)?;
    }

    // 初始化F2存储系统
    println!(" 初始化F2存储系统...");
    let r2_kv = R2Kv::<u64, MigrationData>::new(hot_dir, cold_dir)?;
    let r2_kv_arc = Arc::new(r2_kv);
    println!(" F2存储系统初始化成功");

    // 模拟冷热数据分离
    simulate_cold_hot_separation(&r2_kv_arc);

    // 测试迁移触发机制
    test_migration_triggers(&r2_kv_arc);

    // 并发迁移测试
    test_concurrent_migration(&r2_kv_arc);

    // 迁移性能测试
    test_migration_performance(&r2_kv_arc);

    // 迁移一致性测试
    test_migration_consistency(&r2_kv_arc);

    // 清理
    for dir in [hot_dir, cold_dir] {
        std::fs::remove_dir_all(dir)?;
    }
    println!("\n 清理完成");
    println!("\n F2冷热数据迁移压力测试完成！");

    Ok(())
}

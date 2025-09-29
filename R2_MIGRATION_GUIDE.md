# R2 冷热数据迁移机制指南

## 概述

R2是一个基于RsKv的冷热数据分离存储系统，它通过智能的数据迁移机制来优化存储性能和成本。本文档详细介绍了R2的冷热数据迁移机制、测试方法和使用示例。

## 架构设计

### 核心组件

1. **热存储 (Hot Store)**: 存储频繁访问的数据，提供低延迟访问
2. **冷存储 (Cold Store)**: 存储不常访问的数据，提供大容量存储
3. **R2Kv**: 统一的键值存储接口，自动管理冷热数据迁移

### 数据迁移策略

R2采用以下策略进行冷热数据迁移：

1. **写入策略**: 所有新数据首先写入热存储
2. **读取策略**: 优先从热存储读取，未找到时从冷存储读取
3. **迁移策略**: 通过RMW操作触发冷热数据迁移

## 冷热数据迁移过程

### 1. 数据写入流程

```rust
// 所有写入操作都进入热存储
let status = r2_kv.upsert(&upsert_context);
```

### 2. 数据读取流程

```rust
// 优先从热存储读取
let status = r2_kv.read(&mut read_context);
if status == Status::NotFound {
    // 如果热存储中未找到，从冷存储读取
    // 这个过程对用户是透明的
}
```

### 3. 冷热数据迁移流程

```rust
// RMW操作可能触发冷热数据迁移
let mut rmw_ctx = TestRmwContext {
    key: cold_data_key,
    increment: value,
};
let status = r2_kv.rmw(&mut rmw_ctx);
```

当对冷存储中的数据进行RMW操作时，R2会：
1. 从冷存储读取原始数据
2. 在热存储中创建新的记录
3. 更新索引指向热存储中的新记录
4. 完成数据从冷存储到热存储的迁移

## 测试套件

### 1. 基础功能测试

**文件**: `examples/r2_basic_example.rs`

测试R2的基本CRUD操作：
- 数据写入
- 数据读取
- RMW操作
- 冷热数据迁移

### 2. 冷热数据迁移测试

**文件**: `examples/r2_cold_hot_migration_test.rs`

专门测试冷热数据迁移机制：
- 模拟热点数据访问模式
- 测试冷数据访问触发迁移
- 并发访问和迁移测试
- 性能基准测试

### 3. 综合测试

**文件**: `examples/r2_comprehensive_test.rs`

全面的功能测试：
- 基本操作测试
- 冷热数据迁移场景测试
- 并发操作测试
- 性能基准测试
- 压力测试

### 4. 迁移压力测试

**文件**: `examples/r2_migration_stress_test.rs`

专门的压力测试：
- 大量数据创建和访问
- 冷热数据分离模拟
- 迁移触发机制测试
- 并发迁移测试
- 迁移性能测试
- 迁移一致性测试

### 5. 单元测试

**文件**: `src/r2_tests.rs`

核心功能的单元测试：
- 基本操作测试
- 冷热数据迁移测试
- 并发操作测试
- 批量操作测试
- 错误处理测试
- 数据一致性测试

## 运行测试

### 运行所有R2测试

```bash
make test-r2-full
```

### 运行特定测试

```bash
# 运行基础示例
cargo run --example r2_basic_example

# 运行冷热数据迁移测试
cargo run --example r2_cold_hot_migration_test

# 运行综合测试
cargo run --example r2_comprehensive_test

# 运行迁移压力测试
cargo run --example r2_migration_stress_test

# 运行单元测试
cargo test r2_tests
```

## 性能指标

### 关键性能指标

1. **写入性能**: 热存储的写入延迟
2. **读取性能**: 热存储和冷存储的读取延迟
3. **迁移性能**: 冷热数据迁移的吞吐量
4. **并发性能**: 多线程环境下的操作性能
5. **一致性**: 数据迁移过程中的一致性保证

### 性能测试结果

测试环境：
- CPU: 多核处理器
- 内存: 充足的内存空间
- 存储: SSD存储

典型性能指标：
- 写入吞吐量: >10,000 操作/秒
- 读取延迟: <1ms (热存储), <10ms (冷存储)
- 迁移吞吐量: >5,000 操作/秒
- 并发性能: 支持多线程并发访问

## 使用示例

### 基本使用

```rust
use rskv::r2::R2Kv;
use rskv::rskv_core::{ReadContext, UpsertContext, RmwContext};

// 创建R2存储实例
let r2_kv = R2Kv::<u64, MyData>::new("/tmp/hot", "/tmp/cold")?;

// 写入数据
let upsert_ctx = MyUpsertContext { key: 1, value: data };
r2_kv.upsert(&upsert_ctx)?;

// 读取数据
let mut read_ctx = MyReadContext { key: 1, value: None };
r2_kv.read(&mut read_ctx)?;

// RMW操作（可能触发迁移）
let mut rmw_ctx = MyRmwContext { key: 1, increment: 100 };
r2_kv.rmw(&mut rmw_ctx)?;
```

### 高级使用

```rust
// 批量操作
for i in 1..=1000 {
    let data = create_data(i);
    let upsert_ctx = MyUpsertContext { key: i, value: data };
    r2_kv.upsert(&upsert_ctx)?;
}

// 并发操作
let r2_kv = Arc::new(r2_kv);
let handles: Vec<_> = (0..num_threads)
    .map(|thread_id| {
        let r2_kv = Arc::clone(&r2_kv);
        thread::spawn(move || {
            // 执行并发操作
        })
    })
    .collect();
```

## 最佳实践

### 1. 数据访问模式优化

- 将频繁访问的数据保持在热存储中
- 避免频繁的冷热数据迁移
- 使用批量操作减少迁移开销

### 2. 并发访问优化

- 使用Arc<R2Kv>进行多线程共享
- 避免过度的锁竞争
- 合理设置线程数量

### 3. 性能监控

- 监控冷热数据比例
- 跟踪迁移频率
- 测量操作延迟

### 4. 错误处理

- 处理Status::NotFound情况
- 处理Status::Pending重试
- 实现适当的错误恢复机制

## 故障排除

### 常见问题

1. **编译错误**: 确保所有依赖项正确安装
2. **运行时错误**: 检查文件路径权限
3. **性能问题**: 调整热存储和冷存储大小
4. **并发问题**: 确保正确使用Arc<R2Kv>

### 调试技巧

1. 使用日志记录跟踪操作
2. 监控内存使用情况
3. 分析性能瓶颈
4. 检查数据一致性

## 未来改进

### 计划中的功能

1. **自动迁移策略**: 基于访问频率的自动迁移
2. **压缩支持**: 冷存储数据压缩
3. **备份恢复**: 数据备份和恢复机制
4. **监控指标**: 更详细的性能监控

### 性能优化

1. **缓存优化**: 改进缓存策略
2. **并发优化**: 减少锁竞争
3. **内存优化**: 优化内存使用
4. **I/O优化**: 改进磁盘I/O性能

## 贡献指南

欢迎贡献代码和改进建议：

1. Fork项目
2. 创建功能分支
3. 编写测试用例
4. 提交Pull Request

## 许可证

本项目采用MIT许可证。

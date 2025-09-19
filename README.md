# FASTER Rust KV Store

这是一个基于Microsoft FASTER的Rust实现的高性能键值存储系统。FASTER是一个为点查找和重更新设计的高性能并发键值存储和缓存，支持大于内存的数据和一致恢复。

## 功能特性

- **高性能**: 支持高并发读写操作
- **持久化**: 支持数据持久化和恢复
- **内存管理**: 高效的内存分配和管理
- **并发安全**: 使用epoch-based内存管理确保并发安全
- **可扩展**: 支持大于内存的数据集

## 项目结构

```
src/
├── core/                    # 核心组件
│   ├── address.rs          # 地址管理
│   ├── alloc.rs            # 内存分配
│   ├── checkpoint.rs       # 检查点功能
│   ├── light_epoch.rs      # 轻量级epoch管理
│   ├── malloc_fixed_page_size.rs  # 固定页面大小分配器
│   ├── record.rs           # 记录结构
│   └── status.rs           # 状态码
├── device/                 # 设备抽象
│   └── file_system_disk.rs # 文件系统磁盘实现
├── environment/            # 环境抽象
│   └── file.rs             # 文件操作
├── hlog/                   # 混合日志
│   └── persistent_memory_malloc.rs  # 持久化内存分配器
├── index/                  # 索引实现
│   ├── cold_index.rs       # 冷索引
│   ├── cold_index_contexts.rs  # 冷索引上下文
│   ├── definitions.rs      # 索引定义
│   ├── hash_bucket.rs      # 哈希桶
│   ├── hash_table.rs       # 哈希表
│   ├── mem_index.rs        # 内存索引
│   └── mod.rs              # 索引模块
├── faster.rs               # 主要KV存储实现
└── f2.rs                   # 二级KV存储
```

## 示例

### 基本使用

```rust
use rskv::faster::{FasterKv, UpsertContext, ReadContext};
use rskv::device::file_system_disk::FileSystemDisk;
use rskv::core::status::Status;

// 定义数据结构
#[derive(Debug, Clone, PartialEq)]
struct UserData {
    id: u64,
    name: String,
    email: String,
}

impl Default for UserData {
    fn default() -> Self {
        UserData {
            id: 0,
            name: String::new(),
            email: String::new(),
        }
    }
}

// 实现UpsertContext
struct UserUpsertContext {
    key: u64,
    value: UserData,
}

impl UpsertContext for UserUpsertContext {
    type Key = u64;
    type Value = UserData;

    fn key(&self) -> &Self::Key { &self.key }
    fn value(&self) -> &Self::Value { &self.value }
    fn key_hash(&self) -> u64 { self.key }
    fn put_atomic(&self, _value: &mut Self::Value) -> bool { false }
}

// 实现ReadContext
struct UserReadContext {
    key: u64,
    value: Option<UserData>,
}

impl ReadContext for UserReadContext {
    type Key = u64;
    type Value = UserData;

    fn key(&self) -> &Self::Key { &self.key }
    fn key_hash(&self) -> u64 { self.key }
    fn get(&mut self, value: &Self::Value) {
        self.value = Some(value.clone());
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 创建KV存储
    let disk = FileSystemDisk::new("/tmp/rskv_test")?;
    let mut kv = FasterKv::<u64, UserData, FileSystemDisk>::new(1 << 20, 1 << 16, disk)?;

    // 插入数据
    let upsert_ctx = UserUpsertContext {
        key: 1,
        value: UserData {
            id: 1,
            name: "Alice".to_string(),
            email: "alice@example.com".to_string(),
        },
    };
    let status = kv.upsert(&upsert_ctx);
    println!("Upsert status: {:?}", status);

    // 读取数据
    let mut read_ctx = UserReadContext {
        key: 1,
        value: None,
    };
    let status = kv.read(&mut read_ctx);
    if status == Status::Ok {
        if let Some(user) = read_ctx.value {
            println!("Read user: {:?}", user);
        }
    }

    Ok(())
}
```

## 运行示例

```bash
# 运行基本测试
cargo run --example basic_test

# 运行综合测试
cargo run --example comprehensive_test

# 运行简单性能测试
cargo run --example simple_performance_test
```

## 性能特点

- **高吞吐量**: 支持每秒数十万次操作
- **低延迟**: 微秒级操作延迟
- **内存效率**: 使用固定页面大小分配器优化内存使用
- **并发性能**: 支持多线程并发访问

## 注意事项

1. **指针对齐**: 当前实现在某些情况下可能存在指针对齐问题，特别是在高并发场景下
2. **异步操作**: 某些操作可能返回`Pending`状态，表示操作正在异步处理
3. **内存管理**: 使用epoch-based内存管理，需要正确使用Guard来保护内存访问

## 开发状态

这是一个实验性的Rust实现，基于Microsoft FASTER的C++版本。当前版本已经实现了基本的KV存储功能，但在某些高级功能（如完整的并发支持和数据恢复）方面还需要进一步完善。

## 许可证

本项目基于MIT许可证开源。

## 参考

- [Microsoft FASTER](https://github.com/microsoft/FASTER)
- [FASTER论文](https://www.microsoft.com/en-us/research/publication/faster-a-concurrent-key-value-store-with-in-memory-computing/)

# 🚀 rskv 性能测试指南

## 📊 概述

rskv 提供了全面的性能测试套件，用于评估键值存储在各种工作负载下的性能表现。性能测试基于 Criterion.rs 框架，提供统计学上有意义的基准测试结果。

## 🎯 测试场景

### 1. **写入性能测试** (`write_performance`)
- **目标**: 评估不同 value 大小的写入性能
- **测试范围**: 1B 到 100KB
- **关键指标**: 吞吐量 (MB/s), 平均延迟
- **用例**: 
  - 小数据写入 (元数据、配置)
  - 中等数据写入 (文档、记录)
  - 大数据写入 (文件、二进制数据)

### 2. **读取性能测试** (`read_performance`)
- **目标**: 评估不同 value 大小的读取性能
- **测试范围**: 1B 到 100KB
- **关键指标**: 吞吐量 (MB/s), 缓存命中率
- **用例**:
  - 热数据访问 (内存命中)
  - 冷数据访问 (磁盘读取)
  - 顺序读取模式

### 3. **混合工作负载测试** (`mixed_workload`)
- **目标**: 评估不同读写比例下的性能
- **测试比例**: 0%, 50%, 90%, 95%, 99% 读取
- **关键指标**: 总吞吐量, 延迟分布
- **用例**:
  - 写密集型应用 (日志、监控)
  - 平衡型应用 (一般业务)
  - 读密集型应用 (缓存、查询)

### 4. **并发操作测试** (`concurrent_operations`)
- **目标**: 评估多线程并发访问性能
- **测试线程数**: 1, 2, 4 线程
- **关键指标**: 并发扩展性, 锁争用情况
- **用例**:
  - 单线程基线性能
  - 多线程扩展性验证
  - 高并发场景验证

### 5. **批量操作测试** (`batch_operations`)
- **目标**: 评估批量操作的效率
- **批量大小**: 1, 10, 100 个操作
- **关键指标**: 批量优化效果, 吞吐量提升
- **用例**:
  - 单操作 vs 批量操作
  - 批量大小优化
  - 事务性操作模拟

### 6. **扫描操作测试** (`scan_operations`)
- **目标**: 评估数据扫描和遍历性能
- **数据量**: 10, 100, 1000 条记录
- **关键指标**: 扫描速度, 内存使用
- **用例**:
  - 全表扫描
  - 前缀扫描
  - 范围查询

## 🔧 运行性能测试

### 快速测试
```bash
# 运行核心性能测试
make perf-quick

# 等价于:
cargo bench --bench performance -- write_performance
cargo bench --bench performance -- read_performance
cargo bench --bench performance -- mixed_workload
```

### 完整性能测试
```bash
# 运行所有性能测试并生成详细报告
make performance

# 或者手动运行
./scripts/benchmark.sh
```

### 单个测试组
```bash
# 运行特定测试组
cargo bench --bench performance -- write_performance
cargo bench --bench performance -- concurrent_operations
```

### Windows 用户
```powershell
# 使用 PowerShell 脚本
.\scripts\benchmark.ps1
```

## 📈 性能指标解读

### 吞吐量 (Throughput)
- **单位**: MB/s, Operations/s
- **含义**: 每秒处理的数据量或操作数
- **优化目标**: 数值越高越好

### 延迟 (Latency)
- **单位**: µs (微秒), ms (毫秒)
- **含义**: 单次操作的响应时间
- **优化目标**: 数值越低越好

### 扩展性 (Scalability)
- **指标**: 线程数 vs 性能曲线
- **含义**: 随并发度增加的性能变化
- **优化目标**: 线性扩展最佳

## 🎛️ 性能调优参数

### 内存配置
```rust
Config {
    memory_size: 512 * 1024 * 1024,  // 512MB, 影响缓存命中率
    page_size: 64 * 1024,            // 64KB, 影响I/O效率
    // ...
}
```

### 存储优化
```rust
Config {
    use_mmap: true,                  // 启用内存映射
    enable_readahead: true,          // 启用预读
    sync_mode: SyncMode::None,       // 禁用同步获得最佳性能
    // ...
}
```

### 后台任务
```rust
Config {
    enable_checkpointing: false,     // 测试时禁用以获得一致性能
    enable_gc: false,               // 测试时禁用以避免干扰
    // ...
}
```

## 📊 性能基线

以下是在标准测试环境下的性能基线 (仅供参考):

### 写入性能
| Value Size | Throughput | Latency |
|------------|------------|---------|
| 1B         | ~700 KiB/s | ~140 µs |
| 100B       | ~65 MiB/s  | ~145 µs |
| 1KB        | ~480 MiB/s | ~200 µs |
| 10KB       | ~1.4 GiB/s | ~650 µs |
| 100KB      | ~1.5 GiB/s | ~6 ms   |

### 读取性能
- **内存命中**: 亚微秒级延迟
- **磁盘读取**: 几毫秒级延迟
- **缓存命中率**: 影响总体性能

### 并发性能
- **单线程**: 基线性能
- **2线程**: 1.5-1.8x 性能提升
- **4线程**: 2.5-3.5x 性能提升

## 🔍 性能分析工具

### Criterion 报告
- **位置**: `target/criterion/`
- **格式**: HTML 交互式报告
- **内容**: 详细统计、趋势分析、性能回归检测

### 系统监控
```bash
# CPU 使用情况
top -p $(pgrep benchmark)

# 内存使用情况
ps aux | grep benchmark

# I/O 统计
iostat -x 1
```

### 性能剖析
```bash
# 使用 perf 分析热点
perf record --call-graph=dwarf cargo bench
perf report

# 使用 valgrind 分析内存
valgrind --tool=massif cargo bench
```

## 🚀 性能优化建议

### 1. Value 大小优化
- **小数据 (< 1KB)**: 优化索引效率，减少序列化开销
- **大数据 (> 10KB)**: 优化I/O策略，启用压缩

### 2. 读写比例优化
- **读密集**: 增大内存缓存，优化索引结构
- **写密集**: 优化日志写入，减少同步频率

### 3. 并发优化
- **CPU绑定**: 调整线程数匹配CPU核心数
- **I/O绑定**: 考虑异步I/O和批量操作

### 4. 内存优化
- **热数据**: 增大内存配置提高缓存命中率
- **冷数据**: 优化磁盘访问模式和预读策略

## 📝 性能测试最佳实践

### 1. 测试环境
- 使用专用测试机器，避免其他进程干扰
- 确保足够的磁盘空间和内存
- 关闭不必要的后台服务

### 2. 测试数据
- 使用真实业务数据模式
- 考虑键分布和数据大小分布
- 包含冷热数据混合场景

### 3. 测试结果
- 运行多次测试取平均值
- 记录系统配置和环境信息
- 建立性能基线用于回归测试

### 4. 持续监控
- 定期运行性能测试
- 监控性能趋势变化
- 在代码变更后验证性能影响

## 🔗 相关文档

- [架构文档](docs/ARCHITECTURE.md) - 了解系统设计
- [实现计划](docs/IMPLEMENTATION_PLAN.md) - 了解实现细节
- [测试覆盖率](COVERAGE_REPORT.md) - 了解测试覆盖情况
- [变更日志](CHANGELOG.md) - 了解版本变更对性能的影响

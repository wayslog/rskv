//! rskv 性能演示
//!
//! 这个示例展示了 rskv 在不同场景下的性能表现

use std::time::Instant;

use rskv::{Config, RsKv};
use tempfile::tempdir;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    println!("🚀 rskv 性能演示");
    println!("================");

    // 创建测试实例
    let temp_dir = tempdir()?;
    let config = Config {
        storage_dir: temp_dir.path().to_string_lossy().to_string(),
        memory_size: 256 * 1024 * 1024, // 256MB
        use_mmap: true,
        enable_readahead: true,
        sync_mode: rskv::common::SyncMode::None,
        enable_checkpointing: false,
        enable_gc: false,
        ..Default::default()
    };

    let rskv = RsKv::new(config).await?;

    // 测试不同大小的写入性能
    println!("\n📝 写入性能测试");
    println!("----------------");

    let value_sizes = vec![
        ("1B", 1),
        ("100B", 100),
        ("1KB", 1024),
        ("10KB", 10 * 1024),
        ("100KB", 100 * 1024),
    ];

    for (size_name, size) in &value_sizes {
        let test_data = vec![42u8; *size];
        let num_ops = if *size > 10240 { 100 } else { 1000 };

        let start = Instant::now();
        for i in 0..num_ops {
            let key = format!("write_test_{}_{}", size_name, i).into_bytes();
            rskv.upsert(key, test_data.clone()).await?;
        }
        let elapsed = start.elapsed();

        let throughput =
            (*size as f64 * num_ops as f64) / elapsed.as_secs_f64() / (1024.0 * 1024.0);
        let ops_per_sec = num_ops as f64 / elapsed.as_secs_f64();

        println!(
            "  {}: {:.2} MB/s, {:.0} ops/s, {:.2} µs/op",
            size_name,
            throughput,
            ops_per_sec,
            elapsed.as_micros() as f64 / num_ops as f64
        );
    }

    // 测试读取性能
    println!("\n📖 读取性能测试");
    println!("----------------");

    for (size_name, size) in &value_sizes {
        let num_ops = if *size > 10240 { 100 } else { 1000 };

        let start = Instant::now();
        for i in 0..num_ops {
            let key = format!("write_test_{}_{}", size_name, i).into_bytes();
            let _value = rskv.read(&key).await?;
        }
        let elapsed = start.elapsed();

        let throughput =
            (*size as f64 * num_ops as f64) / elapsed.as_secs_f64() / (1024.0 * 1024.0);
        let ops_per_sec = num_ops as f64 / elapsed.as_secs_f64();

        println!(
            "  {}: {:.2} MB/s, {:.0} ops/s, {:.2} µs/op",
            size_name,
            throughput,
            ops_per_sec,
            elapsed.as_micros() as f64 / num_ops as f64
        );
    }

    // 测试混合工作负载
    println!("\n🔄 混合工作负载测试 (1KB values)");
    println!("--------------------------------");

    let test_data = vec![42u8; 1024];
    let read_percentages = vec![0, 50, 90, 95, 99];

    for read_pct in read_percentages {
        let num_ops = 1000;

        let start = Instant::now();
        for i in 0..num_ops {
            let key = format!("mixed_test_{}", i % 100).into_bytes();

            if (i % 100) < read_pct {
                // 读操作
                let _value = rskv.read(&key).await?;
            } else {
                // 写操作
                rskv.upsert(key, test_data.clone()).await?;
            }
        }
        let elapsed = start.elapsed();

        let ops_per_sec = num_ops as f64 / elapsed.as_secs_f64();

        println!(
            "  {}% 读取: {:.0} ops/s, {:.2} µs/op",
            read_pct,
            ops_per_sec,
            elapsed.as_micros() as f64 / num_ops as f64
        );
    }

    // 测试扫描性能
    println!("\n🔍 扫描操作测试");
    println!("----------------");

    // 准备扫描测试数据
    let scan_data = vec![42u8; 100];
    for i in 0..1000 {
        let key = format!("scan_test_{:04}", i).into_bytes();
        rskv.upsert(key, scan_data.clone()).await?;
    }

    // 全表扫描
    let start = Instant::now();
    let all_results = rskv.scan_all().await?;
    let scan_elapsed = start.elapsed();

    println!(
        "  全表扫描: {} 条记录, {:.2} ms, {:.0} records/s",
        all_results.len(),
        scan_elapsed.as_millis(),
        all_results.len() as f64 / scan_elapsed.as_secs_f64()
    );

    // 前缀扫描
    let start = Instant::now();
    let prefix_results = rskv.scan_prefix(b"scan_test_").await?;
    let prefix_elapsed = start.elapsed();

    println!(
        "  前缀扫描: {} 条记录, {:.2} ms, {:.0} records/s",
        prefix_results.len(),
        prefix_elapsed.as_millis(),
        prefix_results.len() as f64 / prefix_elapsed.as_secs_f64()
    );

    // 显示统计信息
    println!("\n📊 存储统计");
    println!("------------");
    let stats = rskv.stats();
    println!("  索引条目数: {}", stats.index_entries);
    println!("  日志尾地址: 0x{:x}", stats.log_tail_address);
    println!("  可变区域大小: {} bytes", stats.mutable_region_size);
    println!("  只读区域大小: {} bytes", stats.read_only_region_size);
    println!("  磁盘区域大小: {} bytes", stats.disk_region_size);

    println!("\n✅ 性能演示完成!");
    println!("\n💡 提示:");
    println!("  - 运行 'make perf-quick' 进行快速性能测试");
    println!("  - 运行 'make performance' 进行完整性能分析");
    println!("  - 查看 PERFORMANCE.md 了解详细的性能测试指南");

    Ok(())
}

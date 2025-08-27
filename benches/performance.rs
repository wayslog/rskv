//! 性能基准测试
//! 
//! 测试 rskv 在不同场景下的性能表现：
//! - 不同 value size (1B to 100KB)
//! - 不同读写比例 (0%, 50%, 90%, 95%, 99% read)
//! - 并发访问性能
//! - 批量操作性能

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use rskv::{RsKv, Config};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

/// 生成指定大小的测试数据
fn generate_test_data(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 256) as u8).collect()
}

/// 生成测试键
fn generate_key(index: usize) -> Vec<u8> {
    format!("key_{:08}", index).into_bytes()
}

/// 创建测试用的 RsKv 实例
async fn create_test_rskv(memory_size: u64) -> RsKv {
    let temp_dir = tempdir().unwrap();
    let config = Config {
        storage_dir: temp_dir.path().to_string_lossy().to_string(),
        memory_size,
        page_size: 64 * 1024, // 64KB pages
        enable_checkpointing: false, // 禁用后台任务以获得一致的性能
        enable_gc: false,
        use_mmap: true, // 启用内存映射
        enable_readahead: true,
        sync_mode: rskv::common::SyncMode::None, // 禁用同步获得最佳性能
        ..Default::default()
    };
    
    RsKv::new(config).await.unwrap()
}

/// 基础写入性能测试
fn bench_write_performance(c: &mut Criterion) {
    let value_sizes = vec![
        ("1B", 1),
        ("100B", 100),
        ("1KB", 1024),
        ("10KB", 10 * 1024),
        ("100KB", 100 * 1024),
    ];
    
    let mut group = c.benchmark_group("write_performance");
    group.sample_size(20); // 增加样本数量确保统计有效性
    group.measurement_time(Duration::from_secs(10));
    
    for (size_name, size) in value_sizes {
        group.throughput(Throughput::Bytes(size as u64 * 100)); // 100 operations
        
        group.bench_with_input(
            BenchmarkId::new("sequential_write", size_name),
            &size,
            |b, &value_size| {
                b.iter_custom(|iters| {
                    let rt = tokio::runtime::Runtime::new().unwrap();
                    
                    let mut total_duration = Duration::from_nanos(0);
                    
                    for _ in 0..iters {
                        let duration = rt.block_on(async {
                            let rskv = create_test_rskv(256 * 1024 * 1024).await;
                            let test_data = generate_test_data(value_size);
                            
                            let start = std::time::Instant::now();
                            for i in 0..100 {
                                let key = generate_key(i);
                                rskv.upsert(key, test_data.clone()).await.unwrap();
                            }
                            start.elapsed()
                        });
                        
                        total_duration += duration;
                    }
                    
                    total_duration
                });
            },
        );
    }
    
    group.finish();
}

/// 基础读取性能测试
fn bench_read_performance(c: &mut Criterion) {
    let value_sizes = vec![
        ("1B", 1),
        ("100B", 100),
        ("1KB", 1024),
        ("10KB", 10 * 1024),
        ("100KB", 100 * 1024),
    ];
    
    let mut group = c.benchmark_group("read_performance");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(10));
    
    for (size_name, size) in value_sizes {
        group.throughput(Throughput::Bytes(size as u64 * 100));
        
        group.bench_with_input(
            BenchmarkId::new("sequential_read", size_name),
            &size,
            |b, &value_size| {
                b.iter_custom(|iters| {
                    let rt = tokio::runtime::Runtime::new().unwrap();
                    
                    let mut total_duration = Duration::from_nanos(0);
                    
                    for _ in 0..iters {
                        let duration = rt.block_on(async {
                            let rskv = create_test_rskv(256 * 1024 * 1024).await;
                            let test_data = generate_test_data(value_size);
                            
                            // 预填充数据
                            for i in 0..100 {
                                let key = generate_key(i);
                                rskv.upsert(key, test_data.clone()).await.unwrap();
                            }
                            
                            // 测试读取
                            let start = std::time::Instant::now();
                            for i in 0..100 {
                                let key = generate_key(i);
                                let _value = rskv.read(&key).await.unwrap();
                            }
                            start.elapsed()
                        });
                        
                        total_duration += duration;
                    }
                    
                    total_duration
                });
            },
        );
    }
    
    group.finish();
}

/// 混合读写性能测试
fn bench_mixed_workload(c: &mut Criterion) {
    let read_percentages = vec![0, 50, 90, 95, 99];
    let value_size = 1024; // 1KB values
    
    let mut group = c.benchmark_group("mixed_workload");
    group.sample_size(15);
    group.measurement_time(Duration::from_secs(8));
    group.throughput(Throughput::Elements(100));
    
    for read_pct in read_percentages {
        group.bench_with_input(
            BenchmarkId::new("mixed_ops", format!("{}%_read", read_pct)),
            &read_pct,
            |b, &read_percentage| {
                b.iter_custom(|iters| {
                    let rt = tokio::runtime::Runtime::new().unwrap();
                    
                    let mut total_duration = Duration::from_nanos(0);
                    
                    for _ in 0..iters {
                        let duration = rt.block_on(async {
                            let rskv = create_test_rskv(256 * 1024 * 1024).await;
                            let test_data = generate_test_data(value_size);
                            
                            // 预填充一些数据用于读取
                            for i in 0..50 {
                                let key = generate_key(i);
                                rskv.upsert(key, test_data.clone()).await.unwrap();
                            }
                            
                            let start = std::time::Instant::now();
                            
                            for i in 0..100 {
                                let should_read = (i % 100) < read_percentage;
                                let key = generate_key(i % 50); // 复用键以确保读取命中
                                
                                if should_read {
                                    let _value = rskv.read(&key).await.unwrap();
                                } else {
                                    rskv.upsert(key, test_data.clone()).await.unwrap();
                                }
                            }
                            
                            start.elapsed()
                        });
                        
                        total_duration += duration;
                    }
                    
                    total_duration
                });
            },
        );
    }
    
    group.finish();
}

/// 并发性能测试（扩展版）
fn bench_concurrent_operations(c: &mut Criterion) {
    let concurrency_levels = vec![1, 2, 4, 8, 16, 32];
    let value_size = 1024; // 1KB values
    
    let mut group = c.benchmark_group("concurrent_operations");
    group.sample_size(5); // 减少样本数量以加快测试
    group.measurement_time(Duration::from_secs(15));
    
    for concurrency in concurrency_levels {
        group.throughput(Throughput::Elements(100 * concurrency as u64));
        
        group.bench_with_input(
            BenchmarkId::new("concurrent_mixed", format!("{}_threads", concurrency)),
            &concurrency,
            |b, &num_threads| {
                b.iter_custom(|iters| {
                    let rt = tokio::runtime::Runtime::new().unwrap();
                    
                    let mut total_duration = Duration::from_nanos(0);
                    
                    for _ in 0..iters {
                        let duration = rt.block_on(async {
                            let rskv = Arc::new(create_test_rskv(512 * 1024 * 1024).await);
                            let test_data = generate_test_data(value_size);
                            
                            // 预填充数据
                            for i in 0..100 {
                                let key = generate_key(i);
                                rskv.upsert(key, test_data.clone()).await.unwrap();
                            }
                            
                            let start = std::time::Instant::now();
                            
                            let mut handles = Vec::new();
                            
                            for thread_id in 0..num_threads {
                                let rskv_clone = rskv.clone();
                                let data_clone = test_data.clone();
                                
                                let handle = tokio::spawn(async move {
                                    for i in 0..100 {
                                        let key_index = thread_id * 100 + i;
                                        let key = generate_key(key_index);
                                        
                                        // 50% 读 50% 写
                                        if i % 2 == 0 {
                                            let _value = rskv_clone.read(&key).await.unwrap();
                                        } else {
                                            rskv_clone.upsert(key, data_clone.clone()).await.unwrap();
                                        }
                                    }
                                });
                                
                                handles.push(handle);
                            }
                            
                            // 等待所有任务完成
                            for handle in handles {
                                handle.await.unwrap();
                            }
                            
                            start.elapsed()
                        });
                        
                        total_duration += duration;
                    }
                    
                    total_duration
                });
            },
        );
    }
    
    group.finish();
}

/// 批量操作性能测试
fn bench_batch_operations(c: &mut Criterion) {
    let batch_sizes = vec![1, 10, 100];
    let value_size = 1024; // 1KB values
    
    let mut group = c.benchmark_group("batch_operations");
    group.sample_size(10);
    
    for batch_size in batch_sizes {
        group.throughput(Throughput::Elements(batch_size as u64));
        
        group.bench_with_input(
            BenchmarkId::new("batch_write", format!("{}_ops", batch_size)),
            &batch_size,
            |b, &batch_size| {
                b.iter_custom(|iters| {
                    let rt = tokio::runtime::Runtime::new().unwrap();
                    
                    let mut total_duration = Duration::from_nanos(0);
                    
                    for _ in 0..iters {
                        let duration = rt.block_on(async {
                            let rskv = create_test_rskv(256 * 1024 * 1024).await;
                            let test_data = generate_test_data(value_size);
                            
                            let start = std::time::Instant::now();
                            
                            // 模拟批量写入
                            for i in 0..batch_size {
                                let key = generate_key(i);
                                rskv.upsert(key, test_data.clone()).await.unwrap();
                            }
                            
                            start.elapsed()
                        });
                        
                        total_duration += duration;
                    }
                    
                    total_duration
                });
            },
        );
    }
    
    group.finish();
}

/// 扫描操作性能测试
fn bench_scan_operations(c: &mut Criterion) {
    let data_sizes = vec![10, 100, 1000];
    
    let mut group = c.benchmark_group("scan_operations");
    group.sample_size(5); // 扫描操作比较慢，减少样本数量
    
    for data_size in data_sizes {
        group.throughput(Throughput::Elements(data_size as u64));
        
        group.bench_with_input(
            BenchmarkId::new("scan_all", format!("{}_entries", data_size)),
            &data_size,
            |b, &data_size| {
                b.iter_custom(|iters| {
                    let rt = tokio::runtime::Runtime::new().unwrap();
                    
                    let mut total_duration = Duration::from_nanos(0);
                    
                    for _ in 0..iters {
                        let duration = rt.block_on(async {
                            let rskv = create_test_rskv(256 * 1024 * 1024).await;
                            let test_data = generate_test_data(100); // 100B values
                            
                            // 填充数据
                            for i in 0..data_size {
                                let key = generate_key(i);
                                rskv.upsert(key, test_data.clone()).await.unwrap();
                            }
                            
                            let start = std::time::Instant::now();
                            let _results = rskv.scan_all().await.unwrap();
                            start.elapsed()
                        });
                        
                        total_duration += duration;
                    }
                    
                    total_duration
                });
            },
        );
    }
    
    group.finish();
}

/// 专门的多线程扩展性测试
fn bench_thread_scaling(c: &mut Criterion) {
    let thread_counts = vec![1, 2, 4, 8, 16, 24, 32];
    let value_size = 1024; // 1KB values
    
    let mut group = c.benchmark_group("thread_scaling");
    group.sample_size(5);
    group.measurement_time(Duration::from_secs(12));
    
    for thread_count in thread_counts {
        group.throughput(Throughput::Elements(1000 * thread_count as u64));
        
        // 写入扩展性测试
        group.bench_with_input(
            BenchmarkId::new("write_scaling", format!("{}_threads", thread_count)),
            &thread_count,
            |b, &num_threads| {
                b.iter_custom(|iters| {
                    let rt = tokio::runtime::Runtime::new().unwrap();
                    
                    let mut total_duration = Duration::from_nanos(0);
                    
                    for _ in 0..iters {
                        let duration = rt.block_on(async {
                            let rskv = Arc::new(create_test_rskv(512 * 1024 * 1024).await);
                            let test_data = generate_test_data(value_size);
                            
                            let start = std::time::Instant::now();
                            
                            let mut handles = Vec::new();
                            
                            for thread_id in 0..num_threads {
                                let rskv_clone = rskv.clone();
                                let data_clone = test_data.clone();
                                
                                let handle = tokio::spawn(async move {
                                    for i in 0..1000 {
                                        let key = format!("thread_{}_{}", thread_id, i).into_bytes();
                                        rskv_clone.upsert(key, data_clone.clone()).await.unwrap();
                                    }
                                });
                                
                                handles.push(handle);
                            }
                            
                            for handle in handles {
                                handle.await.unwrap();
                            }
                            
                            start.elapsed()
                        });
                        
                        total_duration += duration;
                    }
                    
                    total_duration
                });
            },
        );
        
        // 读取扩展性测试
        group.bench_with_input(
            BenchmarkId::new("read_scaling", format!("{}_threads", thread_count)),
            &thread_count,
            |b, &num_threads| {
                b.iter_custom(|iters| {
                    let rt = tokio::runtime::Runtime::new().unwrap();
                    
                    let mut total_duration = Duration::from_nanos(0);
                    
                    for _ in 0..iters {
                        let duration = rt.block_on(async {
                            let rskv = Arc::new(create_test_rskv(512 * 1024 * 1024).await);
                            let test_data = generate_test_data(value_size);
                            
                            // 预填充数据
                            for i in 0..1000 {
                                let key = format!("read_test_{}", i).into_bytes();
                                rskv.upsert(key, test_data.clone()).await.unwrap();
                            }
                            
                            let start = std::time::Instant::now();
                            
                            let mut handles = Vec::new();
                            
                            for thread_id in 0..num_threads {
                                let rskv_clone = rskv.clone();
                                
                                let handle = tokio::spawn(async move {
                                    for i in 0..1000 {
                                        let key = format!("read_test_{}", i % 1000).into_bytes();
                                        let _value = rskv_clone.read(&key).await.unwrap();
                                    }
                                });
                                
                                handles.push(handle);
                            }
                            
                            for handle in handles {
                                handle.await.unwrap();
                            }
                            
                            start.elapsed()
                        });
                        
                        total_duration += duration;
                    }
                    
                    total_duration
                });
            },
        );
        
        // 混合扩展性测试 (70% 读, 30% 写)
        group.bench_with_input(
            BenchmarkId::new("mixed_scaling", format!("{}_threads", thread_count)),
            &thread_count,
            |b, &num_threads| {
                b.iter_custom(|iters| {
                    let rt = tokio::runtime::Runtime::new().unwrap();
                    
                    let mut total_duration = Duration::from_nanos(0);
                    
                    for _ in 0..iters {
                        let duration = rt.block_on(async {
                            let rskv = Arc::new(create_test_rskv(512 * 1024 * 1024).await);
                            let test_data = generate_test_data(value_size);
                            
                            // 预填充数据
                            for i in 0..500 {
                                let key = format!("mixed_test_{}", i).into_bytes();
                                rskv.upsert(key, test_data.clone()).await.unwrap();
                            }
                            
                            let start = std::time::Instant::now();
                            
                            let mut handles = Vec::new();
                            
                            for thread_id in 0..num_threads {
                                let rskv_clone = rskv.clone();
                                let data_clone = test_data.clone();
                                
                                let handle = tokio::spawn(async move {
                                    for i in 0..1000 {
                                        let key = format!("mixed_test_{}", i % 500).into_bytes();
                                        
                                        // 70% 读, 30% 写
                                        if i % 10 < 7 {
                                            let _value = rskv_clone.read(&key).await.unwrap();
                                        } else {
                                            rskv_clone.upsert(key, data_clone.clone()).await.unwrap();
                                        }
                                    }
                                });
                                
                                handles.push(handle);
                            }
                            
                            for handle in handles {
                                handle.await.unwrap();
                            }
                            
                            start.elapsed()
                        });
                        
                        total_duration += duration;
                    }
                    
                    total_duration
                });
            },
        );
    }
    
    group.finish();
}

/// 高并发压力测试
fn bench_high_concurrency(c: &mut Criterion) {
    let scenarios = vec![
        ("light_load", 1000, 100),  // 1000 threads, 100 ops each
        ("heavy_load", 100, 10000), // 100 threads, 10000 ops each
    ];
    
    let mut group = c.benchmark_group("high_concurrency");
    group.sample_size(3);
    group.measurement_time(Duration::from_secs(20));
    
    for (scenario_name, thread_count, ops_per_thread) in scenarios {
        group.throughput(Throughput::Elements(thread_count * ops_per_thread));
        
        group.bench_with_input(
            BenchmarkId::new("stress_test", scenario_name),
            &(thread_count, ops_per_thread),
            |b, &(num_threads, ops_per_thread)| {
                b.iter_custom(|iters| {
                    let rt = tokio::runtime::Runtime::new().unwrap();
                    
                    let mut total_duration = Duration::from_nanos(0);
                    
                    for _ in 0..iters {
                        let duration = rt.block_on(async {
                            let rskv = Arc::new(create_test_rskv(1024 * 1024 * 1024).await); // 1GB
                            let test_data = generate_test_data(256); // 256B values
                            
                            let start = std::time::Instant::now();
                            
                            let mut handles = Vec::new();
                            
                            for thread_id in 0..num_threads {
                                let rskv_clone = rskv.clone();
                                let data_clone = test_data.clone();
                                
                                let handle = tokio::spawn(async move {
                                    for i in 0..ops_per_thread {
                                        let key = format!("stress_{}_{}", thread_id, i).into_bytes();
                                        
                                        // 80% 写, 20% 读
                                        if i % 5 < 4 {
                                            rskv_clone.upsert(key, data_clone.clone()).await.unwrap();
                                        } else {
                                            let _value = rskv_clone.read(&key).await.ok();
                                        }
                                    }
                                });
                                
                                handles.push(handle);
                            }
                            
                            for handle in handles {
                                handle.await.unwrap();
                            }
                            
                            start.elapsed()
                        });
                        
                        total_duration += duration;
                    }
                    
                    total_duration
                });
            },
        );
    }
    
    group.finish();
}

criterion_group!(
    benches,
    bench_write_performance,
    bench_read_performance,
    bench_mixed_workload,
    bench_concurrent_operations,
    bench_thread_scaling,
    bench_high_concurrency,
    bench_batch_operations,
    bench_scan_operations
);

criterion_main!(benches);
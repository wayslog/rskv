use rskv::core::status::Status;
use rskv::device::file_system_disk::FileSystemDisk;
use rskv::rskv_core::{RsKv, ReadContext, UpsertContext};
use std::path::Path;
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq, Default)]
struct StressData {
    id: u64,
    timestamp: u64,
    data: [u8; 32], // 32 bytes of data
}

struct StressUpsertContext {
    key: u64,
    value: StressData,
}

impl UpsertContext for StressUpsertContext {
    type Key = u64;
    type Value = StressData;

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

struct StressReadContext {
    key: u64,
    value: Option<StressData>,
}

impl ReadContext for StressReadContext {
    type Key = u64;
    type Value = StressData;

    fn key(&self) -> &Self::Key {
        &self.key
    }

    fn key_hash(&self) -> u64 {
        self.key
    }

    fn get(&mut self, value: &Self::Value) {
        self.value = Some(value.clone());
    }
}

fn stress_worker(
    kv: Arc<RsKv<u64, StressData, FileSystemDisk>>,
    barrier: Arc<Barrier>,
    thread_id: usize,
    num_operations: usize,
    results: Arc<Mutex<Vec<(usize, Duration, usize)>>>,
) {
    barrier.wait(); // Wait for all threads to start

    let start_time = Instant::now();
    let mut success_count = 0;

    for i in 0..num_operations {
        let key = (thread_id * 10000) as u64 + i as u64;

        // Create test data with some pattern
        let mut data = [0u8; 32];
        for (j, item) in data.iter_mut().enumerate() {
            *item = ((key + j as u64) % 256) as u8;
        }

        let upsert_ctx = StressUpsertContext {
            key,
            value: StressData {
                id: key,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                data,
            },
        };

        let status = kv.upsert(&upsert_ctx);
        if status == Status::Ok || status == Status::Pending {
            success_count += 1;
        }

        // Occasionally try to read
        if i % 10 == 0 && key > 0 {
            let mut read_ctx = StressReadContext {
                key: key - 1, // Try to read previous key
                value: None,
            };
            let _ = kv.read(&mut read_ctx);
        }
    }

    let duration = start_time.elapsed();

    // Store results
    let mut results = results.lock().unwrap();
    results.push((thread_id, duration, success_count));
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 RsKv Stress Concurrent Test");
    println!("==============================================");

    let temp_dir = "/tmp/rskv_stress_concurrent_test";
    if Path::new(temp_dir).exists() {
        std::fs::remove_dir_all(temp_dir)?;
    }
    std::fs::create_dir_all(temp_dir)?;

    println!("📦 Initializing KV store...");
    let disk = FileSystemDisk::new(temp_dir)?;
    let kv = Arc::new(RsKv::<u64, StressData, FileSystemDisk>::new(
        1 << 25,
        1 << 19,
        disk,
    )?); // 32MB log, 512K table
    println!("✅ KV store initialized successfully");

    // Stress test configurations
    let test_configs = vec![
        (4, 1000, "4 threads, 1K operations each"),
        (8, 1000, "8 threads, 1K operations each"),
        (16, 1000, "16 threads, 1K operations each"),
        (32, 500, "32 threads, 500 operations each"),
    ];

    for (num_threads, ops_per_thread, test_name) in test_configs {
        println!("\n🔧 {}", test_name);
        println!("{}", "=".repeat(test_name.len() + 4));

        let barrier = Arc::new(Barrier::new(num_threads));
        let results = Arc::new(Mutex::new(Vec::new()));
        let mut handles = vec![];

        // Create worker threads
        for thread_id in 0..num_threads {
            let kv_clone = Arc::clone(&kv);
            let barrier_clone = Arc::clone(&barrier);
            let results_clone = Arc::clone(&results);

            let handle = thread::spawn(move || {
                stress_worker(
                    kv_clone,
                    barrier_clone,
                    thread_id,
                    ops_per_thread,
                    results_clone,
                );
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        let start_time = Instant::now();

        for handle in handles {
            handle.join().unwrap();
        }

        let total_duration = start_time.elapsed();

        // Collect and analyze results
        let results = results.lock().unwrap();
        let mut total_success = 0;
        let mut max_duration = Duration::new(0, 0);
        let mut min_duration = Duration::from_secs(3600);

        for (_thread_id, duration, success) in results.iter() {
            total_success += success;
            if *duration > max_duration {
                max_duration = *duration;
            }
            if *duration < min_duration {
                min_duration = *duration;
            }
        }

        let total_ops = num_threads * ops_per_thread;
        let ops_per_sec = total_success as f64 / max_duration.as_secs_f64();
        let success_rate = (total_success as f64 / total_ops as f64) * 100.0;

        println!(
            "  ✅ Completed {} operations across {} threads",
            total_success, num_threads
        );
        println!("  ⏱️  Total time: {:?}", total_duration);
        println!("  ⏱️  Max thread time: {:?}", max_duration);
        println!("  ⏱️  Min thread time: {:?}", min_duration);
        println!("  📊 Total rate: {:.2} ops/sec", ops_per_sec);
        println!(
            "  📊 Per-thread rate: {:.2} ops/sec",
            ops_per_sec / num_threads as f64
        );
        println!("  📈 Success rate: {:.2}%", success_rate);

        // Check for load balancing
        let duration_variance = max_duration.as_nanos() as f64 - min_duration.as_nanos() as f64;
        let avg_duration = (max_duration.as_nanos() as f64 + min_duration.as_nanos() as f64) / 2.0;
        let load_balance = 1.0 - (duration_variance / avg_duration);
        println!("  ⚖️  Load balance: {:.2}%", load_balance * 100.0);
    }

    // Memory stress test
    println!("\n💾 Memory Stress Test");
    println!("{}", "=".repeat(25));

    let barrier = Arc::new(Barrier::new(8));
    let results = Arc::new(Mutex::new(Vec::new()));
    let mut handles = vec![];

    // Create 8 threads with high memory usage
    for thread_id in 0..8 {
        let kv_clone = Arc::clone(&kv);
        let barrier_clone = Arc::clone(&barrier);
        let results_clone = Arc::clone(&results);

        let handle = thread::spawn(move || {
            barrier_clone.wait();

            let start_time = Instant::now();
            let mut success_count = 0;

            // Each thread does 2000 operations with large data
            for i in 0..2000 {
                let key = (thread_id * 20000) as u64 + i as u64;

                let mut data = [0u8; 32];
                for (j, item) in data.iter_mut().enumerate() {
                    *item = ((key + j as u64) % 256) as u8;
                }

                let upsert_ctx = StressUpsertContext {
                    key,
                    value: StressData {
                        id: key,
                        timestamp: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_secs(),
                        data,
                    },
                };

                let status = kv_clone.upsert(&upsert_ctx);
                if status == Status::Ok || status == Status::Pending {
                    success_count += 1;
                }
            }

            let duration = start_time.elapsed();
            let mut results = results_clone.lock().unwrap();
            results.push((thread_id, duration, success_count));
        });
        handles.push(handle);
    }

    let start_time = Instant::now();

    for handle in handles {
        handle.join().unwrap();
    }

    let total_duration = start_time.elapsed();

    let results = results.lock().unwrap();
    let mut total_success = 0;
    let mut max_duration = Duration::new(0, 0);

    for (_, duration, success) in results.iter() {
        total_success += success;
        if *duration > max_duration {
            max_duration = *duration;
        }
    }

    let ops_per_sec = total_success as f64 / max_duration.as_secs_f64();
    println!(
        "  ✅ Completed {} operations in {:?}",
        total_success, total_duration
    );
    println!("  📊 Rate: {:.2} ops/sec", ops_per_sec);
    println!(
        "  💾 Memory usage: ~{:.2} MB",
        (total_success * std::mem::size_of::<StressData>()) as f64 / (1024.0 * 1024.0)
    );

    // Clean up
    std::fs::remove_dir_all(temp_dir)?;
    println!("\n🧹 Cleanup completed");
    println!("\n🎉 Stress concurrent test completed successfully!");

    Ok(())
}

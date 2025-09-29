use rskv::core::status::Status;
use rskv::device::file_system_disk::FileSystemDisk;
use rskv::rskv_core::{RsKv, ReadContext, UpsertContext};
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq, Default)]
struct SimpleData {
    value: u64,
}

struct SimpleUpsertContext {
    key: u64,
    value: SimpleData,
}

impl UpsertContext for SimpleUpsertContext {
    type Key = u64;
    type Value = SimpleData;

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

struct SimpleReadContext {
    key: u64,
    value: Option<SimpleData>,
}

impl ReadContext for SimpleReadContext {
    type Key = u64;
    type Value = SimpleData;

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

fn writer_thread(
    kv: Arc<RsKv<u64, SimpleData, FileSystemDisk>>,
    barrier: Arc<Barrier>,
    thread_id: usize,
    num_operations: usize,
) -> (usize, Duration) {
    barrier.wait(); // Wait for all threads to start

    let start_time = Instant::now();
    let mut success_count = 0;

    for i in 0..num_operations {
        let key = (thread_id * 1000) as u64 + i as u64;
        let upsert_ctx = SimpleUpsertContext {
            key,
            value: SimpleData { value: key * 2 },
        };

        let status = kv.upsert(&upsert_ctx);
        if status == Status::Ok || status == Status::Pending {
            success_count += 1;
        }
    }

    let duration = start_time.elapsed();
    (success_count, duration)
}

fn reader_thread(
    kv: Arc<RsKv<u64, SimpleData, FileSystemDisk>>,
    barrier: Arc<Barrier>,
    thread_id: usize,
    num_operations: usize,
) -> (usize, Duration) {
    barrier.wait(); // Wait for all threads to start

    let start_time = Instant::now();
    let mut success_count = 0;

    for i in 0..num_operations {
        let key = (thread_id * 1000) as u64 + i as u64;

        let mut read_ctx = SimpleReadContext { key, value: None };

        let status = kv.read(&mut read_ctx);
        if status == Status::Ok && read_ctx.value.is_some() {
            success_count += 1;
        }
    }

    let duration = start_time.elapsed();
    (success_count, duration)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 RsKv Concurrent Test");
    println!("=======================================");

    let temp_dir = "/tmp/rskv_concurrent_test";
    if Path::new(temp_dir).exists() {
        std::fs::remove_dir_all(temp_dir)?;
    }
    std::fs::create_dir_all(temp_dir)?;

    println!("📦 Initializing KV store...");
    let disk = FileSystemDisk::new(temp_dir)?;
    let kv = Arc::new(RsKv::<u64, SimpleData, FileSystemDisk>::new(
        1 << 24,
        1 << 18,
        disk,
    )?); // 16MB log, 256K table
    println!("✅ KV store initialized successfully");

    // Test configurations
    let test_configs = vec![
        (2, 100, "2 threads, 100 operations each"),
        (4, 100, "4 threads, 100 operations each"),
        (8, 100, "8 threads, 100 operations each"),
    ];

    for (num_threads, ops_per_thread, test_name) in test_configs {
        println!("\n🔧 {}", test_name);
        println!("{}", "=".repeat(test_name.len() + 4));

        let barrier = Arc::new(Barrier::new(num_threads));
        let mut handles = vec![];

        // Create writer threads
        for thread_id in 0..num_threads / 2 {
            let kv_clone = Arc::clone(&kv);
            let barrier_clone = Arc::clone(&barrier);

            let handle = thread::spawn(move || {
                writer_thread(kv_clone, barrier_clone, thread_id, ops_per_thread)
            });
            handles.push(handle);
        }

        // Create reader threads
        for thread_id in num_threads / 2..num_threads {
            let kv_clone = Arc::clone(&kv);
            let barrier_clone = Arc::clone(&barrier);

            let handle = thread::spawn(move || {
                reader_thread(kv_clone, barrier_clone, thread_id, ops_per_thread)
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        let start_time = Instant::now();
        let mut total_success = 0;
        let mut max_duration = Duration::new(0, 0);

        for handle in handles {
            let (success, duration) = handle.join().unwrap();
            total_success += success;
            if duration > max_duration {
                max_duration = duration;
            }
        }

        let total_duration = start_time.elapsed();
        let total_ops = num_threads * ops_per_thread;
        let ops_per_sec = total_success as f64 / max_duration.as_secs_f64();

        println!(
            "  ✅ Completed {} operations across {} threads",
            total_success, num_threads
        );
        println!("  ⏱️  Total time: {:?}", total_duration);
        println!("  ⏱️  Total OPS: {:?}", total_ops);
        println!("  ⏱️  Max thread time: {:?}", max_duration);
        println!("  📊 Total rate: {:.2} ops/sec", ops_per_sec);
        println!(
            "  📊 Per-thread rate: {:.2} ops/sec",
            ops_per_sec / num_threads as f64
        );
    }

    // Clean up
    std::fs::remove_dir_all(temp_dir)?;
    println!("\n🧹 Cleanup completed");
    println!("\n🎉 Concurrent test completed successfully!");

    Ok(())
}

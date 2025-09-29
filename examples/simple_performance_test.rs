use rskv::core::status::Status;
use rskv::device::file_system_disk::FileSystemDisk;
use rskv::rskv_core::{RsKv, ReadContext, UpsertContext};
use std::path::Path;
use std::time::Instant;

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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 RsKv Simple Performance Test");
    println!("===============================================");

    let temp_dir = "/tmp/rskv_simple_performance_test";
    if Path::new(temp_dir).exists() {
        std::fs::remove_dir_all(temp_dir)?;
    }
    std::fs::create_dir_all(temp_dir)?;

    println!("📦 Initializing KV store...");
    let disk = FileSystemDisk::new(temp_dir)?;
    let kv = RsKv::<u64, SimpleData, FileSystemDisk>::new(1 << 22, 1 << 17, disk)?; // 4MB log, 128K table
    println!("✅ KV store initialized successfully");

    // Test 1: Small batch upsert
    println!("\n🔧 Test 1: Small Batch Upsert (100 operations)");
    let start_time = Instant::now();
    let mut success_count = 0;

    for i in 1..=100 {
        let upsert_ctx = SimpleUpsertContext {
            key: i,
            value: SimpleData { value: i * 2 },
        };

        let status = kv.upsert(&upsert_ctx);
        if status == Status::Ok || status == Status::Pending {
            success_count += 1;
        }
    }

    let duration = start_time.elapsed();
    println!(
        "  ✅ Completed {} upsert operations in {:?}",
        success_count, duration
    );
    println!(
        "  📊 Rate: {:.2} ops/sec",
        success_count as f64 / duration.as_secs_f64()
    );

    // Test 2: Small batch read
    println!("\n📖 Test 2: Small Batch Read (100 operations)");
    let start_time = Instant::now();
    let mut success_count = 0;

    for i in 1..=100 {
        let mut read_ctx = SimpleReadContext {
            key: i,
            value: None,
        };

        let status = kv.read(&mut read_ctx);
        if status == Status::Ok && read_ctx.value.is_some() {
            success_count += 1;
        }
    }

    let duration = start_time.elapsed();
    println!(
        "  ✅ Completed {} read operations in {:?}",
        success_count, duration
    );
    println!(
        "  📊 Rate: {:.2} ops/sec",
        success_count as f64 / duration.as_secs_f64()
    );

    // Test 3: Medium batch upsert
    println!("\n🔧 Test 3: Medium Batch Upsert (1K operations)");
    let start_time = Instant::now();
    let mut success_count = 0;

    for i in 1..=1000 {
        let upsert_ctx = SimpleUpsertContext {
            key: i + 1000,
            value: SimpleData { value: i * 3 },
        };

        let status = kv.upsert(&upsert_ctx);
        if status == Status::Ok || status == Status::Pending {
            success_count += 1;
        }

        if i % 100 == 0 {
            println!("  Progress: {}/1000 operations completed", i);
        }
    }

    let duration = start_time.elapsed();
    println!(
        "  ✅ Completed {} upsert operations in {:?}",
        success_count, duration
    );
    println!(
        "  📊 Rate: {:.2} ops/sec",
        success_count as f64 / duration.as_secs_f64()
    );

    // Test 4: Mixed workload
    println!("\n🔄 Test 4: Mixed Workload (500 reads + 500 upserts)");
    let start_time = Instant::now();
    let mut success_count = 0;

    for i in 1..=1000 {
        if i % 2 == 0 {
            // Read operation
            let mut read_ctx = SimpleReadContext {
                key: i / 2,
                value: None,
            };
            let status = kv.read(&mut read_ctx);
            if status == Status::Ok && read_ctx.value.is_some() {
                success_count += 1;
            }
        } else {
            // Upsert operation
            let upsert_ctx = SimpleUpsertContext {
                key: i + 2000,
                value: SimpleData { value: i * 4 },
            };
            let status = kv.upsert(&upsert_ctx);
            if status == Status::Ok || status == Status::Pending {
                success_count += 1;
            }
        }

        if i % 200 == 0 {
            println!("  Progress: {}/1000 operations completed", i);
        }
    }

    let duration = start_time.elapsed();
    println!(
        "  ✅ Completed {} mixed operations in {:?}",
        success_count, duration
    );
    println!(
        "  📊 Rate: {:.2} ops/sec",
        success_count as f64 / duration.as_secs_f64()
    );

    // Clean up
    std::fs::remove_dir_all(temp_dir)?;
    println!("\n🧹 Cleanup completed");
    println!("\n🎉 Simple performance test completed successfully!");

    Ok(())
}

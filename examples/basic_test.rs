use rskv::core::status::Status;
use rskv::device::file_system_disk::FileSystemDisk;
use rskv::rskv_core::{RsKv, ReadContext, UpsertContext};
use std::path::Path;

// Simple test context implementations
struct TestUpsertContext {
    key: u64,
    value: String,
}

impl UpsertContext for TestUpsertContext {
    type Key = u64;
    type Value = String;

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
        false // Always use RCU path for simplicity
    }
}

struct TestReadContext {
    key: u64,
    value: Option<String>,
}

impl ReadContext for TestReadContext {
    type Key = u64;
    type Value = String;

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
    println!("RsKv Basic Test");

    // Create a temporary directory for testing
    let temp_dir = "/tmp/rskv_test";
    if Path::new(temp_dir).exists() {
        std::fs::remove_dir_all(temp_dir)?;
    }
    std::fs::create_dir_all(temp_dir)?;

    // Initialize the KV store
    let disk = FileSystemDisk::new(temp_dir)?;
    let kv = RsKv::<u64, String, FileSystemDisk>::new(1 << 20, 1 << 16, disk)?; // 1MB log, 64K table

    println!("✓ KV store initialized successfully");

    // Test upsert operation
    let upsert_ctx = TestUpsertContext {
        key: 123,
        value: "Hello, RsKv!".to_string(),
    };

    let status = kv.upsert(&upsert_ctx);
    match status {
        Status::Ok => println!("✓ Upsert operation successful"),
        _ => println!("✗ Upsert operation failed: {:?}", status),
    }

    // Test read operation
    let mut read_ctx = TestReadContext {
        key: 123,
        value: None,
    };

    let status = kv.read(&mut read_ctx);
    match status {
        Status::Ok => {
            if let Some(value) = read_ctx.value {
                println!("✓ Read operation successful: {}", value);
            } else {
                println!("✗ Read operation returned Ok but no value");
            }
        }
        Status::NotFound => println!("✗ Key not found"),
        _ => println!("✗ Read operation failed: {:?}", status),
    }

    // Clean up
    std::fs::remove_dir_all(temp_dir)?;
    println!("✓ Test completed and cleaned up");

    Ok(())
}

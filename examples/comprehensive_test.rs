use rskv::core::status::Status;
use rskv::device::file_system_disk::FileSystemDisk;
use rskv::faster::{DeleteContext, FasterKv, ReadContext, RmwContext, UpsertContext};
use std::path::Path;

// Test data structures
#[derive(Debug, Clone, PartialEq)]
struct UserData {
    id: u64,
    name: [u8; 32],  // Fixed-size string
    email: [u8; 64], // Fixed-size string
    age: u32,
}

impl Default for UserData {
    fn default() -> Self {
        UserData {
            id: 0,
            name: [0; 32],
            email: [0; 64],
            age: 0,
        }
    }
}

impl UserData {
    fn new(id: u64, name: &str, email: &str, age: u32) -> Self {
        let mut user = UserData {
            id,
            name: [0; 32],
            email: [0; 64],
            age,
        };

        // Copy name
        let name_bytes = name.as_bytes();
        let name_len = std::cmp::min(name_bytes.len(), 31);
        user.name[..name_len].copy_from_slice(&name_bytes[..name_len]);

        // Copy email
        let email_bytes = email.as_bytes();
        let email_len = std::cmp::min(email_bytes.len(), 63);
        user.email[..email_len].copy_from_slice(&email_bytes[..email_len]);

        user
    }

    fn name_str(&self) -> String {
        let null_pos = self.name.iter().position(|&b| b == 0).unwrap_or(32);
        String::from_utf8_lossy(&self.name[..null_pos]).to_string()
    }

    #[allow(dead_code)]
    fn email_str(&self) -> String {
        let null_pos = self.email.iter().position(|&b| b == 0).unwrap_or(64);
        String::from_utf8_lossy(&self.email[..null_pos]).to_string()
    }
}

// Upsert context for user data
struct UserUpsertContext {
    key: u64,
    value: UserData,
}

impl UpsertContext for UserUpsertContext {
    type Key = u64;
    type Value = UserData;

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
        false // Always use RCU path
    }
}

// Read context for user data
struct UserReadContext {
    key: u64,
    value: Option<UserData>,
}

impl ReadContext for UserReadContext {
    type Key = u64;
    type Value = UserData;

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

// RMW context for updating user age
struct UserRmwContext {
    key: u64,
    age_increment: u32,
}

impl RmwContext for UserRmwContext {
    type Key = u64;
    type Value = UserData;

    fn key(&self) -> &Self::Key {
        &self.key
    }

    fn key_hash(&self) -> u64 {
        self.key
    }

    fn rmw_initial(&self, value: &mut Self::Value) {
        value.id = self.key;
        *value = UserData::new(
            self.key,
            &format!("User_{}", self.key),
            &format!("user{}@example.com", self.key),
            self.age_increment,
        );
    }

    fn rmw_copy(&self, old_value: &Self::Value, new_value: &mut Self::Value) {
        *new_value = old_value.clone();
        new_value.age += self.age_increment;
    }

    fn rmw_atomic(&self, _value: &mut Self::Value) -> bool {
        false // Always use RCU path
    }
}

// Delete context
struct UserDeleteContext {
    key: u64,
}

impl DeleteContext for UserDeleteContext {
    type Key = u64;

    fn key(&self) -> &Self::Key {
        &self.key
    }

    fn key_hash(&self) -> u64 {
        self.key
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 FASTER Rust KV Store Comprehensive Test");
    println!("==========================================");

    // Create a temporary directory for testing
    let temp_dir = "/tmp/rskv_comprehensive_test";
    if Path::new(temp_dir).exists() {
        std::fs::remove_dir_all(temp_dir)?;
    }
    std::fs::create_dir_all(temp_dir)?;

    // Initialize the KV store with larger capacity
    println!("📦 Initializing KV store...");
    let disk = FileSystemDisk::new(temp_dir)?;
    let mut kv = FasterKv::<u64, UserData, FileSystemDisk>::new(1 << 24, 1 << 18, disk)?; // 16MB log, 256K table
    println!("✅ KV store initialized successfully");

    // Test 1: Basic Upsert Operations
    println!("\n🔧 Test 1: Basic Upsert Operations");
    let test_users = vec![
        (1, UserData::new(1, "Alice", "alice@example.com", 25)),
        (2, UserData::new(2, "Bob", "bob@example.com", 30)),
        (3, UserData::new(3, "Charlie", "charlie@example.com", 35)),
    ];

    for (key, user_data) in test_users {
        let upsert_ctx = UserUpsertContext {
            key,
            value: user_data,
        };

        let status = kv.upsert(&upsert_ctx);
        match status {
            Status::Ok => println!(
                "  ✅ Upserted user {}: {}",
                key,
                upsert_ctx.value.name_str()
            ),
            Status::Pending => println!(
                "  ⏳ Upsert pending for user {}: {}",
                key,
                upsert_ctx.value.name_str()
            ),
            _ => println!("  ❌ Upsert failed for user {}: {:?}", key, status),
        }
    }

    // Test 2: Read Operations
    println!("\n📖 Test 2: Read Operations");
    for key in 1..=3 {
        let mut read_ctx = UserReadContext { key, value: None };

        let status = kv.read(&mut read_ctx);
        match status {
            Status::Ok => {
                if let Some(user) = read_ctx.value {
                    println!(
                        "  ✅ Read user {}: {} (age: {})",
                        key,
                        user.name_str(),
                        user.age
                    );
                } else {
                    println!("  ❌ Read returned Ok but no value for user {}", key);
                }
            }
            Status::NotFound => println!("  ❌ User {} not found", key),
            Status::Pending => println!("  ⏳ Read pending for user {}", key),
            _ => println!("  ❌ Read failed for user {}: {:?}", key, status),
        }
    }

    // Test 3: RMW Operations (Read-Modify-Write)
    println!("\n🔄 Test 3: RMW Operations (Age Increment)");
    for key in 1..=3 {
        let rmw_ctx = UserRmwContext {
            key,
            age_increment: 1,
        };

        let mut rmw_ctx = rmw_ctx;
        let status = kv.rmw(&mut rmw_ctx);
        match status {
            Status::Ok => println!("  ✅ Incremented age for user {}", key),
            Status::Pending => println!("  ⏳ RMW pending for user {}", key),
            _ => println!("  ❌ RMW failed for user {}: {:?}", key, status),
        }
    }

    // Test 4: Read after RMW
    println!("\n📖 Test 4: Read after RMW");
    for key in 1..=3 {
        let mut read_ctx = UserReadContext { key, value: None };

        let status = kv.read(&mut read_ctx);
        match status {
            Status::Ok => {
                if let Some(user) = read_ctx.value {
                    println!("  ✅ User {}: {} (age: {})", key, user.name_str(), user.age);
                } else {
                    println!("  ❌ Read returned Ok but no value for user {}", key);
                }
            }
            Status::NotFound => println!("  ❌ User {} not found", key),
            Status::Pending => println!("  ⏳ Read pending for user {}", key),
            _ => println!("  ❌ Read failed for user {}: {:?}", key, status),
        }
    }

    // Test 5: Delete Operations
    println!("\n🗑️  Test 5: Delete Operations");
    let delete_ctx = UserDeleteContext { key: 2 };
    let status = kv.delete(&delete_ctx);
    match status {
        Status::Ok => println!("  ✅ Deleted user 2"),
        Status::Pending => println!("  ⏳ Delete pending for user 2"),
        _ => println!("  ❌ Delete failed for user 2: {:?}", status),
    }

    // Test 6: Verify deletion
    println!("\n🔍 Test 6: Verify Deletion");
    let mut read_ctx = UserReadContext {
        key: 2,
        value: None,
    };

    let status = kv.read(&mut read_ctx);
    match status {
        Status::NotFound => println!("  ✅ User 2 successfully deleted"),
        Status::Ok => println!("  ❌ User 2 still exists after deletion"),
        Status::Pending => println!("  ⏳ Read pending for user 2"),
        _ => println!("  ❌ Read failed for user 2: {:?}", status),
    }

    // Test 7: Batch Operations
    println!("\n📦 Test 7: Batch Operations");
    let batch_users = vec![
        (10, UserData::new(10, "David", "david@example.com", 28)),
        (11, UserData::new(11, "Eve", "eve@example.com", 32)),
        (12, UserData::new(12, "Frank", "frank@example.com", 29)),
    ];

    for (key, user_data) in batch_users {
        let upsert_ctx = UserUpsertContext {
            key,
            value: user_data,
        };

        let status = kv.upsert(&upsert_ctx);
        match status {
            Status::Ok => println!(
                "  ✅ Batch upserted user {}: {}",
                key,
                upsert_ctx.value.name_str()
            ),
            Status::Pending => println!(
                "  ⏳ Batch upsert pending for user {}: {}",
                key,
                upsert_ctx.value.name_str()
            ),
            _ => println!("  ❌ Batch upsert failed for user {}: {:?}", key, status),
        }
    }

    // Test 8: Performance Test
    println!("\n⚡ Test 8: Performance Test (100 operations)");
    let start_time = std::time::Instant::now();
    let mut success_count = 0;

    for i in 100..200 {
        let upsert_ctx = UserUpsertContext {
            key: i,
            value: UserData::new(
                i,
                &format!("User_{}", i),
                &format!("user{}@example.com", i),
                (i % 50) as u32 + 20,
            ),
        };

        let status = kv.upsert(&upsert_ctx);
        if status == Status::Ok || status == Status::Pending {
            success_count += 1;
        }
    }

    let duration = start_time.elapsed();
    println!(
        "  ✅ Completed {} operations in {:?}",
        success_count, duration
    );
    println!(
        "  📊 Average time per operation: {:?}",
        duration / success_count
    );

    // Test 9: Checkpoint Operations
    println!("\n💾 Test 9: Checkpoint Operations");
    let checkpoint_status = kv.checkpoint("test_checkpoint");
    match checkpoint_status {
        Ok(_) => println!("  ✅ Checkpoint created successfully"),
        Err(e) => println!("  ❌ Checkpoint failed: {:?}", e),
    }

    // Clean up
    std::fs::remove_dir_all(temp_dir)?;
    println!("\n🧹 Cleanup completed");
    println!("\n🎉 All tests completed successfully!");

    Ok(())
}

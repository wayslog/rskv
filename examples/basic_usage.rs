//! Basic usage example for rskv
//!
//! This example demonstrates the core functionality of the rskv key-value store.

use rskv::{Config, RsKv};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    env_logger::init();

    // Create a temporary directory for this example
    let temp_dir = tempfile::tempdir()?;

    // Configure the rskv instance
    let config = Config {
        storage_dir: temp_dir.path().to_string_lossy().to_string(),
        memory_size: 64 * 1024 * 1024, // 64MB
        enable_checkpointing: true,
        checkpoint_interval_ms: 5000,
        enable_gc: true,
        gc_interval_ms: 10000,
        ..Default::default()
    };

    println!("🚀 Initializing rskv with config: {:?}", config);

    // Create the key-value store
    let store = RsKv::new(config).await?;

    println!("✅ rskv initialized successfully");

    // Basic operations
    println!("\n📝 Demonstrating basic operations...");

    // Insert some data
    let key1 = b"user:1001".to_vec();
    let value1 = b"Alice".to_vec();

    let key2 = b"user:1002".to_vec();
    let value2 = b"Bob".to_vec();

    let key3 = b"config:timeout".to_vec();
    let value3 = b"30".to_vec();

    store.upsert(key1.clone(), value1.clone()).await?;
    store.upsert(key2.clone(), value2.clone()).await?;
    store.upsert(key3.clone(), value3.clone()).await?;

    println!("✅ Inserted 3 key-value pairs");

    // Read the data back
    if let Some(value) = store.read(&key1).await? {
        println!(
            "🔍 Read key 'user:1001': {}",
            String::from_utf8_lossy(&value)
        );
    }

    if let Some(value) = store.read(&key2).await? {
        println!(
            "🔍 Read key 'user:1002': {}",
            String::from_utf8_lossy(&value)
        );
    }

    if let Some(value) = store.read(&key3).await? {
        println!(
            "🔍 Read key 'config:timeout': {}",
            String::from_utf8_lossy(&value)
        );
    }

    // Check if keys exist
    println!("\n🔎 Checking key existence...");
    println!(
        "Key 'user:1001' exists: {}",
        store.contains_key(&key1).await?
    );
    println!(
        "Key 'user:9999' exists: {}",
        store.contains_key(&b"user:9999".to_vec()).await?
    );

    // Update a value
    println!("\n✏️  Updating values...");
    let new_value1 = b"Alice Smith".to_vec();
    store.upsert(key1.clone(), new_value1.clone()).await?;

    if let Some(value) = store.read(&key1).await? {
        println!(
            "🔍 Updated 'user:1001': {}",
            String::from_utf8_lossy(&value)
        );
    }

    // Demonstrate prefix scanning
    println!("\n🔍 Prefix scan for 'user:' keys...");
    let user_entries = store.scan_prefix(b"user:").await?;
    for (key, value) in user_entries {
        println!(
            "  {} = {}",
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(&value)
        );
    }

    // Delete a key
    println!("\n🗑️  Deleting key 'user:1002'...");
    store.delete(&key2).await?;

    println!(
        "Key 'user:1002' exists after deletion: {}",
        store.contains_key(&key2).await?
    );

    // Show statistics
    println!("\n📊 Store statistics:");
    let stats = store.stats();
    println!("  Index entries: {}", stats.index_entries);
    println!("  Log tail address: 0x{:x}", stats.log_tail_address);
    println!("  Mutable region size: {} bytes", stats.mutable_region_size);
    println!(
        "  Read-only region size: {} bytes",
        stats.read_only_region_size
    );
    println!("  Disk region size: {} bytes", stats.disk_region_size);

    // Perform a checkpoint
    println!("\n💾 Performing checkpoint...");
    store.checkpoint().await?;
    println!("✅ Checkpoint completed");

    // Scan all remaining data
    println!("\n📋 All remaining data:");
    let all_entries = store.scan_all().await?;
    for (key, value) in all_entries {
        println!(
            "  {} = {}",
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(&value)
        );
    }

    // Close the store
    println!("\n🔒 Closing store...");
    store.close().await?;
    println!("✅ Store closed successfully");

    println!("\n🎉 Example completed successfully!");

    Ok(())
}

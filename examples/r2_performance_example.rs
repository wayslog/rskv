use rskv::core::status::Status;
use rskv::r2::R2Kv;
use rskv::rskv_core::{ReadContext, UpsertContext};
use std::hash::{Hash, Hasher};

// Simple context implementations
struct SimpleUpsertContext {
    key: u64,
    value: u64,
    key_hash: u64,
}

impl SimpleUpsertContext {
    fn new(key: u64, value: u64) -> Self {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        Self {
            key,
            value,
            key_hash: hasher.finish(),
        }
    }
}

impl UpsertContext for SimpleUpsertContext {
    type Key = u64;
    type Value = u64;

    fn key(&self) -> &u64 {
        &self.key
    }

    fn value(&self) -> &u64 {
        &self.value
    }

    fn key_hash(&self) -> u64 {
        self.key_hash
    }

    fn put_atomic(&self, value: &mut u64) -> bool {
        *value = self.value;
        true
    }
}

struct SimpleReadContext {
    key: u64,
    key_hash: u64,
    value: Option<u64>,
}

impl SimpleReadContext {
    fn new(key: u64) -> Self {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        Self {
            key,
            key_hash: hasher.finish(),
            value: None,
        }
    }
}

impl ReadContext for SimpleReadContext {
    type Key = u64;
    type Value = u64;

    fn key(&self) -> &u64 {
        &self.key
    }

    fn key_hash(&self) -> u64 {
        self.key_hash
    }

    fn get(&mut self, value: &u64) {
        self.value = Some(*value);
    }
}

fn main() {
    println!("R2Kv Performance Optimization Example");
    println!("=====================================\n");

    // Create R2Kv instance with performance tracking
    let r2 = R2Kv::<u64, u64>::new("r2_perf_hot", "r2_perf_cold")
        .expect("Failed to create R2Kv");

    println!("Phase 1: Writing data with hotspot pattern");
    println!("-------------------------------------------");

    // Write data with hotspot pattern: 80% access to 20% of keys
    let total_keys = 1000;
    let hot_keys = 200;

    for i in 0..10000 {
        let key = if i % 10 < 8 {
            // 80% access to hot keys
            i % hot_keys
        } else {
            // 20% access to cold keys
            hot_keys + (i % (total_keys - hot_keys))
        };

        let ctx = SimpleUpsertContext::new(key, key * 100);
        let status = r2.upsert(&ctx);
        if status != Status::Ok {
            eprintln!("Upsert failed for key {}: {:?}", key, status);
        }
    }

    println!("Written 10000 operations with hotspot pattern\n");

    // Get access statistics
    println!("Phase 2: Analyzing access patterns");
    println!("-----------------------------------");

    let access_stats = r2.get_access_stats();
    println!("Access Statistics:");
    println!("  Total accesses: {}", access_stats.total_accesses);
    println!("  Unique keys: {}", access_stats.unique_keys);
    println!("  Read count: {}", access_stats.read_count);
    println!("  Write count: {}", access_stats.write_count);
    println!("  Dominant pattern: {:?}", access_stats.dominant_pattern);
    println!("  Hot spot concentration: {:.2}%", access_stats.hotspot_concentration * 100.0);
    println!("  Sequential ratio: {:.2}%", access_stats.sequential_ratio * 100.0);
    println!("  Temporal locality score: {:.2}", access_stats.temporal_locality_score);
    println!();

    // Get migration statistics
    let migration_stats = r2.get_migration_stats();
    println!("Migration Statistics:");
    println!("  Current hot storage: {} bytes", migration_stats.current_hot_size);
    println!("  Max hot storage: {} bytes", migration_stats.max_hot_size);
    println!("  Hot utilization: {:.2}%", migration_stats.hot_utilization() * 100.0);
    println!("  Total migrations: {}", migration_stats.total_migrations);
    println!("  Successful migrations: {}", migration_stats.successful_migrations);
    println!("  Success rate: {:.2}%", migration_stats.success_rate() * 100.0);
    println!();

    // Get recommendations
    let recommendation = r2.get_access_recommendation();
    println!("Performance Recommendation:");
    println!("  Detected pattern: {:?}", recommendation.pattern);
    println!("  Suggestion: {}", recommendation.suggestion);
    println!("  Recommended cache size factor: {:.2}x", recommendation.cache_size_factor);
    println!("  Migration aggressiveness: {:.2}", recommendation.migration_aggressiveness);
    println!();

    // Get hot keys
    println!("Top 10 Hot Keys:");
    println!("----------------");
    let hot_keys = r2.get_hot_keys(10);
    for (rank, (key_hash, access_count)) in hot_keys.iter().enumerate() {
        println!("  #{}: Key hash {} - {} accesses", rank + 1, key_hash, access_count);
    }
    println!();

    // Phase 3: Read operations
    println!("Phase 3: Performing read operations");
    println!("------------------------------------");

    let mut hits = 0;
    let mut misses = 0;

    for key in 0..100 {
        let mut ctx = SimpleReadContext::new(key);
        let status = r2.read(&mut ctx);
        if status == Status::Ok {
            hits += 1;
        } else {
            misses += 1;
        }
    }

    println!("Read 100 keys:");
    println!("  Hits: {}", hits);
    println!("  Misses: {}", misses);
    println!("  Hit rate: {:.2}%", (hits as f64 / 100.0) * 100.0);
    println!();

    // Final statistics
    println!("Final Statistics");
    println!("----------------");
    let final_stats = r2.get_access_stats();
    println!("  Total operations: {}", final_stats.total_accesses);
    println!("  Read/Write ratio: {:.2}",
        final_stats.read_count as f64 / final_stats.write_count.max(1) as f64);
    println!();

    println!("Performance analysis complete!");
    println!("\nNote: This example demonstrates the integrated performance");
    println!("monitoring and optimization features of R2Kv, including:");
    println!("  - Access pattern analysis");
    println!("  - Migration management");
    println!("  - Hot key detection");
    println!("  - Automatic performance recommendations");
}
#![no_main]

use libfuzzer_sys::fuzz_target;
use rskv::rskv_core::{RsKv, UpsertContext, ReadContext, RmwContext, DeleteContext};
use rskv::device::file_system_disk::FileSystemDisk;
use rskv::core::status::Status;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

// 简化的测试数据结构
#[derive(Debug, Clone, PartialEq)]
struct FuzzData {
    id: u64,
    counter: u64,
    data: [u8; 16], // 16字节的随机数据
    checksum: u32,
}

impl Default for FuzzData {
    fn default() -> Self {
        FuzzData {
            id: 0,
            counter: 0,
            data: [0; 16],
            checksum: 0,
        }
    }
}

impl FuzzData {
    fn new(id: u64, counter: u64, data: &[u8]) -> Self {
        let mut fuzz_data = [0u8; 16];
        let copy_len = std::cmp::min(data.len(), 16);
        fuzz_data[..copy_len].copy_from_slice(&data[..copy_len]);
        
        let checksum = Self::calculate_checksum(&fuzz_data);
        
        FuzzData {
            id,
            counter,
            data: fuzz_data,
            checksum,
        }
    }
    
    fn calculate_checksum(data: &[u8; 16]) -> u32 {
        let mut checksum = 0u32;
        for &byte in data.iter() {
            checksum = checksum.wrapping_add(byte as u32);
        }
        checksum
    }
    
    fn verify_checksum(&self) -> bool {
        self.checksum == Self::calculate_checksum(&self.data)
    }
}

// Upsert context
struct FuzzUpsertContext {
    key: u64,
    value: FuzzData,
}

impl UpsertContext for FuzzUpsertContext {
    type Key = u64;
    type Value = FuzzData;

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

// Read context
struct FuzzReadContext {
    key: u64,
    value: Option<FuzzData>,
}

impl ReadContext for FuzzReadContext {
    type Key = u64;
    type Value = FuzzData;

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

// RMW context
struct FuzzRmwContext {
    key: u64,
    increment: u64,
}

impl RmwContext for FuzzRmwContext {
    type Key = u64;
    type Value = FuzzData;

    fn key(&self) -> &Self::Key {
        &self.key
    }

    fn key_hash(&self) -> u64 {
        self.key
    }

    fn rmw_initial(&self, value: &mut Self::Value) {
        *value = FuzzData::new(self.key, self.increment, &[]);
    }

    fn rmw_copy(&self, old_value: &Self::Value, new_value: &mut Self::Value) {
        *new_value = old_value.clone();
        new_value.counter += self.increment;
    }

    fn rmw_atomic(&self, _value: &mut Self::Value) -> bool {
        false
    }
}

// Delete context
struct FuzzDeleteContext {
    key: u64,
}

impl DeleteContext for FuzzDeleteContext {
    type Key = u64;

    fn key(&self) -> &Self::Key {
        &self.key
    }

    fn key_hash(&self) -> u64 {
        self.key
    }
}

fuzz_target!(|data: &[u8]| {
    // 确保有足够的数据进行测试
    if data.len() < 16 {
        return;
    }
    
    // 创建临时目录
    let temp_dir = "/tmp/rskv_concurrent_fuzz_test";
    if Path::new(temp_dir).exists() {
        let _ = std::fs::remove_dir_all(temp_dir);
    }
    let _ = std::fs::create_dir_all(temp_dir);
    
    // 初始化KV存储
    let disk = match FileSystemDisk::new(temp_dir) {
        Ok(disk) => disk,
        Err(_) => return,
    };
    
    let kv = match RsKv::<u64, FuzzData, FileSystemDisk>::new(
        1 << 20, // 1MB log
        1 << 16, // 64KB table
        disk
    ) {
        Ok(kv) => Arc::new(kv),
        Err(_) => return,
    };
    
    // 从fuzz数据中提取测试参数
    let mut offset = 0;
    
    // 提取线程数 (1-4)
    let num_threads = if data.len() > offset {
        (data[offset] % 4) + 1
    } else {
        return;
    };
    offset += 1;
    
    // 提取键范围
    let key_range = if data.len() >= offset + 8 {
        let key_bytes = &data[offset..offset + 8];
        let key = u64::from_le_bytes([
            key_bytes[0], key_bytes[1], key_bytes[2], key_bytes[3],
            key_bytes[4], key_bytes[5], key_bytes[6], key_bytes[7],
        ]);
        offset += 8;
        key % 100 // 限制键范围到0-99
    } else {
        return;
    };
    
    // 提取操作数
    let num_ops = if data.len() > offset {
        (data[offset] % 10) + 1
    } else {
        return;
    };
    offset += 1;
    
    // 使用剩余数据作为fuzz数据
    let fuzz_data = if data.len() > offset {
        &data[offset..]
    } else {
        &[]
    };
    
    // 创建线程
    let mut handles = Vec::new();
    
    for thread_id in 0..num_threads {
        let kv_clone = Arc::clone(&kv);
        let thread_fuzz_data = fuzz_data.to_vec();
        let thread_key_range = key_range + (thread_id as u64 * 10); // 每个线程使用不同的键范围
        
        let handle = thread::spawn(move || {
            for op_id in 0..num_ops {
                let operation = (op_id as u64 + thread_id as u64) % 4;
                let key = thread_key_range + (op_id as u64 % 10);
                let increment = (op_id as u64 % 5) + 1;
                
                // 为每个操作创建不同的fuzz数据
                let op_fuzz_data = if !thread_fuzz_data.is_empty() {
                    let start = (op_id as usize * 4) % thread_fuzz_data.len();
                    let end = std::cmp::min(start + 4, thread_fuzz_data.len());
                    &thread_fuzz_data[start..end]
                } else {
                    &[]
                };
                
                match operation {
                    0 => {
                        // Upsert操作
                        let value = FuzzData::new(key, 1, op_fuzz_data);
                        let ctx = FuzzUpsertContext { key, value };
                        let _ = kv_clone.upsert(&ctx);
                    }
                    1 => {
                        // Read操作
                        let mut ctx = FuzzReadContext { key, value: None };
                        let _ = kv_clone.read(&mut ctx);
                    }
                    2 => {
                        // RMW操作
                        let mut ctx = FuzzRmwContext { key, increment };
                        let _ = kv_clone.rmw(&mut ctx);
                    }
                    3 => {
                        // Delete操作
                        let ctx = FuzzDeleteContext { key };
                        let _ = kv_clone.delete(&ctx);
                    }
                    _ => unreachable!(),
                }
            }
        });
        
        handles.push(handle);
    }
    
    // 等待所有线程完成
    for handle in handles {
        let _ = handle.join();
    }
    
    // 清理
    let _ = std::fs::remove_dir_all(temp_dir);
});

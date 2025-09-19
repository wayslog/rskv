#![no_main]

use libfuzzer_sys::fuzz_target;
use rskv::faster::{FasterKv, UpsertContext, ReadContext, RmwContext, DeleteContext};
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
    data: [u8; 32], // 32字节的随机数据
    checksum: u32,
}

impl Default for FuzzData {
    fn default() -> Self {
        FuzzData {
            id: 0,
            counter: 0,
            data: [0; 32],
            checksum: 0,
        }
    }
}

impl FuzzData {
    fn new(id: u64, counter: u64, data: &[u8]) -> Self {
        let mut fuzz_data = [0u8; 32];
        let copy_len = std::cmp::min(data.len(), 32);
        fuzz_data[..copy_len].copy_from_slice(&data[..copy_len]);
        
        let checksum = Self::calculate_checksum(&fuzz_data);
        
        FuzzData {
            id,
            counter,
            data: fuzz_data,
            checksum,
        }
    }
    
    fn calculate_checksum(data: &[u8; 32]) -> u32 {
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
    if data.len() < 8 {
        return;
    }
    
    // 创建临时目录
    let temp_dir = "/tmp/rskv_fuzz_test";
    if Path::new(temp_dir).exists() {
        let _ = std::fs::remove_dir_all(temp_dir);
    }
    let _ = std::fs::create_dir_all(temp_dir);
    
    // 初始化KV存储
    let disk = match FileSystemDisk::new(temp_dir) {
        Ok(disk) => disk,
        Err(_) => return,
    };
    
    let kv = match FasterKv::<u64, FuzzData, FileSystemDisk>::new(
        1 << 20, // 1MB log
        1 << 16, // 64KB table
        disk
    ) {
        Ok(kv) => Arc::new(kv),
        Err(_) => return,
    };
    
    // 从fuzz数据中提取测试参数
    let mut offset = 0;
    
    // 提取键范围
    let key_range = if data.len() >= 8 {
        let key_bytes = &data[offset..offset + 8];
        let key = u64::from_le_bytes([
            key_bytes[0], key_bytes[1], key_bytes[2], key_bytes[3],
            key_bytes[4], key_bytes[5], key_bytes[6], key_bytes[7],
        ]);
        offset += 8;
        key % 1000 // 限制键范围到0-999
    } else {
        return;
    };
    
    // 提取操作类型
    let operation = if data.len() > offset {
        data[offset] % 4
    } else {
        return;
    };
    offset += 1;
    
    // 提取增量值
    let increment = if data.len() > offset {
        data[offset] as u64 % 10 + 1
    } else {
        1
    };
    offset += 1;
    
    // 使用剩余数据作为fuzz数据
    let fuzz_data = if data.len() > offset {
        &data[offset..]
    } else {
        &[]
    };
    
    // 执行操作
    match operation {
        0 => {
            // Upsert操作
            let value = FuzzData::new(key_range, 1, fuzz_data);
            let ctx = FuzzUpsertContext { key: key_range, value };
            let _ = kv.upsert(&ctx);
        }
        1 => {
            // Read操作
            let mut ctx = FuzzReadContext { key: key_range, value: None };
            let _ = kv.read(&mut ctx);
        }
        2 => {
            // RMW操作
            let mut ctx = FuzzRmwContext { key: key_range, increment };
            let _ = kv.rmw(&mut ctx);
        }
        3 => {
            // Delete操作
            let ctx = FuzzDeleteContext { key: key_range };
            let _ = kv.delete(&ctx);
        }
        _ => unreachable!(),
    }
    
    // 清理
    let _ = std::fs::remove_dir_all(temp_dir);
});

# rskv Implementation Plan (Cursor Rules)

This document provides a step-by-step guide for implementing the `rskv` key-value store. Follow these rules sequentially. Create new files and modules as specified.

## Rule 0: Project Setup & Basic Types

1.  **Create `src/common.rs`**.
2.  In this file, define the core data types:
    -   `pub type Address = u64;`
    -   `pub type Key = Vec<u8>;`
    -   `pub type Value = Vec<u8>;`
3.  Define a custom error enum using `thiserror` for handling all internal errors.
    ```rust
    use thiserror::Error;

    #[derive(Error, Debug)]
    pub enum RsKvError {
        #[error("IO Error: {0}")]
        Io(#[from] std::io::Error),
        // Add more error variants as needed
    }
    ```
4.  **Create `src/epoch.rs`**.
5.  In this file, create a struct `EpochManager` that wraps `crossbeam_epoch`. It should provide simple `protect()` and `defer()` methods for the rest of the codebase to use.

## Rule 1: `HybridLog` (hlog) Implementation

1.  **Create `src/hlog.rs`**.
2.  Define the `HybridLog` struct. It should contain:
    -   `pages: Vec<Box<[u8]>>` for the in-memory ring buffer.
    -   The four atomic pointers: `head_address`, `read_only_address`, etc., using `std::sync::atomic::AtomicU64`.
    -   A `tail_page_offset` using a custom `AtomicPageOffset` struct wrapping `AtomicU64`.
    -   An `Arc<EpochManager>`.
    -   A handle to the disk/storage layer (define a `StorageDevice` trait for this).
3.  Implement the `allocate(&self, size: u32) -> Option<Address>` method using `fetch_add` on the tail offset.
4.  Implement the `get(&self, address: Address) -> &[u8]` method, which includes the `address % buffer_size` logic.
5.  Implement the `new_page(&self)` logic using `compare_exchange`.
6.  The state transition logic (`shift_read_only_address`, etc.) should be implemented as internal methods that are called by a background task. For now, you can leave stubs for these.

## Rule 2: `MemHashIndex` Implementation

1.  **Create `src/index.rs`**.
2.  Define the `MemHashIndex` struct.
3.  For the initial implementation, use `dashmap::DashMap<Key, Address>` as the internal storage.
4.  Implement the following methods:
    -   `new() -> Self`
    -   `find(&self, key: &Key) -> Option<Address>`
    -   `insert(&self, key: Key, address: Address)`
    -   `remove(&self, key: &Key)`

## Rule 3: `RsKv` Top-Level Struct

1.  **Modify `src/lib.rs`**.
2.  Define the main `RsKv<K, V>` struct. It should contain:
    -   `hlog: Arc<HybridLog>`
    -   `index: Arc<MemHashIndex>`
    -   `epoch: Arc<EpochManager>`
3.  Implement the public API methods:
    -   `new(config: Config) -> Self` (define a `Config` struct for initialization parameters).
    -   `upsert(&self, key: Key, value: Value) -> Result<(), RsKvError>`
        -   This method will first serialize the key and value, then call `hlog.allocate()` to get an address, write the data to the log, and finally call `index.insert()`.
    -   `read(&self, key: &Key) -> Result<Option<Value>, RsKvError>`
        -   This method will call `index.find()`, and if an address is found, call `hlog.get()` to retrieve and deserialize the data.
    -   `delete(&self, key: &Key) -> Result<(), RsKvError>`
        -   This can be implemented by inserting a special "tombstone" record into the log.

## Rule 4: `CheckpointState` and `GcState`

1.  **Create `src/checkpoint.rs`**.
2.  Define the `CheckpointMetadata` and `LogMetadata` structs. Derive `serde::Serialize` and `serde::Deserialize`.
3.  Define the `CheckpointState` struct to hold the metadata and state atomics (`flush_pending`, etc.).
4.  **Create `src/gc.rs`**.
5.  Define the `GcState` struct. It should contain `new_begin_address` and the `next_chunk` atomic counter.

## Rule 5: Background Task Manager

1.  **Create `src/background.rs`**.
2.  Define a `BackgroundTaskManager` struct.
3.  It should have a `start(self)` method that spawns several `tokio` background tasks:
    -   **Hlog State Task**: A task that periodically calls internal methods on `hlog` to advance the `read_only_address` and `head_address`.
    -   **Checkpoint Task**: A task that, when triggered, executes the checkpointing logic using the `CheckpointState` machine.
    -   **GC Task**: A task that, when triggered, executes the GC logic using the `GcState` machine and `rayon` for parallel index scanning.
4.  The `RsKv` struct should hold an instance of this manager and start it upon creation.

## Rule 6: Integration and Testing

1.  Flesh out the logic for all methods, ensuring components interact correctly.
2.  Write unit tests for each module.
3.  Write integration tests that perform concurrent reads, writes, and deletions, and verify the state after a checkpoint and recovery cycle.

# rskv Architecture

This document outlines the internal architecture of `rskv`, a high-performance key-value store inspired by FASTER.

## 1. Core Components

The system is composed of four primary, interacting components:

1.  **`RsKv`**: The top-level struct and public API, orchestrating all other components.
2.  **`HybridLog` (`hlog`)**: A concurrent, append-only, ring-buffer-based log for storing all key-value data.
3.  **`MemHashIndex`**: A concurrent hash map that stores `Key -> Address` mappings.
4.  **`BackgroundTaskManager`**: A component responsible for managing background tasks like checkpointing and garbage collection.

---

## 2. `HybridLog` (The Hybrid Log)

The `HybridLog` is the heart of the storage engine. It is a large, in-memory, circular buffer (ring buffer) composed of fixed-size pages.

### Key Concepts:

- **Logical vs. Physical Space**: The log has a logically infinite, ever-increasing address space. This logical space is mapped onto the finite, physical ring buffer in memory.
- **Four Atomic Pointers**: The state of the log is defined by four atomic pointers that delineate regions in the logical address space:
    - `begin_address`: The logical start of the log. Data before this is truncated.
    - `head_address`: The start of the in-memory portion of the log. Data between `begin` and `head` is on disk only.
    - `read_only_address`: The boundary between the immutable and mutable regions. Data between `head` and `read_only` is in memory, read-only, and can be safely flushed to disk.
    - `tail_address`: The end of the log, where new data is appended. Data between `read_only` and `tail` is in memory and mutable.

### Operations:

- **Allocation**: Implemented via a lock-free `fetch_add` on the `tail_address`, making writes extremely fast.
- **State Transitions**: The `read_only_address` and `head_address` are advanced via background tasks, coordinated by an epoch-based synchronization mechanism (`crossbeam-epoch`). When the `read_only_address` is advanced, a flush-to-disk operation is triggered on the newly immutable region.

---

## 3. `MemHashIndex` (The Hash Index)

The `MemHashIndex` provides fast, concurrent access to the location of data within the `HybridLog`.

### Key Concepts:

- **Structure**: It is a concurrent hash map. A good starting point is `dashmap::DashMap`, but a custom implementation using sharded locks or lock-free techniques may be a future optimization.
- **Mapping**: It stores a mapping from a `Key` to an `Address` (a `u64` pointing into the `HybridLog`).
- **Concurrency**: All operations on the index must be thread-safe. Updates to an existing key's address are performed using atomic Compare-and-Swap (CAS) operations to prevent lost updates.

---

## 4. Checkpointing and Recovery

Non-blocking checkpoints provide crash consistency.

### Key Concepts:

- **`CheckpointState`**: A state machine object that tracks the progress of a checkpoint.
- **Two-Part Snapshot**: A complete checkpoint consists of two files:
    1.  A snapshot of the `MemHashIndex` at a specific point in time.
    2.  A snapshot of the `HybridLog` up to the `tail_address` corresponding to that point in time.
- **Process**:
    1.  Initiate checkpoint: Freeze the `read_only_address` and record key metadata.
    2.  Flush the `HybridLog` and `MemHashIndex` to disk concurrently.
    3.  Write a final metadata file containing the tokens and addresses needed for recovery.
- **Recovery**: On startup, the system loads the latest checkpoint metadata, restores the index from its snapshot, and replays the log from the checkpointed address to recover the most recent state.

---

## 5. Garbage Collection (GC)

GC reclaims disk and memory space from obsolete log records.

### Key Concepts:

- **`GcState`**: A state machine object that tracks the progress of a GC cycle.
- **Parallel Cleanup**: The process is designed to be parallel and run in the background.
    1.  Determine a new `begin_address` based on the last successful checkpoint.
    2.  Scan the `MemHashIndex` in parallel (using `rayon`) to remove any entries pointing to addresses older than the new `begin_address`.
    3.  Once the index is clean, truncate the physical log file.

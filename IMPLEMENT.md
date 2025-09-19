# IMPLEMENT PLAN

   1. 地基 (`core` 模块): 我将从最核心的数据结构开始，这些是整个系统的基石。
       * Address 和 AtomicAddress: 日志的逻辑寻址方案。这是最基本的部分。
       * Record: 日志中数据的内存布局。
       * Epoch: 基于 crossbeam-epoch 的 Rust 风格封装，用于并发内存管理。
       * Utilities: 哈希函数和其他辅助工具。


   2. 哈希索引 (`index` 模块): 接下来，我会实现内存中的哈希索引。
       * HashBucket 和 HashTable: 核心的哈希表结构。
       * MemHashIndex: 主要的内存索引实现。

   3. 日志分配器 (`hlog` 模块): 然后，我会实现日志结构的分配器，它负责管理日志中的内存。


   4. 存储抽象 (`device` 和 `environment` 模块): 我会为文件系统 I/O 创建抽象层，使存储持久化。

   5. `FasterKv` 存储: 当基础组件就绪后，我会实现主要的 FasterKv 结构，将所有组件整合在一起，并移植核心的 KV 操作 (Read,
      Upsert, Rmw, Delete)。


   6. 高级功能: 最后，我会处理更高级的功能，如检查点、恢复以及 F2Kv 两层存储。
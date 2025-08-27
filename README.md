# rskv: A High-Performance Key-Value Store in Rust

![Coverage](https://img.shields.io/badge/coverage-59.12%25-yellow)
![Tests](https://img.shields.io/badge/tests-44%20passed-brightgreen)
![Build](https://img.shields.io/badge/build-passing-brightgreen)

`rskv` is a high-performance, concurrent, persistent key-value store inspired by the design of Microsoft's FASTER. It is designed from the ground up in Rust to leverage modern hardware and concurrency models, focusing on safety, speed, and low-latency operations.

This project is being developed with the assistance of an AI programming agent, following a detailed architectural and implementation plan.

## Core Features

- **Hybrid Storage Engine**: Combines a large in-memory hot data region with a fast, disk-backed log for data larger than memory. All writes go to a sequential, append-only log.
- **Concurrent Hash Index**: A highly concurrent, lock-free hash index that maps keys to their latest data versions in the log.
- **Non-Blocking Checkpoints**: A mechanism to create consistent snapshots of the entire database estado without pausing incoming write or read operations, enabling robust persistence and fast recovery.
- **Epoch-Based Garbage Collection**: A safe, background garbage collection mechanism to reclaim log space without blocking foreground operations.

## Project Structure

- `src/`: Main source code.
  - `lib.rs`: Main library entry point, defines the top-level `RsKv` struct.
  - `hlog.rs`: The Hybrid Log (`hlog`) implementation.
  - `index.rs`: The concurrent hash index implementation.
  - `checkpoint.rs`: Checkpoint and recovery logic.
  - `gc.rs`: Garbage collection logic.
  - `epoch.rs`: Epoch management utilities.
  - `common.rs`: Common types, like `Address` and custom errors.
- `docs/`: Project documentation and implementation plan.
  - `ARCHITECTURE.md`: Detailed description of the system architecture.
  - `IMPLEMENTATION_PLAN.md`: Step-by-step implementation guide (Cursor Rules).
- `scripts/`: Development and testing scripts.
  - `coverage.sh`: Generate test coverage reports.
  - `coverage.ps1`: PowerShell version for Windows.
- `coverage/`: Test coverage reports and artifacts.

## Testing & Coverage

rskv has comprehensive test coverage with **44 test cases** covering all major functionality:

### Test Coverage: 59.12% (655/1108 lines)

| Module | Coverage | Status |
|--------|----------|---------|
| **index.rs** | 91.11% | 🟢 Excellent |
| **gc.rs** | 86.11% | 🟢 Excellent |
| **checkpoint.rs** | 85.45% | 🟢 Excellent |
| **background.rs** | 71.55% | 🟡 Good |
| **rskv.rs** | 71.43% | 🟡 Good |
| **metrics.rs** | 56.39% | 🟡 Moderate |
| **epoch.rs** | 57.45% | 🟡 Moderate |
| **hlog.rs** | 37.37% | 🔴 Needs Improvement |
| **common.rs** | 16.36% | 🔴 Needs Improvement |

### Running Tests

```bash
# Run all tests
cargo test

# Generate coverage report
./scripts/coverage.sh

# Or use Makefile
make coverage
make coverage-open  # Opens HTML report
```

See [`COVERAGE_REPORT.md`](COVERAGE_REPORT.md) for detailed coverage analysis.

## 🚀 Performance Testing

rskv includes comprehensive performance benchmarks to evaluate performance under various workloads:

### Performance Test Categories

- **Write Performance**: Different value sizes (1B to 100KB)
- **Read Performance**: Cache hit rates and disk I/O efficiency  
- **Mixed Workloads**: Various read/write ratios (0% to 99% reads)
- **Concurrent Operations**: Multi-threaded scalability (1-32 threads)
- **Thread Scaling**: Linear scalability analysis and efficiency metrics
- **High Concurrency**: Stress testing with 200+ concurrent threads
- **Batch Operations**: Bulk operation efficiency
- **Scan Operations**: Data traversal and prefix scanning

### Running Performance Tests

```bash
# Quick performance test (core scenarios)
make perf-quick

# Thread scaling & concurrency tests
make perf-threads

# Interactive performance demos
make perf-demo
make perf-concurrency

# Comprehensive performance test with reports
make performance

# Individual test groups
cargo bench --bench performance -- write_performance
cargo bench --bench performance -- thread_scaling
cargo bench --bench performance -- mixed_workload
```

### Performance Highlights

**🚀 Multi-threaded Write Performance**
- Single thread: 27,717 ops/s  
- 4 threads: 109,271 ops/s (3.94x scaling)
- 8 threads: 168,711 ops/s (peak performance)

**🔥 Mixed Workload Performance**  
- Peak: 572,794 ops/s (8 threads)
- Ultra-low latency: 1.75 µs
- High concurrency: 200+ threads supported

### Performance Reports

Performance tests generate detailed reports including:
- **HTML Reports**: Interactive Criterion.rs reports in `target/criterion/`
- **System Analysis**: Comprehensive performance summaries in [`PERFORMANCE_RESULTS.md`](PERFORMANCE_RESULTS.md)
- **Concurrency Analysis**: Multi-threading deep dive in [`CONCURRENCY_RESULTS.md`](CONCURRENCY_RESULTS.md)
- **Trend Analysis**: Performance regression detection

See [`PERFORMANCE.md`](PERFORMANCE.md) for detailed performance testing guide.

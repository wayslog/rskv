# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

RSKV is a high-performance key-value store implementation in Rust based on Microsoft FASTER. It provides concurrent access, persistence, and efficient memory management using epoch-based concurrency control. The project includes two main implementations:

1. **FasterKv**: Core FASTER implementation with hot data storage
2. **F2Kv**: Two-tier storage system with hot/cold data separation and automatic migration

## Build Commands

```bash
# Standard Rust build
cargo build --all-features

# Release build
cargo build --release --all-features

# Build with make
make build
make build-release
```

## Test Commands

```bash
# Run all tests
cargo test --all-features
make test

# Run F2 specific tests
cargo test f2_tests
./run_f2_tests.sh

# Run specific examples
cargo run --example f2_basic_example
cargo run --example f2_cold_hot_migration_test
cargo run --example f2_comprehensive_test
cargo run --example f2_migration_stress_test

# Run with make
make run-example EXAMPLE=f2_basic_example
```

## Lint and Format Commands

```bash
# Format code
cargo fmt --all
make format

# Check formatting
cargo fmt --all -- --check
make format-check

# Run clippy
cargo clippy --all-targets --all-features -- -D warnings
make clippy

# Run all checks
make check
make lint
```

## Development Workflow

```bash
# Quick development check
make quick-check

# Full CI pipeline simulation
make ci

# Generate coverage report
make coverage

# Run security audit
make audit
```

## Architecture Overview

### Core Components

- **`core/` module**: Foundation components including address management, memory allocation, epoch-based concurrency, and record structures
- **`index/` module**: Hash table implementations with bucket-based indexing for both hot (mem_index) and cold (cold_index) data
- **`hlog/` module**: Hybrid log implementation with persistent memory allocation
- **`device/` module**: Storage device abstraction with file system disk implementation
- **`environment/` module**: Environment abstraction for file operations

### Key Implementations

- **`faster.rs`**: Main FasterKv implementation providing core KV operations (Read, Upsert, RMW, Delete)
- **`f2.rs`**: F2Kv two-tier storage with automatic hot/cold data migration

### Data Flow Architecture

1. **Write Path**: All new data goes to hot storage first
2. **Read Path**: Check hot storage first, fallback to cold storage
3. **Migration**: RMW operations on cold data trigger migration to hot storage
4. **Concurrency**: Uses crossbeam-epoch for lock-free memory management

### Context Pattern

The codebase uses context patterns for operations:
- `UpsertContext`: For insert/update operations
- `ReadContext`: For read operations
- `RmwContext`: For read-modify-write operations

## Testing Strategy

### F2 Testing Suite

The F2 implementation has comprehensive testing:
- Basic functionality tests
- Cold/hot migration tests
- Concurrent access tests
- Performance benchmarks
- Stress tests

Run the complete F2 test suite with: `./run_f2_tests.sh`

### Unit Tests

Core functionality tests are located in `src/f2_tests.rs` and `src/f2/tests.rs`.

## Key Development Notes

- This is a Rust port of Microsoft FASTER C++ implementation
- Uses epoch-based memory management for concurrent access
- Supports data sets larger than memory through hot/cold separation
- All operations return Status enum (Ok, NotFound, Pending, etc.)
- Async operations may return Pending status requiring retry logic
- Uses fixed-page-size allocators for memory efficiency

## Examples Location

All usage examples are in the `examples/` directory with comprehensive documentation in F2_MIGRATION_GUIDE.md.
Never use emoji in test cases and docs.
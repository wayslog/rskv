# Makefile for rskv project
# Provides convenient commands for development and CI

.PHONY: help check test build clean format clippy audit coverage fuzz benchmark install-tools test-r2 test-r2-full

# Default target
help:
	@echo "Available commands:"
	@echo "  make check        - Run all checks (format, clippy, audit, build, test)"
	@echo "  make test         - Run tests"
	@echo "  make test-r2      - Run R2Kv test suite"
	@echo "  make test-r2-full - Run complete R2Kv test suite with examples"
	@echo "  make build        - Build the project"
	@echo "  make clean        - Clean build artifacts"
	@echo "  make format       - Format code"
	@echo "  make clippy       - Run clippy lints"
	@echo "  make audit        - Run security audit"
	@echo "  make coverage     - Generate coverage report"
	@echo "  make fuzz         - Run fuzz tests"
	@echo "  make benchmark    - Run benchmarks"
	@echo "  make install-tools - Install required development tools"

# Install development tools
install-tools:
	@echo "Installing development tools..."
	rustup component add rustfmt clippy llvm-tools-preview
	cargo install cargo-audit cargo-tarpaulin cargo-fuzz cargo-criterion

# Format code
format:
	@echo "Formatting code..."
	cargo fmt --all

# Check formatting
format-check:
	@echo "Checking code formatting..."
	cargo fmt --all -- --check

# Run clippy
clippy:
	@echo "Running clippy..."
	cargo clippy --all-targets --all-features -- -D warnings

# Run security audit
audit:
	@echo "Running security audit..."
	cargo audit

# Build project
build:
	@echo "Building project..."
	cargo build --all-features

# Build release
build-release:
	@echo "Building release..."
	cargo build --release --all-features

# Run tests
test:
	@echo "Running tests..."
	cargo test --all-features

# Run R2Kv test suite (unit tests only)
test-r2:
	@echo "Running R2Kv Test Suite"
	@echo "======================="
	@echo ""
	@echo "Running R2Kv unit tests..."
	cargo test --lib r2::tests --all-features
	@echo ""
	@echo "Running performance optimization tests..."
	cargo test --lib performance:: --all-features
	@echo ""
	@echo "R2Kv Test Suite Complete!"

# Run complete R2Kv test suite with examples
test-r2-full:
	@echo "Running Complete R2Kv Test Suite"
	@echo "================================="
	@echo ""
	@echo "1. Running basic operations test..."
	cargo run --example r2_basic_example
	@echo ""
	@echo "2. Running cold/hot migration test..."
	cargo run --example r2_cold_hot_migration_test
	@echo ""
	@echo "3. Running comprehensive test..."
	cargo run --example r2_comprehensive_test
	@echo ""
	@echo "4. Running migration stress test..."
	cargo run --example r2_migration_stress_test
	@echo ""
	@echo "5. Running performance example..."
	cargo run --example r2_performance_example
	@echo ""
	@echo "6. Running all unit tests..."
	cargo test --all-features
	@echo ""
	@echo "Complete R2Kv Test Suite Finished!"

# Run examples
examples:
	@echo "Running examples..."
	cargo run --example basic_test

# Generate coverage report
coverage:
	@echo "Generating coverage report..."
	cargo tarpaulin --out Xml --output-dir coverage/ --all-features
	@echo "Coverage report generated in coverage/"

# Run fuzz tests
fuzz:
	@echo "Running fuzz tests..."
	cd fuzz && cargo fuzz run rskv_fuzz -- -max_total_time=300
	cd fuzz && cargo fuzz run concurrent_fuzz -- -max_total_time=300

# Run benchmarks
benchmark:
	@echo "Running benchmarks..."
	cargo bench --all-features

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	cargo clean

# Run all checks
check: format-check clippy audit build test
	@echo "All checks passed!"

# CI pipeline simulation
ci: format-check clippy audit build test coverage
	@echo "CI pipeline completed!"

# Development setup
dev-setup: install-tools
	@echo "Development environment setup complete!"

# Quick check (without coverage)
quick-check: format-check build test
	@echo "Quick check completed!"

# Full check with everything
full-check: format-check clippy audit build test coverage fuzz benchmark
	@echo "Full check completed!"

# Run specific example
run-example:
	@echo "Available examples:"
	@echo ""
	@echo "R2Kv Examples (Two-tier storage with performance monitoring):"
	@echo "  make run-example EXAMPLE=r2_basic_example"
	@echo "  make run-example EXAMPLE=r2_cold_hot_migration_test"
	@echo "  make run-example EXAMPLE=r2_comprehensive_test"
	@echo "  make run-example EXAMPLE=r2_migration_stress_test"
	@echo "  make run-example EXAMPLE=r2_performance_example"
	@echo ""
	@echo "RsKv Examples (Core storage):"
	@echo "  make run-example EXAMPLE=basic_test"
	@echo "  make run-example EXAMPLE=comprehensive_test"
	@echo "  make run-example EXAMPLE=concurrent_test"
	@echo "  make run-example EXAMPLE=simple_performance_test"
	@echo "  make run-example EXAMPLE=stress_concurrent_test"
ifdef EXAMPLE
	cargo run --example $(EXAMPLE)
else
	@echo ""
	@echo "Please specify EXAMPLE=example_name"
endif

# Documentation
docs:
	@echo "Generating documentation..."
	cargo doc --all-features --no-deps --open

# Check documentation
docs-check:
	@echo "Checking documentation..."
	cargo doc --all-features --no-deps --document-private-items

# Lint everything
lint: format-check clippy audit
	@echo "Linting completed!"

# Test everything
test-all: test examples fuzz benchmark
	@echo "All tests completed!"

# Clean everything
clean-all: clean
	@echo "Cleaning fuzz artifacts..."
	rm -rf fuzz/artifacts/
	rm -rf fuzz/corpus/
	@echo "Everything cleaned!"

# Show project info
info:
	@echo "Project: rskv"
	@echo "Rust version: $(shell rustc --version)"
	@echo "Cargo version: $(shell cargo --version)"
	@echo "Project structure:"
	@find . -name "*.rs" -not -path "./target/*" | head -10
	@echo "..."

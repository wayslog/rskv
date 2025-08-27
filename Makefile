# Makefile for rskv project

.PHONY: test coverage coverage-open clean build release docs lint fmt check help

# Default target
help:
	@echo "Available targets:"
	@echo "  test          - Run all tests"
	@echo "  coverage      - Generate test coverage report"
	@echo "  coverage-open - Generate coverage and open HTML report"
	@echo "  clean         - Clean build artifacts"
	@echo "  build         - Build the project"
	@echo "  release       - Build in release mode"
	@echo "  docs          - Generate documentation"
	@echo "  lint          - Run clippy linter"
	@echo "  fmt           - Format code"
	@echo "  check         - Check code without building"
	@echo "  bench         - Run criterion benchmarks"
	@echo "  performance   - Run comprehensive performance tests"
	@echo "  perf-quick    - Run quick performance tests"
	@echo "  perf-threads  - Run thread scaling tests"
	@echo "  perf-demo     - Run interactive performance demo"
	@echo "  perf-concurrency - Run concurrency performance demo"

# Run tests
test:
	cargo test --all-features --workspace

# Generate test coverage
coverage:
	@if command -v cargo-tarpaulin >/dev/null 2>&1; then \
		cargo tarpaulin --all-features --workspace --out xml --out html --out json --out lcov && \
		mkdir -p coverage && \
		mv cobertura.xml coverage/ && \
		mv lcov.info coverage/ && \
		mv tarpaulin-report.html coverage/ && \
		mv tarpaulin-report.json coverage/ && \
		date > coverage/timestamp.txt && \
		echo "![Coverage](https://img.shields.io/badge/coverage-$$(grep -o 'line-rate="[0-9.]*"' coverage/cobertura.xml | grep -o '[0-9.]*' | awk '{printf "%.2f", $$1*100}')%25-yellow)" > coverage/coverage_badge.md; \
	else \
		echo "cargo-tarpaulin not found. Installing..."; \
		cargo install cargo-tarpaulin; \
		chmod +x scripts/coverage.sh && ./scripts/coverage.sh; \
	fi

# Generate coverage and open HTML report
coverage-open: coverage
	@if command -v open >/dev/null 2>&1; then \
		open coverage/tarpaulin-report.html; \
	elif command -v xdg-open >/dev/null 2>&1; then \
		xdg-open coverage/tarpaulin-report.html; \
	elif command -v start >/dev/null 2>&1; then \
		start coverage/tarpaulin-report.html; \
	else \
		echo "Please open coverage/tarpaulin-report.html in your browser"; \
	fi

# Clean build artifacts
clean:
	cargo clean
	rm -rf coverage/

# Build the project
build:
	cargo build --all-features

# Build in release mode
release:
	cargo build --release --all-features

# Generate documentation
docs:
	cargo doc --all-features --open

# Run clippy linter
lint:
	cargo clippy --all-features --workspace -- -D warnings

# Format code
fmt:
	cargo fmt --all

# Check code without building
check:
	cargo check --all-features --workspace

# Install development dependencies
install-deps:
	cargo install cargo-tarpaulin
	cargo install cargo-audit
	cargo install cargo-outdated
	cargo install cargo-deny
	cargo install cargo-geiger
	cargo install cargo-deadlinks
	cargo install cargo-deny
	cargo install cargo-geiger
	cargo install cargo-deadlinks

# Security audit
audit:
	cargo audit

# Check for outdated dependencies
outdated:
	cargo outdated

# Run all quality checks
quality: fmt lint test coverage audit deny geiger deadlinks
	@echo "All quality checks completed!"

# Run cargo-deny
deny:
	cargo deny check

# Run cargo-geiger
geiger:
	cargo geiger --all-features --workspace

# Run cargo-deadlinks
deadlinks:
	cargo doc --all-features --no-deps --document-private-items
	cargo deadlinks --dir target/doc

# Benchmark
bench:
	cargo bench

# Performance testing
performance:
	@if command -v bash >/dev/null 2>&1; then \
		chmod +x scripts/benchmark.sh && ./scripts/benchmark.sh; \
	else \
		echo "Bash not available. Please run: cargo bench"; \
		cargo bench; \
	fi

# Quick performance test (subset of benchmarks)
perf-quick:
	cargo bench --bench performance -- write_performance
	cargo bench --bench performance -- read_performance
	cargo bench --bench performance -- mixed_workload

# Thread scaling performance test
perf-threads:
	cargo bench --bench performance -- thread_scaling
	cargo bench --bench performance -- high_concurrency

# Concurrency performance demo
perf-concurrency:
	RUST_LOG=info cargo run --example concurrency_demo

# Run example
example:
	cargo run --example basic_usage

# Run performance demo
perf-demo:
	RUST_LOG=info cargo run --example performance_demo

# Quick development cycle
dev: fmt check test

# CI pipeline simulation
ci: fmt lint test coverage audit
	@echo "CI pipeline completed successfully!"

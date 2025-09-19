#!/bin/bash
# Local development check script
# Run this script to perform the same checks as GitHub Actions

set -e

echo "🔍 Running local development checks..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}✓${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

# Check if we're in the right directory
if [ ! -f "Cargo.toml" ]; then
    print_error "Please run this script from the project root directory"
    exit 1
fi

# Install required tools if not present
echo "📦 Installing required tools..."

if ! command -v cargo-fmt &> /dev/null; then
    print_warning "rustfmt not found, installing..."
    rustup component add rustfmt
fi

if ! command -v cargo-clippy &> /dev/null; then
    print_warning "clippy not found, installing..."
    rustup component add clippy
fi

if ! command -v cargo-audit &> /dev/null; then
    print_warning "cargo-audit not found, installing..."
    cargo install cargo-audit
fi

if ! command -v cargo-tarpaulin &> /dev/null; then
    print_warning "cargo-tarpaulin not found, installing..."
    cargo install cargo-tarpaulin
fi

# Format check
echo "🎨 Checking code formatting..."
if cargo fmt --all -- --check; then
    print_status "Code formatting is correct"
else
    print_error "Code formatting issues found. Run 'cargo fmt' to fix."
    exit 1
fi

# Clippy check
echo "🔍 Running clippy..."
if cargo clippy --all-targets --all-features -- -D warnings; then
    print_status "Clippy checks passed"
else
    print_error "Clippy found issues. Please fix them."
    exit 1
fi

# Security audit
echo "🔒 Running security audit..."
if cargo audit; then
    print_status "Security audit passed"
else
    print_warning "Security vulnerabilities found. Please review and update dependencies."
fi

# Build check
echo "🔨 Building project..."
if cargo build --all-features; then
    print_status "Build successful"
else
    print_error "Build failed"
    exit 1
fi

# Test check
echo "🧪 Running tests..."
if cargo test --all-features; then
    print_status "All tests passed"
else
    print_error "Tests failed"
    exit 1
fi

# Coverage check (optional)
if [ "$1" = "--coverage" ]; then
    echo "📊 Generating coverage report..."
    if cargo tarpaulin --out Xml --output-dir coverage/ --all-features; then
        print_status "Coverage report generated in coverage/"
    else
        print_warning "Coverage generation failed"
    fi
fi

# Fuzz check (optional)
if [ "$1" = "--fuzz" ]; then
    echo "🔬 Running fuzz tests..."
    if [ -d "fuzz" ]; then
        cd fuzz
        if cargo fuzz build; then
            print_status "Fuzz targets built successfully"
            if timeout 60 cargo fuzz run faster_kv_fuzz -- -max_total_time=60; then
                print_status "Fuzz tests completed"
            else
                print_warning "Fuzz tests timed out or failed"
            fi
        else
            print_error "Fuzz build failed"
        fi
        cd ..
    else
        print_warning "Fuzz directory not found"
    fi
fi

print_status "All checks completed successfully! 🎉"

echo ""
echo "Usage:"
echo "  ./scripts/check.sh              # Run basic checks"
echo "  ./scripts/check.sh --coverage   # Include coverage report"
echo "  ./scripts/check.sh --fuzz       # Include fuzz testing"

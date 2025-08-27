#!/bin/bash

# Pre-commit hook script for rskv project
# This script runs before each commit to ensure code quality

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🔍 Running pre-commit checks...${NC}"

# Check if we're in a git repository
if ! git rev-parse --git-dir > /dev/null 2>&1; then
    echo -e "${RED}❌ Not in a git repository${NC}"
    exit 1
fi

# Get staged files
STAGED_FILES=$(git diff --cached --name-only --diff-filter=ACM | grep '\.rs$' || true)

if [ -z "$STAGED_FILES" ]; then
    echo -e "${YELLOW}⚠️  No Rust files staged for commit${NC}"
    exit 0
fi

echo -e "${BLUE}📁 Staged Rust files:${NC}"
echo "$STAGED_FILES"

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check code formatting
echo -e "${YELLOW}🔧 Checking code formatting...${NC}"
if ! cargo fmt --all -- --check; then
    echo -e "${RED}❌ Code formatting check failed${NC}"
    echo -e "${YELLOW}💡 Run 'cargo fmt --all' to fix formatting issues${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Code formatting check passed${NC}"

# Check clippy
echo -e "${YELLOW}🔍 Running clippy...${NC}"
if ! cargo clippy --all-features --workspace -- -D warnings; then
    echo -e "${RED}❌ Clippy check failed${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Clippy check passed${NC}"

# Check compilation
echo -e "${YELLOW}🔨 Checking compilation...${NC}"
if ! cargo check --all-features --workspace; then
    echo -e "${RED}❌ Compilation check failed${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Compilation check passed${NC}"

# Run tests
echo -e "${YELLOW}🧪 Running tests...${NC}"
if ! cargo test --all-features --workspace; then
    echo -e "${RED}❌ Tests failed${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Tests passed${NC}"

# Security audit (if cargo-audit is available)
if command_exists cargo-audit; then
    echo -e "${YELLOW}🔒 Running security audit...${NC}"
    if ! cargo audit; then
        echo -e "${RED}❌ Security audit failed${NC}"
        exit 1
    fi
    echo -e "${GREEN}✅ Security audit passed${NC}"
else
    echo -e "${YELLOW}⚠️  cargo-audit not found, skipping security audit${NC}"
fi

# Cargo-deny check (if available)
if command_exists cargo-deny; then
    echo -e "${YELLOW}📋 Running cargo-deny...${NC}"
    if ! cargo deny check; then
        echo -e "${RED}❌ Cargo-deny check failed${NC}"
        exit 1
    fi
    echo -e "${GREEN}✅ Cargo-deny check passed${NC}"
else
    echo -e "${YELLOW}⚠️  cargo-deny not found, skipping dependency check${NC}"
fi

echo -e "${GREEN}🎉 All pre-commit checks passed!${NC}"
echo -e "${BLUE}📝 Ready to commit${NC}"

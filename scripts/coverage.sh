#!/bin/bash

# Test Coverage Generation Script for rskv
# This script generates comprehensive test coverage reports using cargo-tarpaulin

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PROJECT_NAME="rskv"
COVERAGE_DIR="coverage"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

echo -e "${BLUE}🔍 Starting test coverage analysis for ${PROJECT_NAME}${NC}"
echo "Timestamp: $(date)"
echo "----------------------------------------"

# Create coverage directory if it doesn't exist
mkdir -p ${COVERAGE_DIR}

# Clean previous build artifacts to ensure accurate coverage
echo -e "${YELLOW}🧹 Cleaning previous build artifacts...${NC}"
cargo clean

echo -e "${YELLOW}📊 Running tests with coverage analysis...${NC}"

# Generate coverage report with multiple output formats
cargo tarpaulin \
    --verbose \
    --all-features \
    --workspace \
    --timeout 120 \
    --exclude-files "target/*" \
    --exclude-files "examples/*" \
    --exclude-files "benches/*" \
    --exclude-files "tests/*" \
    --ignore-panics \
    --ignore-tests \
    --out Html \
    --out Json \
    --out Lcov \
    --out Xml \
    --output-dir ${COVERAGE_DIR} \
    --skip-clean

# Check if coverage generation was successful
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Coverage analysis completed successfully!${NC}"
else
    echo -e "${RED}❌ Coverage analysis failed!${NC}"
    exit 1
fi

# Generate summary report
echo -e "${BLUE}📋 Generating coverage summary...${NC}"

# Extract coverage percentage from tarpaulin output
COVERAGE_JSON="${COVERAGE_DIR}/tarpaulin-report.json"

if [ -f "$COVERAGE_JSON" ]; then
    # Create a detailed summary using jq if available
    if command -v jq &> /dev/null; then
        echo -e "${BLUE}📊 Detailed Coverage Report${NC}"
        echo "========================================"
        
        # Overall coverage
        TOTAL_COVERAGE=$(jq -r '.files | to_entries | map(.value.coverage) | add / length' "$COVERAGE_JSON" 2>/dev/null || echo "N/A")
        echo "Overall Coverage: ${TOTAL_COVERAGE}%"
        echo ""
        
        # Per-file coverage
        echo "Per-file Coverage:"
        echo "------------------"
        jq -r '.files | to_entries[] | "\(.key): \(.value.coverage)%"' "$COVERAGE_JSON" 2>/dev/null | sort
        echo ""
        
        # Summary statistics
        echo "Summary Statistics:"
        echo "-------------------"
        TOTAL_LINES=$(jq -r '.files | to_entries | map(.value.coverage_count.covered + .value.coverage_count.uncovered) | add' "$COVERAGE_JSON" 2>/dev/null || echo "N/A")
        COVERED_LINES=$(jq -r '.files | to_entries | map(.value.coverage_count.covered) | add' "$COVERAGE_JSON" 2>/dev/null || echo "N/A")
        UNCOVERED_LINES=$(jq -r '.files | to_entries | map(.value.coverage_count.uncovered) | add' "$COVERAGE_JSON" 2>/dev/null || echo "N/A")
        
        echo "Total Lines: ${TOTAL_LINES}"
        echo "Covered Lines: ${COVERED_LINES}"
        echo "Uncovered Lines: ${UNCOVERED_LINES}"
    else
        echo -e "${YELLOW}⚠️  jq not available, showing basic coverage info${NC}"
        echo "JSON report generated at: ${COVERAGE_JSON}"
    fi
fi

# Create a timestamp file
echo "Coverage generated at: $(date)" > "${COVERAGE_DIR}/timestamp.txt"
echo "Git commit: $(git rev-parse HEAD 2>/dev/null || echo 'N/A')" >> "${COVERAGE_DIR}/timestamp.txt"
echo "Git branch: $(git branch --show-current 2>/dev/null || echo 'N/A')" >> "${COVERAGE_DIR}/timestamp.txt"

# List generated files
echo -e "${BLUE}📁 Generated files:${NC}"
ls -la ${COVERAGE_DIR}/

echo ""
echo -e "${GREEN}🎉 Coverage analysis complete!${NC}"
echo -e "${BLUE}📊 Open ${COVERAGE_DIR}/tarpaulin-report.html in your browser to view the detailed report${NC}"

# Create a simple coverage badge if coverage percentage is available
if command -v jq &> /dev/null && [ -f "$COVERAGE_JSON" ]; then
    COVERAGE_PERCENT=$(jq -r '.files | to_entries | map(.value.coverage) | add / length | floor' "$COVERAGE_JSON" 2>/dev/null)
    if [ "$COVERAGE_PERCENT" != "null" ] && [ "$COVERAGE_PERCENT" != "N/A" ]; then
        # Color based on coverage percentage
        if [ "$COVERAGE_PERCENT" -ge 90 ]; then
            BADGE_COLOR="brightgreen"
        elif [ "$COVERAGE_PERCENT" -ge 80 ]; then
            BADGE_COLOR="green"
        elif [ "$COVERAGE_PERCENT" -ge 70 ]; then
            BADGE_COLOR="yellow"
        elif [ "$COVERAGE_PERCENT" -ge 60 ]; then
            BADGE_COLOR="orange"
        else
            BADGE_COLOR="red"
        fi
        
        # Create a simple text badge
        echo "![Coverage](https://img.shields.io/badge/coverage-${COVERAGE_PERCENT}%25-${BADGE_COLOR})" > "${COVERAGE_DIR}/coverage_badge.md"
        echo -e "${BLUE}📊 Coverage badge created: ${COVERAGE_PERCENT}% (${BADGE_COLOR})${NC}"
    fi
fi

echo ""
echo "Coverage report locations:"
echo "  HTML: ${COVERAGE_DIR}/tarpaulin-report.html"
echo "  JSON: ${COVERAGE_DIR}/tarpaulin-report.json"
echo "  LCOV: ${COVERAGE_DIR}/lcov.info"
echo "  XML:  ${COVERAGE_DIR}/cobertura.xml"

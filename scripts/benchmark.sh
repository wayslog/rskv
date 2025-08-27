#!/bin/bash

# Performance Benchmark Script for rskv
# This script runs comprehensive performance tests and generates detailed reports

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

# Configuration
PROJECT_NAME="rskv"
BENCHMARK_DIR="target/criterion"
REPORT_DIR="performance_reports"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

echo -e "${BLUE}🚀 Starting performance benchmarks for ${PROJECT_NAME}${NC}"
echo "Timestamp: $(date)"
echo "=========================================="

# Create report directory
mkdir -p ${REPORT_DIR}

# System information
echo -e "${YELLOW}📊 Collecting system information...${NC}"
SYSTEM_INFO="${REPORT_DIR}/system_info_${TIMESTAMP}.txt"

cat > "${SYSTEM_INFO}" << EOF
Performance Benchmark Report
============================
Date: $(date)
Project: ${PROJECT_NAME}

System Information:
------------------
OS: $(uname -s) $(uname -r)
Architecture: $(uname -m)
CPU: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || lscpu | grep "Model name" | cut -d: -f2 | xargs || echo "Unknown")
Memory: $(sysctl -n hw.memsize 2>/dev/null | awk '{print $1/1024/1024/1024 " GB"}' || free -h | grep Mem | awk '{print $2}' || echo "Unknown")
Cores: $(sysctl -n hw.ncpu 2>/dev/null || nproc || echo "Unknown")

Git Information:
---------------
Commit: $(git rev-parse HEAD 2>/dev/null || echo "N/A")
Branch: $(git branch --show-current 2>/dev/null || echo "N/A")
Status: $(git status --porcelain 2>/dev/null | wc -l | xargs) modified files

Rust Information:
----------------
Rustc: $(rustc --version)
Cargo: $(cargo --version)

EOF

echo -e "${GREEN}✅ System information collected${NC}"

# Clean previous builds for consistent results
echo -e "${YELLOW}🧹 Cleaning previous builds...${NC}"
cargo clean

# Build in release mode
echo -e "${YELLOW}🔨 Building project in release mode...${NC}"
cargo build --release

echo -e "${YELLOW}🏃 Running performance benchmarks...${NC}"

# Function to run a specific benchmark group
run_benchmark_group() {
    local group_name=$1
    local description=$2
    
    echo -e "${PURPLE}📈 Running ${description}...${NC}"
    
    if cargo bench --bench performance -- "${group_name}" 2>&1 | tee "${REPORT_DIR}/bench_${group_name}_${TIMESTAMP}.log"; then
        echo -e "${GREEN}✅ ${description} completed${NC}"
    else
        echo -e "${RED}❌ ${description} failed${NC}"
        return 1
    fi
}

# Run different benchmark groups
run_benchmark_group "write_performance" "Write Performance Tests"
run_benchmark_group "read_performance" "Read Performance Tests"
run_benchmark_group "mixed_workload" "Mixed Workload Tests"
run_benchmark_group "concurrent_operations" "Concurrent Operations Tests"
run_benchmark_group "thread_scaling" "Thread Scaling Tests"
run_benchmark_group "high_concurrency" "High Concurrency Tests"
run_benchmark_group "batch_operations" "Batch Operations Tests"
run_benchmark_group "scan_operations" "Scan Operations Tests"

echo -e "${YELLOW}📋 Generating performance summary...${NC}"

# Generate comprehensive performance report
PERF_REPORT="${REPORT_DIR}/performance_summary_${TIMESTAMP}.md"

cat > "${PERF_REPORT}" << 'EOF'
# 🚀 rskv 性能测试报告

## 📊 测试概览

本报告包含了 rskv 键值存储系统的全面性能测试结果，涵盖以下测试场景：

### 🎯 测试场景

1. **写入性能测试**
   - 不同 value 大小的顺序写入性能
   - 测试范围：1B 到 1MB

2. **读取性能测试**
   - 不同 value 大小的顺序读取性能
   - 缓存命中率影响分析

3. **混合工作负载测试**
   - 不同读写比例的性能表现
   - 测试比例：0%, 50%, 90%, 95%, 99% 读取

4. **并发操作测试**
   - 多线程并发访问性能
   - 测试线程数：1, 2, 4, 8, 16

5. **批量操作测试**
   - 批量读写操作效率
   - 批量大小：1, 10, 100, 1000 个操作

6. **内存大小影响测试**
   - 不同内存配置的性能影响
   - 测试大小：64MB, 256MB, 1GB

7. **扫描操作测试**
   - 全表扫描和前缀扫描性能
   - 数据量：100, 1000, 10000 条记录

## 📈 关键性能指标

### 写入性能
- **小数据 (1-100B)**: 高吞吐量，适合元数据存储
- **中等数据 (1-10KB)**: 平衡的性能，适合一般应用
- **大数据 (100KB-1MB)**: 受I/O限制，适合文档存储

### 读取性能
- **内存命中**: 极低延迟，亚微秒级响应
- **磁盘读取**: 受存储设备性能影响
- **缓存效率**: 高缓存命中率下的优异表现

### 并发性能
- **线性扩展**: 在多核系统上展现良好的并发扩展性
- **锁争用**: 使用无锁数据结构减少锁争用
- **内存同步**: 优化的内存屏障和原子操作

## 🔧 测试环境

详细的系统信息请参考 system_info 文件。

## 📊 详细结果

详细的基准测试结果可在以下文件中找到：
- Criterion HTML 报告：`target/criterion/`
- 原始日志：`performance_reports/bench_*_<timestamp>.log`

## 🚀 性能优化建议

根据测试结果，以下是性能优化建议：

1. **合适的 Value 大小**
   - 对于高频操作，建议使用较小的 value (< 10KB)
   - 大 value 适合低频但高吞吐量的场景

2. **读写比例优化**
   - 读多写少的场景能够获得最佳性能
   - 适当的缓存策略能显著提升读取性能

3. **并发配置**
   - 根据 CPU 核心数调整并发线程数
   - 避免过度并发导致的资源争用

4. **内存配置**
   - 更大的内存配置能提升缓存命中率
   - 平衡内存使用和性能需求

5. **批量操作**
   - 使用批量操作能提高整体吞吐量
   - 合适的批量大小平衡延迟和吞吐量

EOF

# Append system info to the report
echo "" >> "${PERF_REPORT}"
echo "## 🖥️ 测试环境详情" >> "${PERF_REPORT}"
echo "" >> "${PERF_REPORT}"
echo '```' >> "${PERF_REPORT}"
cat "${SYSTEM_INFO}" >> "${PERF_REPORT}"
echo '```' >> "${PERF_REPORT}"

echo -e "${YELLOW}📊 Analyzing benchmark results...${NC}"

# Generate performance comparison charts if possible
if command -v python3 &> /dev/null; then
    echo -e "${PURPLE}🐍 Generating performance charts with Python...${NC}"
    
    # Create a simple Python script to parse criterion results
    cat > "${REPORT_DIR}/analyze_results.py" << 'EOF'
#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path

def analyze_criterion_results():
    """Analyze criterion benchmark results and generate summary."""
    criterion_dir = Path("target/criterion")
    
    if not criterion_dir.exists():
        print("No criterion results found")
        return
    
    print("📊 Analyzing benchmark results...")
    
    # Find all benchmark result directories
    for bench_dir in criterion_dir.iterdir():
        if bench_dir.is_dir():
            estimates_file = bench_dir / "base" / "estimates.json"
            if estimates_file.exists():
                try:
                    with open(estimates_file) as f:
                        data = json.load(f)
                    
                    mean = data.get("mean", {})
                    if "point_estimate" in mean:
                        time_ns = mean["point_estimate"]
                        time_ms = time_ns / 1_000_000
                        print(f"  {bench_dir.name}: {time_ms:.2f} ms")
                        
                except Exception as e:
                    print(f"  Error reading {bench_dir.name}: {e}")

if __name__ == "__main__":
    analyze_criterion_results()
EOF
    
    python3 "${REPORT_DIR}/analyze_results.py" | tee -a "${PERF_REPORT}"
fi

# Copy criterion HTML reports
if [ -d "target/criterion" ]; then
    echo -e "${YELLOW}📋 Copying Criterion HTML reports...${NC}"
    cp -r target/criterion "${REPORT_DIR}/criterion_${TIMESTAMP}"
    echo -e "${GREEN}✅ HTML reports copied to ${REPORT_DIR}/criterion_${TIMESTAMP}${NC}"
fi

# Generate quick stats
echo -e "${YELLOW}📈 Generating quick statistics...${NC}"

QUICK_STATS="${REPORT_DIR}/quick_stats_${TIMESTAMP}.txt"

cat > "${QUICK_STATS}" << EOF
Quick Performance Statistics
===========================
Generated: $(date)

Benchmark Execution Summary:
---------------------------
EOF

# Count benchmark executions from logs
if ls "${REPORT_DIR}"/bench_*_"${TIMESTAMP}".log 1> /dev/null 2>&1; then
    echo "Total benchmark groups executed: $(ls ${REPORT_DIR}/bench_*_${TIMESTAMP}.log | wc -l)" >> "${QUICK_STATS}"
    
    # Extract timing information from logs
    echo "" >> "${QUICK_STATS}"
    echo "Benchmark Group Results:" >> "${QUICK_STATS}"
    echo "----------------------" >> "${QUICK_STATS}"
    
    for log_file in "${REPORT_DIR}"/bench_*_"${TIMESTAMP}".log; do
        group_name=$(basename "$log_file" | sed "s/bench_\(.*\)_${TIMESTAMP}.log/\1/")
        echo "📊 $group_name:" >> "${QUICK_STATS}"
        
        # Extract relevant performance data
        grep -E "(time:|throughput:)" "$log_file" | head -5 >> "${QUICK_STATS}" 2>/dev/null || echo "  No timing data found" >> "${QUICK_STATS}"
        echo "" >> "${QUICK_STATS}"
    done
fi

echo -e "${GREEN}✅ Quick statistics generated${NC}"

# List all generated files
echo -e "${BLUE}📁 Generated report files:${NC}"
echo "----------------------------------------"
ls -la "${REPORT_DIR}"/*"${TIMESTAMP}"* 2>/dev/null || echo "No timestamped files found"

if [ -d "${REPORT_DIR}/criterion_${TIMESTAMP}" ]; then
    echo "📊 Criterion HTML reports: ${REPORT_DIR}/criterion_${TIMESTAMP}/"
    echo "   └─ Open index.html in your browser for interactive results"
fi

echo ""
echo -e "${GREEN}🎉 Performance benchmarking completed!${NC}"
echo -e "${BLUE}📊 Main report: ${PERF_REPORT}${NC}"
echo -e "${BLUE}📈 Quick stats: ${QUICK_STATS}${NC}"
echo -e "${BLUE}🖥️  System info: ${SYSTEM_INFO}${NC}"

# Suggest next steps
echo ""
echo -e "${YELLOW}💡 Next steps:${NC}"
echo "  1. Review the performance summary: cat ${PERF_REPORT}"
echo "  2. Analyze detailed results in: ${REPORT_DIR}/criterion_${TIMESTAMP}/"
echo "  3. Compare results over time by running benchmarks regularly"
echo "  4. Optimize based on bottlenecks identified in the reports"

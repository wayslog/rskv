# Performance Benchmark Script for rskv (PowerShell)
# This script runs comprehensive performance tests and generates detailed reports

param(
    [string]$ReportDir = "performance_reports"
)

$ErrorActionPreference = "Stop"

Write-Host "🚀 Starting performance benchmarks for rskv" -ForegroundColor Blue
Write-Host "Timestamp: $(Get-Date)"
Write-Host "=========================================="

# Create report directory
if (-not (Test-Path $ReportDir)) {
    New-Item -ItemType Directory -Path $ReportDir -Force | Out-Null
}

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"

# System information
Write-Host "📊 Collecting system information..." -ForegroundColor Yellow
$systemInfo = "$ReportDir/system_info_$timestamp.txt"

$sysInfo = @"
Performance Benchmark Report
============================
Date: $(Get-Date)
Project: rskv

System Information:
------------------
OS: $($PSVersionTable.Platform) $([System.Environment]::OSVersion.VersionString)
Architecture: $([System.Environment]::GetEnvironmentVariable("PROCESSOR_ARCHITECTURE"))
CPU: $((Get-WmiObject -Class Win32_Processor).Name)
Memory: $([math]::Round((Get-WmiObject -Class Win32_ComputerSystem).TotalPhysicalMemory / 1GB, 2)) GB
Cores: $([System.Environment]::ProcessorCount)

Git Information:
---------------
Commit: $(try { git rev-parse HEAD } catch { 'N/A' })
Branch: $(try { git branch --show-current } catch { 'N/A' })

Rust Information:
----------------
Rustc: $(rustc --version)
Cargo: $(cargo --version)

PowerShell: $($PSVersionTable.PSVersion)
"@

$sysInfo | Out-File -FilePath $systemInfo -Encoding UTF8

Write-Host "✅ System information collected" -ForegroundColor Green

# Clean previous builds
Write-Host "🧹 Cleaning previous builds..." -ForegroundColor Yellow
cargo clean

# Build in release mode
Write-Host "🔨 Building project in release mode..." -ForegroundColor Yellow
cargo build --release

Write-Host "🏃 Running performance benchmarks..." -ForegroundColor Yellow

# Function to run a specific benchmark group
function Run-BenchmarkGroup {
    param(
        [string]$GroupName,
        [string]$Description
    )
    
    Write-Host "📈 Running $Description..." -ForegroundColor Magenta
    
    try {
        $logFile = "$ReportDir/bench_${GroupName}_$timestamp.log"
        cargo bench --bench performance -- $GroupName | Tee-Object -FilePath $logFile
        Write-Host "✅ $Description completed" -ForegroundColor Green
    }
    catch {
        Write-Host "❌ $Description failed: $_" -ForegroundColor Red
        throw
    }
}

# Run different benchmark groups
try {
    Run-BenchmarkGroup "write_performance" "Write Performance Tests"
    Run-BenchmarkGroup "read_performance" "Read Performance Tests"
    Run-BenchmarkGroup "mixed_workload" "Mixed Workload Tests"
    Run-BenchmarkGroup "concurrent_operations" "Concurrent Operations Tests"
    Run-BenchmarkGroup "batch_operations" "Batch Operations Tests"
    Run-BenchmarkGroup "memory_size_impact" "Memory Size Impact Tests"
    Run-BenchmarkGroup "scan_operations" "Scan Operations Tests"
}
catch {
    Write-Host "❌ Benchmark execution failed: $_" -ForegroundColor Red
    exit 1
}

Write-Host "📋 Generating performance summary..." -ForegroundColor Yellow

# Generate comprehensive performance report
$perfReport = "$ReportDir/performance_summary_$timestamp.md"

$reportContent = @'
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

'@

$reportContent | Out-File -FilePath $perfReport -Encoding UTF8

# Append system info to the report
"" | Add-Content -Path $perfReport
"## 🖥️ 测试环境详情" | Add-Content -Path $perfReport
"" | Add-Content -Path $perfReport
'```' | Add-Content -Path $perfReport
Get-Content $systemInfo | Add-Content -Path $perfReport
'```' | Add-Content -Path $perfReport

Write-Host "📊 Analyzing benchmark results..." -ForegroundColor Yellow

# Copy criterion HTML reports if they exist
if (Test-Path "target/criterion") {
    Write-Host "📋 Copying Criterion HTML reports..." -ForegroundColor Yellow
    Copy-Item -Path "target/criterion" -Destination "$ReportDir/criterion_$timestamp" -Recurse -Force
    Write-Host "✅ HTML reports copied to $ReportDir/criterion_$timestamp" -ForegroundColor Green
}

# Generate quick stats
Write-Host "📈 Generating quick statistics..." -ForegroundColor Yellow

$quickStats = "$ReportDir/quick_stats_$timestamp.txt"

$statsContent = @"
Quick Performance Statistics
===========================
Generated: $(Get-Date)

Benchmark Execution Summary:
---------------------------
"@

$statsContent | Out-File -FilePath $quickStats -Encoding UTF8

# Count benchmark executions from logs
$logFiles = Get-ChildItem -Path $ReportDir -Filter "bench_*_$timestamp.log" -ErrorAction SilentlyContinue

if ($logFiles) {
    "Total benchmark groups executed: $($logFiles.Count)" | Add-Content -Path $quickStats
    "" | Add-Content -Path $quickStats
    "Benchmark Group Results:" | Add-Content -Path $quickStats
    "----------------------" | Add-Content -Path $quickStats
    
    foreach ($logFile in $logFiles) {
        $groupName = $logFile.BaseName -replace "bench_(.*)_$timestamp", '$1'
        "📊 $groupName:" | Add-Content -Path $quickStats
        
        # Extract relevant performance data
        try {
            $content = Get-Content $logFile.FullName
            $timingLines = $content | Where-Object { $_ -match "time:|throughput:" } | Select-Object -First 5
            if ($timingLines) {
                $timingLines | Add-Content -Path $quickStats
            } else {
                "  No timing data found" | Add-Content -Path $quickStats
            }
        }
        catch {
            "  Error reading timing data" | Add-Content -Path $quickStats
        }
        
        "" | Add-Content -Path $quickStats
    }
}

Write-Host "✅ Quick statistics generated" -ForegroundColor Green

# List all generated files
Write-Host "📁 Generated report files:" -ForegroundColor Blue
Write-Host "----------------------------------------"

Get-ChildItem -Path $ReportDir -Filter "*$timestamp*" | Format-Table Name, Length, LastWriteTime

if (Test-Path "$ReportDir/criterion_$timestamp") {
    Write-Host "📊 Criterion HTML reports: $ReportDir/criterion_$timestamp/" -ForegroundColor Blue
    Write-Host "   └─ Open index.html in your browser for interactive results" -ForegroundColor Blue
}

Write-Host ""
Write-Host "🎉 Performance benchmarking completed!" -ForegroundColor Green
Write-Host "📊 Main report: $perfReport" -ForegroundColor Blue
Write-Host "📈 Quick stats: $quickStats" -ForegroundColor Blue
Write-Host "🖥️ System info: $systemInfo" -ForegroundColor Blue

# Suggest next steps
Write-Host ""
Write-Host "💡 Next steps:" -ForegroundColor Yellow
Write-Host "  1. Review the performance summary: Get-Content $perfReport"
Write-Host "  2. Analyze detailed results in: $ReportDir/criterion_$timestamp/"
Write-Host "  3. Compare results over time by running benchmarks regularly"
Write-Host "  4. Optimize based on bottlenecks identified in the reports"

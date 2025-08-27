# Test Coverage Generation Script for rskv (PowerShell)
# This script generates comprehensive test coverage reports using cargo-tarpaulin

param(
    [string]$OutputDir = "coverage"
)

$ErrorActionPreference = "Stop"

Write-Host "🔍 Starting test coverage analysis for rskv" -ForegroundColor Blue
Write-Host "Timestamp: $(Get-Date)"
Write-Host "----------------------------------------"

# Create coverage directory if it doesn't exist
if (-not (Test-Path $OutputDir)) {
    New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null
}

# Clean previous build artifacts
Write-Host "🧹 Cleaning previous build artifacts..." -ForegroundColor Yellow
cargo clean

Write-Host "📊 Running tests with coverage analysis..." -ForegroundColor Yellow

# Generate coverage report
try {
    cargo tarpaulin `
        --verbose `
        --all-features `
        --workspace `
        --timeout 120 `
        --exclude-files "target/*" `
        --exclude-files "examples/*" `
        --exclude-files "benches/*" `
        --exclude-files "tests/*" `
        --ignore-panics `
        --ignore-tests `
        --out Html `
        --out Json `
        --out Lcov `
        --out Xml `
        --output-dir $OutputDir `
        --skip-clean

    Write-Host "✅ Coverage analysis completed successfully!" -ForegroundColor Green
}
catch {
    Write-Host "❌ Coverage analysis failed!" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    exit 1
}

# Generate timestamp file
$timestamp = @"
Coverage generated at: $(Get-Date)
Git commit: $(try { git rev-parse HEAD } catch { 'N/A' })
Git branch: $(try { git branch --show-current } catch { 'N/A' })
"@

$timestamp | Out-File -FilePath "$OutputDir/timestamp.txt" -Encoding UTF8

# List generated files
Write-Host "📁 Generated files:" -ForegroundColor Blue
Get-ChildItem -Path $OutputDir | Format-Table Name, Length, LastWriteTime

Write-Host ""
Write-Host "🎉 Coverage analysis complete!" -ForegroundColor Green
Write-Host "📊 Open $OutputDir/tarpaulin-report.html in your browser to view the detailed report" -ForegroundColor Blue

Write-Host ""
Write-Host "Coverage report locations:"
Write-Host "  HTML: $OutputDir/tarpaulin-report.html"
Write-Host "  JSON: $OutputDir/tarpaulin-report.json"
Write-Host "  LCOV: $OutputDir/lcov.info"
Write-Host "  XML:  $OutputDir/cobertura.xml"

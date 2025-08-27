# Pre-commit hook script for rskv project (PowerShell version)
# This script runs before each commit to ensure code quality

param(
    [switch]$SkipTests = $false
)

# Colors for output
$Red = "Red"
$Green = "Green"
$Yellow = "Yellow"
$Blue = "Blue"

function Write-ColorOutput {
    param(
        [string]$Message,
        [string]$Color = "White"
    )
    Write-Host $Message -ForegroundColor $Color
}

Write-ColorOutput "🔍 Running pre-commit checks..." $Blue

# Check if we're in a git repository
try {
    $null = git rev-parse --git-dir 2>$null
} catch {
    Write-ColorOutput "❌ Not in a git repository" $Red
    exit 1
}

# Get staged files
$StagedFiles = git diff --cached --name-only --diff-filter=ACM | Where-Object { $_ -match '\.rs$' }

if (-not $StagedFiles) {
    Write-ColorOutput "⚠️  No Rust files staged for commit" $Yellow
    exit 0
}

Write-ColorOutput "📁 Staged Rust files:" $Blue
$StagedFiles | ForEach-Object { Write-Host "  $_" }

# Function to check if a command exists
function Test-Command {
    param([string]$Command)
    try {
        Get-Command $Command -ErrorAction Stop | Out-Null
        return $true
    } catch {
        return $false
    }
}

# Check code formatting
Write-ColorOutput "🔧 Checking code formatting..." $Yellow
try {
    cargo fmt --all -- --check
    if ($LASTEXITCODE -ne 0) {
        Write-ColorOutput "❌ Code formatting check failed" $Red
        Write-ColorOutput "💡 Run 'cargo fmt --all' to fix formatting issues" $Yellow
        exit 1
    }
    Write-ColorOutput "✅ Code formatting check passed" $Green
} catch {
    Write-ColorOutput "❌ Code formatting check failed" $Red
    exit 1
}

# Check clippy
Write-ColorOutput "🔍 Running clippy..." $Yellow
try {
    cargo clippy --all-features --workspace -- -D warnings
    if ($LASTEXITCODE -ne 0) {
        Write-ColorOutput "❌ Clippy check failed" $Red
        exit 1
    }
    Write-ColorOutput "✅ Clippy check passed" $Green
} catch {
    Write-ColorOutput "❌ Clippy check failed" $Red
    exit 1
}

# Check compilation
Write-ColorOutput "🔨 Checking compilation..." $Yellow
try {
    cargo check --all-features --workspace
    if ($LASTEXITCODE -ne 0) {
        Write-ColorOutput "❌ Compilation check failed" $Red
        exit 1
    }
    Write-ColorOutput "✅ Compilation check passed" $Green
} catch {
    Write-ColorOutput "❌ Compilation check failed" $Red
    exit 1
}

# Run tests (unless skipped)
if (-not $SkipTests) {
    Write-ColorOutput "🧪 Running tests..." $Yellow
    try {
        cargo test --all-features --workspace
        if ($LASTEXITCODE -ne 0) {
            Write-ColorOutput "❌ Tests failed" $Red
            exit 1
        }
        Write-ColorOutput "✅ Tests passed" $Green
    } catch {
        Write-ColorOutput "❌ Tests failed" $Red
        exit 1
    }
} else {
    Write-ColorOutput "⚠️  Skipping tests (--SkipTests flag used)" $Yellow
}

# Security audit (if cargo-audit is available)
if (Test-Command "cargo-audit") {
    Write-ColorOutput "🔒 Running security audit..." $Yellow
    try {
        cargo audit
        if ($LASTEXITCODE -ne 0) {
            Write-ColorOutput "❌ Security audit failed" $Red
            exit 1
        }
        Write-ColorOutput "✅ Security audit passed" $Green
    } catch {
        Write-ColorOutput "❌ Security audit failed" $Red
        exit 1
    }
} else {
    Write-ColorOutput "⚠️  cargo-audit not found, skipping security audit" $Yellow
}

# Cargo-deny check (if available)
if (Test-Command "cargo-deny") {
    Write-ColorOutput "📋 Running cargo-deny..." $Yellow
    try {
        cargo deny check
        if ($LASTEXITCODE -ne 0) {
            Write-ColorOutput "❌ Cargo-deny check failed" $Red
            exit 1
        }
        Write-ColorOutput "✅ Cargo-deny check passed" $Green
    } catch {
        Write-ColorOutput "❌ Cargo-deny check failed" $Red
        exit 1
    }
} else {
    Write-ColorOutput "⚠️  cargo-deny not found, skipping dependency check" $Yellow
}

Write-ColorOutput "🎉 All pre-commit checks passed!" $Green
Write-ColorOutput "📝 Ready to commit" $Blue

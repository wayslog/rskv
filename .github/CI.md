# GitHub Actions Workflows

This repository includes several GitHub Actions workflows to ensure code quality, security, and performance.

## Workflows

### 1. CI (`ci.yml`)
Main continuous integration workflow that runs on every push and pull request.

**Features:**
- Tests on multiple Rust versions (stable, beta, nightly)
- Format checking with `rustfmt`
- Linting with `clippy`
- Security audit with `cargo-audit`
- Documentation generation
- Code coverage reporting with `cargo-tarpaulin`

**Triggers:**
- Push to `main` or `develop` branches
- Pull requests to `main` or `develop` branches

### 2. Fuzz Testing (`fuzz.yml`)
Comprehensive fuzz testing workflow using multiple fuzzing engines.

**Features:**
- Short fuzz tests on every push/PR (5 minutes)
- Extended fuzz tests on schedule/push (1 hour)
- AFL++ fuzzing on schedule
- Artifact upload on failure

**Triggers:**
- Push to `main` or `develop` branches
- Pull requests to `main` or `develop` branches
- Daily at 2 AM UTC

### 3. Code Quality (`quality.yml`)
Advanced code quality checks and analysis.

**Features:**
- Format checking
- Clippy linting
- Unused dependency detection
- Security vulnerability scanning
- License compatibility checking
- Dependency graph generation
- Code complexity analysis

**Triggers:**
- Push to `main` or `develop` branches
- Pull requests to `main` or `develop` branches

### 4. Benchmark (`benchmark.yml`)
Performance benchmarking and comparison.

**Features:**
- Performance benchmarks on every push/PR
- Benchmark comparison on pull requests
- Historical benchmark tracking

**Triggers:**
- Push to `main` or `develop` branches
- Pull requests to `main` or `develop` branches
- Daily at 3 AM UTC

### 5. Release (`release.yml`)
Automated release creation.

**Features:**
- Builds release binaries
- Creates release archives
- Generates release notes

**Triggers:**
- Push of version tags (e.g., `v1.0.0`)

### 6. Dependabot (`dependabot.yml`)
Dependency update monitoring.

**Features:**
- Weekly dependency update checks
- Automatic issue creation for outdated dependencies

**Triggers:**
- Weekly on Monday at 9 AM UTC

## Configuration Files

### `rust-toolchain.toml`
Specifies the Rust toolchain version and components used across all workflows.

### `clippy.toml`
Configures clippy lints and allows certain lints that are too strict for this project.

### `rustfmt.toml`
Configures code formatting rules for consistent code style.

### `tarpaulin.toml`
Configures code coverage collection and reporting.

### `codecov.yml`
Configures Codecov integration for coverage reporting.

### `.github/dependabot.yml`
Configures automatic dependency updates for Rust and GitHub Actions.

## Usage

### Running Locally

To run the same checks locally:

```bash
# Format check
cargo fmt --all -- --check

# Clippy linting
cargo clippy --all-targets --all-features -- -D warnings

# Security audit
cargo audit

# Code coverage
cargo install cargo-tarpaulin
cargo tarpaulin --out Xml --output-dir coverage/ --all-features

# Fuzz testing
cd fuzz
cargo install cargo-fuzz
cargo fuzz run faster_kv_fuzz -- -max_total_time=300
```

### Coverage Reports

Code coverage reports are automatically generated and uploaded to Codecov. You can view the coverage report at:
- Codecov: https://codecov.io/gh/your-username/rskv

### Fuzz Testing

Fuzz testing helps find edge cases and potential security vulnerabilities. The workflow runs:
- Short tests (5 minutes) on every push/PR
- Extended tests (1 hour) on schedule
- AFL++ fuzzing on schedule

### Performance Monitoring

Benchmark results are automatically generated and can be compared across commits to track performance regressions.

## Troubleshooting

### Common Issues

1. **Formatting errors**: Run `cargo fmt` to fix formatting issues
2. **Clippy warnings**: Address clippy suggestions or add `#[allow(clippy::lint_name)]` for justified cases
3. **Security vulnerabilities**: Update dependencies or add exceptions in `Cargo.toml`
4. **Coverage drops**: Add tests for uncovered code or adjust coverage thresholds

### Workflow Failures

If a workflow fails:
1. Check the logs for specific error messages
2. Run the failing command locally
3. Fix the issue and push again
4. For fuzz testing failures, check the uploaded artifacts for crash inputs

## Contributing

When contributing to this repository:
1. Ensure all CI checks pass
2. Add tests for new functionality
3. Update documentation as needed
4. Consider performance implications
5. Run fuzz tests locally for critical changes

# 开发指南

本文档描述了 rskv 项目的开发工作流程和代码质量工具的使用方法。

## 代码质量工具

### 1. 代码格式化 (rustfmt)

我们使用 `rustfmt` 来确保代码格式的一致性。

```bash
# 格式化所有代码
cargo fmt --all

# 检查代码格式（不修改）
cargo fmt --all -- --check
```

### 2. 代码质量检查 (clippy)

我们使用 `clippy` 来检查代码质量和潜在问题。

```bash
# 运行 clippy 检查
cargo clippy --all-features --workspace -- -D warnings

# 运行严格的 clippy 检查
cargo clippy --all-features --workspace -- \
  -D warnings \
  -D clippy::all \
  -D clippy::pedantic \
  -D clippy::nursery \
  -D clippy::cargo
```

### 3. 安全审计 (cargo-audit)

我们使用 `cargo-audit` 来检查依赖项的安全漏洞。

```bash
# 安装 cargo-audit
cargo install cargo-audit

# 运行安全审计
cargo audit
```

### 4. 依赖管理 (cargo-deny)

我们使用 `cargo-deny` 来管理依赖项的许可证和安全性。

```bash
# 安装 cargo-deny
cargo install cargo-deny

# 检查依赖项
cargo deny check
```

### 5. 代码覆盖率 (cargo-tarpaulin)

我们使用 `cargo-tarpaulin` 来生成代码覆盖率报告。

```bash
# 安装 cargo-tarpaulin
cargo install cargo-tarpaulin

# 生成覆盖率报告
cargo tarpaulin --all-features --workspace --out Html --out Json --out Lcov --out Xml
```

### 6. 代码复杂度分析 (cargo-geiger)

我们使用 `cargo-geiger` 来分析代码的复杂度。

```bash
# 安装 cargo-geiger
cargo install cargo-geiger

# 运行复杂度分析
cargo geiger --all-features --workspace
```

### 7. 文档链接检查 (cargo-deadlinks)

我们使用 `cargo-deadlinks` 来检查文档中的死链接。

```bash
# 安装 cargo-deadlinks
cargo install cargo-deadlinks

# 生成文档
cargo doc --all-features --no-deps --document-private-items

# 检查死链接
cargo deadlinks --dir target/doc
```

## 开发工作流程

### 1. 设置开发环境

```bash
# 安装所有开发工具
make install-deps

# 或者手动安装
cargo install cargo-tarpaulin cargo-audit cargo-outdated cargo-deny cargo-geiger cargo-deadlinks
```

### 2. 日常开发

```bash
# 快速开发循环
make dev

# 或者分步执行
cargo fmt --all
cargo clippy --all-features --workspace -- -D warnings
cargo test --all-features --workspace
```

### 3. 提交前检查

```bash
# 运行完整的质量检查
make quality

# 或者使用 pre-commit 脚本
chmod +x scripts/pre-commit.sh
./scripts/pre-commit.sh

# Windows PowerShell
.\scripts\pre-commit.ps1
```

### 4. 性能测试

```bash
# 运行基准测试
make bench

# 运行性能测试
make performance

# 运行并发性能测试
make perf-concurrency
```

## GitHub Actions

我们使用 GitHub Actions 来自动化以下任务：

### 1. CI 工作流 (`.github/workflows/ci.yml`)

- 代码格式化和 clippy 检查
- 多平台编译和测试
- 代码覆盖率报告
- 性能基准测试
- 文档生成
- 发布构建

### 2. 代码质量工作流 (`.github/workflows/code-quality.yml`)

- 严格的代码质量检查
- 文档检查
- 代码复杂度分析
- 定期安全审计

## 配置文件

### 1. rustfmt.toml

配置代码格式化规则，包括：
- 最大行宽：100 字符
- 缩进：4 个空格
- 导入排序和分组
- 代码风格偏好

### 2. .clippy.toml

配置 clippy 检查规则，包括：
- 性能优化建议
- 代码风格检查
- 安全性检查
- 复杂度限制

### 3. deny.toml

配置依赖项管理规则，包括：
- 许可证检查
- 安全漏洞检查
- 依赖项白名单
- 禁止的依赖项

## 代码覆盖率

我们使用 Codecov.io 来跟踪代码覆盖率：

1. 覆盖率报告会自动上传到 Codecov.io
2. 覆盖率徽章会显示在 README 中
3. 覆盖率报告包括：
   - HTML 报告（详细视图）
   - JSON 报告（API 集成）
   - LCOV 报告（IDE 集成）
   - XML 报告（CI 集成）

## 最佳实践

### 1. 代码风格

- 遵循 Rust 官方风格指南
- 使用有意义的变量和函数名
- 添加适当的注释和文档
- 保持函数简洁（不超过 50 行）

### 2. 错误处理

- 使用 `Result` 和 `Option` 类型
- 避免使用 `unwrap()` 和 `expect()`
- 提供有意义的错误信息
- 使用 `?` 操作符传播错误

### 3. 性能优化

- 避免不必要的分配
- 使用适当的集合类型
- 考虑内存布局和缓存友好性
- 使用基准测试验证性能改进

### 4. 并发安全

- 使用适当的同步原语
- 避免数据竞争
- 考虑死锁风险
- 使用 `unsafe` 代码时要特别小心

### 5. 测试

- 编写单元测试和集成测试
- 测试边界条件和错误情况
- 使用属性测试（proptest）
- 保持测试覆盖率在 80% 以上

## 故障排除

### 常见问题

1. **clippy 警告过多**
   - 查看 `.clippy.toml` 配置
   - 考虑使用 `#[allow(clippy::warning_name)]` 注解

2. **格式化不一致**
   - 确保使用相同的 rustfmt 版本
   - 检查 `rustfmt.toml` 配置

3. **依赖项冲突**
   - 运行 `cargo tree` 查看依赖关系
   - 检查 `deny.toml` 配置

4. **覆盖率报告不准确**
   - 确保测试覆盖了所有代码路径
   - 检查 `cargo-tarpaulin` 配置

### 获取帮助

- 查看 [Rust 官方文档](https://doc.rust-lang.org/)
- 参考 [clippy 文档](https://rust-lang.github.io/rust-clippy/)
- 查看项目中的示例代码
- 提交 issue 或讨论

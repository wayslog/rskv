#!/bin/bash

echo "运行F2冷热数据迁移测试套件"
echo "====================================="

# 设置错误时退出
set -e

# 清理之前的测试数据
echo "清理之前的测试数据..."
rm -rf /tmp/f2_*_test*
rm -rf /tmp/f2_*_hot*
rm -rf /tmp/f2_*_cold*

# 编译项目
echo "编译项目..."
cargo build --release

# 运行基础示例
echo ""
echo "运行F2基础示例..."
cargo run --example f2_basic_example

# 运行冷热数据迁移测试
echo ""
echo "运行冷热数据迁移测试..."
cargo run --example f2_cold_hot_migration_test

# 运行综合测试
echo ""
echo "运行F2综合测试..."
cargo run --example f2_comprehensive_test

# 运行迁移压力测试
echo ""
echo "运行迁移压力测试..."
cargo run --example f2_migration_stress_test

# 运行单元测试
echo ""
echo "运行F2单元测试..."
cargo test f2_tests

echo ""
echo "所有F2测试完成！"
echo ""
echo " 测试总结:"
echo "   F2基础功能测试"
echo "   冷热数据迁移测试"
echo "   并发访问测试"
echo "   性能基准测试"
echo "   迁移一致性测试"
echo "   压力测试"

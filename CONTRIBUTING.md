# Contributing to hermes-agent-cluster

Thank you for your interest in contributing to hermes-agent-cluster! This document provides guidelines and instructions for contributing.

---

# 为 hermes-agent-cluster 做贡献

感谢您对 hermes-agent-cluster 的关注！本文档提供贡献指南和说明。

---

## Table of Contents / 目录

- [Code of Conduct / 行为准则](#code-of-conduct--行为准则)
- [Getting Started / 快速开始](#getting-started--快速开始)
- [Development Workflow / 开发工作流](#development-workflow--开发工作流)
- [Code Style / 代码风格](#code-style--代码风格)
- [Testing / 测试](#testing--测试)
- [Commit Conventions / 提交约定](#commit-conventions--提交约定)
- [Pull Request Process / PR 流程](#pull-request-process--pr-流程)
- [Reporting Issues / 报告问题](#reporting-issues--报告问题)

---

## Code of Conduct / 行为准则

Please be respectful and constructive in all interactions. We are committed to providing a welcoming and inclusive experience for everyone.

请在所有交互中保持尊重和建设性。我们致力于为每个人提供友善和包容的体验。

---

## Getting Started / 快速开始

### Prerequisites / 前置条件

- Go 1.25 or later / Go 1.25 或更高版本
- gRPC tools (protoc, protoc-gen-go, protoc-gen-go-grpc) / gRPC 工具
- Docker (for integration tests) / Docker（用于集成测试）
- Git / Git

### Setup / 设置

```bash
# Fork and clone the repository / Fork 并克隆仓库
git clone https://github.com/<your-username>/hermes-agent-cluster.git
cd hermes-agent-cluster

# Install dependencies / 安装依赖
go mod download

# Generate protobuf code (if modifying .proto files) / 生成 protobuf 代码（如修改 .proto 文件）
make proto

# Run tests to verify setup / 运行测试验证设置
make test
```

---

## Development Workflow / 开发工作流

1. Create a new branch from `main` / 从 `main` 创建新分支
   ```bash
   git checkout -b feat/my-feature main
   ```

2. Make your changes with clear, focused commits / 进行修改，保持提交清晰且聚焦

3. Write or update tests for your changes / 为修改编写或更新测试

4. Ensure all tests pass / 确保所有测试通过
   ```bash
   make test
   make lint
   ```

5. Push your branch and open a Pull Request / 推送分支并创建 PR

### Branch Naming / 分支命名

- `feat/description` — new features / 新功能
- `fix/description` — bug fixes / 修复
- `docs/description` — documentation / 文档
- `refactor/description` — code refactoring / 代码重构
- `test/description` — test additions or fixes / 测试添加或修复
- `chore/description` — maintenance tasks / 维护任务

---

## Code Style / 代码风格

### General / 通用

- Follow the standard Go formatting / 遵循标准 Go 格式化
- Run `gofmt` and `goimports` before committing / 提交前运行 `gofmt` 和 `goimports`
- Use `make lint` to check for style issues / 使用 `make lint` 检查风格问题

### Naming / 命名

- Use mixedCaps for Go identifiers (camelCase) / 使用 mixedCaps（驼峰命名）
- Exported names must have doc comments / 导出的名称必须有文档注释
- Keep names short and descriptive / 名称保持简短且有描述性

### Error Handling / 错误处理

- Always check and handle errors / 始终检查并处理错误
- Use `fmt.Errorf` with `%w` for wrapping / 使用 `fmt.Errorf` 配合 `%w` 进行错误包装
- Define sentinel errors with `errors.New` or `%w` / 使用 `errors.New` 或 `%w` 定义哨兵错误

### Package Structure / 包结构

- Keep packages focused on a single concern / 每个包专注于单一职责
- Avoid circular imports / 避免循环导入
- Place interfaces close to their consumers / 将接口放在其消费者附近

---

## Testing / 测试

### Unit Tests / 单元测试

- Write table-driven tests / 编写表驱动测试
- Use `t.Helper()` in helper functions / 在辅助函数中使用 `t.Helper()`
- Test both success and failure paths / 测试成功和失败路径

```bash
# Run unit tests / 运行单元测试
make test

# Run with coverage / 带覆盖率运行
make test-coverage

# View coverage report / 查看覆盖率报告
go tool cover -html=coverage.out
```

### Integration Tests / 集成测试

- Place integration tests in `*_integration_test.go` files / 将集成测试放在 `*_integration_test.go` 文件中
- Use `//go:build integration` build tag / 使用 `//go:build integration` 构建标签
- Docker required for integration tests / 集成测试需要 Docker

```bash
# Run integration tests / 运行集成测试
make test-integration
```

### E2E Tests / 端到端测试

- E2E tests validate full cluster workflows / E2E 测试验证完整集群工作流
- Place in `e2e/` directory / 放在 `e2e/` 目录下

```bash
# Run E2E tests / 运行 E2E 测试
make test-e2e
```

---

## Commit Conventions / 提交约定

We follow [Conventional Commits](https://www.conventionalcommits.org/).
我们遵循 [Conventional Commits](https://www.conventionalcommits.org/)。

### Format / 格式

```
<type>(<scope>): <description>

[optional body]

[optional footer(s)]
```

### Types / 类型

| Type | Description | 描述 |
|------|-------------|------|
| `feat` | New feature | 新功能 |
| `fix` | Bug fix | 修复 |
| `docs` | Documentation only | 仅文档 |
| `style` | Code style (no logic change) | 代码风格（无逻辑变更） |
| `refactor` | Code refactoring | 代码重构 |
| `perf` | Performance improvement | 性能改进 |
| `test` | Adding or updating tests | 添加或更新测试 |
| `chore` | Build process or tooling | 构建流程或工具 |
| `ci` | CI/CD changes | CI/CD 变更 |

### Scope / 范围

Common scopes include: `cluster`, `task`, `agent`, `scheduler`, `lease`, `dashboard`, `otel`, `proto`.

常用范围包括：`cluster`, `task`, `agent`, `scheduler`, `lease`, `dashboard`, `otel`, `proto`.

### Examples / 示例

```
feat(scheduler): add capability-based task matching
fix(lease): prevent double-acquisition on concurrent claims
docs(readme): update installation instructions
test(e2e): add cluster scaling scenarios
```

---

## Pull Request Process / PR 流程

### Before Opening a PR / 创建 PR 前

1. Ensure your code compiles and passes all tests / 确保代码编译通过且所有测试通过
2. Run `make lint` and fix any issues / 运行 `make lint` 并修复所有问题
3. Update documentation if your change affects the public API / 如修改影响公共 API，更新文档
4. Update CHANGELOG.md under the `[Unreleased]` section / 在 `[Unreleased]` 部分更新 CHANGELOG.md

### PR Title / PR 标题

Follow the same Conventional Commits format / 遵循相同的 Conventional Commits 格式

```
feat(cluster): add node auto-scaling support
```

### PR Description / PR 描述

Include the following / 包含以下内容:

- **What** — A clear summary of the changes / 变更的清晰摘要
- **Why** — The motivation or issue being addressed / 动机或正在解决的问题
- **How** — High-level approach taken / 采用的高层方法
- **Testing** — How the changes were tested / 如何测试了变更

### Review Checklist / 审查清单

- [ ] Code compiles without errors / 代码编译无错误
- [ ] All existing tests pass / 所有现有测试通过
- [ ] New tests added for new functionality / 为新功能添加了测试
- [ ] Code follows style guidelines / 代码遵循风格指南
- [ ] Documentation updated (if applicable) / 文档已更新（如适用）
- [ ] No unrelated changes included / 不包含无关变更
- [ ] Commit messages follow conventions / 提交消息遵循约定

### After Approval / 审批后

- Squash and merge will be used for most PRs / 大多数 PR 将使用 squash and merge
- Ensure CI passes before merging / 合并前确保 CI 通过

---

## Reporting Issues / 报告问题

When reporting issues, please include / 报告问题时，请包含:

- Go version / Go 版本
- OS and architecture / 操作系统和架构
- Steps to reproduce / 复现步骤
- Expected behavior / 期望行为
- Actual behavior / 实际行为
- Relevant logs or error messages / 相关日志或错误消息

---

## Questions / 有问题？

Open a discussion at https://github.com/heventure/hermes-agent-cluster/discussions or reach out to the maintainers.

请在 https://github.com/heventure/hermes-agent-cluster/discussions 创建讨论，或联系维护者。

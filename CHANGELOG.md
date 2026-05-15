# Changelog

All notable changes to hermes-agent-cluster will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

# 变更日志

所有对 hermes-agent-cluster 的重要变更都将记录在此文件中。

格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.0.0/)，
本项目遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

---

## [v0.8.0] - 2026-05-10

### Added / 新增
- Global status view for unified cluster monitoring across all nodes / 全局状态视图，支持跨所有节点的统一集群监控
- Dynamic capability registration and unregistration at runtime / 运行时动态能力注册与注销
- End-to-end integration test suite covering all core workflows / 端到端集成测试套件，覆盖所有核心工作流
- Fixed several E2E test edge cases and race conditions / 修复多个端到端测试边界条件与竞态问题

### Fixed / 修复
- Race condition in dynamic capability update propagation / 动态能力更新传播中的竞态条件
- Status view inconsistency when nodes join/leave mid-cycle / 节点在周期中间加入/离开时的状态视图不一致

---

## [v0.7.0] - 2026-04-15

### Added / 新增
- Web dashboard for real-time cluster monitoring / Web 仪表盘，支持实时集群监控
- Cluster visualization with node topology and task flow graphs / 集群可视化，包含节点拓扑与任务流图
- Dashboard REST API endpoints for external integrations / 仪表盘 REST API 端点，支持外部集成
- WebSocket support for live dashboard updates / WebSocket 支持，实现仪表盘实时更新

### Changed / 变更
- Improved logging format for dashboard consumption / 优化日志格式，便于仪表盘消费

---

## [v0.6.0] - 2026-03-20

### Added / 新增
- OpenTelemetry integration for distributed tracing / OpenTelemetry 集成，支持分布式链路追踪
- Prometheus metrics export for cluster and task performance / Prometheus 指标导出，覆盖集群与任务性能
- Configurable trace sampling rates / 可配置的追踪采样率
- Custom metric labels for task priority and node roles / 自定义指标标签，支持任务优先级与节点角色

### Changed / 变更
- Refactored internal instrumentation to use OTel SDK / 重构内部插桩，使用 OTel SDK

---

## [v0.5.0] - 2026-02-25

### Added / 新增
- Automatic fault detection for unresponsive agent nodes / 自动故障检测，识别无响应的代理节点
- Node health check with configurable intervals / 节点健康检查，支持可配置间隔
- Task reassignment on node failure / 节点故障时自动重新分配任务
- Alert hooks for external notification systems / 告警钩子，支持外部通知系统

### Fixed / 修复
- Heartbeat timeout not triggering node removal in some edge cases / 心跳超时在某些边界情况下未触发节点移除

---

## [v0.4.0] - 2026-01-30

### Added / 新增
- Lease management system for task ownership / 租约管理系统，用于任务所有权控制
- Automatic lease renewal and expiry handling / 自动租约续期与过期处理
- Lease conflict resolution for concurrent task claims / 租约冲突解决，支持并发任务认领
- Configurable lease duration per task priority / 按任务优先级配置租约时长

### Changed / 变更
- Task lifecycle now tied to lease state / 任务生命周期现与租约状态绑定

---

## [v0.3.0] - 2025-12-20

### Added / 新增
- Task dependency graph support / 任务依赖图支持
- Automatic scheduling of dependent tasks upon completion / 任务完成后自动调度依赖任务
- Dependency cycle detection with clear error messages / 依赖环检测，提供清晰错误信息
- Priority-based topological ordering for task execution / 基于优先级的拓扑排序用于任务执行

### Fixed / 修复
- Deadlock when multiple tasks had circular dependencies / 多任务循环依赖时的死锁问题

---

## [v0.2.0] - 2025-11-15

### Added / 新增
- Capability-based task scheduling / 基于能力的任务调度
- Agent capability registration and discovery / 代理能力注册与发现
- Matchmaking engine for pairing tasks with capable agents / 任务与有能力代理的匹配引擎
- Capability tags with hierarchical naming support / 能力标签，支持层级命名

### Changed / 变更
- Task assignment now respects agent capabilities / 任务分配现遵循代理能力

---

## [v0.1.0] - 2025-10-01

### Added / 新增
- Initial cluster framework with basic agent registration / 初始集群框架，支持基本代理注册
- Centralized task queue with FIFO dispatch / 集中式任务队列，FIFO 调度
- Agent heartbeat and liveness detection / 代理心跳与存活检测
- Basic gRPC communication between coordinator and agents / 协调器与代理之间的基本 gRPC 通信
- Configuration via YAML / 通过 YAML 进行配置
- CLI tool for cluster management / 集群管理命令行工具

---

[Unreleased]: https://github.com/heventure/hermes-agent-cluster/compare/v0.8.0...HEAD
[v0.8.0]: https://github.com/heventure/hermes-agent-cluster/compare/v0.7.0...v0.8.0
[v0.7.0]: https://github.com/heventure/hermes-agent-cluster/compare/v0.6.0...v0.7.0
[v0.6.0]: https://github.com/heventure/hermes-agent-cluster/compare/v0.5.0...v0.6.0
[v0.5.0]: https://github.com/heventure/hermes-agent-cluster/compare/v0.4.0...v0.5.0
[v0.4.0]: https://github.com/heventure/hermes-agent-cluster/compare/v0.3.0...v0.4.0
[v0.3.0]: https://github.com/heventure/hermes-agent-cluster/compare/v0.2.0...v0.3.0
[v0.2.0]: https://github.com/heventure/hermes-agent-cluster/compare/v0.1.0...v0.2.0
[v0.1.0]: https://github.com/heventure/hermes-agent-cluster/releases/tag/v0.1.0

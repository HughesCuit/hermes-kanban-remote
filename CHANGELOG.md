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

## [v1.1.0] - 2026-05-15

### Added / 新增
- **CLI subcommands:** `serve`, `status`, `health`, `config init`, `config validate` — unified command interface / CLI子命令：serve、status、health、config init、config validate——统一命令接口
- **GET /health endpoint:** Lightweight health check for load balancers and orchestrators / GET /health端点：轻量级健康检查，适用于负载均衡器和编排器
- **GET /api/v1/summary:** Cluster overview with node count, task stats, and federation status / GET /api/v1/summary：集群概览，包含节点数、任务统计和联邦状态
- **ValidateDetailed():** Comprehensive config validation with field-level error messages / ValidateDetailed()：全面配置验证，提供字段级错误信息
- **Plugin auto-start:** Cluster service lifecycle hooks — `on_session_start` auto-starts hermes-cluster, `on_session_end` gracefully shuts down / 插件自动启动：集群服务生命周期钩子——on_session_start自动启动hermes-cluster，on_session_end优雅关闭
- **Federation auth:** Token-based authentication for cross-cluster endpoints (`federation.token` in config) / 联邦认证：跨集群端点的令牌认证（配置中federation.token）

### Changed / 变更
- Dashboard recovery stats: field names now match API response (total/completed/partial/failed) / 仪表盘恢复统计：字段名与API响应匹配（total/completed/partial/failed）
- Removed phantom Dependencies column from task table / 移除任务表中的虚拟Dependencies列
- Federation dispatcher now uses WaitGroup for graceful shutdown / 联邦调度器使用WaitGroup实现优雅关闭
- Plugin README restructured with clearer install/usage instructions / 插件README重组，安装/使用说明更清晰

### Fixed / 修复
- Plugin integration tests added (312 lines) / 新增插件集成测试（312行）
- Federation registry tests for idempotency keys and response size limits / 联邦注册表测试：幂等键和响应大小限制
- Fixed `QueryClusterStatus` return type to `*StatusResponse` / 修复QueryClusterStatus返回类型为*StatusResponse

---

## [v1.0.0] - 2026-05-15

### Added / 新增
- **Plugin SDK (v0.9.0):** Webhook system for third-party integration — register/deregister webhook URLs, HMAC-SHA256 signature verification, automatic retry with exponential backoff / 插件SDK：Webhook系统，支持第三方集成——注册/注销Webhook URL，HMAC-SHA256签名验证，指数退避自动重试
- **Dynamic Scheduler (v0.10.0):** Priority-based task scheduling with load-aware node selection, scheduling decision recording / 动态调度器：基于优先级的任务调度，负载感知节点选择，调度决策记录
- **Multi-cluster Federation (v0.11.0):** Cross-cluster collaboration — register remote clusters, forward tasks between clusters, unified status view / 多集群联邦：跨集群协作——注册远程集群，跨集群任务转发，统一状态视图
- **WAN Cluster (v0.12.0):** TLS support (cert/key config, StartTLS), configurable heartbeat (30s WAN default), auto-reconnect with exponential backoff (1s→60s), batch sync / WAN集群：TLS支持（证书/密钥配置，StartTLS），可配置心跳（WAN默认30s），指数退避自动重连（1s→60s），批量同步

### Changed / 变更
- All Phase 3 features integrated and tested / Phase 3全部功能已集成并通过测试
- Updated documentation to reflect v1.0.0 capabilities / 更新文档以反映v1.0.0功能

### Known Issues / 已知问题
- Federation dispatcher health check tests have a race condition under `-race` detector (cosmetic, does not affect production code) / 联邦调度器健康检查测试在 `-race` 检测器下存在竞态条件（仅影响测试，不影响生产代码）

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

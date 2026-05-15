# hermes-agent-cluster

## Hermes 分布式 Agent 集群扩展

---

# 1. 项目概述

`hermes-agent-cluster` 是 Hermes 的分布式集群扩展插件。

它允许多台设备上的独立 Hermes 实例进行协同工作，实现：

* 多机任务协同
* 远程任务同步
* 能力感知调度
* 分布式 Worker 执行
* 集群级任务租约管理

本项目**不会共享 SQLite 数据库**。

每个节点始终维护：

* 自己的 Hermes Runtime
* 自己的 dispatcher
* 自己的本地 kanban.db

节点之间仅同步：

* 任务元数据
* 生命周期事件
* 租约状态
* 执行状态

---

# 2. 项目目标

## 核心目标

* 支持多机器 Hermes 协同
* 保持 Hermes 原有 dispatcher 架构
* 避免 SMB/NFS 共享 SQLite
* 支持异构设备：

  * Windows
  * Linux
  * NAS
  * GPU 工作站
  * 云服务器
* 基于 capability 的任务调度
* 故障恢复与任务重派
* 本地优先执行（Local-first）
* 分布式任务同步

---

# 3. 非目标（Non-Goals）

本项目不打算：

* 替代 Hermes dispatcher
* 替代 Hermes Kanban
* 实现 Agent 之间实时聊天
* 共享单一 kanban.db
* 远程直接 spawn 进程
* 实现完全去中心化 P2P Mesh

---

# 4. 总体架构

```text id="j5j9kr"
                   ┌────────────────────┐
                   │     Main Node      │
                   │--------------------│
                   │ Cluster Registry   │
                   │ Lease Manager      │
                   │ Task Router        │
                   │ Remote API         │
                   └─────────┬──────────┘
                             │
                     HTTP / WebSocket
                             │
      ┌──────────────────────┼──────────────────────┐
      │                      │                      │
┌──────────────┐    ┌──────────────┐      ┌──────────────┐
│ Windows Node │    │   NAS Node  │      │   VPS Node   │
│--------------│    │--------------│      │--------------│
│ Local Hermes │    │ Local Hermes │      │ Local Hermes │
│ Local DB     │    │ Local DB     │      │ Local DB     │
│ Dispatcher   │    │ Dispatcher   │      │ Dispatcher   │
│ Remote Agent │    │ Remote Agent │      │ Remote Agent │
└──────────────┘    └──────────────┘      └──────────────┘
```

---

# 5. 核心设计原则

---

## 5.1 Local-First（本地优先）

每台机器都运行：

* 自己的 Hermes
* 自己的 dispatcher
* 自己的 kanban.db

远程同步只传输：

* 任务元数据
* 生命周期事件
* 任务状态
* 租约信息

---

## 5.2 Dispatcher 本地隔离

dispatcher 永远只管理本机 worker。

dispatcher 负责：

* 本地 worker 生命周期
* 本地进程 spawn
* 本地任务执行监控
* 本地 retry

dispatcher 不允许：

* 管理远程 worker
* 直接访问远程文件系统
* 跨机器 spawn

---

## 5.3 事件同步模型

系统同步的是“事件”，不是数据库。

---

## 支持的事件类型

```text id="l4f19n"
TaskCreated
TaskAssigned
TaskClaimed
TaskStarted
TaskHeartbeat
TaskCommentAdded
TaskCompleted
TaskFailed
TaskBlocked
TaskReleased
TaskCancelled
```

---

# 6. 集群模型

---

## 6.1 Main Node（主节点）

Main Node 负责：

* 节点注册
* 任务路由
* lease 管理
* heartbeat 检测
* 权限认证
* 任务同步
* 故障恢复

同时它也可以执行本地任务。

---

## 6.2 Worker Node（工作节点）

Worker Node：

* 运行本地 Hermes
* 运行本地 dispatcher
* 维护本地 kanban.db
* 执行任务
* 向 Main Node 回报状态

---

# 7. Capability 能力调度

每个节点会声明自身能力。

示例：

```yaml id="1c7s68"
node:
  id: pc-5070ti
  capabilities:
    - coding
    - gpu
    - browser
    - windows
```

任务可以声明需求：

```yaml id="tbfrh8"
task:
  requires:
    - coding
    - gpu
```

Main Node 根据 capability 进行调度。

---

# 8. 任务租约（Lease）

---

## 8.1 Lease 模型

同一时间：

```text id="jzwmzm"
一个任务只能被一个节点持有
```

示例：

```yaml id="x4c5fp"
lease:
  owner_node: pc-5070ti
  lease_until: 2026-05-14T20:00:00Z
```

---

## 8.2 Lease 过期

若节点 heartbeat 超时：

* lease 自动失效
* task 回到 READY
* 可重新调度

---

## 8.3 Heartbeat

节点需定期发送 heartbeat。

推荐：

```yaml id="39nzzj"
heartbeat_interval: 15s
lease_timeout: 60s
```

---

# 9. Local Mirror Task（本地镜像任务）

远程任务会映射为本地任务。

示例：

```text id="zq3kln"
远程任务:
  task_id = task_123

本地镜像:
  local_task_id = local_abc
  remote_task_id = task_123
```

dispatcher 只处理本地镜像任务。

---

# 10. 初始化流程

---

## 10.1 创建集群

```bash id="y7v8l1"
hermes remote init --create-cluster
```

初始化引导：

```text id="06fjlwm"
Cluster name:
Node name:
Capabilities:
Bind address:
API port:
```

生成配置：

```yaml id="vdic1k"
cluster:
  id: cluster_01
  role: main

node:
  id: node_main
  capabilities:
    - planner
    - reviewer
```

---

## 10.2 加入已有集群

```bash id="ddwjlwm"
hermes remote join http://nas.local:8787 --token xxx
```

生成：

```yaml id="1k1xx0"
cluster:
  id: cluster_01
  role: worker
  endpoint: http://nas.local:8787

node:
  id: node_pc
  capabilities:
    - coding
    - gpu
```

---

# 11. API 设计

---

## 11.1 初始化集群

```http id="0v52vg"
POST /api/v1/cluster/init
```

---

## 11.2 加入节点

```http id="8lbq5x"
POST /api/v1/nodes/join
```

请求：

```json id="0v3p7e"
{
  "node_name": "pc-5070ti",
  "capabilities": ["coding", "gpu"]
}
```

---

## 11.3 Heartbeat

```http id="n8glnj"
POST /api/v1/nodes/heartbeat
```

---

## 11.4 拉取任务

```http id="jlwmhr"
GET /api/v1/tasks/pull
```

---

## 11.5 Claim 任务

```http id="8vbbpr"
POST /api/v1/tasks/{id}/claim
```

---

## 11.6 推送事件

```http id="vjlwmj"
POST /api/v1/tasks/{id}/events
```

---

## 11.7 完成任务

```http id="jlwm3r"
POST /api/v1/tasks/{id}/complete
```

---

# 12. 故障恢复

---

## 12.1 节点断线

节点失联后：

* lease 过期
* task 回到 READY
* 可重新调度

---

## 12.2 本地崩溃恢复

节点重启后：

* 恢复 mirror task
* 校验 lease
* 清理 stale task

---

# 13. 安全模型

---

## 13.1 节点认证

支持：

* cluster token
* node key
* TLS（未来）

---

## 13.2 权限控制（未来）

示例：

```yaml id="jlwmte"
permissions:
  allow_task_types:
    - coding
    - gpu
```

---

# 14. Workspace 隔离

每个 task 应使用独立 workspace。

推荐：

```text id="0p28zv"
/workspaces/task-<id>
```

推荐 Git 策略：

```text id="1c8rpg"
git worktree
```

避免多个 Agent 共享同一工作目录。

---

# 15. 推荐技术栈

---

## Runtime

推荐：

```text id="9rjlwm"
Go
```

原因：

* goroutine 并发优秀
* 静态编译
* CLI/runtime 生态成熟
* 网络开发方便

---

## HTTP Framework

推荐：

* chi
* gin
* fiber

---

## 本地存储

推荐：

```text id="jlwmh7"
SQLite（仅本地）
```

---

# 16. MVP 范围

---

## 第一阶段包含

* 创建集群
* 加入集群
* 节点注册
* capability 调度
* task lease
* heartbeat
* 本地 mirror task
* 状态同步
* 故障恢复

---

## 第一阶段不包含

* 真 P2P mesh
* Raft
* 分布式 SQLite
* 远程 spawn
* 全局共享文件系统
* 多主冲突解决

---

# 17. 后续路线图

---

## Phase 2 ✅

* [x] Web Dashboard — v0.5.0
* [x] Cluster 可视化 — v0.6.0
* [x] Metrics (Prometheus) — v0.7.0
* [x] OpenTelemetry — v0.8.0

---

## Phase 3

### P3-1: Plugin SDK

通过 Webhook 机制允许第三方扩展接入集群生命周期事件。

**设计：**
- 注册/注销 Webhook URL（POST /api/v1/hooks）
- 每次集群事件触发后异步调用已注册的 hook
- 支持事件类型：`task_created`, `task_completed`, `task_failed`, `node_joined`, `node_left`, `lease_created`, `lease_expired`
- Webhook payload 包含事件类型 + 完整事件数据
- 带重试机制（3 次指数退避）
- 支持签名验证（HMAC-SHA256）

**API：**

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/v1/hooks` | 注册 webhook |
| DELETE | `/api/v1/hooks/{id}` | 注销 webhook |
| GET | `/api/v1/hooks` | 列出已注册 hooks |
| GET | `/api/v1/hooks/{id}/deliveries` | 查看投递记录 |

### P3-2: 动态调度器

基于节点负载 + 任务优先级的智能调度。

**设计：**
- 任务支持 `priority` 字段（1=最高，5=最低）
- 调度器按优先级排序，同优先级按 FIFO
- 节点负载追踪（已有 `load` 字段，增强计算）
- 策略：least-loaded-first + capability 匹配
- 调度决策记录（谁被调度到谁、为什么）
- 调度指标暴露（调度次数、平均等待时间、调度失败原因）

**API：**

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/v1/schedule/stats` | 调度统计 |
| POST | `/api/v1/tasks` | （扩展）支持 priority 字段 |

### P3-3: 多集群联邦

多个 hermes-agent-cluster 实例之间可以协作。

**设计：**
- 集群注册：通过 API 注册远程集群地址
- 任务转发：提交任务时可指定目标集群
- 跨集群状态查询：查询远程集群的节点/任务状态
- 联邦拓扑：本地集群可以看到所有已知集群的结构
- 心跳保活：定期检查远程集群可用性

**API：**

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/v1/federation/clusters` | 注册远程集群 |
| DELETE | `/api/v1/federation/clusters/{id}` | 移除集群 |
| GET | `/api/v1/federation/clusters` | 列出已知集群 |
| GET | `/api/v1/federation/clusters/{id}/status` | 查询远程集群状态 |
| POST | `/api/v1/federation/tasks` | 转发任务到远程集群 |

### P3-4: WAN 集群

支持跨公网部署的集群节点。

**设计：**
- TLS 支持：API 服务器可选 TLS 证书
- 可配置心跳间隔（WAN 默认 30s，LAN 默认 15s）
- 自动重连：断开后指数退避重连（1s → 2s → 4s → ... → 60s max）
- WAN 优化的批量事件同步（合并多个事件一次性发送）
- 节点 endpoint 支持 URL 格式（http:// 或 https://）

**配置扩展：**

```yaml
server:
  bind: "0.0.0.0"
  port: 8787
  tls:
    enabled: true
    cert_file: "/etc/certs/cluster.crt"
    key_file: "/etc/certs/cluster.key"

heartbeat:
  interval: 30s          # WAN: 30s, LAN: 15s
  lease_timeout: 120s    # WAN: 120s, LAN: 60s

reconnect:
  initial_interval: 1s
  max_interval: 60s
  multiplier: 2.0
```

---

## v1.0.0 发布

Phase 3 全部完成后发布 v1.0.0。

包含：
* Phase 1: 集群基础（节点管理、心跳、capability调度、租约、同步、故障恢复）
* Phase 2: 可观测性（Dashboard、可视化、Metrics、OpenTelemetry）
* Phase 3: 扩展性（Plugin SDK、动态调度、联邦、WAN）

质量要求：
- 所有集成测试通过
- 构建/测试 CI 绿色
- 双语 README + 完整文档
- CHANGELOG 覆盖所有版本

---

# 18. 示例部署

---

## Main Node

```text id="wjlwm3"
NAS / Home Server
- planner
- reviewer
- scheduler
- cluster coordinator
```

---

## PC Node

```text id="jlwm4p"
Windows 工作站
- coding
- GPU inference
- browser automation
```

---

## VPS Node

```text id="jlwmc2"
云服务器
- research
- web access
- 长任务
```

---

# 19. 设计哲学

系统核心：

```text id="6jlwmw"
Local Execution
+
Distributed Coordination
+
Event Synchronization
```

而不是：

```text id="jlwm4d"
Shared Database
+
Remote Process Management
```

---

# 20. 总结

`hermes-agent-cluster` 将 Hermes 从：

```text id="jlwmj2"
单机 Kanban Runtime
```

扩展为：

```text id="jlwm5h"
分布式多节点 Agent Orchestration Platform
```

同时保持 Hermes 原有：

* 本地 dispatcher
* 本地 worker
* 本地 workspace
* 本地执行模型

不被破坏。

---

# 21. 分布式工作流（Distributed Workflow）

---

## 21.1 Task Dependencies Model

任务可以声明对其他任务的依赖关系，形成有向无环图（DAG）。

### 依赖声明

```yaml
task:
  id: task_deploy
  depends_on: [task_build, task_test]
```

### 核心规则

* **下游自动触发**：当 `depends_on` 中所有任务状态变为 `DONE`，下游任务自动进入 `READY`
* **并行分支**：多个任务可以依赖同一个前置任务，互不影响地并行执行
* **失败传播**：若前置任务失败，下游任务标记为 `BLOCKED`，不会执行
* **循环检测**：系统拒绝任何会导致循环依赖的依赖声明

### 依赖图可视化

```text
task_research ──┬──→ task_write ──→ task_review ──→ task_publish
                │
task_translate ─┘
```

* `task_write` 和 `task_translate` 并行执行
* 两者完成后 `task_review` 才进入 `READY`
* `task_review` 完成后 `task_publish` 自动触发

### CLI 查看依赖

```bash
hermes remote workflow graph --project my-project
```

---

## 21.2 Trigger Mechanism

当任务完成时，系统自动评估并触发下游工作流。

### 自动触发（Auto-trigger）

任务状态变为 `DONE` 时，Cluster Registry 检查是否有下游任务：

```yaml
trigger:
  type: auto
  on: task_completed
```

### 条件触发（Conditional Trigger）

仅在满足条件时触发：

```yaml
trigger:
  type: conditional
  condition: "result.quality_score >= 0.8"
  on: task_completed
```

### 手动触发（Manual Trigger）

PM 可通过 CLI 手动推进工作流：

```bash
hermes remote workflow advance task_review --force
```

### 触发链（Trigger Chain）

```text
task_a completes
  → triggers task_b
    → triggers task_c
      → triggers task_d
```

系统支持最多 **10 层**的触发链深度，防止无限递归。

### 触发配置示例

```yaml
workflow:
  triggers:
    - id: trigger_1
      type: auto
      on: task_completed
      source_task: task_build
      target_task: task_test
    - id: trigger_2
      type: conditional
      on: task_completed
      source_task: task_test
      target_task: task_deploy
      condition: "all_tests_passed"
```

---

## 21.3 Global Status View

PM 可通过单一命令查看所有节点的任务状态。

### CLI 命令

```bash
# 查看所有节点的任务状态
hermes remote status

# 按节点过滤
hermes remote status --node pc-5070ti

# 按状态过滤
hermes remote status --status BLOCKED

# 按 capability 过滤
hermes remote status --capability gpu

# 按项目过滤
hermes remote status --project ai-daily-news

# 输出示例
┌─────────────────┬──────────────┬──────────┬─────────┬──────────┐
│ Task            │ Node         │ Status   │ Cap     │ Progress │
├─────────────────┼──────────────┼──────────┼─────────┼──────────┤
│ task_research   │ vps-cloud    │ DONE     │ research│ 100%     │
│ task_write      │ nas-main     │ RUNNING  │ planner │ 65%      │
│ task_translate  │ pc-5070ti    │ BLOCKED  │ gpu     │ 0%       │
│ task_review     │ nas-main     │ WAITING  │ reviewer│ 0%       │
└─────────────────┴──────────────┴──────────┴─────────┴──────────┘
```

### Dashboard Web UI

Phase 2 将提供 Web Dashboard：

```text
http://nas.local:8787/dashboard
```

功能：

* 集群节点拓扑图
* 实时任务状态流
* 依赖关系可视化
* 节点资源使用率
* 历史任务统计

---

## 21.4 Capability Configuration

### 节点启动时声明能力

节点加入集群时声明自身能力：

```yaml
node:
  id: pc-5070ti
  capabilities:
    - coding
    - gpu
    - browser
    - windows
```

### 动态能力更新

节点升级/降级时可动态更新能力：

```bash
hermes remote capability update --add gpu-inference
hermes remote capability remove --capability browser
```

### 能力匹配算法

调度器使用 **精确匹配 + 优先级排序**：

```text
1. 过滤：仅保留 capabilities 包含 task.requires 所有项的节点
2. 排序：按 capability 数量降序（能力越匹配越优先）
3. 负载均衡：同等条件下选择负载最低的节点
4. 回退：无匹配节点时标记 task 为 PENDING
```

### 能力声明示例

```yaml
# NAS 主节点
capabilities:
  - planner
  - reviewer
  - scheduler
  - cluster-coordinator

# PC 工作站
capabilities:
  - coding
  - gpu
  - gpu-inference
  - browser
  - windows

# VPS 云服务器
capabilities:
  - research
  - web-access
  - long-running
  - linux
```

---

## 21.5 Cross-Node Workflow Example

### AI Daily News Auto-Generation 项目

#### 工作流定义

```yaml
workflow:
  id: ai-daily-news
  name: AI Daily News Auto-Generation

  tasks:
    - id: task_research
      name: Research news sources
      requires: [web-access]
      node: vps-cloud

    - id: task_write
      name: Write news summary
      requires: [planner]
      depends_on: [task_research]
      node: nas-main

    - id: task_translate
      name: Translate to Chinese
      requires: [gpu]
      depends_on: [task_research]
      node: pc-5070ti

    - id: task_review
      name: Review and edit
      requires: [reviewer]
      depends_on: [task_write, task_translate]
      node: nas-main

    - id: task_publish
      name: Publish to blog
      requires: [web-access]
      depends_on: [task_review]
      node: vps-cloud
```

#### 依赖关系图

```text
              task_research
              /            \
    task_write              task_translate
         \                   /
          \                 /
           task_review
                |
          task_publish
```

#### 执行顺序和并行策略

```text
时间轴 →
─────────────────────────────────────────────────────────

vps-cloud:    [task_research: 10min]──────────[task_publish: 2min]
nas-main:                [task_write: 15min]──[task_review: 5min]
pc-5070ti:              [task_translate: 12min]

─────────────────────────────────────────────────────────
总耗时: ~30min（若串行执行需 ~44min，节省 32%）
```

#### 执行流程

1. PM 创建 workflow，系统按 `depends_on` 构建 DAG
2. `task_research` 无依赖，立即调度到 `vps-cloud`（匹配 `web-access`）
3. `task_research` 完成 → 自动触发 `task_write`（→ `nas-main`）和 `task_translate`（→ `pc-5070ti`）并行执行
4. 两者都完成后 → `task_review` 进入 `READY`，调度到 `nas-main`
5. `task_review` 完成 → `task_publish` 触发，调度到 `vps-cloud`
6. 全部完成，workflow 标记为 `DONE`

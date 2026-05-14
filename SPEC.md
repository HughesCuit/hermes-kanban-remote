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

## Phase 2

* Web Dashboard
* Cluster 可视化
* Metrics
* OpenTelemetry

---

## Phase 3

* Plugin SDK
* 动态调度器
* 多集群联邦
* WAN 集群

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

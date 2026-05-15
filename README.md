# 🚀 hermes-agent-cluster

[![Go 1.25+](https://img.shields.io/badge/Go-1.25+-00ADD8?logo=go&logoColor=white)](https://go.dev/)
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen)](https://github.com/heventure/hermes-agent-cluster)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Version](https://img.shields.io/badge/version-v0.8.0-orange)](https://github.com/heventure/hermes-agent-cluster/releases)

**Distributed Cluster Extension for Hermes Agent Kanban** — v0.8.0

---

## 📖 English

### 🌟 Introduction

`hermes-agent-cluster` is a distributed cluster extension for Hermes Agent. It enables multiple Hermes instances running on different devices to coordinate and collaborate as a unified distributed system. Each node maintains its own local `kanban.db` — no shared database required. Nodes communicate via HTTP API for cluster coordination, syncing only task metadata and lifecycle events.

### 🏗 Architecture

```
┌──────────────────────────────────────────────────┐
│                   Main Node                       │
│──────────────────────────────────────────────────│
│  🗂 Cluster Registry    📋 Task Router            │
│  🔒 Lease Manager       📊 Status View            │
│  ⚙️  Workflow Resolver   🔄 State Sync             │
│──────────────────────────────────────────────────│
│  🖥 Web Dashboard (embed)    📈 Prometheus (/metrics)  │
│  🗺 Cluster Viz (topo/metric/timeline)                 │
│  🔭 OpenTelemetry (OTLP/gRPC traces)                  │
│──────────────────────────────────────────────────│
│              Remote API (HTTP/REST)                │
└──────────────┬───────────────────┬────────────────┘
               │                   │
          ┌────┴────┐    ┌────────┴────────┐
          │         │    │                 │
     ┌────┴───┐ ┌──┴───┐ ┌──────┐  ┌──────┐
     │ Win 🖥 │ │ NAS  │ │ VPS  │  │ Mac  │
     │ Node   │ │Node 🗄│ │Node ☁│  │Node 🍎│
     └────────┘ └──────┘ └──────┘  └──────┘

Each node runs independently → HTTP coordination → sync task metadata only
```

### ✅ Feature Matrix

| Feature | Description | Status |
|---------|-------------|--------|
| 🎯 Multi-node Task Coordination | Distributed worker execution, cross-device scheduling | ✅ Stable |
| 🧠 Capability-aware Scheduling | Smart task assignment based on node capabilities | ✅ Stable |
| 🔄 Dynamic Capability Updates | Runtime capability updates with auto-reschedule | ✅ Stable |
| 📊 Global Status View | Cross-cluster query with multi-dimensional filters | ✅ Stable |
| 🖥 Web Dashboard | Embedded HTML/CSS/JS dashboard, real-time cluster view | ✅ Stable |
| 🗺 Cluster Visualization | Topology graphs, metrics panels, timeline events | ✅ Stable |
| 🔗 Workflow Dependencies | Dependency chain management, auto-advance, trigger chain | ✅ Stable |
| 🔒 Task Lease Management | Cluster-level lease mechanism, prevent duplicate execution | ✅ Stable |
| 🚨 Fault Detection & Recovery | Auto-detect offline nodes, task rescheduling | ✅ Stable |
| 🔄 State Sync | Real-time task metadata & lifecycle event sync between nodes | ✅ Stable |
| 🔭 OpenTelemetry | Distributed tracing (OTLP/gRPC or stdout export) | ✅ Stable |
| 📈 Prometheus Metrics | `/metrics` endpoint, `hac_` prefixed metric set | ✅ Stable |
| 💻 Local-first Execution | Each node maintains its own kanban.db, no shared DB | ✅ Stable |

### ⚡ Quick Start

#### Prerequisites

- Go 1.25+
- One or more devices running Hermes Agent

#### Method 1: Hermes Plugin (Recommended)

```bash
hermes plugins install HughesCuit/hermes-agent-cluster-plugin
bash ~/.hermes/plugins/hermes-agent-cluster/install.sh
```

#### Method 2: Build from Source

```bash
git clone https://github.com/heventure/hermes-agent-cluster.git
cd hermes-agent-cluster
go build -o kanban-cluster ./cmd/cluster
```

#### Method 3: Docker

```bash
docker build -t hermes-agent-cluster .
docker run -p 8787:8787 -v $(pwd)/cluster.yaml:/app/cluster.yaml hermes-agent-cluster
```

#### Configuration

Edit `cluster.yaml`:

```yaml
cluster:
  id: cluster_01
  role: main
  token: ""

node:
  id: node_main
  name: main-node
  capabilities:
    - planning
    - reviewing
    - scheduling

server:
  bind: "0.0.0.0"
  port: 8787

lease:
  ttl: 60s
  scan_rate: 10s

watchdog:
  check_interval: 5s
  degraded_after: 15s
  offline_after: 30s

telemetry:
  enabled: false
  exporter: "none"       # "otlp", "stdout", "none"
  endpoint: "localhost:4317"
  service_name: "hermes-agent-cluster"
  sample_rate: 1.0
  batch_timeout: 5s
```

#### Launch

```bash
# Main node
./kanban-cluster

# Worker nodes (modify node.id and capabilities in cluster.yaml)
./kanban-cluster
```

### 📡 API Reference

All endpoints prefixed: `/api/v1`

#### Node Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/nodes/join` | Register node to join cluster |
| `POST` | `/nodes/heartbeat` | Node heartbeat report |
| `GET` | `/nodes` | List all registered nodes |
| `PATCH` | `/nodes/{id}/capabilities` | Update node capabilities (auto-reschedule) |

#### Task Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/tasks` | Submit new task |
| `GET` | `/tasks` | List all tasks |
| `POST` | `/tasks/{id}/complete` | Mark task completed |
| `POST` | `/tasks/{id}/fail` | Mark task failed (auto-block downstream) |
| `POST` | `/tasks/{id}/unblock` | Manually unblock task |
| `POST` | `/tasks/{id}/advance` | Manually advance workflow |
| `POST` | `/tasks/{id}/dependencies` | Set task dependencies |

#### Task Dependencies & Workflow

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/tasks/{id}/dependents` | Get downstream tasks depending on this task |
| `GET` | `/tasks/{id}/trigger-chain` | Get trigger chain |
| `GET` | `/workflow/graph` | Get workflow dependency graph |

#### Lease Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/leases` | Create task lease |
| `DELETE` | `/leases/{id}` | Revoke lease |
| `GET` | `/leases` | List active leases |

#### Global Status View

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/status` | Global status query (supports `?node=`, `?status=`, `?capability=` filters) |

**Query Parameters** (composable):
- `?node=<node_id>` — Filter by node
- `?status=<pending|running|completed|failed|blocked>` — Filter by task status
- `?capability=<cap>` — Filter by node capability

**Response Example:**

```json
{
  "entries": [
    {
      "task_id": "t_xxx",
      "task_title": "my task",
      "task_status": "running",
      "node_id": "node_main",
      "node_name": "main-node",
      "node_status": "online",
      "capabilities": ["planning", "reviewing"],
      "requires": [],
      "lease_status": "active"
    }
  ],
  "summary": {
    "total_nodes": 3,
    "online_nodes": 2,
    "total_tasks": 15,
    "tasks_by_status": {"pending": 5, "running": 3, "completed": 7},
    "active_leases": 2
  }
}
```

> Note: `summary` always returns unfiltered aggregate data; `entries` respects query filters.

#### Sync & Recovery

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/sync/receive` | Receive sync data |
| `GET` | `/sync/status` | Query sync status |
| `POST` | `/recovery/trigger` | Trigger fault recovery |
| `GET` | `/recovery/log` | View recovery log |
| `GET` | `/recovery/stats` | View recovery stats |

#### Schedule Trigger

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/schedule/trigger` | Manually trigger pending task scheduling |

#### Cluster Visualization

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/cluster/topology` | Cluster topology (nodes, tasks, lease relations) |
| `GET` | `/cluster/metrics` | Aggregated metrics (node/task/lease stats) |
| `GET` | `/cluster/timeline` | Event timeline (recent N events) |
| `GET` | `/cluster/viz` | Combined visualization data (topology + metrics + timeline) |

#### Web Dashboard

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/dashboard/` | Embedded dashboard entry |
| `GET` | `/dashboard/*` | Dashboard static assets (HTML/CSS/JS) |

#### Prometheus Metrics

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/metrics` | Prometheus-format metrics (`hac_` prefix) |

### 📁 Project Structure

```
hermes-agent-cluster/
├── cmd/cluster/main.go           # 🚀 Main entry point
├── internal/
│   ├── api/                      # 🌐 HTTP API server
│   ├── capability/               # 🧠 Capability matching & scoring
│   ├── cluster/                  # 🗂 Cluster adapter & node management
│   ├── config/                   # ⚙️  Configuration loader
│   ├── dashboard/                # 🖥 Web Dashboard (embedded static files)
│   ├── heartbeat/                # 💓 Watchdog health checks
│   ├── lease/                    # 🔒 Lease management
│   ├── metrics/                  # 📈 Prometheus metrics collector
│   ├── recovery/                 # 🚨 Fault detection & recovery
│   ├── scheduler/                # 📋 Task scheduling & storage
│   ├── status/                   # 📊 Global status view
│   ├── sync/                     # 🔄 State sync protocol
│   ├── telemetry/                # 🔭 OpenTelemetry config & middleware
│   ├── visualization/            # 🗺 Cluster visualization (topo/metrics/timeline)
│   └── workflow/                 # 🔗 Workflow dependency resolver
├── tests/integration/            # 🧪 Integration tests
├── plugins/hermes-agent-cluster/ # 🔌 Hermes plugin integration
├── cluster.yaml                  # 📝 Configuration file
├── go.mod
└── go.sum
```

### 🛠 Development Guide

```bash
# Build
go build -o kanban-cluster ./cmd/cluster

# Unit tests (with race detection)
go test -race ./...

# Integration tests
cd tests/integration && go test -v

# E2E verification
bash test-e2e.sh
```

**E2E verification covers:** build, race detection, service startup, node registration, task scheduling, lease management, heartbeat, state sync, graceful shutdown, web dashboard endpoints, cluster visualization endpoints.

### 🔧 Key Dependencies

| Dependency | Purpose |
|------------|---------|
| `go-chi/chi` | HTTP router |
| `prometheus/client_golang` | Prometheus metrics |
| `go.opentelemetry.io/otel` | OpenTelemetry tracing |
| `google.golang.org/grpc` | gRPC for OTLP export |
| `gopkg.in/yaml.v3` | YAML configuration |

---

## 📖 中文

### 🌟 项目介绍

`hermes-agent-cluster` 是 Hermes Agent 的分布式集群扩展插件，允许多台设备上的独立 Hermes 实例协同工作，组成统一的分布式系统。每个节点维护独立的 `kanban.db`，无需共享数据库。节点间通过 HTTP API 进行集群协调，仅同步任务元数据和生命周期事件。

### 🏗 架构概览

```
┌──────────────────────────────────────────────────┐
│                   主节点 (Main Node)                │
│──────────────────────────────────────────────────│
│  🗂 集群注册表    📋 任务路由器            │
│  🔒 租约管理器    📊 全局状态视图            │
│  ⚙️  工作流解析器  🔄 状态同步             │
│──────────────────────────────────────────────────│
│  🖥 Web 仪表板 (内嵌)   📈 Prometheus (/metrics)  │
│  🗺 集群可视化 (拓扑/指标/时间线)                    │
│  🔭 OpenTelemetry (OTLP/gRPC 追踪)                 │
│──────────────────────────────────────────────────│
│              远程 API (HTTP/REST)                   │
└──────────────┬───────────────────┬────────────────┘
               │                   │
          ┌────┴────┐    ┌────────┴────────┐
          │         │    │                 │
     ┌────┴───┐ ┌──┴───┐ ┌──────┐  ┌──────┐
     │ Win 🖥 │ │ NAS  │ │ VPS  │  │ Mac  │
     │ 节点   │ │节点 🗄│ │节点 ☁│  │节点 🍎│
     └────────┘ └──────┘ └──────┘  └──────┘

每个节点独立运行 → HTTP 协调 → 仅同步任务元数据
```

### ✅ 功能矩阵

| 功能 | 说明 | 状态 |
|------|------|------|
| 🎯 多机任务协同 | 分布式 Worker 执行，跨设备任务调度 | ✅ 稳定 |
| 🧠 能力感知调度 | 基于节点 capabilities 的智能任务分配 | ✅ 稳定 |
| 🔄 动态能力更新 | 运行时更新节点能力，自动重新调度 pending tasks | ✅ 稳定 |
| 📊 全局状态视图 | 跨集群统一查询节点、任务、租约状态，支持多维过滤 | ✅ 稳定 |
| 🖥 Web Dashboard | 内嵌静态 HTML/CSS/JS 仪表板，实时查看集群状态 | ✅ 稳定 |
| 🗺 集群可视化 | 拓扑图、指标面板、时间线事件流 | ✅ 稳定 |
| 🔗 工作流依赖 | 任务依赖链管理、自动推进、触发链追踪 | ✅ 稳定 |
| 🔒 任务租约管理 | 集群级 lease 机制，防止任务重复执行 | ✅ 稳定 |
| 🚨 故障检测与恢复 | 自动检测离线节点，任务重新调度 | ✅ 稳定 |
| 🔄 状态同步 | 节点间任务元数据、生命周期事件实时同步 | ✅ 稳定 |
| 🔭 OpenTelemetry | 分布式追踪（OTLP/gRPC 或 stdout 导出） | ✅ 稳定 |
| 📈 Prometheus 指标 | `/metrics` 端点，`hac_` 前缀指标集 | ✅ 稳定 |
| 💻 本地优先执行 | 每个节点维护独立 kanban.db，无需共享数据库 | ✅ 稳定 |

### ⚡ 快速开始

#### 前置要求

- Go 1.25+
- 一个或多个运行 Hermes 的设备

#### 方式一：Hermes 插件安装（推荐）

```bash
hermes plugins install HughesCuit/hermes-agent-cluster-plugin
bash ~/.hermes/plugins/hermes-agent-cluster/install.sh
```

#### 方式二：从源码构建

```bash
git clone https://github.com/heventure/hermes-agent-cluster.git
cd hermes-agent-cluster
go build -o kanban-cluster ./cmd/cluster
```

#### 方式三：Docker

```bash
docker build -t hermes-agent-cluster .
docker run -p 8787:8787 -v $(pwd)/cluster.yaml:/app/cluster.yaml hermes-agent-cluster
```

#### 配置

编辑 `cluster.yaml`：

```yaml
cluster:
  id: cluster_01
  role: main
  token: ""

node:
  id: node_main
  name: main-node
  capabilities:
    - planning
    - reviewing
    - scheduling

server:
  bind: "0.0.0.0"
  port: 8787

lease:
  ttl: 60s
  scan_rate: 10s

watchdog:
  check_interval: 5s
  degraded_after: 15s
  offline_after: 30s

telemetry:
  enabled: false
  exporter: "none"       # "otlp", "stdout", "none"
  endpoint: "localhost:4317"
  service_name: "hermes-agent-cluster"
  sample_rate: 1.0
  batch_timeout: 5s
```

#### 启动

```bash
# 主节点
./kanban-cluster

# 工作节点（修改 cluster.yaml 中的 node.id 和 capabilities）
./kanban-cluster
```

### 📡 API 端点

所有端点前缀：`/api/v1`

#### 节点管理

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/nodes/join` | 节点注册加入集群 |
| `POST` | `/nodes/heartbeat` | 节点心跳上报 |
| `GET` | `/nodes` | 列出所有已注册节点 |
| `PATCH` | `/nodes/{id}/capabilities` | 动态更新节点能力（自动重调度） |

#### 任务管理

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/tasks` | 提交新任务 |
| `GET` | `/tasks` | 列出所有任务 |
| `POST` | `/tasks/{id}/complete` | 标记任务完成 |
| `POST` | `/tasks/{id}/fail` | 标记任务失败（自动阻塞下游任务） |
| `POST` | `/tasks/{id}/unblock` | 手动解除任务阻塞 |
| `POST` | `/tasks/{id}/advance` | 手动推进工作流 |
| `POST` | `/tasks/{id}/dependencies` | 设置任务依赖 |

#### 任务依赖与工作流

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/tasks/{id}/dependents` | 获取依赖某任务的下游任务 |
| `GET` | `/tasks/{id}/trigger-chain` | 获取触发链 |
| `GET` | `/workflow/graph` | 获取工作流依赖图 |

#### 租约管理

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/leases` | 创建任务租约 |
| `DELETE` | `/leases/{id}` | 吊销租约 |
| `GET` | `/leases` | 列出活跃租约 |

#### 全局状态视图

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/status` | 查询全局状态（支持 `?node=`, `?status=`, `?capability=` 过滤） |

**查询参数**（可组合）：
- `?node=<node_id>` — 按节点过滤
- `?status=<pending|running|completed|failed|blocked>` — 按任务状态过滤
- `?capability=<cap>` — 按节点能力过滤

**响应格式**：

```json
{
  "entries": [
    {
      "task_id": "t_xxx",
      "task_title": "my task",
      "task_status": "running",
      "node_id": "node_main",
      "node_name": "main-node",
      "node_status": "online",
      "capabilities": ["planning", "reviewing"],
      "requires": [],
      "lease_status": "active"
    }
  ],
  "summary": {
    "total_nodes": 3,
    "online_nodes": 2,
    "total_tasks": 15,
    "tasks_by_status": {"pending": 5, "running": 3, "completed": 7},
    "active_leases": 2
  }
}
```

> 注意：`summary` 始终返回全量未过滤数据，`entries` 按查询参数过滤。

#### 同步与恢复

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/sync/receive` | 接收同步数据 |
| `GET` | `/sync/status` | 查询同步状态 |
| `POST` | `/recovery/trigger` | 触发故障恢复 |
| `GET` | `/recovery/log` | 查看恢复日志 |
| `GET` | `/recovery/stats` | 查看恢复统计 |

#### 调度触发

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/schedule/trigger` | 手动触发待调度任务 |

#### 集群可视化

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/cluster/topology` | 集群拓扑（节点、任务、租约关系） |
| `GET` | `/cluster/metrics` | 聚合指标（节点/任务/租约统计） |
| `GET` | `/cluster/timeline` | 事件时间线（最近 N 条事件） |
| `GET` | `/cluster/viz` | 可视化数据包（拓扑 + 指标 + 时间线合并） |

#### Web Dashboard

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/dashboard/` | 内嵌仪表板入口 |
| `GET` | `/dashboard/*` | 仪表板静态资源（HTML/CSS/JS） |

#### Prometheus 指标

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/metrics` | Prometheus 格式指标（`hac_` 前缀） |

### 📁 项目结构

```
hermes-agent-cluster/
├── cmd/cluster/main.go           # 🚀 主入口
├── internal/
│   ├── api/                      # 🌐 HTTP API 服务
│   ├── capability/               # 🧠 能力匹配与评分
│   ├── cluster/                  # 🗂 集群适配器与节点管理
│   ├── config/                   # ⚙️  配置加载
│   ├── dashboard/                # 🖥 Web Dashboard（内嵌静态文件）
│   ├── heartbeat/                # 💓 看门狗健康检查
│   ├── lease/                    # 🔒 租约管理
│   ├── metrics/                  # 📈 Prometheus 指标收集器
│   ├── recovery/                 # 🚨 故障检测与恢复
│   ├── scheduler/                # 📋 任务调度与存储
│   ├── status/                   # 📊 全局状态视图
│   ├── sync/                     # 🔄 状态同步协议
│   ├── telemetry/                # 🔭 OpenTelemetry 配置与中间件
│   ├── visualization/            # 🗺 集群可视化（拓扑/指标/时间线）
│   └── workflow/                 # 🔗 工作流依赖解析
├── tests/integration/            # 🧪 集成测试
├── plugins/hermes-agent-cluster/ # 🔌 Hermes 插件集成
├── cluster.yaml                  # 📝 配置文件
├── go.mod
└── go.sum
```

### 🛠 开发指南

```bash
# 构建
go build -o kanban-cluster ./cmd/cluster

# 单元测试（含竞态检测）
go test -race ./...

# 集成测试
cd tests/integration && go test -v

# 端到端验证
bash test-e2e.sh
```

**E2E 验证项：** 构建、竞态检测、服务启动、节点注册、任务调度、租约管理、心跳上报、状态同步、优雅关闭、Web Dashboard 端点、集群可视化端点。

### 🔧 核心依赖

| 依赖 | 用途 |
|------|------|
| `go-chi/chi` | HTTP 路由器 |
| `prometheus/client_golang` | Prometheus 指标 |
| `go.opentelemetry.io/otel` | OpenTelemetry 分布式追踪 |
| `google.golang.org/grpc` | gRPC（OTLP 导出） |
| `gopkg.in/yaml.v3` | YAML 配置解析 |

---

## 📄 License

MIT License — Copyright (c) 2025 HeVenture

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

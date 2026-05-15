# hermes-agent-cluster

**Hermes 分布式 Kanban 集群扩展** — v0.8.0

`hermes-agent-cluster` 是 Hermes 的分布式集群扩展插件，允许多台设备上的独立 Hermes 实例协同工作。

---

## 功能特性

- **多机任务协同** — 分布式 Worker 执行，跨设备任务调度
- **能力感知调度** — 基于节点 capabilities 的智能任务分配
- **动态能力更新** — 运行时更新节点能力，自动重新调度 pending tasks
- **全局状态视图** — 跨集群统一查询节点、任务、租约状态，支持多维过滤
- **Web Dashboard** — 内嵌静态 HTML/CSS/JS 仪表板，实时查看集群状态
- **集群可视化** — 拓扑图、指标面板、时间线事件流
- **工作流依赖** — 任务依赖链管理、自动推进、触发链追踪
- **任务租约管理** — 集群级 lease 机制，防止任务重复执行
- **故障检测与恢复** — 自动检测离线节点，任务重新调度
- **状态同步** — 节点间任务元数据、生命周期事件实时同步
- **OpenTelemetry** — 分布式追踪（OTLP/gRPC 或 stdout 导出）
- **Prometheus 指标** — `/metrics` 端点，hac_ 前缀指标集
- **本地优先执行** — 每个节点维护独立 kanban.db，无需共享数据库

---

## 快速开始

### 前置要求

- Go 1.25+
- 一个或多个运行 Hermes 的设备

### 安装

```bash
git clone https://github.com/heventure/hermes-agent-cluster.git
cd hermes-agent-cluster
go build -o kanban-cluster ./cmd/cluster
```

### 配置

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

### 启动

```bash
# 主节点
./kanban-cluster

# 工作节点（修改 cluster.yaml 中的 node.id 和 capabilities）
./kanban-cluster
```

---

## API 端点

所有端点前缀：`/api/v1`

### 节点管理

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/nodes/join` | 节点注册加入集群 |
| POST | `/nodes/heartbeat` | 节点心跳上报 |
| GET | `/nodes` | 列出所有已注册节点 |
| PATCH | `/nodes/{id}/capabilities` | 动态更新节点能力（自动重调度） |

### 任务管理

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/tasks` | 提交新任务 |
| GET | `/tasks` | 列出所有任务 |
| POST | `/tasks/{id}/complete` | 标记任务完成 |
| POST | `/tasks/{id}/fail` | 标记任务失败（自动阻塞下游任务） |
| POST | `/tasks/{id}/unblock` | 手动解除任务阻塞 |
| POST | `/tasks/{id}/advance` | 手动推进工作流 |
| POST | `/tasks/{id}/dependencies` | 设置任务依赖 |

### 任务依赖与工作流

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/tasks/{id}/dependents` | 获取依赖某任务的下游任务 |
| GET | `/tasks/{id}/trigger-chain` | 获取触发链 |
| GET | `/workflow/graph` | 获取工作流依赖图 |

### 租约管理

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/leases` | 创建任务租约 |
| DELETE | `/leases/{id}` | 吊销租约 |
| GET | `/leases` | 列出活跃租约 |

### 全局状态视图

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/status` | 查询全局状态（支持 `?node=`, `?status=`, `?capability=` 过滤） |

查询参数（可组合）：
- `?node=<node_id>` — 按节点过滤
- `?status=<pending|running|completed|failed|blocked>` — 按任务状态过滤
- `?capability=<cap>` — 按节点能力过滤（匹配拥有该能力的节点上的任务）

响应格式：
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

### 同步与恢复

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/sync/receive` | 接收同步数据 |
| GET | `/sync/status` | 查询同步状态 |
| POST | `/recovery/trigger` | 触发故障恢复 |
| GET | `/recovery/log` | 查看恢复日志 |
| GET | `/recovery/stats` | 查看恢复统计 |

### 调度触发

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/schedule/trigger` | 手动触发待调度任务 |

### 集群可视化

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/cluster/topology` | 集群拓扑（节点、任务、租约关系） |
| GET | `/cluster/metrics` | 聚合指标（节点/任务/租约统计） |
| GET | `/cluster/timeline` | 事件时间线（最近 N 条事件） |
| GET | `/cluster/viz` | 可视化数据包（拓扑 + 指标 + 时间线合并） |

### Web Dashboard

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/dashboard/` | 内嵌仪表板入口 |
| GET | `/dashboard/*` | 仪表板静态资源（HTML/CSS/JS） |

### Prometheus 指标

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/metrics` | Prometheus 格式指标（hac_ 前缀） |

---

## 项目结构

```
hermes-agent-cluster/
├── cmd/cluster/main.go       # 主入口
├── internal/
│   ├── api/                  # HTTP API 服务
│   ├── capability/           # 能力匹配与评分
│   ├── cluster/              # 集群适配器与节点管理
│   ├── config/               # 配置加载
│   ├── dashboard/            # Web Dashboard（内嵌静态文件）
│   ├── heartbeat/            # 看门狗健康检查
│   ├── lease/                # 租约管理
│   ├── metrics/              # Prometheus 指标收集器
│   ├── recovery/             # 故障检测与恢复
│   ├── scheduler/            # 任务调度与存储
│   ├── status/               # 全局状态视图
│   ├── sync/                 # 状态同步协议
│   ├── telemetry/            # OpenTelemetry 配置与中间件
│   ├── visualization/        # 集群可视化（拓扑/指标/时间线）
│   └── workflow/             # 工作流依赖解析
├── tests/integration/        # 集成测试
├── cluster.yaml              # 配置文件
├── go.mod
└── go.sum
```

---

## 架构概览

```
┌────────────────────────────────┐
│          Main Node             │
│--------------------------------│
│ Cluster Registry               │
│ Lease Manager                  │
│ Task Router                    │
│ Status View                    │
│ Workflow Resolver              │
│────────────────────────────────│
│ Web Dashboard   (embed)        │
│ Cluster Viz     (topo/metric)  │
│ Prometheus      (/metrics)     │
│ OpenTelemetry   (OTLP trace)   │
│────────────────────────────────│
│ Remote API                     │
└───────────┬────────────────────┘
            │ HTTP
      ┌─────┼─────┐
      │     │     │
  ┌───┴──┐┌─┴───┐┌─┴───┐
  │ Win  ││ NAS ││ VPS │
  │ Node ││Node ││Node │
  └──────┘└─────┘└─────┘
```

每个节点独立运行 Hermes 实例，通过 HTTP API 进行集群协调。节点之间仅同步任务元数据和事件，不共享数据库。v0.8.0 包含完整的可观测性栈：Web Dashboard、集群可视化、OpenTelemetry 追踪、Prometheus 指标和 E2E 验证修复。

---

## E2E 验证

运行端到端验证：

```bash
bash e2e_test.sh
```

验证项包括：构建、竞态检测、服务启动、节点注册、任务调度、租约管理、心跳上报、状态同步、优雅关闭、Web Dashboard 端点、集群可视化端点。

---

## 开发

```bash
# 构建
go build -o kanban-cluster ./cmd/cluster

# 测试
go test -race ./...

# 集成测试
cd tests/integration && go test -v
```

---

## 许可证

MIT License

# hermes-agent-cluster

**Hermes 分布式 Agent 集群扩展**

`hermes-agent-cluster` 是 Hermes 的分布式集群扩展插件，允许多台设备上的独立 Hermes 实例协同工作。

---

## 功能特性

- **多机任务协同** — 分布式 Worker 执行，跨设备任务调度
- **能力感知调度** — 基于节点 capabilities 的智能任务分配
- **任务租约管理** — 集群级 lease 机制，防止任务重复执行
- **故障检测与恢复** — 自动检测离线节点，任务重新调度
- **状态同步** — 节点间任务元数据、生命周期事件实时同步
- **本地优先执行** — 每个节点维护独立 kanban.db，无需共享数据库

---

## 快速开始

### 前置要求

- Go 1.21+
- 一个或多个运行 Hermes 的设备

### 安装

```bash
git clone https://github.com/heventure/hermes-agent-cluster.git
cd hermes-agent-cluster
go build -o agent-cluster ./cmd/cluster
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
```

### 启动

```bash
# 主节点
./agent-cluster

# 工作节点（修改 cluster.yaml 中的 node.id 和 capabilities）
./agent-cluster
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

### 任务管理

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/tasks` | 提交新任务 |
| GET | `/tasks` | 列出所有任务 |
| POST | `/tasks/{id}/complete` | 标记任务完成 |

### 租约管理

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/leases` | 创建任务租约 |
| DELETE | `/leases/{id}` | 吊销租约 |
| GET | `/leases` | 列出活跃租约 |

### 同步与恢复

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/sync/receive` | 接收同步数据 |
| GET | `/sync/status` | 查询同步状态 |
| POST | `/recovery/trigger` | 触发故障恢复 |
| GET | `/recovery/log` | 查看恢复日志 |
| GET | `/recovery/stats` | 查看恢复统计 |

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
│   ├── heartbeat/            # 看门狗健康检查
│   ├── lease/                # 租约管理
│   ├── recovery/             # 故障检测与恢复
│   ├── scheduler/            # 任务调度与存储
│   └── sync/                 # 状态同步协议
├── tests/integration/        # 集成测试
├── cluster.yaml              # 配置文件
├── go.mod
└── go.sum
```

---

## 架构概览

```
┌────────────────────┐
│     Main Node      │
│--------------------│
│ Cluster Registry   │
│ Lease Manager      │
│ Task Router        │
│ Remote API         │
└─────────┬──────────┘
          │ HTTP
    ┌─────┼─────┐
    │     │     │
┌───┴──┐┌──┴──┐┌──┴──┐
│ Win  ││ NAS ││ VPS │
│ Node ││Node ││Node │
└──────┘└─────┘└─────┘
```

每个节点独立运行 Hermes 实例，通过 HTTP API 进行集群协调。节点之间仅同步任务元数据和事件，不共享数据库。

---

## E2E 验证

运行端到端验证：

```bash
bash e2e_test.sh
```

验证项包括：构建、竞态检测、服务启动、节点注册、任务调度、租约管理、心跳上报、状态同步、优雅关闭。

---

## 开发

```bash
# 构建
go build -o agent-cluster ./cmd/cluster

# 测试
go test -race ./...

# 集成测试
cd tests/integration && go test -v
```

---

## 许可证

MIT License

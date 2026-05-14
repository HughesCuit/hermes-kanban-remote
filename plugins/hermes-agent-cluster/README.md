# hermes-agent-cluster

**v0.1.0** — Distributed agent cluster coordination for Hermes Agent.

A Hermes Agent plugin that enables multi-node task coordination via the hermes-agent-cluster Go service. Nodes can initialize/join clusters, submit tasks, track progress, and coordinate distributed work across multiple agents.

## Installation

```bash
hermes plugin install hermes-agent-cluster
```

Or manually copy the plugin directory:

```bash
cp -r ~/.hermes/plugins/hermes-agent-cluster ~/.hermes/plugins/
```

## Prerequisites

- **hermes-cluster** binary must be in your `PATH` (build from `cmd/cluster/main.go`)
- A running Hermes Agent instance

## Available Tools

| Tool | Description |
|------|-------------|
| `agent_cluster_init` | Initialize a new cluster (main/coordinator node) |
| `agent_cluster_join` | Join an existing cluster as a worker node |
| `agent_cluster_submit` | Submit a task for distributed execution |
| `agent_cluster_list` | List all tasks in the cluster |
| `agent_cluster_nodes` | List all nodes in the cluster |
| `agent_cluster_heartbeat` | Send heartbeat to indicate node is alive |
| `agent_cluster_complete` | Mark a task as completed with results |

## Configuration

### Cluster Config (auto-generated)

The plugin writes YAML configs to `~/.hermes/agent-cluster/cluster.yaml`:

```yaml
cluster:
  id: hermes-cluster
  role: main
  token: "your-auth-token"

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
  ttl: 30s
  scan_rate: 5s

watchdog:
  check_interval: 3s
  degraded_after: 10s
  offline_after: 20s
```

### Environment Variables

- `HERMES_HOME` — Override config directory (default: `~/.hermes`)

## Usage Examples

### Initialize a Cluster (Main Node)

```python
agent_cluster_init(port=8787, node_id="node_main", capabilities=["planning", "reviewing"])
```

### Join as Worker

```python
agent_cluster_join(endpoint="http://main-node:8787", node_id="worker_1", capabilities=["coding", "gpu"])
```

### Submit a Task

```python
agent_cluster_submit(title="Build the feature", requires=["coding"])
```

### Monitor the Cluster

```python
agent_cluster_nodes()   # List all connected nodes
agent_cluster_list()    # List all tasks and their status
```

### Complete a Task

```python
agent_cluster_complete(task_id="task-123", result="Feature built successfully")
```

## Architecture

- **Main Node**: Coordinates task distribution, maintains cluster state
- **Worker Nodes**: Execute tasks, send heartbeats, report completion
- **Scheduler**: Matches tasks to nodes based on capabilities
- **Lease Manager**: Manages task leases with TTL-based expiration
- **Watchdog**: Detects degraded/offline nodes
- **Recovery**: Reschedules tasks from failed nodes

## License

Part of the Hermes Agent ecosystem by Heventure Group.

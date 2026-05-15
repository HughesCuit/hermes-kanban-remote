# hermes-agent-cluster Plugin

Distributed Kanban cluster extension for Hermes Agent — multi-node task coordination via hermes-agent-cluster Go service with **auto-start**.

## Features

- **Auto-start**: Cluster service starts automatically when the plugin loads
- **Health checking**: Automatically detects if cluster is already running
- **Graceful shutdown**: Cluster stops cleanly when session ends
- **7 cluster tools**: init, join, submit, list, nodes, heartbeat, complete

## Quick Start

1. Install the plugin:
```bash
hermes plugin install hermes-agent-cluster
```

2. Ensure `hermes-cluster` binary is in your PATH:
```bash
which hermes-cluster
```

3. Start Hermes Agent — the cluster will auto-start on port 8787:
```bash
hermes
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `HERMES_CLUSTER_AUTO_START` | `true` | Enable/disable auto-start |
| `HERMES_CLUSTER_PORT` | `8787` | Cluster listen port |
| `HERMES_CLUSTER_ID` | `hermes-cluster` | Cluster identifier |
| `HERMES_CLUSTER_NODE_ID` | `node_main` | This node's ID |
| `HERMES_CLUSTER_NODE_NAME` | `main-node` | This node's name |
| `HERMES_CLUSTER_TOKEN` | `""` | Auth token |
| `HERMES_CLUSTER_CONFIG` | `""` | Path to cluster.yaml |
| `HERMES_CLUSTER_BINARY` | `hermes-cluster` | Binary path/name |

### Plugin Config File

Create `~/.hermes/agent-cluster/plugin.yaml`:

```yaml
auto_start: true
port: 8787
cluster_id: my-cluster
node_id: node-main
node_name: main-node
capabilities:
  - planning
  - reviewing
  - scheduling
token: "your-secret-token"
```

## How It Works

### Auto-Start Lifecycle

1. **Session Start** (`on_session_start` hook):
   - Checks if cluster is already running (health check)
   - If not running, finds `hermes-cluster` binary
   - Generates default config if needed
   - Starts cluster in background thread
   - Waits for cluster to become healthy (5s timeout)

2. **During Session**:
   - All cluster tools work against the running instance
   - Health checks verify cluster status before API calls

3. **Session End** (`on_session_end` hook):
   - Stops heartbeat thread if running
   - Gracefully terminates cluster process (SIGTERM)
   - Falls back to SIGKILL if graceful shutdown fails

### Health Check

The plugin checks cluster health via `GET /health` endpoint:
- Returns `{"status": "ok"}` when healthy
- Retries every 200ms during startup (up to 5s timeout)

### Binary Discovery

1. Checks if `HERMES_CLUSTER_BINARY` is an absolute path
2. Searches system PATH using `shutil.which()`
3. Falls back to manual PATH search

## Tools

| Tool | Description |
|------|-------------|
| `kanban_cluster_init` | Initialize a new cluster (main node) |
| `kanban_cluster_join` | Join existing cluster as worker |
| `kanban_cluster_submit` | Submit task to cluster |
| `kanban_cluster_list` | List all tasks |
| `kanban_cluster_nodes` | List all nodes |
| `kanban_cluster_heartbeat` | Send heartbeat |
| `kanban_cluster_complete` | Mark task completed |

## Development

### Running Tests

```bash
cd plugins/hermes-agent-cluster
python -m pytest test_plugin.py -v
```

### Test Coverage

- Configuration loading (env vars, defaults, config files)
- Health checking (success, failure, timeout)
- Binary discovery (absolute path, PATH search)
- Config generation (create, no-overwrite)
- Cluster lifecycle (start, stop, already running)
- Hook execution (on_session_start, on_session_end)
- API helper (success, error handling)

## Architecture

```
Hermes Agent
    │
    ├── on_session_start hook
    │   └── _start_cluster_auto()
    │       ├── _check_cluster_health()
    │       ├── _find_binary()
    │       ├── _generate_default_config()
    │       └── subprocess.Popen()
    │
    ├── Tool handlers
    │   ├── handle_cluster_init()
    │   ├── handle_cluster_join()
    │   ├── handle_cluster_submit()
    │   └── ...
    │
    └── on_session_end hook
        └── _stop_cluster_auto()
            └── process.terminate()
```

## License

MIT License - Heventure Group

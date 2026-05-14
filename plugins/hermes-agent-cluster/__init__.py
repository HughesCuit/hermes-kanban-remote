"""hermes-agent-cluster v0.1.0 — Distributed agent cluster coordination for Hermes Agent.

Registers 7 tools for multi-node agent task coordination:
- agent_cluster_init: Initialize a new cluster (main/coordinator node)
- agent_cluster_join: Join an existing cluster as a worker node
- agent_cluster_submit: Submit a task for distributed execution
- agent_cluster_list: List all tasks in the cluster
- agent_cluster_nodes: List all nodes in the cluster
- agent_cluster_heartbeat: Send heartbeat to indicate node is alive
- agent_cluster_complete: Mark a task as completed with results

Part of the Hermes Agent plugin ecosystem by Heventure Group.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.request import Request, urlopen
from urllib.error import URLError

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

_cluster_config: Dict[str, Any] = {}
_heartbeat_thread: Optional[threading.Thread] = None
_heartbeat_stop = threading.Event()


# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------

def _api_call(base_url: str, method: str, path: str, data: dict = None) -> dict:
    """Make HTTP request to hermes-cluster API."""
    url = f"{base_url}{path}"
    body = json.dumps(data).encode() if data else None
    req = Request(url, data=body, method=method)
    req.add_header("Content-Type", "application/json")
    try:
        with urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except URLError as e:
        return {"error": str(e)}
    except Exception as e:
        return {"error": str(e)}


# ---------------------------------------------------------------------------
# Tool handlers
# ---------------------------------------------------------------------------

def handle_cluster_init(args: dict, **kwargs) -> str:
    """Initialize a new cluster (main node)."""
    port = args.get("port", 8787)
    node_id = args.get("node_id", "node_main")
    capabilities = args.get("capabilities", ["planning", "reviewing", "scheduling"])

    # Write config
    config_dir = Path(os.environ.get("HERMES_HOME", Path.home() / ".hermes")) / "agent-cluster"
    config_dir.mkdir(parents=True, exist_ok=True)
    config_path = config_dir / "cluster.yaml"

    config_content = f"""cluster:
  id: {args.get("cluster_id", "hermes-cluster")}
  role: main
  token: "{args.get("token", "")}"

node:
  id: {node_id}
  name: main-node
  capabilities:
{chr(10).join(f"    - {c}" for c in capabilities)}

server:
  bind: "0.0.0.0"
  port: {port}

lease:
  ttl: 30s
  scan_rate: 5s

watchdog:
  check_interval: 3s
  degraded_after: 10s
  offline_after: 20s
"""
    config_path.write_text(config_content)

    # Start hermes-cluster process
    try:
        proc = subprocess.Popen(
            ["hermes-cluster", "-config", str(config_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        _cluster_config["process"] = proc
        _cluster_config["base_url"] = f"http://127.0.0.1:{port}"
        _cluster_config["node_id"] = node_id
        _cluster_config["role"] = "main"
        time.sleep(1)  # Wait for server to start

        return json.dumps({
            "status": "initialized",
            "node_id": node_id,
            "role": "main",
            "port": port,
            "pid": proc.pid,
            "config": str(config_path),
        })
    except FileNotFoundError:
        return json.dumps({"error": "hermes-cluster binary not found in PATH"})
    except Exception as e:
        return json.dumps({"error": str(e)})


def handle_cluster_join(args: dict, **kwargs) -> str:
    """Join an existing cluster as worker."""
    endpoint = args.get("endpoint", "http://127.0.0.1:8787")
    node_id = args.get("node_id", "node_worker")
    capabilities = args.get("capabilities", ["coding", "gpu", "browser"])

    port = args.get("port", 8788)

    # Write config
    config_dir = Path(os.environ.get("HERMES_HOME", Path.home() / ".hermes")) / "agent-cluster"
    config_dir.mkdir(parents=True, exist_ok=True)
    config_path = config_dir / "cluster-worker.yaml"

    config_content = f"""cluster:
  id: {args.get("cluster_id", "hermes-cluster")}
  role: worker
  endpoint: "{endpoint}"
  token: "{args.get("token", "")}"

node:
  id: {node_id}
  name: worker-node
  capabilities:
{chr(10).join(f"    - {c}" for c in capabilities)}

server:
  bind: "0.0.0.0"
  port: {port}

lease:
  ttl: 30s
  scan_rate: 5s

watchdog:
  check_interval: 3s
  degraded_after: 10s
  offline_after: 20s
"""
    config_path.write_text(config_content)

    # Start hermes-cluster process
    try:
        proc = subprocess.Popen(
            ["hermes-cluster", "-config", str(config_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        _cluster_config["process"] = proc
        _cluster_config["base_url"] = f"http://127.0.0.1:{port}"
        _cluster_config["node_id"] = node_id
        _cluster_config["role"] = "worker"
        time.sleep(1)

        # Register with main node
        result = _api_call(endpoint, "POST", "/api/v1/nodes/join", {
            "node_name": node_id,
            "capabilities": capabilities,
            "endpoint": f"http://127.0.0.1:{port}",
        })

        return json.dumps({
            "status": "joined",
            "node_id": node_id,
            "role": "worker",
            "endpoint": endpoint,
            "port": port,
            "pid": proc.pid,
            "join_response": result,
        })
    except FileNotFoundError:
        return json.dumps({"error": "hermes-cluster binary not found in PATH"})
    except Exception as e:
        return json.dumps({"error": str(e)})


def handle_cluster_submit(args: dict, **kwargs) -> str:
    """Submit a task to the cluster."""
    base_url = _cluster_config.get("base_url")
    if not base_url:
        return json.dumps({"error": "Not connected to cluster. Run agent_cluster_init or agent_cluster_join first."})

    result = _api_call(base_url, "POST", "/api/v1/tasks", {
        "title": args.get("title", "Untitled task"),
        "requires": args.get("requires", []),
    })
    return json.dumps(result)


def handle_cluster_list(args: dict, **kwargs) -> str:
    """List tasks on the cluster."""
    base_url = _cluster_config.get("base_url")
    if not base_url:
        return json.dumps({"error": "Not connected to cluster."})

    result = _api_call(base_url, "GET", "/api/v1/tasks")
    return json.dumps(result, indent=2)


def handle_cluster_nodes(args: dict, **kwargs) -> str:
    """List cluster nodes."""
    base_url = _cluster_config.get("base_url")
    if not base_url:
        return json.dumps({"error": "Not connected to cluster."})

    result = _api_call(base_url, "GET", "/api/v1/nodes")
    return json.dumps(result, indent=2)


def handle_cluster_heartbeat(args: dict, **kwargs) -> str:
    """Send heartbeat to cluster."""
    base_url = _cluster_config.get("base_url")
    node_id = _cluster_config.get("node_id")
    if not base_url or not node_id:
        return json.dumps({"error": "Not connected to cluster."})

    result = _api_call(base_url, "POST", "/api/v1/nodes/heartbeat", {
        "node_id": node_id,
    })
    return json.dumps(result)


def handle_cluster_complete(args: dict, **kwargs) -> str:
    """Mark a task as completed."""
    base_url = _cluster_config.get("base_url")
    node_id = _cluster_config.get("node_id")
    if not base_url:
        return json.dumps({"error": "Not connected to cluster."})

    task_id = args.get("task_id")
    if not task_id:
        return json.dumps({"error": "task_id is required"})

    result = _api_call(base_url, "POST", f"/api/v1/tasks/{task_id}/complete", {
        "node_id": node_id,
        "result": args.get("result", "completed"),
    })
    return json.dumps(result)


# ---------------------------------------------------------------------------
# Tool schemas
# ---------------------------------------------------------------------------

CLUSTER_INIT_SCHEMA = {
    "name": "agent_cluster_init",
    "description": "Initialize a new hermes-agent-cluster cluster. This node becomes the main/coordinator node.",
    "parameters": {
        "type": "object",
        "properties": {
            "port": {"type": "integer", "description": "Port to listen on", "default": 8787},
            "node_id": {"type": "string", "description": "This node's unique ID"},
            "cluster_id": {"type": "string", "description": "Cluster identifier"},
            "capabilities": {"type": "array", "items": {"type": "string"}, "description": "Node capabilities"},
            "token": {"type": "string", "description": "Cluster auth token"},
        },
    },
}

CLUSTER_JOIN_SCHEMA = {
    "name": "agent_cluster_join",
    "description": "Join an existing hermes-agent-cluster cluster as a worker node.",
    "parameters": {
        "type": "object",
        "properties": {
            "endpoint": {"type": "string", "description": "Main node URL (e.g. http://main:8787)"},
            "node_id": {"type": "string", "description": "This node's unique ID"},
            "port": {"type": "integer", "description": "Port to listen on", "default": 8788},
            "cluster_id": {"type": "string", "description": "Cluster identifier"},
            "capabilities": {"type": "array", "items": {"type": "string"}, "description": "Node capabilities"},
            "token": {"type": "string", "description": "Cluster auth token"},
        },
        "required": ["endpoint"],
    },
}

CLUSTER_SUBMIT_SCHEMA = {
    "name": "agent_cluster_submit",
    "description": "Submit a task to the cluster for distributed execution.",
    "parameters": {
        "type": "object",
        "properties": {
            "title": {"type": "string", "description": "Task title/description"},
            "requires": {"type": "array", "items": {"type": "string"}, "description": "Required capabilities"},
        },
        "required": ["title"],
    },
}

CLUSTER_LIST_SCHEMA = {
    "name": "agent_cluster_list",
    "description": "List all tasks in the cluster.",
    "parameters": {"type": "object", "properties": {}},
}

CLUSTER_NODES_SCHEMA = {
    "name": "agent_cluster_nodes",
    "description": "List all nodes in the cluster.",
    "parameters": {"type": "object", "properties": {}},
}

CLUSTER_HEARTBEAT_SCHEMA = {
    "name": "agent_cluster_heartbeat",
    "description": "Send heartbeat to the cluster to indicate this node is alive.",
    "parameters": {"type": "object", "properties": {}},
}

CLUSTER_COMPLETE_SCHEMA = {
    "name": "agent_cluster_complete",
    "description": "Mark a task as completed with results.",
    "parameters": {
        "type": "object",
        "properties": {
            "task_id": {"type": "string", "description": "Task ID to complete"},
            "result": {"type": "string", "description": "Task result/description"},
        },
        "required": ["task_id"],
    },
}


# ---------------------------------------------------------------------------
# Plugin registration
# ---------------------------------------------------------------------------

def register(ctx) -> None:
    """Register agent cluster tools with Hermes Agent."""
    ctx.register_tool(
        name="agent_cluster_init",
        toolset="agent_cluster",
        schema=CLUSTER_INIT_SCHEMA,
        handler=handle_cluster_init,
        description="Initialize a distributed agent cluster",
        emoji="🏗️",
    )

    ctx.register_tool(
        name="agent_cluster_join",
        toolset="agent_cluster",
        schema=CLUSTER_JOIN_SCHEMA,
        handler=handle_cluster_join,
        description="Join an existing agent cluster",
        emoji="🔗",
    )

    ctx.register_tool(
        name="agent_cluster_submit",
        toolset="agent_cluster",
        schema=CLUSTER_SUBMIT_SCHEMA,
        handler=handle_cluster_submit,
        description="Submit a task to the cluster",
        emoji="📋",
    )

    ctx.register_tool(
        name="agent_cluster_list",
        toolset="agent_cluster",
        schema=CLUSTER_LIST_SCHEMA,
        handler=handle_cluster_list,
        description="List cluster tasks",
        emoji="📊",
    )

    ctx.register_tool(
        name="agent_cluster_nodes",
        toolset="agent_cluster",
        schema=CLUSTER_NODES_SCHEMA,
        handler=handle_cluster_nodes,
        description="List cluster nodes",
        emoji="🖥️",
    )

    ctx.register_tool(
        name="agent_cluster_heartbeat",
        toolset="agent_cluster",
        schema=CLUSTER_HEARTBEAT_SCHEMA,
        handler=handle_cluster_heartbeat,
        description="Send cluster heartbeat",
        emoji="💓",
    )

    ctx.register_tool(
        name="agent_cluster_complete",
        toolset="agent_cluster",
        schema=CLUSTER_COMPLETE_SCHEMA,
        handler=handle_cluster_complete,
        description="Complete a cluster task",
        emoji="✅",
    )

    logger.info("hermes-agent-cluster plugin registered 7 cluster tools")

"""hermes-agent-cluster plugin — Distributed Kanban cluster for Hermes Agent.

Registers tools that let the agent interact with a hermes-agent-cluster cluster:
- kanban_cluster_join: Join a cluster as worker
- kanban_cluster_submit: Submit a task to the cluster
- kanban_cluster_list: List tasks on the cluster
- kanban_cluster_nodes: List cluster nodes
- kanban_cluster_heartbeat: Send heartbeat
- kanban_cluster_complete: Mark task as completed

Auto-start: When the plugin loads, it automatically starts the cluster service
if not already running. Configure via plugin config or environment variables.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import signal
import subprocess
import sys
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
_auto_start_process: Optional[subprocess.Popen] = None
_auto_start_lock = threading.Lock()

# Default configuration
DEFAULT_PORT = 8787
DEFAULT_CLUSTER_ID = "hermes-cluster"
DEFAULT_NODE_ID = "node_main"
DEFAULT_NODE_NAME = "main-node"
DEFAULT_CAPABILITIES = ["planning", "reviewing", "scheduling"]
AUTO_START_TIMEOUT = 5  # seconds to wait for cluster to start
HEALTH_CHECK_TIMEOUT = 2  # seconds for health check request


# ---------------------------------------------------------------------------
# Configuration loading
# ---------------------------------------------------------------------------

def _get_plugin_config() -> Dict[str, Any]:
    """Load plugin configuration from environment or config file.
    
    Configuration sources (in priority order):
    1. Environment variables (HERMES_CLUSTER_*)
    2. Plugin config file (~/.hermes/agent-cluster/plugin.yaml)
    3. Default values
    """
    config = {
        "auto_start": True,
        "port": DEFAULT_PORT,
        "cluster_id": DEFAULT_CLUSTER_ID,
        "node_id": DEFAULT_NODE_ID,
        "node_name": DEFAULT_NODE_NAME,
        "capabilities": DEFAULT_CAPABILITIES,
        "token": "",
        "config_path": None,
        "binary_path": "hermes-cluster",
    }
    
    # Environment overrides
    env_map = {
        "HERMES_CLUSTER_AUTO_START": ("auto_start", lambda x: x.lower() in ("true", "1", "yes")),
        "HERMES_CLUSTER_PORT": ("port", int),
        "HERMES_CLUSTER_ID": ("cluster_id", str),
        "HERMES_CLUSTER_NODE_ID": ("node_id", str),
        "HERMES_CLUSTER_NODE_NAME": ("node_name", str),
        "HERMES_CLUSTER_TOKEN": ("token", str),
        "HERMES_CLUSTER_CONFIG": ("config_path", str),
        "HERMES_CLUSTER_BINARY": ("binary_path", str),
    }
    
    for env_var, (key, converter) in env_map.items():
        value = os.environ.get(env_var)
        if value is not None:
            try:
                config[key] = converter(value)
            except (ValueError, TypeError):
                logger.warning("Invalid value for %s: %s", env_var, value)
    
    # Try to load from plugin config file
    config_dir = Path(os.environ.get("HERMES_HOME", Path.home() / ".hermes")) / "agent-cluster"
    plugin_config_path = config_dir / "plugin.yaml"
    
    if plugin_config_path.exists():
        try:
            import yaml
            with open(plugin_config_path) as f:
                plugin_config = yaml.safe_load(f)
                if isinstance(plugin_config, dict):
                    # Merge with defaults, env vars take priority
                    for key, value in plugin_config.items():
                        if key in config and key not in _get_env_set_keys():
                            config[key] = value
        except Exception as e:
            logger.debug("Failed to load plugin config: %s", e)
    
    return config


def _get_env_set_keys() -> set:
    """Return set of config keys that are explicitly set via environment variables."""
    env_keys = set()
    env_map = {
        "HERMES_CLUSTER_AUTO_START": "auto_start",
        "HERMES_CLUSTER_PORT": "port",
        "HERMES_CLUSTER_ID": "cluster_id",
        "HERMES_CLUSTER_NODE_ID": "node_id",
        "HERMES_CLUSTER_NODE_NAME": "node_name",
        "HERMES_CLUSTER_TOKEN": "token",
        "HERMES_CLUSTER_CONFIG": "config_path",
        "HERMES_CLUSTER_BINARY": "binary_path",
    }
    for env_var, key in env_map.items():
        if os.environ.get(env_var) is not None:
            env_keys.add(key)
    return env_keys


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
# Health check
# ---------------------------------------------------------------------------

def _check_cluster_health(base_url: str) -> bool:
    """Check if cluster is running and healthy."""
    try:
        result = _api_call(base_url, "GET", "/api/v1/status")
        # Cluster is healthy if it responds with a valid status object
        return "error" not in result and ("summary" in result or "entries" in result)
    except Exception:
        return False


def _wait_for_cluster(base_url: str, timeout: float = AUTO_START_TIMEOUT) -> bool:
    """Wait for cluster to become healthy after starting."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if _check_cluster_health(base_url):
            return True
        time.sleep(0.2)
    return False


# ---------------------------------------------------------------------------
# Cluster lifecycle
# ---------------------------------------------------------------------------

def _find_binary(config: Dict[str, Any]) -> Optional[str]:
    """Find the hermes-cluster binary."""
    binary_path = config.get("binary_path", "hermes-cluster")
    
    # If it's a full path, check it exists
    if os.path.isabs(binary_path):
        return binary_path if os.path.isfile(binary_path) else None
    
    # Search in PATH
    return shutil.which(binary_path)


def _generate_default_config(config: Dict[str, Any]) -> Path:
    """Generate a default cluster configuration file."""
    config_dir = Path(os.environ.get("HERMES_HOME", Path.home() / ".hermes")) / "agent-cluster"
    config_dir.mkdir(parents=True, exist_ok=True)
    config_path = config_dir / "cluster.yaml"
    
    # Only write if doesn't exist
    if not config_path.exists():
        config_content = f"""cluster:
  id: {config['cluster_id']}
  role: main
  token: "{config['token']}"

node:
  id: {config['node_id']}
  name: {config['node_name']}
  capabilities:
{chr(10).join(f"    - {c}" for c in config['capabilities'])}

server:
  bind: "0.0.0.0"
  port: {config['port']}

lease:
  ttl: 30s
  scan_rate: 5s

watchdog:
  check_interval: 3s
  degraded_after: 10s
  offline_after: 20s
"""
        config_path.write_text(config_content)
        logger.info("Generated default cluster config at %s", config_path)
    
    return config_path


def _start_cluster_auto(config: Dict[str, Any]) -> bool:
    """Auto-start the cluster service if not already running."""
    global _auto_start_process
    
    base_url = f"http://127.0.0.1:{config['port']}"
    
    # Check if already running
    if _check_cluster_health(base_url):
        logger.info("Cluster already running on port %d", config['port'])
        _cluster_config["base_url"] = base_url
        _cluster_config["node_id"] = config["node_id"]
        _cluster_config["role"] = "main"
        return True
    
    # Find binary
    binary = _find_binary(config)
    if not binary:
        logger.warning("hermes-cluster binary not found, skipping auto-start")
        return False
    
    # Get or generate config file
    if config.get("config_path"):
        config_file = Path(config["config_path"])
        if not config_file.exists():
            logger.warning("Config file not found: %s", config_file)
            return False
    else:
        config_file = _generate_default_config(config)
    
    # Start the process
    try:
        with _auto_start_lock:
            if _auto_start_process is not None and _auto_start_process.poll() is None:
                logger.info("Cluster process already running (PID %d)", _auto_start_process.pid)
                return True
            
            logger.info("Starting hermes-cluster on port %d...", config['port'])
            _auto_start_process = subprocess.Popen(
                [binary, "-config", str(config_file)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0) if sys.platform == "win32" else 0,
            )
        
        # Wait for cluster to become healthy
        if _wait_for_cluster(base_url):
            logger.info("Cluster started successfully (PID %d)", _auto_start_process.pid)
            _cluster_config["base_url"] = base_url
            _cluster_config["node_id"] = config["node_id"]
            _cluster_config["role"] = "main"
            _cluster_config["process"] = _auto_start_process
            return True
        else:
            logger.error("Cluster failed to start within timeout")
            _stop_cluster_auto()
            return False
            
    except FileNotFoundError:
        logger.error("Failed to start cluster: binary not found")
        return False
    except Exception as e:
        logger.error("Failed to start cluster: %s", e)
        return False


def _stop_cluster_auto():
    """Stop the auto-started cluster process."""
    global _auto_start_process
    
    with _auto_start_lock:
        if _auto_start_process is not None:
            try:
                logger.info("Stopping cluster process (PID %d)...", _auto_start_process.pid)
                _auto_start_process.terminate()
                try:
                    _auto_start_process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    logger.warning("Cluster process did not terminate gracefully, killing...")
                    _auto_start_process.kill()
                    _auto_start_process.wait(timeout=2)
                logger.info("Cluster process stopped")
            except Exception as e:
                logger.warning("Error stopping cluster process: %s", e)
            finally:
                _auto_start_process = None


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
        return json.dumps({"error": "Not connected to cluster. Run kanban_cluster_init or kanban_cluster_join first."})

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
    "name": "kanban_cluster_init",
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
    "name": "kanban_cluster_join",
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
    "name": "kanban_cluster_submit",
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
    "name": "kanban_cluster_list",
    "description": "List all tasks in the cluster.",
    "parameters": {"type": "object", "properties": {}},
}

CLUSTER_NODES_SCHEMA = {
    "name": "kanban_cluster_nodes",
    "description": "List all nodes in the cluster.",
    "parameters": {"type": "object", "properties": {}},
}

CLUSTER_HEARTBEAT_SCHEMA = {
    "name": "kanban_cluster_heartbeat",
    "description": "Send heartbeat to the cluster to indicate this node is alive.",
    "parameters": {"type": "object", "properties": {}},
}

CLUSTER_COMPLETE_SCHEMA = {
    "name": "kanban_cluster_complete",
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
# Hook handlers
# ---------------------------------------------------------------------------

def _on_session_start(**kwargs) -> None:
    """Auto-start cluster service when session begins."""
    config = _get_plugin_config()
    
    if not config.get("auto_start", True):
        logger.debug("Auto-start disabled, skipping cluster startup")
        return
    
    # Run auto-start in a background thread to avoid blocking session start
    def _start_in_background():
        try:
            success = _start_cluster_auto(config)
            if success:
                logger.info("Cluster auto-started successfully")
            else:
                logger.debug("Cluster auto-start skipped (not running, binary not found, or disabled)")
        except Exception as e:
            logger.warning("Cluster auto-start failed: %s", e)
    
    thread = threading.Thread(target=_start_in_background, daemon=True, name="cluster-auto-start")
    thread.start()


def _on_session_end(**kwargs) -> None:
    """Gracefully stop cluster service when session ends."""
    global _auto_start_process
    
    # Stop heartbeat if running
    _heartbeat_stop.set()
    if _heartbeat_thread and _heartbeat_thread.is_alive():
        _heartbeat_thread.join(timeout=2)
    
    # Stop auto-started process
    if _auto_start_process is not None:
        _stop_cluster_auto()
    
    logger.info("Cluster plugin session ended")


# ---------------------------------------------------------------------------
# Plugin registration
# ---------------------------------------------------------------------------

def register(ctx) -> None:
    """Register kanban cluster tools with Hermes Agent."""
    ctx.register_tool(
        name="kanban_cluster_init",
        toolset="kanban_cluster",
        schema=CLUSTER_INIT_SCHEMA,
        handler=handle_cluster_init,
        description="Initialize a distributed kanban cluster",
        emoji="🏗️",
    )

    ctx.register_tool(
        name="kanban_cluster_join",
        toolset="kanban_cluster",
        schema=CLUSTER_JOIN_SCHEMA,
        handler=handle_cluster_join,
        description="Join an existing kanban cluster",
        emoji="🔗",
    )

    ctx.register_tool(
        name="kanban_cluster_submit",
        toolset="kanban_cluster",
        schema=CLUSTER_SUBMIT_SCHEMA,
        handler=handle_cluster_submit,
        description="Submit a task to the cluster",
        emoji="📋",
    )

    ctx.register_tool(
        name="kanban_cluster_list",
        toolset="kanban_cluster",
        schema=CLUSTER_LIST_SCHEMA,
        handler=handle_cluster_list,
        description="List cluster tasks",
        emoji="📊",
    )

    ctx.register_tool(
        name="kanban_cluster_nodes",
        toolset="kanban_cluster",
        schema=CLUSTER_NODES_SCHEMA,
        handler=handle_cluster_nodes,
        description="List cluster nodes",
        emoji="🖥️",
    )

    ctx.register_tool(
        name="kanban_cluster_heartbeat",
        toolset="kanban_cluster",
        schema=CLUSTER_HEARTBEAT_SCHEMA,
        handler=handle_cluster_heartbeat,
        description="Send cluster heartbeat",
        emoji="💓",
    )

    ctx.register_tool(
        name="kanban_cluster_complete",
        toolset="kanban_cluster",
        schema=CLUSTER_COMPLETE_SCHEMA,
        handler=handle_cluster_complete,
        description="Complete a cluster task",
        emoji="✅",
    )

    # Register lifecycle hooks for auto-start
    ctx.register_hook("on_session_start", _on_session_start)
    ctx.register_hook("on_session_end", _on_session_end)

    logger.info("hermes-agent-cluster plugin registered 7 cluster tools + auto-start hooks")

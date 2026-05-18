"""hermes-cluster Python plugin — replaces Go binary with native Python backend.

Registers tools that let the agent interact with the cluster:
- kanban_cluster_init: Start the Python cluster backend
- kanban_cluster_join: Join a cluster as worker
- kanban_cluster_submit: Submit a task to the cluster
- kanban_cluster_list: List tasks on the cluster
- kanban_cluster_nodes: List cluster nodes
- kanban_cluster_heartbeat: Send heartbeat
- kanban_cluster_complete: Mark task as completed
- kanban_cluster_status: Get cluster status
- kanban_cluster_config: Get/update cluster configuration

Auto-start: When the plugin loads, it starts the Python FastAPI server
in a background thread. No Go binary required.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.request import Request, urlopen
from urllib.error import URLError

# Setup wizard (lazy-import safe; lives next to this file)
try:
    from setup_wizard import (
        load_config as _load_wizard_config,
        save_config as _save_wizard_config,
        SETUP_PROMPT as _SETUP_PROMPT,
        WizardState as _WizardState,
    )
except ImportError:
    _load_wizard_config = None  # type: ignore[assignment]
    _save_wizard_config = None  # type: ignore[assignment]
    _SETUP_PROMPT = ""  # type: ignore[assignment]
    _WizardState = None  # type: ignore[assignment]

# Ensure hermes_cluster package is importable when installed as a Hermes plugin.
# After `hermes plugins install`, the repo root lands at ~/.hermes/plugins/<name>/
# and hermes_cluster/ sits next to this __init__.py.
_plugin_dir = str(Path(__file__).resolve().parent)
if _plugin_dir not in sys.path:
    sys.path.insert(0, _plugin_dir)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

_cluster_config: Dict[str, Any] = {}
_server_thread: Optional[threading.Thread] = None
_server_stop = threading.Event()
_base_url: str = ""
# Set to True by on_session_start when no config exists
_needs_setup: bool = False
# Wizard state tracker (created when the user invokes /cluster-setup)
_wizard: Optional[Any] = None

DEFAULT_PORT = 8787
DEFAULT_CLUSTER_ID = "hermes-cluster"
DEFAULT_NODE_ID = "node_main"
DEFAULT_NODE_NAME = "main-node"
DEFAULT_CAPABILITIES = ["planning", "reviewing", "scheduling"]


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def _get_plugin_config() -> Dict[str, Any]:
    """Load plugin configuration from environment or config file."""
    config = {
        "auto_start": True,
        "port": DEFAULT_PORT,
        "cluster_id": DEFAULT_CLUSTER_ID,
        "node_id": DEFAULT_NODE_ID,
        "node_name": DEFAULT_NODE_NAME,
        "capabilities": DEFAULT_CAPABILITIES,
        "token": "",
        "config_path": "",
        "static_dir": "",
    }

    env_map = {
        "HERMES_CLUSTER_AUTO_START": ("auto_start", lambda x: x.lower() in ("true", "1", "yes")),
        "HERMES_CLUSTER_PORT": ("port", int),
        "HERMES_CLUSTER_ID": ("cluster_id", str),
        "HERMES_CLUSTER_NODE_ID": ("node_id", str),
        "HERMES_CLUSTER_NODE_NAME": ("node_name", str),
        "HERMES_CLUSTER_TOKEN": ("token", str),
        "HERMES_CLUSTER_CONFIG": ("config_path", str),
        "HERMES_CLUSTER_STATIC_DIR": ("static_dir", str),
    }

    for env_var, (key, converter) in env_map.items():
        value = os.environ.get(env_var)
        if value is not None:
            try:
                config[key] = converter(value)
            except (ValueError, TypeError):
                logger.warning("Invalid value for %s: %s", env_var, value)

    return config


# ---------------------------------------------------------------------------
# Server lifecycle
# ---------------------------------------------------------------------------

def _start_server(config: Dict[str, Any]) -> bool:
    """Start the Python FastAPI server in a background thread."""
    global _server_thread, _base_url

    port = config["port"]
    _base_url = f"http://127.0.0.1:{port}"

    # Check if already running
    if _health_check():
        logger.info("Cluster already running on port %d", port)
        return True

    def _run_server():
        try:
            import uvicorn
            from hermes_cluster.app import create_app

            # Determine static directory
            static_dir = config.get("static_dir", "")
            if not static_dir:
                # Look for dashboard static files in the Go project
                go_static = Path(__file__).parent.parent.parent / "internal" / "dashboard" / "static"
                if go_static.exists():
                    static_dir = str(go_static)

            app = create_app(
                cluster_id=config["cluster_id"],
                node_id=config["node_id"],
                node_role="main",
                config_path=config.get("config_path", ""),
                fed_token=config.get("token", ""),
                static_dir=static_dir if static_dir else None,
            )

            uvicorn.run(app, host="0.0.0.0", port=port, log_level="warning")
        except Exception as e:
            logger.error("Failed to start cluster server: %s", e)

    _server_thread = threading.Thread(target=_run_server, daemon=True, name="cluster-server")
    _server_thread.start()

    # Wait for server to be ready
    deadline = time.time() + 5
    while time.time() < deadline:
        if _health_check():
            logger.info("Cluster server started on port %d", port)
            return True
        time.sleep(0.2)

    logger.error("Cluster server failed to start within timeout")
    return False


def _stop_server():
    """Stop the server (sets stop event, daemon thread exits)."""
    _server_stop.set()
    logger.info("Cluster server stop requested")


def _health_check() -> bool:
    """Check if the server is running."""
    try:
        req = Request(f"{_base_url}/health", method="GET")
        with urlopen(req, timeout=2) as resp:
            data = json.loads(resp.read().decode())
            return data.get("status") == "ok"
    except Exception:
        return False


# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------

def _api_call(method: str, path: str, data: dict = None) -> dict:
    """Make HTTP request to the Python cluster API."""
    url = f"{_base_url}{path}"
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
    """Start the Python cluster backend."""
    config = _get_plugin_config()
    config.update(args)

    success = _start_server(config)
    if success:
        return json.dumps({
            "status": "started",
            "backend": "python",
            "port": config["port"],
            "cluster_id": config["cluster_id"],
            "node_id": config["node_id"],
            "dashboard": f"http://127.0.0.1:{config['port']}/dashboard/",
            "api_docs": f"http://127.0.0.1:{config['port']}/docs",
        })
    return json.dumps({"error": "Failed to start cluster server"})


def handle_cluster_join(args: dict, **kwargs) -> str:
    """Join an existing cluster as worker."""
    endpoint = args.get("endpoint", "http://127.0.0.1:8787")
    result = _api_call("POST", "/api/v1/nodes/join", {
        "node_name": args.get("node_id", "worker"),
        "capabilities": args.get("capabilities", ["coding"]),
        "endpoint": endpoint,
    })
    return json.dumps(result)


def handle_cluster_submit(args: dict, **kwargs) -> str:
    """Submit a task to the cluster."""
    result = _api_call("POST", "/api/v1/tasks", {
        "title": args.get("title", "Untitled task"),
        "requires": args.get("requires", []),
        "priority": args.get("priority", 3),
    })
    return json.dumps(result)


def handle_cluster_list(args: dict, **kwargs) -> str:
    """List tasks on the cluster."""
    result = _api_call("GET", "/api/v1/tasks")
    return json.dumps(result, indent=2)


def handle_cluster_nodes(args: dict, **kwargs) -> str:
    """List cluster nodes."""
    result = _api_call("GET", "/api/v1/nodes")
    return json.dumps(result, indent=2)


def handle_cluster_heartbeat(args: dict, **kwargs) -> str:
    """Send heartbeat to cluster."""
    node_id = args.get("node_id", _cluster_config.get("node_id", "node_main"))
    result = _api_call("POST", "/api/v1/nodes/heartbeat", {"node_id": node_id})
    return json.dumps(result)


def handle_cluster_complete(args: dict, **kwargs) -> str:
    """Mark a task as completed."""
    task_id = args.get("task_id")
    if not task_id:
        return json.dumps({"error": "task_id is required"})
    result = _api_call("POST", f"/api/v1/tasks/{task_id}/complete")
    return json.dumps(result)


def handle_cluster_status(args: dict, **kwargs) -> str:
    """Get cluster status."""
    result = _api_call("GET", "/api/v1/summary")
    return json.dumps(result, indent=2)


def handle_cluster_config(args: dict, **kwargs) -> str:
    """Get or update cluster configuration."""
    if args:
        result = _api_call("PUT", "/api/v1/config", args)
    else:
        result = _api_call("GET", "/api/v1/config")
    return json.dumps(result, indent=2)


# ---------------------------------------------------------------------------
# Tool schemas
# ---------------------------------------------------------------------------

SCHEMAS = {
    "kanban_cluster_init": {
        "name": "kanban_cluster_init",
        "description": "Start the Python cluster backend server. No Go binary required.",
        "parameters": {
            "type": "object",
            "properties": {
                "port": {"type": "integer", "description": "Port to listen on", "default": 8787},
                "cluster_id": {"type": "string", "description": "Cluster identifier"},
                "node_id": {"type": "string", "description": "This node's unique ID"},
                "capabilities": {"type": "array", "items": {"type": "string"}, "description": "Node capabilities"},
            },
        },
    },
    "kanban_cluster_join": {
        "name": "kanban_cluster_join",
        "description": "Join an existing cluster as a worker node.",
        "parameters": {
            "type": "object",
            "properties": {
                "endpoint": {"type": "string", "description": "Main node URL"},
                "node_id": {"type": "string", "description": "This node's unique ID"},
                "capabilities": {"type": "array", "items": {"type": "string"}, "description": "Node capabilities"},
            },
            "required": ["endpoint"],
        },
    },
    "kanban_cluster_submit": {
        "name": "kanban_cluster_submit",
        "description": "Submit a task to the cluster for distributed execution.",
        "parameters": {
            "type": "object",
            "properties": {
                "title": {"type": "string", "description": "Task title/description"},
                "requires": {"type": "array", "items": {"type": "string"}, "description": "Required capabilities"},
                "priority": {"type": "integer", "description": "Priority (1=highest, 5=lowest)", "default": 3},
            },
            "required": ["title"],
        },
    },
    "kanban_cluster_list": {
        "name": "kanban_cluster_list",
        "description": "List all tasks in the cluster.",
        "parameters": {"type": "object", "properties": {}},
    },
    "kanban_cluster_nodes": {
        "name": "kanban_cluster_nodes",
        "description": "List all nodes in the cluster.",
        "parameters": {"type": "object", "properties": {}},
    },
    "kanban_cluster_heartbeat": {
        "name": "kanban_cluster_heartbeat",
        "description": "Send heartbeat to indicate this node is alive.",
        "parameters": {
            "type": "object",
            "properties": {
                "node_id": {"type": "string", "description": "Node ID"},
            },
        },
    },
    "kanban_cluster_complete": {
        "name": "kanban_cluster_complete",
        "description": "Mark a task as completed.",
        "parameters": {
            "type": "object",
            "properties": {
                "task_id": {"type": "string", "description": "Task ID to complete"},
            },
            "required": ["task_id"],
        },
    },
    "kanban_cluster_status": {
        "name": "kanban_cluster_status",
        "description": "Get cluster status summary.",
        "parameters": {"type": "object", "properties": {}},
    },
    "kanban_cluster_config": {
        "name": "kanban_cluster_config",
        "description": "Get or update cluster configuration.",
        "parameters": {"type": "object", "properties": {}},
    },
}

HANDLERS = {
    "kanban_cluster_init": handle_cluster_init,
    "kanban_cluster_join": handle_cluster_join,
    "kanban_cluster_submit": handle_cluster_submit,
    "kanban_cluster_list": handle_cluster_list,
    "kanban_cluster_nodes": handle_cluster_nodes,
    "kanban_cluster_heartbeat": handle_cluster_heartbeat,
    "kanban_cluster_complete": handle_cluster_complete,
    "kanban_cluster_status": handle_cluster_status,
    "kanban_cluster_config": handle_cluster_config,
}


# ---------------------------------------------------------------------------
# Hook handlers
# ---------------------------------------------------------------------------

def _on_session_start(**kwargs) -> None:
    """Auto-start cluster server when session begins.

    If no persisted config exists, sets ``_needs_setup = True`` and injects
    the SETUP_PROMPT into the conversation so the LLM guides the user
    through interactive configuration.
    """
    global _needs_setup, _wizard

    # Check for persisted wizard config first
    wizard_cfg = None
    if _load_wizard_config is not None:
        try:
            wizard_cfg = _load_wizard_config()
        except Exception as exc:
            logger.debug("Could not load wizard config: %s", exc)

    config = _get_plugin_config()

    # No config at all → trigger setup wizard
    if wizard_cfg is None and not config.get("token"):
        _needs_setup = True
        if _WizardState is not None:
            _wizard = _WizardState()
        logger.info("No cluster config detected — setup wizard activated")

        # Inject the setup prompt into the conversation
        prompt_text = _SETUP_PROMPT or (
            "🔧 No cluster config found. Run `/cluster-setup` to configure."
        )
        inject = kwargs.get("inject_message") or kwargs.get("inject")
        if callable(inject):
            try:
                inject(prompt_text)
            except Exception as exc:
                logger.warning("Failed to inject setup prompt: %s", exc)
        return

    # Config exists — use wizard config if plugin config is bare
    if wizard_cfg is not None:
        config.update(wizard_cfg)

    if not config.get("auto_start", True):
        return

    def _start_in_background():
        try:
            success = _start_server(config)
            if success:
                logger.info("Python cluster auto-started successfully")
            else:
                logger.debug("Cluster auto-start skipped")
        except Exception as e:
            logger.warning("Cluster auto-start failed: %s", e)

    thread = threading.Thread(target=_start_in_background, daemon=True, name="cluster-auto-start")
    thread.start()


# ---------------------------------------------------------------------------
# /cluster-setup slash command handler
# ---------------------------------------------------------------------------

def _handle_cluster_setup(args: dict, **kwargs) -> str:
    """Handle the /cluster-setup slash command.

    Without arguments: returns the interactive setup prompt.
    With arguments: performs quick-setup in one shot.
    """
    global _needs_setup, _wizard

    # Quick-setup path: all required args supplied at once
    role = args.get("role", "")
    if role == "main":
        from setup_wizard import quick_setup_main
        config = quick_setup_main(
            cluster_id=args.get("cluster_id", "hermes-cluster"),
            node_id=args.get("node_id", "main-node"),
            node_name=args.get("node_name", "Main Node"),
            capabilities=args.get("capabilities"),
            port=int(args.get("port", 8787)),
        )
        _needs_setup = False
        return json.dumps({
           "status": "configured",
           "role": "main",
           "token": config["token"],
           "port": config["port"],
           "join_hint": (
               f"Workers join with endpoint=http://<ip>:{config['port']} "
               f"token={config['token']}"
           ),
       }, indent=2)

    if role == "worker":
        from setup_wizard import quick_setup_worker
        endpoint = args.get("endpoint", "")
        token = args.get("token", "")
        if not endpoint or not token:
            return json.dumps({"error": "endpoint and token are required for worker setup"})
        config = quick_setup_worker(
            endpoint=endpoint,
            token=token,
            node_id=args.get("node_id"),
            node_name=args.get("node_name"),
            capabilities=args.get("capabilities"),
        )
        _needs_setup = False
        return json.dumps({"status": "configured", "role": "worker", "endpoint": endpoint}, indent=2)

    # Interactive path: start (or restart) the wizard
    if _WizardState is not None:
        _wizard = _WizardState()
        _needs_setup = True
        return _wizard.current_prompt

    return (
        "Setup wizard not available (module not loaded). "
        "Please configure manually via plugin config."
    )


def _handle_wizard_answer(answer: str) -> str:
    """Feed a user answer into the active wizard.  Returns next prompt or summary."""
    global _wizard, _needs_setup
    if _wizard is None:
        return "No wizard active. Run `/cluster-setup` first."
    result = _wizard.process_answer(answer)
    if _wizard.complete:
        _needs_setup = False
    return result


def _on_session_end(**kwargs) -> None:
    """Stop cluster server when session ends."""
    _stop_server()
    logger.info("Cluster plugin session ended")


# Plugin registration
# ---------------------------------------------------------------------------

def register(ctx) -> None:
    """Register cluster tools with Hermes Agent."""
    for name, schema in SCHEMAS.items():
        ctx.register_tool(
            name=name,
            toolset="kanban_cluster",
            schema=schema,
            handler=HANDLERS[name],
            description=schema["description"],
            emoji="🏗️",
        )

    # Register /cluster-setup slash command
    ctx.register_command(
        name="cluster-setup",
        handler=_handle_cluster_setup,
        description="Interactive cluster setup wizard — create or join a cluster",
        schema={
            "type": "object",
            "properties": {
                "role": {
                    "type": "string",
                    "enum": ["main", "worker"],
                    "description": "Node role (main or worker)",
                },
                "cluster_id": {
                    "type": "string",
                    "description": "Cluster identifier (main only)",
                },
                "node_id": {
                    "type": "string",
                    "description": "Unique node identifier",
                },
                "node_name": {
                    "type": "string",
                    "description": "Friendly display name",
                },
                "capabilities": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Node capabilities",
                },
                "port": {
                    "type": "integer",
                    "description": "Server port (main only)",
                },
                "endpoint": {
                    "type": "string",
                    "description": "Main node URL (worker only)",
                },
                "token": {
                    "type": "string",
                    "description": "Cluster auth token (worker only)",
                },
            },
        },
    )

    # Register lifecycle hooks
    ctx.register_hook("on_session_start", _on_session_start)
    ctx.register_hook("on_session_end", _on_session_end)

    logger.info("hermes-cluster Python plugin registered %d tools + auto-start hooks", len(SCHEMAS))

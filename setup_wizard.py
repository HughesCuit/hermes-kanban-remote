"""Cluster Setup Wizard for hermes-agent-cluster v3.0.0.

Provides an interactive LLM-driven setup flow — no input() calls.
Instead, injects a conversational SETUP_PROMPT that the agent processes.

Two flows:
  1. Main node  — creates a new cluster, generates a token, shows join command
  2. Worker node — joins an existing cluster using endpoint + token

Config is persisted to ~/.hermes/plugins/hermes-agent-cluster/config.json
"""

from __future__ import annotations

import json
import logging
import os
import secrets
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

CONFIG_DIR = Path.home() / ".hermes" / "plugins" / "hermes-agent-cluster"
CONFIG_PATH = CONFIG_DIR / "config.json"

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

DEFAULT_PORT = 8787
DEFAULT_CLUSTER_ID = "hermes-cluster"
DEFAULT_CAPABILITIES: List[str] = ["planning", "reviewing", "scheduling"]
ALL_CAPABILITIES: List[str] = [
    "planning", "reviewing", "scheduling", "coding",
    "testing", "debugging", "documentation", "deployment",
]

# ---------------------------------------------------------------------------
# Setup prompt injected into the LLM conversation
# ---------------------------------------------------------------------------

SETUP_PROMPT = """🔧 **Hermes Cluster — Setup Required**

No cluster configuration was detected for the `hermes-agent-cluster` plugin.
Let's set one up! Please answer the following questions (you can reply with
just the answers, e.g. "1" for the first option):

**Step 1 — Role**
What role should this node play?
  `1` — **Main node** (create a new cluster — other nodes will join yours)
  `2` — **Worker node** (join an existing cluster)

Just reply with `1` or `2` and I'll walk you through the rest.
You can also run `/cluster-setup` at any time to re-run this wizard.
"""

# ---------------------------------------------------------------------------
# Conversational step prompts (injected one at a time)
# ---------------------------------------------------------------------------

_MAIN_STEPS: List[Dict[str, Any]] = [
    {
        "key": "cluster_id",
        "prompt": (
            "**Step 2 — Cluster Name**\n"
            "What name would you like for your cluster?\n"
            f"(default: `{DEFAULT_CLUSTER_ID}` — press Enter to accept)\n"
        ),
        "default": DEFAULT_CLUSTER_ID,
    },
    {
        "key": "node_id",
        "prompt": (
            "**Step 3 — Node ID**\n"
            "Choose a unique identifier for this main node.\n"
            f"(default: `main-node` — press Enter to accept)\n"
        ),
        "default": "main-node",
    },
    {
        "key": "node_name",
        "prompt": (
            "**Step 4 — Node Display Name**\n"
            "A friendly display name for this node.\n"
            f"(default: `Main Node` — press Enter to accept)\n"
        ),
        "default": "Main Node",
    },
    {
        "key": "capabilities",
        "prompt": (
            "**Step 5 — Capabilities**\n"
            f"Available capabilities: {', '.join(ALL_CAPABILITIES)}\n"
            "Enter comma-separated capabilities, or press Enter for the defaults.\n"
            f"(default: `{', '.join(DEFAULT_CAPABILITIES)}`)\n"
        ),
        "default": DEFAULT_CAPABILITIES,
    },
    {
        "key": "port",
        "prompt": (
            "**Step 6 — Port**\n"
            "Which port should the cluster server listen on?\n"
            f"(default: `{DEFAULT_PORT}` — press Enter to accept)\n"
        ),
        "default": DEFAULT_PORT,
    },
]

_WORKER_STEPS: List[Dict[str, Any]] = [
    {
        "key": "endpoint",
        "prompt": (
            "**Step 2 — Cluster Endpoint**\n"
            "Enter the main node's URL (e.g. `http://192.168.1.10:8787`).\n"
        ),
        "required": True,
    },
    {
        "key": "token",
        "prompt": (
            "**Step 3 — Authentication Token**\n"
            "Enter the cluster token provided by the main node admin.\n"
        ),
        "required": True,
    },
    {
        "key": "node_id",
        "prompt": (
            "**Step 4 — Node ID**\n"
            "Choose a unique identifier for this worker node.\n"
            f"(default: `worker-{secrets.token_hex(4)}` — press Enter to accept)\n"
        ),
        "default": None,  # filled dynamically
    },
    {
        "key": "node_name",
        "prompt": (
            "**Step 5 — Node Display Name**\n"
            "A friendly display name for this node.\n"
            "(default: same as Node ID — press Enter to accept)\n"
        ),
        "default": None,  # filled dynamically from node_id
    },
    {
        "key": "capabilities",
        "prompt": (
            "**Step 6 — Capabilities**\n"
            f"Available capabilities: {', '.join(ALL_CAPABILITIES)}\n"
            "Enter comma-separated capabilities, or press Enter for the defaults.\n"
            f"(default: `coding, testing`)\n"
        ),
        "default": ["coding", "testing"],
    },
]


# ---------------------------------------------------------------------------
# Config persistence
# ---------------------------------------------------------------------------

def load_config() -> Optional[Dict[str, Any]]:
    """Load existing config, or return None if not found / incomplete."""
    if not CONFIG_PATH.exists():
        return None
    try:
        data = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        if data.get("setup_complete"):
            return data
        return None
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Failed to load cluster config: %s", exc)
        return None


def save_config(config: Dict[str, Any]) -> Path:
    """Persist config to disk.  Creates parent dirs if needed."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    CONFIG_PATH.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
    logger.info("Cluster config saved to %s", CONFIG_PATH)
    return CONFIG_PATH


def generate_token() -> str:
    """Generate a cryptographically secure 32-hex-char token."""
    return secrets.token_hex(16)


# ---------------------------------------------------------------------------
# Answer parsing helpers
# ---------------------------------------------------------------------------

def parse_role(answer: str) -> Optional[str]:
    """Parse role choice from user answer.  Returns 'main', 'worker', or None."""
    stripped = answer.strip()
    if stripped in ("1", "main"):
        return "main"
    if stripped in ("2", "worker"):
        return "worker"
    return None


def parse_capabilities(answer: str, default: List[str]) -> List[str]:
    """Parse comma-separated capabilities, falling back to default."""
    stripped = answer.strip()
    if not stripped:
        return list(default)
    caps = [c.strip().lower() for c in stripped.split(",") if c.strip()]
    return caps if caps else list(default)


def parse_port(answer: str, default: int = DEFAULT_PORT) -> int:
    """Parse port number, falling back to default."""
    stripped = answer.strip()
    if not stripped:
        return default
    try:
        port = int(stripped)
        if 1 <= port <= 65535:
            return port
    except ValueError:
        pass
    return default


def parse_int(answer: str, default: int) -> int:
    """Parse an integer from user answer."""
    stripped = answer.strip()
    if not stripped:
        return default
    try:
        return int(stripped)
    except ValueError:
        return default


# ---------------------------------------------------------------------------
# Build final config for main node
# ---------------------------------------------------------------------------

def build_main_config(
    cluster_id: str = DEFAULT_CLUSTER_ID,
    node_id: str = "main-node",
    node_name: str = "Main Node",
    capabilities: Optional[List[str]] = None,
    port: int = DEFAULT_PORT,
    token: Optional[str] = None,
) -> Dict[str, Any]:
    """Build a complete main-node config dict."""
    if capabilities is None:
        capabilities = list(DEFAULT_CAPABILITIES)
    if token is None:
        token = generate_token()

    return {
        "cluster_id": cluster_id,
        "role": "main",
        "node_id": node_id,
        "node_name": node_name,
        "capabilities": capabilities,
        "port": port,
        "token": token,
        "endpoint": f"http://127.0.0.1:{port}",
        "auto_start": True,
        "setup_complete": True,
    }


# ---------------------------------------------------------------------------
# Build final config for worker node
# ---------------------------------------------------------------------------

def build_worker_config(
    endpoint: str,
    token: str,
    node_id: Optional[str] = None,
    node_name: Optional[str] = None,
    capabilities: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Build a complete worker-node config dict."""
    if node_id is None:
        node_id = f"worker-{secrets.token_hex(4)}"
    if node_name is None:
        node_name = node_id
    if capabilities is None:
        capabilities = ["coding", "testing"]

    return {
        "cluster_id": "",  # learned from main node on join
        "role": "worker",
        "node_id": node_id,
        "node_name": node_name,
        "capabilities": capabilities,
        "port": 0,  # workers don't serve by default
        "token": token,
        "endpoint": endpoint,
        "auto_start": True,
        "setup_complete": True,
    }


# ---------------------------------------------------------------------------
# Wizard state machine (for conversational step tracking)
# ---------------------------------------------------------------------------

class WizardState:
    """Tracks which step the user is on during the conversational wizard.

    The LLM agent creates one of these, then feeds each user reply through
    ``process_answer()`` to advance the state machine.
    """

    VALID_ROLES = ("main", "worker")

    def __init__(self) -> None:
        self.role: Optional[str] = None
        self.answers: Dict[str, Any] = {}
        self._step_index: int = 0  # 0 = role selection, 1+ = detail steps
        self.complete: bool = False
        self.error: Optional[str] = None

    # -- steps ---------------------------------------------------------------

    @property
    def steps(self) -> List[Dict[str, Any]]:
        if self.role == "main":
            return _MAIN_STEPS
        return _WORKER_STEPS

    @property
    def current_step(self) -> Optional[Dict[str, Any]]:
        if self._step_index == 0:
            return None  # role selection
        idx = self._step_index - 1
        steps = self.steps
        if idx < len(steps):
            return steps[idx]
        return None

    @property
    def current_prompt(self) -> str:
        """Return the prompt text for the current step."""
        if self._step_index == 0:
            return SETUP_PROMPT
        step = self.current_step
        if step is None:
            return ""
        return step["prompt"]

    # -- answer processing ---------------------------------------------------

    def process_answer(self, answer: str) -> str:
        """Process a user answer, advance state, return next prompt or summary.

        Returns:
            The next prompt string to inject, or a final summary/config JSON.
        """
        answer = answer.strip()

        # Step 0: role selection
        if self._step_index == 0:
            role = parse_role(answer)
            if role is None:
                self.error = "Please reply with `1` (main) or `2` (worker)."
                return self.error
            self.role = role
            self._step_index = 1
            return self.current_prompt

        # Subsequent steps
        step = self.current_step
        if step is None:
            return ""

        key = step["key"]
        default = step.get("default")

        # Dynamic defaults for worker node_id / node_name
        if key == "node_id" and default is None:
            default = f"worker-{secrets.token_hex(4)}"
        if key == "node_name" and default is None:
            default = self.answers.get("node_id", f"worker-{secrets.token_hex(4)}")

        # Validate required fields
        if step.get("required") and not answer:
            self.error = f"This field is required. Please provide a value."
            return self.error

        # Parse per-field
        if key == "capabilities":
            cap_default = default if isinstance(default, list) else DEFAULT_CAPABILITIES
            self.answers[key] = parse_capabilities(answer, cap_default)
        elif key == "port":
            port_default = default if isinstance(default, int) else DEFAULT_PORT
            self.answers[key] = parse_port(answer, port_default)
        elif key == "endpoint":
            self.answers[key] = answer if answer else (default or "")
        elif key == "token":
            self.answers[key] = answer if answer else (default or "")
        else:
            self.answers[key] = answer if answer else (str(default) if default is not None else "")

        self._step_index += 1

        # Check if we've reached the end
        if self._step_index > len(self.steps):
            self.complete = True
            return self._build_summary()

        return self.current_prompt

    # -- summary & config building -------------------------------------------

    def _build_summary(self) -> str:
        """Build the final configuration and return a human-readable summary."""
        if self.role == "main":
            token = generate_token()
            config = build_main_config(
                cluster_id=self.answers.get("cluster_id", DEFAULT_CLUSTER_ID),
                node_id=self.answers.get("node_id", "main-node"),
                node_name=self.answers.get("node_name", "Main Node"),
                capabilities=self.answers.get("capabilities", DEFAULT_CAPABILITIES),
                port=self.answers.get("port", DEFAULT_PORT),
                token=token,
            )
        else:
            token = self.answers.get("token", "")
            config = build_worker_config(
                endpoint=self.answers.get("endpoint", ""),
                token=token,
                node_id=self.answers.get("node_id"),
                node_name=self.answers.get("node_name"),
                capabilities=self.answers.get("capabilities", ["coding", "testing"]),
            )

        save_config(config)

        # Build human-readable summary
        lines = ["✅ **Cluster Configuration Complete!**\n"]
        lines.append(f"  **Role:** {config['role']}")
        lines.append(f"  **Cluster ID:** {config['cluster_id']}")
        lines.append(f"  **Node ID:** {config['node_id']}")
        lines.append(f"  **Node Name:** {config['node_name']}")
        lines.append(f"  **Capabilities:** {', '.join(config['capabilities'])}")
        if config["role"] == "main":
            lines.append(f"  **Port:** {config['port']}")
            lines.append(f"  **Token:** `{config['token']}`")
            lines.append("")
            lines.append("📋 **Worker join command:**")
            lines.append(
                f"  Workers can join with: endpoint=`http://<your-ip>:{config['port']}`, "
                f"token=`{config['token']}`"
            )
        else:
            lines.append(f"  **Endpoint:** {config['endpoint']}")
            lines.append(f"  **Token:** `{config['token']}`")

        lines.append("")
        lines.append("Configuration saved. The cluster service will auto-start on next session.")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Convenience: one-shot build from known answers (used by /cluster-setup cmd)
# ---------------------------------------------------------------------------

def quick_setup_main(
    cluster_id: str = DEFAULT_CLUSTER_ID,
    node_id: str = "main-node",
    node_name: str = "Main Node",
    capabilities: Optional[List[str]] = None,
    port: int = DEFAULT_PORT,
) -> Dict[str, Any]:
    """Create and save a main-node config in one call.  Returns the config."""
    config = build_main_config(
        cluster_id=cluster_id,
        node_id=node_id,
        node_name=node_name,
        capabilities=capabilities,
        port=port,
    )
    save_config(config)
    return config


def quick_setup_worker(
    endpoint: str,
    token: str,
    node_id: Optional[str] = None,
    node_name: Optional[str] = None,
    capabilities: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Create and save a worker-node config in one call.  Returns the config."""
    config = build_worker_config(
        endpoint=endpoint,
        token=token,
        node_id=node_id,
        node_name=node_name,
        capabilities=capabilities,
    )
    save_config(config)
    return config


# ---------------------------------------------------------------------------
# Join helper (registers worker with main node)
# ---------------------------------------------------------------------------

def join_cluster(config: Dict[str, Any]) -> Dict[str, Any]:
    """Attempt to join a cluster as a worker.

    Sends a POST to the main node's /api/v1/nodes/join endpoint.
    Returns the response dict or an error dict.
    """
    from urllib.request import Request, urlopen
    from urllib.error import URLError

    endpoint = config.get("endpoint", "")
    if not endpoint:
        return {"error": "No endpoint configured"}

    url = f"{endpoint}/api/v1/nodes/join"
    payload = json.dumps({
        "node_id": config["node_id"],
        "node_name": config.get("node_name", config["node_id"]),
        "capabilities": config.get("capabilities", []),
        "token": config.get("token", ""),
    }).encode()

    req = Request(url, data=payload, method="POST")
    req.add_header("Content-Type", "application/json")
    try:
        with urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except URLError as exc:
        return {"error": f"Failed to join cluster: {exc}"}
    except Exception as exc:
        return {"error": f"Unexpected error: {exc}"}


def init_cluster_server(config: Dict[str, Any]) -> bool:
    """Start the cluster FastAPI server for a main-node config.

    Returns True if the server started successfully.
    """
    # Defer to the existing server-start logic in __init__.py
    # by importing the module dynamically.
    try:
        import hermes_cluster.serve as serve  # type: ignore[import-untyped]
        return serve.start_server(port=config.get("port", DEFAULT_PORT), config=config)
    except ImportError:
        logger.warning("hermes_cluster.serve not available; server start skipped")
        return False
    except Exception as exc:
        logger.error("Failed to start cluster server: %s", exc)
        return False

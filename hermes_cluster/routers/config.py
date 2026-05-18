"""Config API — 5 endpoints matching Go internal/config/config.go.

Endpoints:
  GET    /api/v1/config          — current config (or defaults)
  PUT    /api/v1/config          — update config
  POST   /api/v1/config/validate — validate config fields
  GET    /api/v1/config/yaml     — config as YAML string
  POST   /api/v1/config/restart  — trigger graceful restart
"""

from __future__ import annotations

import os
import signal
import sys
from typing import Any, Dict, List, Optional

import yaml
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import PlainTextResponse

from ..models import ConfigJSON

router = APIRouter(prefix="/api/v1/config", tags=["config"])

# ---------------------------------------------------------------------------
# Module-level state — initialized by app.py via init()
# ---------------------------------------------------------------------------
_store = None  # ClusterStore or ClusterState
_config_path: str = ""
_restart_callback = None  # Optional callable invoked on restart request


def init(store, config_path: str = "", restart_callback=None):
    """Bind the config router to a store instance.

    Args:
        store: ClusterStore or ClusterState with get_config/set_config/get_config_path
        config_path: optional path to cluster.yaml file
        restart_callback: optional callable() invoked when restart is requested
    """
    global _store, _config_path, _restart_callback
    _store = store
    _config_path = config_path or getattr(store, "_config_path", "")
    _restart_callback = restart_callback


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _current_config() -> Dict[str, Any]:
    """Return current config dict, falling back to defaults."""
    cfg = _store.get_config() if _store else None
    if cfg is None:
        return ConfigJSON().model_dump()
    return cfg


def _validate_config(cfg: Dict[str, Any]) -> List[str]:
    """Validate config dict against Go's Validate() rules.

    Returns list of error strings. Empty list = valid.
    """
    errors: List[str] = []

    # cluster.id required
    cluster = cfg.get("cluster", {})
    if not cluster.get("id"):
        errors.append("cluster.id is required")

    # node.id required
    node = cfg.get("node", {})
    if not node.get("id"):
        errors.append("node.id is required")

    # server.port 1-65535
    server = cfg.get("server", {})
    port = server.get("port", 8787)
    if not isinstance(port, int) or port < 1 or port > 65535:
        errors.append(f"server.port must be 1-65535, got {port}")

    # cluster.role must be "main" or "worker"
    role = cluster.get("role", "main")
    if role not in ("main", "worker"):
        errors.append(f"cluster.role must be 'main' or 'worker', got '{role}'")

    # worker nodes require endpoint
    if role == "worker" and not cluster.get("endpoint"):
        errors.append("cluster.endpoint is required for worker nodes")

    return errors


def _config_to_yaml(cfg: Dict[str, Any]) -> str:
    """Convert config dict to YAML string."""
    return yaml.dump(cfg, default_flow_style=False, sort_keys=False)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("")
async def get_config(defaults: bool = Query(False, description="Return default config")):
    """GET /api/v1/config — return current config or defaults."""
    if defaults:
        return ConfigJSON().model_dump()
    cfg = _current_config()
    return cfg


@router.put("")
async def update_config(cfg: ConfigJSON):
    """PUT /api/v1/config — update config and persist to file."""
    config_dict = cfg.model_dump()

    # Validate before saving
    errors = _validate_config(config_dict)
    if errors:
        raise HTTPException(
            status_code=422,
            detail={"validation_errors": errors},
        )

    # Update in-memory store
    if _store:
        _store.set_config(config_dict)

    # Persist to file if path configured
    path = _config_path or (getattr(_store, "get_config_path", lambda: "")() if _store else "")
    if path:
        try:
            with open(path, "w") as f:
                yaml.dump(config_dict, f, default_flow_style=False, sort_keys=False)
        except OSError as e:
            raise HTTPException(
                status_code=500,
                detail=f"failed to save config to {path}: {e}",
            )

    return {"status": "saved", "config": config_dict}


@router.post("/validate")
async def validate_config(cfg: Optional[ConfigJSON] = None):
    """POST /api/v1/config/validate — validate config (current or provided).

    If no body is provided, validates the current config.
    If a body is provided, validates that config.
    """
    if cfg is not None:
        config_dict = cfg.model_dump()
    else:
        config_dict = _current_config()

    errors = _validate_config(config_dict)
    valid = len(errors) == 0

    return {
        "valid": valid,
        "errors": errors,
        "config": config_dict,
    }


@router.get("/yaml")
async def get_config_yaml(defaults: bool = Query(False, description="Return default config as YAML")):
    """GET /api/v1/config/yaml — return config as YAML string."""
    if defaults:
        cfg = ConfigJSON().model_dump()
    else:
        cfg = _current_config()

    yaml_str = _config_to_yaml(cfg)
    return PlainTextResponse(content=yaml_str, media_type="text/yaml")


@router.post("/restart")
async def restart_config():
    """POST /api/v1/config/restart — trigger graceful service restart.

    Invokes the registered restart_callback if available.
    Otherwise returns restart_unavailable.
    """
    pid = os.getpid()

    # Invoke callback if registered (testable path)
    if _restart_callback is not None:
        try:
            _restart_callback()
        except Exception as e:
            return {"status": "restart_failed", "pid": pid, "error": str(e)}
        return {"status": "restart_requested", "pid": pid, "method": "callback"}

    return {"status": "restart_unavailable", "pid": pid, "message": "no restart handler registered"}

"""
hermes-agent-cluster dashboard plugin — backend API routes.

Mounts at /api/plugins/agent-cluster/ via the Hermes Dashboard plugin system.
Proxies requests to the hermes-cluster Go service.
Supports config management: read/write cluster.yaml, runtime capability updates.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, List, Optional
from urllib.error import URLError
from urllib.request import Request, urlopen

import yaml
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter()

# Default cluster endpoint (can be overridden via POST /config)
_CLUSTER_ENDPOINT = "http://127.0.0.1:8787"

# Config file paths (checked in order)
_CONFIG_PATHS = [
    Path.home() / ".hermes" / "agent-cluster" / "cluster.yaml",
    Path.home() / ".hermes" / "agent-cluster" / "cluster-worker.yaml",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _proxy(method: str, path: str, data: dict = None) -> Any:
    """Proxy an API call to the hermes-cluster Go service."""
    url = f"{_CLUSTER_ENDPOINT}{path}"
    body = json.dumps(data).encode() if data else None
    req = Request(url, data=body, method=method)
    req.add_header("Content-Type", "application/json")
    try:
        with urlopen(req, timeout=10) as resp:
            raw = resp.read().decode()
            if raw.strip():
                return json.loads(raw)
            return {}
    except URLError as e:
        raise HTTPException(status_code=502, detail=f"Cluster proxy error: {e.reason}")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Cluster proxy error: {e}")


def _read_config_file() -> tuple[Optional[dict], Optional[str], Optional[str]]:
    """Read cluster.yaml, return (parsed_dict, raw_yaml, path)."""
    for p in _CONFIG_PATHS:
        if p.exists():
            try:
                raw = p.read_text(encoding="utf-8")
                parsed = yaml.safe_load(raw) or {}
                return parsed, raw, str(p)
            except Exception as e:
                logger.warning("Failed to read %s: %s", p, e)
                continue
    return None, None, None


def _write_config_file(cfg: dict) -> str:
    """Write config dict to the first available path, creating dir if needed."""
    path = _CONFIG_PATHS[0]
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = yaml.dump(cfg, default_flow_style=False, allow_unicode=True)
    path.write_text(raw, encoding="utf-8")
    return str(path)


# ---------------------------------------------------------------------------
# Config CRUD
# ---------------------------------------------------------------------------


class EndpointBody(BaseModel):
    endpoint: str


@router.post("/config")
async def set_endpoint(body: EndpointBody):
    """Set the hermes-cluster Go service endpoint URL."""
    global _CLUSTER_ENDPOINT
    _CLUSTER_ENDPOINT = body.endpoint.rstrip("/")
    logger.info("Cluster endpoint set to %s", _CLUSTER_ENDPOINT)
    return {"ok": True, "endpoint": _CLUSTER_ENDPOINT}


@router.get("/config")
async def get_endpoint():
    """Get the current hermes-cluster Go service endpoint URL."""
    return {"ok": True, "endpoint": _CLUSTER_ENDPOINT}


@router.get("/config/node")
async def get_node_config():
    """Get node configuration from config file and runtime."""
    cfg, raw_yaml, cfg_path = _read_config_file()

    result = {
        "ok": True,
        "config_file": cfg_path,
        "cluster": None,
        "node": None,
        "server": None,
        "lease": None,
        "watchdog": None,
    }

    if cfg:
        result["cluster"] = cfg.get("cluster", {})
        result["node"] = cfg.get("node", {})
        result["server"] = cfg.get("server", {})
        result["lease"] = cfg.get("lease", {})
        result["watchdog"] = cfg.get("watchdog", {})
        result["telemetry"] = cfg.get("telemetry", {})

    # Try to get runtime node info from Go service
    try:
        nodes = _proxy("GET", "/api/v1/nodes")
        if isinstance(nodes, list) and nodes:
            # Merge runtime status (online/offline) into result
            runtime_map = {n["id"]: n for n in nodes}
            node_id = (result.get("node") or {}).get("id", "")
            if node_id and node_id in runtime_map:
                result["runtime"] = runtime_map[node_id]
    except HTTPException:
        result["runtime"] = None

    return result


class CapabilitiesBody(BaseModel):
    capabilities: List[str]


@router.put("/config/capabilities")
async def update_capabilities(body: CapabilitiesBody):
    """Update node capabilities at runtime AND persist to config file."""
    cfg, raw_yaml, cfg_path = _read_config_file()
    if not cfg:
        raise HTTPException(status_code=404, detail="Config file not found")

    node_id = (cfg.get("node") or {}).get("id", "node_main")
    caps = body.capabilities

    # 1. Update in config file
    if "node" not in cfg:
        cfg["node"] = {}
    cfg["node"]["capabilities"] = caps
    saved_path = _write_config_file(cfg)
    logger.info("Saved capabilities to %s: %s", saved_path, caps)

    # 2. Update at runtime via Go API (best-effort)
    runtime_result = None
    try:
        runtime_result = _proxy("PATCH", f"/api/v1/nodes/{node_id}/capabilities", {
            "capabilities": caps,
        })
        logger.info("Runtime capability update result: %s", runtime_result)
    except HTTPException:
        runtime_result = {"warning": "Cluster service not reachable, saved to config only"}

    return {
        "ok": True,
        "node_id": node_id,
        "capabilities": caps,
        "config_file": saved_path,
        "runtime": runtime_result,
    }


class NodeConfigBody(BaseModel):
    """Update persistent node config (requires restart to take full effect)."""
    name: Optional[str] = None
    capabilities: Optional[List[str]] = None


@router.put("/config/node")
async def update_node_config(body: NodeConfigBody):
    """Update node identity in config file (requires restart for most fields)."""
    cfg, raw_yaml, cfg_path = _read_config_file()
    if not cfg:
        raise HTTPException(status_code=404, detail="Config file not found")

    if "node" not in cfg:
        cfg["node"] = {}

    changed = []
    if body.name is not None:
        cfg["node"]["name"] = body.name
        changed.append("name")
    if body.capabilities is not None:
        cfg["node"]["capabilities"] = body.capabilities
        changed.append("capabilities")

    saved_path = _write_config_file(cfg)

    # Runtime capability update if capabilities changed
    runtime_result = None
    if "capabilities" in changed:
        node_id = cfg["node"].get("id", "node_main")
        try:
            runtime_result = _proxy(
                "PATCH",
                f"/api/v1/nodes/{node_id}/capabilities",
                {"capabilities": cfg["node"]["capabilities"]},
            )
        except HTTPException:
            runtime_result = {"warning": "Cluster not reachable"}

    return {
        "ok": True,
        "changed": changed,
        "config_file": saved_path,
        "runtime": runtime_result,
        "needs_restart": [f for f in changed if f != "capabilities"],
    }


@router.get("/config/yaml")
async def get_config_yaml():
    """Get full config as raw YAML string."""
    cfg, raw_yaml, cfg_path = _read_config_file()
    return {
        "ok": True,
        "config_file": cfg_path,
        "yaml": raw_yaml or "",
    }


class YamlBody(BaseModel):
    yaml: str


@router.put("/config/yaml")
async def save_config_yaml(body: YamlBody):
    """Save full config YAML (requires restart to take effect)."""
    try:
        parsed = yaml.safe_load(body.yaml)
        if not isinstance(parsed, dict):
            raise ValueError("Config must be a YAML mapping")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid YAML: {e}")

    saved_path = _write_config_file(parsed)
    return {"ok": True, "config_file": saved_path, "needs_restart": True}


@router.post("/config/restart")
async def restart_service():
    """Attempt to restart the hermes-cluster service."""
    import shutil
    import subprocess

    cfg_path = str(_CONFIG_PATHS[0])
    if not Path(cfg_path).exists():
        raise HTTPException(status_code=404, detail="Config file not found")

    binary = shutil.which("hermes-cluster")
    if not binary:
        raise HTTPException(status_code=404, detail="hermes-cluster binary not found in PATH")

    try:
        # Find and kill existing process
        result = subprocess.run(
            ["pkill", "-f", "hermes-cluster"],
            capture_output=True, text=True, timeout=5,
        )
        logger.info("pkill result: %s", result.stdout or result.stderr or "ok")

        # Start new process
        proc = subprocess.Popen(
            [binary, "-config", cfg_path],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return {"ok": True, "pid": proc.pid, "config": cfg_path}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Restart failed: {e}")


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


@router.get("/health")
async def health():
    """Check if the cluster service is reachable."""
    try:
        result = _proxy("GET", "/api/v1/nodes")
        return {"ok": True, "nodes": len(result) if isinstance(result, list) else 0}
    except HTTPException:
        return {"ok": False, "error": "Cluster service unreachable"}


# ---------------------------------------------------------------------------
# Cluster Data
# ---------------------------------------------------------------------------


@router.get("/nodes")
async def list_nodes():
    """List all cluster nodes."""
    return _proxy("GET", "/api/v1/nodes")


@router.get("/tasks")
async def list_tasks():
    """List all cluster tasks."""
    return _proxy("GET", "/api/v1/tasks")


@router.get("/leases")
async def list_leases():
    """List all active leases."""
    return _proxy("GET", "/api/v1/leases")


@router.get("/status")
async def get_status(
    node: str = "",
    status: str = "",
    capability: str = "",
):
    """Get global cluster status view with optional filters."""
    params = []
    if node:
        params.append(f"node={node}")
    if status:
        params.append(f"status={status}")
    if capability:
        params.append(f"capability={capability}")
    qs = "?" + "&".join(params) if params else ""
    return _proxy("GET", f"/api/v1/status{qs}")


@router.get("/topology")
async def get_topology():
    """Get cluster topology."""
    return _proxy("GET", "/api/v1/cluster/topology")


@router.get("/cluster-metrics")
async def get_cluster_metrics():
    """Get aggregated cluster metrics."""
    return _proxy("GET", "/api/v1/cluster/metrics")


@router.get("/timeline")
async def get_timeline():
    """Get cluster event timeline."""
    return _proxy("GET", "/api/v1/cluster/timeline")


@router.get("/workflow/graph")
async def get_workflow_graph():
    """Get workflow dependency graph."""
    return _proxy("GET", "/api/v1/workflow/graph")

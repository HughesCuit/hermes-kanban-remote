"""Setup Wizard API — 4 endpoints for dashboard-driven cluster setup.

Endpoints:
  GET  /api/v1/setup/status        — whether setup is complete
  GET  /api/v1/setup/config        — current config (token masked)
  POST /api/v1/setup/config        — save config and optionally start server
  POST /api/v1/setup/test-connection — test worker→main connectivity
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.error import URLError

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/setup", tags=["setup"])


# ---------------------------------------------------------------------------
# Pydantic request models
# ---------------------------------------------------------------------------

class SetupConfigRequest(BaseModel):
    """Body for POST /api/v1/setup/config."""
    cluster_id: str = "hermes-cluster"
    role: str = "main"  # "main" or "worker"
    node_id: str = "main-node"
    node_name: str = ""
    capabilities: List[str] = ["planning", "reviewing", "scheduling"]
    port: int = 8787
    token: str = ""
    endpoint: str = ""
    auto_start: bool = True


class TestConnectionRequest(BaseModel):
    """Body for POST /api/v1/setup/test-connection."""
    endpoint: str


# ---------------------------------------------------------------------------
# Helpers — import from setup_wizard
# ---------------------------------------------------------------------------

def _get_setup_wizard():
    """Lazy import to avoid circular imports and allow test patching."""
    import setup_wizard as sw
    return sw


def _mask_token(token: str) -> str:
    """Mask token: show first 8 and last 4 chars, mask the rest."""
    if len(token) <= 12:
        return token  # too short to meaningfully mask
    return token[:8] + "*" * (len(token) - 12) + token[-4:]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/status")
async def setup_status():
    """GET /api/v1/setup/status — check whether setup is complete."""
    sw = _get_setup_wizard()
    config_path: Path = sw.CONFIG_PATH

    has_config = config_path.exists()
    setup_complete = False

    if has_config:
        try:
            data = json.loads(config_path.read_text(encoding="utf-8"))
            setup_complete = data.get("setup_complete", False)
        except (json.JSONDecodeError, OSError):
            setup_complete = False

    return {"setup_complete": setup_complete, "has_config": has_config}


@router.get("/config")
async def get_setup_config():
    """GET /api/v1/setup/config — return current config with token masked."""
    sw = _get_setup_wizard()
    config_path: Path = sw.CONFIG_PATH

    if not config_path.exists():
        raise HTTPException(status_code=404, detail="No configuration found")

    try:
        data = json.loads(config_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read config: {exc}",
        )

    # Mask the token for display
    masked = dict(data)
    if "token" in masked:
        masked["token"] = _mask_token(masked["token"])

    return masked


@router.post("/config")
async def save_setup_config(req: SetupConfigRequest):
    """POST /api/v1/setup/config — save configuration and optionally start server."""
    sw = _get_setup_wizard()

    # Build the config dict using the wizard's builder functions
    if req.role == "main":
        token = req.token or sw.generate_token()
        config = sw.build_main_config(
            cluster_id=req.cluster_id,
            node_id=req.node_id,
            node_name=req.node_name or "Main Node",
            capabilities=req.capabilities,
            port=req.port,
            token=token,
        )
    else:
        if not req.endpoint:
            raise HTTPException(
                status_code=422,
                detail="endpoint is required for worker nodes",
            )
        if not req.token:
            raise HTTPException(
                status_code=422,
                detail="token is required for worker nodes",
            )
        config = sw.build_worker_config(
            endpoint=req.endpoint,
            token=req.token,
            node_id=req.node_id if req.node_id else None,
            node_name=req.node_name if req.node_name else None,
            capabilities=req.capabilities,
        )
        # Override auto_start from request
        config["auto_start"] = req.auto_start

    # Force setup_complete
    config["setup_complete"] = True

    # Save config
    sw.save_config(config)
    logger.info("Setup config saved via dashboard API")

    # Optionally start the cluster server
    server_started = False
    if req.auto_start and req.role == "main":
        try:
            server_started = sw.init_cluster_server(config)
        except Exception as exc:
            logger.warning("Failed to auto-start server: %s", exc)

    return {
        "status": "saved",
        "config": config,
        "server_started": server_started,
    }


@router.post("/test-connection")
async def test_connection(req: TestConnectionRequest):
    """POST /api/v1/setup/test-connection — test worker→main connectivity."""
    from urllib.request import Request, urlopen

    endpoint = req.endpoint.rstrip("/")
    url = f"{endpoint}/api/v1/cluster/status"

    try:
        http_req = Request(url, method="GET")
        http_req.add_header("Accept", "application/json")
        with urlopen(http_req, timeout=5) as resp:
            data = json.loads(resp.read().decode())
            return {
                "status": "connected",
                "cluster_id": data.get("cluster_id", ""),
                "node_count": data.get("node_count", 0),
            }
    except URLError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Connection failed: {exc}",
        )
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Connection test failed: {exc}",
        )

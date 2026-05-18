"""FastAPI application factory — replaces Go's Server + Chi router.

Serves:
  - REST API at /api/v1/*
  - Health check at /health
  - Web Dashboard at /dashboard/*
  - Metrics placeholder at /metrics
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from .state import ClusterState
from .routers import (
    nodes_router,
    tasks_router,
    leases_router,
    sync_router,
    recovery_router,
    schedule_router,
    federation_router,
    hooks_router,
    workflow_router,
    status_router,
    config_router,
    visualization_router,
    setup_router,
)
from .routers import nodes as nodes_mod
from .routers import tasks as tasks_mod
from .routers import leases as leases_mod
from .routers import sync as sync_mod
from .routers import recovery as recovery_mod
from .routers import schedule as schedule_mod
from .routers import federation as federation_mod
from .routers import hooks as hooks_mod
from .routers import workflow as workflow_mod
from .routers import status as status_mod
from .routers import config as config_mod
from .routers import visualization as visualization_mod
from .routers import setup as setup_mod


def create_app(
    cluster_id: str = "cluster_default",
    node_id: str = "node_main",
    node_role: str = "main",
    config_path: str = "",
    fed_token: str = "",
    static_dir: Optional[str] = None,
) -> FastAPI:
    """Create and configure the FastAPI application.

    Args:
        cluster_id: Cluster identifier
        node_id: This node's identifier
        node_role: "main" or "worker"
        config_path: Path to cluster.yaml for config save/load
        fed_token: Shared secret for federation auth
        static_dir: Path to dashboard static files (HTML/CSS/JS)
    """
    app = FastAPI(
        title="hermes-agent-cluster",
        description="Python backend for Hermes Agent Cluster — replaces Go implementation",
        version="1.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
    )

    # CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Initialize state
    state = ClusterState()
    state.cluster_id = cluster_id
    state.node_id = node_id
    state.node_role = node_role
    if config_path:
        state.set_config_path(config_path)

    # Wire up all routers with shared state
    nodes_mod.init(state)
    tasks_mod.init(state)
    leases_mod.init(state)
    sync_mod.init(state)
    recovery_mod.init(state)
    schedule_mod.init(state)
    federation_mod.init(state, fed_token)
    # Initialize HookManager for the hooks router
    from .hooks.manager import HookManager
    hook_manager = HookManager()
    hooks_mod.set_hook_manager(hook_manager)
    workflow_mod.init(state)
    status_mod.init(state)
    config_mod.init(state)
    visualization_mod.init(state)

    # Register routers
    app.include_router(nodes_router)
    app.include_router(tasks_router)
    app.include_router(leases_router)
    app.include_router(sync_router)
    app.include_router(recovery_router)
    app.include_router(schedule_router)
    app.include_router(federation_router)
    app.include_router(hooks_router)
    app.include_router(workflow_router)
    app.include_router(status_router)
    app.include_router(config_router)
    app.include_router(visualization_router)
    app.include_router(setup_router)

    # Health endpoint (outside /api/v1)
    @app.get("/health")
    async def health():
        uptime = int((time.time() - state.started_at.timestamp()))
        return {
            "status": "ok",
            "cluster_id": state.cluster_id,
            "node_id": state.node_id,
            "role": state.node_role,
            "uptime_seconds": uptime,
            "version": "python-1.0.0",
        }

    # Metrics endpoint (placeholder)
    @app.get("/metrics")
    async def metrics():
        # Prometheus text format placeholder
        return JSONResponse(
            content="# No metrics collected yet\n",
            media_type="text/plain",
        )

    # Dashboard static file serving
    if static_dir:
        static_path = Path(static_dir)
        if static_path.exists():
            # Mount static files
            app.mount("/dashboard/static", StaticFiles(directory=str(static_path)), name="static")

            # Serve index.html at /dashboard/
            @app.get("/dashboard/")
            async def dashboard_index():
                index_file = static_path / "index.html"
                if index_file.exists():
                    return FileResponse(str(index_file))
                return HTMLResponse("<h1>Dashboard not found</h1>", status_code=404)

            # Serve guide.html at /dashboard/guide.html
            @app.get("/dashboard/guide.html")
            async def dashboard_guide():
                guide_file = static_path / "guide.html"
                if guide_file.exists():
                    return FileResponse(str(guide_file))
                return HTMLResponse("<h1>Guide not found</h1>", status_code=404)

            # Serve config.html at /dashboard/config.html
            @app.get("/dashboard/config.html")
            async def dashboard_config():
                config_file = static_path / "config.html"
                if config_file.exists():
                    return FileResponse(str(config_file))
                return HTMLResponse("<h1>Config page not found</h1>", status_code=404)

    # Redirect /dashboard to /dashboard/ (only when static dir is NOT configured)
    if not static_dir:
        @app.get("/dashboard/{path:path}")
        async def dashboard_fallback(path: str = ""):
            from fastapi.responses import HTMLResponse
            return HTMLResponse("<h1>Dashboard not configured</h1>", status_code=404)

    return app

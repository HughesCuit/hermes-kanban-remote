"""FastAPI router for state sync HTTP endpoints.

Endpoints:
  POST /api/v1/sync/batch    — receive BatchSyncMessage (delta sync)
  POST /api/v1/sync/full     — receive full state snapshot
  GET  /api/v1/sync/version  — get current sync version
  GET  /api/v1/sync/stats    — get sync statistics
  GET  /api/v1/sync/peers    — list connected peers

All endpoints use LWW conflict resolution via SyncReceiver.
"""

from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from ..models import BatchSyncMessage, SyncMessage


# ---------------------------------------------------------------------------
# Request/Response models
# ---------------------------------------------------------------------------

class SyncBatchRequest(BaseModel):
    """Incoming batch sync message."""
    messages: list[dict] = []


class SyncFullStateRequest(BaseModel):
    """Full state snapshot from a remote node."""
    node_id: str = ""
    sync_version: int = 0
    timestamp: int = 0
    nodes: list[dict] = []
    tasks: list[dict] = []


class SyncVersionResponse(BaseModel):
    """Current sync version."""
    version: int = 0
    node_id: str = ""


class SyncStatsResponse(BaseModel):
    """Sync statistics."""
    client: dict = {}
    receiver: dict = {}
    peers: int = 0
    pending_messages: int = 0
    running: bool = False


class SyncBatchResponse(BaseModel):
    """Response to batch sync."""
    accepted: int = 0
    rejected: int = 0
    total: int = 0


class SyncFullResponse(BaseModel):
    """Response to full state sync."""
    nodes_applied: int = 0
    nodes_rejected: int = 0
    tasks_applied: int = 0
    tasks_rejected: int = 0


# ---------------------------------------------------------------------------
# Router factory
# ---------------------------------------------------------------------------

def create_sync_router(coordinator: Any) -> APIRouter:
    """Create a FastAPI router for sync endpoints.

    Args:
        coordinator: SyncCoordinator instance with store, receiver, client

    Returns:
        APIRouter with all sync endpoints registered.
    """
    router = APIRouter(prefix="/api/v1/sync", tags=["sync"])

    receiver = coordinator.receiver
    store = coordinator._store

    @router.post("/batch", response_model=SyncBatchResponse)
    async def receive_batch(request: SyncBatchRequest) -> SyncBatchResponse:
        """Receive a batch of sync messages from a remote node."""
        try:
            batch = BatchSyncMessage(
                messages=[SyncMessage(**m) for m in request.messages]
            )
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid sync message: {e}")

        result = receiver.receive_batch(batch)
        return SyncBatchResponse(
            accepted=result["accepted"],
            rejected=result["rejected"],
            total=result["total"],
        )

    @router.post("/full", response_model=SyncFullResponse)
    async def receive_full_state(request: SyncFullStateRequest) -> SyncFullResponse:
        """Receive a full state snapshot from a remote node."""
        state_data = {
            "node_id": request.node_id,
            "sync_version": request.sync_version,
            "timestamp": request.timestamp,
            "nodes": request.nodes,
            "tasks": request.tasks,
        }
        result = receiver.receive_full_state(state_data)
        return SyncFullResponse(
            nodes_applied=result["nodes_applied"],
            nodes_rejected=result["nodes_rejected"],
            tasks_applied=result["tasks_applied"],
            tasks_rejected=result["tasks_rejected"],
        )

    @router.get("/version", response_model=SyncVersionResponse)
    async def get_sync_version() -> SyncVersionResponse:
        """Get current sync version."""
        return SyncVersionResponse(
            version=store.sync_version(),
            node_id=getattr(store, "node_id", "node_main"),
        )

    @router.get("/stats", response_model=SyncStatsResponse)
    async def get_sync_stats() -> SyncStatsResponse:
        """Get sync statistics."""
        stats = coordinator.get_stats()
        return SyncStatsResponse(
            client=stats.get("client", {}),
            receiver=stats.get("receiver", {}),
            peers=stats.get("peers", 0),
            pending_messages=stats.get("pending_messages", 0),
            running=stats.get("running", False),
        )

    @router.get("/peers")
    async def list_peers() -> dict:
        """List configured peer nodes."""
        return {
            "peers": coordinator.peers,
            "count": len(coordinator.peers),
        }

    @router.post("/peers/add")
    async def add_peer(endpoint: str) -> dict:
        """Add a peer node for sync."""
        coordinator.add_peer(endpoint)
        return {"added": endpoint, "peers": coordinator.peers}

    @router.post("/peers/remove")
    async def remove_peer(endpoint: str) -> dict:
        """Remove a peer node from sync."""
        coordinator.remove_peer(endpoint)
        return {"removed": endpoint, "peers": coordinator.peers}

    @router.post("/send")
    async def send_sync_message(msg: SyncMessage) -> dict:
        """Manually queue a sync message for sending."""
        coordinator.queue_sync_message(msg)
        return {"queued": True, "version": msg.version}

    return router

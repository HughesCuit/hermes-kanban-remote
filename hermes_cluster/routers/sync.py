"""Sync endpoints — /api/v1/sync"""

from fastapi import APIRouter

from ..models import SyncMessage, BatchSyncMessage
from ..state import ClusterState

router = APIRouter(prefix="/api/v1/sync", tags=["sync"])

_state: ClusterState = None


def init(state: ClusterState):
    global _state
    _state = state


@router.post("/receive")
async def sync_receive(msg: SyncMessage):
    applied = _state.handle_sync_message(msg)
    return {"applied": applied}


@router.post("/receive-batch")
async def sync_receive_batch(batch: BatchSyncMessage):
    applied = _state.handle_batch_sync(batch)
    return {"applied": applied}


@router.get("/status")
async def sync_status():
    return {"version": _state.sync_version()}

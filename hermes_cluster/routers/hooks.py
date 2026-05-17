"""Webhook management endpoints — /api/v1/hooks"""

from fastapi import APIRouter, HTTPException

from ..models import RegisterHookRequest, EventType
from ..state import ClusterState

router = APIRouter(prefix="/api/v1/hooks", tags=["hooks"])

_state: ClusterState = None


def init(state: ClusterState):
    global _state
    _state = state


@router.post("")
async def register_hook(req: RegisterHookRequest):
    if not req.url:
        raise HTTPException(status_code=400, detail="url is required")
    hook = _state.register_hook(req.url, req.events, req.secret or "")
    return hook


@router.delete("/{hook_id}")
async def deregister_hook(hook_id: str):
    if not _state.deregister_hook(hook_id):
        raise HTTPException(status_code=404, detail="hook not found")
    return {"status": "deregistered"}


@router.get("")
async def list_hooks():
    return _state.list_hooks()


@router.get("/{hook_id}/deliveries")
async def hook_deliveries(hook_id: str):
    return _state.get_hook_deliveries(hook_id)

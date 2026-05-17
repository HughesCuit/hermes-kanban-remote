"""Node management endpoints — /api/v1/nodes"""

from fastapi import APIRouter, HTTPException

from ..models import (
    JoinRequest,
    JoinResponse,
    HeartbeatRequest,
    UpdateCapabilitiesRequest,
    Node,
    TaskStatus,
)
from ..state import ClusterState

router = APIRouter(prefix="/api/v1/nodes", tags=["nodes"])

# Will be set by the app factory
_state: ClusterState = None


def init(state: ClusterState):
    global _state
    _state = state


@router.post("/join", response_model=JoinResponse)
async def join(req: JoinRequest):
    node_id = "node_" + req.node_name
    node = Node(
        id=node_id,
        name=req.node_name,
        capabilities=req.capabilities,
    )
    _state.register_node(node)
    return JoinResponse(node_id=node_id, status="registered")


@router.post("/heartbeat")
async def heartbeat(req: HeartbeatRequest):
    _state.update_heartbeat(req.node_id)
    return {"status": "ok"}


@router.get("")
async def list_nodes():
    return _state.get_all_nodes()


@router.patch("/{node_id}/capabilities")
async def update_capabilities(node_id: str, req: UpdateCapabilitiesRequest):
    node = _state.get_node(node_id)
    if not node:
        raise HTTPException(status_code=404, detail="node not found")
    _state.update_capabilities(node_id, req.capabilities)
    # Re-trigger scheduling
    _state.trigger_pending_tasks()
    _state.schedule_pending()
    return {
        "node_id": node_id,
        "capabilities": req.capabilities,
        "status": "updated",
    }

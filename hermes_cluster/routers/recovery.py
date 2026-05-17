"""Recovery endpoints — /api/v1/recovery"""

import threading

from fastapi import APIRouter

from ..models import RecoveryTriggerRequest
from ..state import ClusterState

router = APIRouter(prefix="/api/v1/recovery", tags=["recovery"])

_state: ClusterState = None


def init(state: ClusterState):
    global _state
    _state = state


@router.post("/trigger")
async def recovery_trigger(req: RecoveryTriggerRequest):
    # Run async to match Go's `go s.Recovery.NotifyOffline(req.NodeID)`
    threading.Thread(
        target=_state.trigger_recovery,
        args=(req.node_id,),
        daemon=True,
    ).start()
    return {"status": "accepted"}


@router.get("/log")
async def recovery_log():
    return _state.get_recovery_events()


@router.get("/stats")
async def recovery_stats():
    return _state.recovery_stats()

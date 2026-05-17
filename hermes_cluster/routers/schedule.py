"""Schedule endpoints — /api/v1/schedule"""

from fastapi import APIRouter

from ..state import ClusterState

router = APIRouter(prefix="/api/v1/schedule", tags=["schedule"])

_state: ClusterState = None


def init(state: ClusterState):
    global _state
    _state = state


@router.post("/trigger")
async def schedule_trigger():
    promoted = _state.trigger_pending_tasks()
    scheduled = _state.schedule_pending()
    return {"promoted": promoted, "scheduled": scheduled}


@router.get("/stats")
async def schedule_stats():
    return _state.get_schedule_stats()


@router.get("/decisions")
async def schedule_decisions():
    decisions = _state.get_decisions()
    return {"decisions": decisions, "count": len(decisions)}

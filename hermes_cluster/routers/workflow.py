"""Workflow / Dependency endpoints — /api/v1/workflow"""

from fastapi import APIRouter

from ..state import ClusterState

router = APIRouter(prefix="/api/v1/workflow", tags=["workflow"])

_state: ClusterState = None


def init(state: ClusterState):
    global _state
    _state = state


@router.get("/graph")
async def get_graph():
    return _state.get_workflow_graph()

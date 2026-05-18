"""Cluster status endpoint — /api/v1/cluster"""

from datetime import datetime

from fastapi import APIRouter

from ..state import ClusterState

router = APIRouter(prefix="/api/v1/cluster", tags=["cluster"])

_state: ClusterState = None


def init(state: ClusterState):
    global _state
    _state = state


@router.get("/status")
async def cluster_status():
    """Cluster overview for Dashboard."""
    task_counts = _state.task_counts()
    uptime_seconds = int((datetime.utcnow() - _state.started_at).total_seconds())
    return {
        "cluster_id": _state.cluster_id,
        "node_count": _state.node_count(),
        "online_nodes": _state.online_count(),
        "task_count": task_counts["total"],
        "tasks_by_status": {
            "pending": task_counts.get("pending", 0),
            "ready": task_counts.get("ready", 0),
            "running": task_counts.get("running", 0),
            "completed": task_counts.get("completed", 0),
            "failed": task_counts.get("failed", 0),
        },
        "uptime_seconds": uptime_seconds,
        "version": "python-1.0.0",
    }

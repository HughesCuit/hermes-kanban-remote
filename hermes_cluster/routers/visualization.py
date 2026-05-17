"""Cluster visualization endpoints — /api/v1/cluster"""

from fastapi import APIRouter, Query

from ..state import ClusterState

router = APIRouter(prefix="/api/v1/cluster", tags=["visualization"])

_state: ClusterState = None


def init(state: ClusterState):
    global _state
    _state = state


@router.get("/topology")
async def cluster_topology():
    nodes = _state.get_all_nodes()
    topology = {
        "nodes": [
            {
                "id": n.id,
                "name": n.name,
                "status": n.status.value if hasattr(n.status, 'value') else n.status,
                "capabilities": n.capabilities,
                "load": n.load,
            }
            for n in nodes
        ],
    }
    return topology


@router.get("/metrics")
async def cluster_metrics():
    task_counts = _state.task_counts()
    return {
        "nodes": {"total": _state.node_count(), "online": _state.online_count()},
        "tasks": task_counts,
        "sync_version": _state.sync_version(),
    }


@router.get("/timeline")
async def cluster_timeline(limit: int = Query(50, ge=1, le=1000)):
    events = _state.get_recovery_events()
    # Return most recent events
    return events[-limit:] if len(events) > limit else events


@router.get("/viz")
async def cluster_viz(limit: int = Query(50, ge=1, le=1000)):
    nodes = _state.get_all_nodes()
    task_counts = _state.task_counts()
    events = _state.get_recovery_events()

    topology = {
        "nodes": [
            {
                "id": n.id,
                "name": n.name,
                "status": n.status.value if hasattr(n.status, 'value') else n.status,
                "capabilities": n.capabilities,
                "load": n.load,
            }
            for n in nodes
        ],
    }
    metrics = {
        "nodes": {"total": _state.node_count(), "online": _state.online_count()},
        "tasks": task_counts,
        "sync_version": _state.sync_version(),
    }
    timeline = events[-limit:] if len(events) > limit else events

    return {
        "topology": topology,
        "metrics": metrics,
        "timeline": timeline,
    }

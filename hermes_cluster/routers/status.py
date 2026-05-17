"""Status / Summary endpoints — /api/v1/status, /api/v1/summary"""

from fastapi import APIRouter, Query

from ..state import ClusterState

router = APIRouter(prefix="/api/v1", tags=["status"])

_state: ClusterState = None


def init(state: ClusterState):
    global _state
    _state = state


@router.get("/status")
async def status(
    node: str = Query("", description="Filter by node ID"),
    status: str = Query("", description="Filter by status"),
    capability: str = Query("", description="Filter by capability"),
):
    tasks = _state.get_all_tasks()
    nodes = _state.get_all_nodes()

    # Build entries matching Go's status view
    entries = []
    for task in tasks:
        entry = {
            "task_id": task.id,
            "title": task.title,
            "status": task.status.value if hasattr(task.status, 'value') else task.status,
            "priority": task.priority,
            "assigned_to": task.assigned_to,
            "requires": task.requires,
        }
        # Filter
        if node and task.assigned_to != node:
            continue
        if status and entry["status"] != status:
            continue
        if capability and capability not in task.requires:
            continue
        entries.append(entry)

    # Build summary
    task_counts = _state.task_counts()
    summary = {
        "total_tasks": task_counts["total"],
        "pending": task_counts.get("pending", 0),
        "ready": task_counts.get("ready", 0),
        "running": task_counts.get("running", 0),
        "completed": task_counts.get("completed", 0),
        "failed": task_counts.get("failed", 0),
        "total_nodes": _state.node_count(),
        "online_nodes": _state.online_count(),
    }

    return {"entries": entries, "summary": summary}


@router.get("/summary")
async def summary():
    return _state.get_summary()

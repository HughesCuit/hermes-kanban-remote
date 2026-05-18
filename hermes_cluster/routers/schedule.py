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
    """Trigger scheduler to assign ready tasks to idle nodes."""
    promoted = _state.trigger_pending_tasks()
    scheduled = _state.schedule_pending()

    # Build list of current assignments (tasks that are running with an assigned node)
    assignments = []
    tasks = _state.get_all_tasks()
    for task in tasks:
        if task.status.value == "running" and task.assigned_to:
            assignments.append({
                "task_id": task.id,
                "task_title": task.title,
                "node_id": task.assigned_to,
                "priority": task.priority,
            })

    return {
        "promoted": promoted,
        "scheduled": scheduled,
        "assignments": assignments,
    }


@router.get("/stats")
async def schedule_stats():
    return _state.get_schedule_stats()


@router.get("/decisions")
async def schedule_decisions():
    decisions = _state.get_decisions()
    return {"decisions": decisions, "count": len(decisions)}

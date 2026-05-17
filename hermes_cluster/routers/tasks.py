"""Task management endpoints — /api/v1/tasks"""

from fastapi import APIRouter, HTTPException, Request

from ..models import (
    SubmitTaskRequest,
    FailTaskRequest,
    SetDependenciesRequest,
    Task,
    TaskStatus,
)
from ..state import ClusterState

router = APIRouter(prefix="/api/v1/tasks", tags=["tasks"])

_state: ClusterState = None


def init(state: ClusterState):
    global _state
    _state = state


def _generate_task_id() -> str:
    import secrets
    return "task_" + secrets.token_hex(8)


@router.post("")
async def submit_task(req: SubmitTaskRequest):
    task_id = _generate_task_id()
    priority = req.priority if req.priority > 0 else 3
    task = _state.create_task(task_id, req.title, req.requires, priority)
    # Promote pending → ready (tasks with no deps go to ready immediately)
    # But do NOT auto-assign to nodes — use /schedule/trigger for that
    _state.trigger_pending_tasks()
    return task


@router.get("")
async def list_tasks():
    return _state.get_all_tasks()


@router.post("/{task_id}/complete")
async def complete_task(task_id: str):
    task = _state.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="task not found")
    _state.set_task_status(task_id, TaskStatus.completed)
    # Auto-transition downstream tasks
    _trigger_downstream(task_id)
    return {"status": "completed"}


@router.post("/{task_id}/fail")
async def fail_task(task_id: str, req: FailTaskRequest = None):
    task = _state.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="task not found")
    reason = req.reason if req else "failed"
    _state.set_task_status(task_id, TaskStatus.failed, fail_reason=reason)
    # Block downstream tasks
    blocked = _state.get_dependents(task_id)
    for dep_id in blocked:
        dep_task = _state.get_task(dep_id)
        if dep_task and dep_task.status == TaskStatus.pending:
            _state.set_task_status(dep_id, TaskStatus.blocked)
    return {"status": "failed", "blocked": blocked}


@router.post("/{task_id}/unblock")
async def unblock_task(task_id: str):
    if not _state.unblock_task(task_id):
        raise HTTPException(status_code=400, detail="task not in blocked state")
    return {"status": "unblocked"}


@router.post("/{task_id}/advance")
async def manual_advance(task_id: str):
    task = _state.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="task not found")
    # Try to resolve dependencies
    if task.depends_on:
        all_done = all(
            (dep := _state.get_task(dep_id)) is not None
            and dep.status == TaskStatus.completed
            for dep_id in task.depends_on
        )
        if not all_done:
            raise HTTPException(status_code=400, detail="dependencies not met")
    _state.set_task_status(task_id, TaskStatus.ready)
    _state.schedule_pending()
    return {"status": "advanced"}


@router.post("/{task_id}/dependencies")
async def set_dependencies(task_id: str, req: SetDependenciesRequest):
    task = _state.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="task not found")
    _state.set_dependencies(task_id, req.depends_on)
    return _state.get_task(task_id)


@router.get("/{task_id}/dependents")
async def get_dependents(task_id: str):
    dependents = _state.get_dependents(task_id)
    return {"task_id": task_id, "dependents": dependents, "count": len(dependents)}


@router.get("/{task_id}/trigger-chain")
async def get_trigger_chain(task_id: str):
    task = _state.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="task not found")
    chain = _state.get_trigger_chain(task_id)
    return {"task_id": task_id, "chain": chain, "count": len(chain)}


def _trigger_downstream(task_id: str):
    """When a task completes, check if any dependent tasks can now be promoted."""
    dependents = _state.get_dependents(task_id)
    for dep_id in dependents:
        dep_task = _state.get_task(dep_id)
        if not dep_task or dep_task.status != TaskStatus.pending:
            continue
        # Check if all dependencies of this dependent are met
        all_done = all(
            (d := _state.get_task(d_id)) is not None
            and d.status == TaskStatus.completed
            for d_id in dep_task.depends_on
        )
        if all_done:
            _state.set_task_status(dep_id, TaskStatus.ready)
    # Then schedule any newly ready tasks
    _state.schedule_pending()

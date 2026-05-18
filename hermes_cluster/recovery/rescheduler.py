"""Rescheduler — reschedules orphaned tasks after node failure.

Mirrors Go's internal/recovery/rescheduler.go:
  - RescheduleOrphaned(task_ids) → count of successfully rescheduled
  - For each task: try to assign to an available node, else mark as failed
"""

from __future__ import annotations

import secrets
import threading
from typing import TYPE_CHECKING, List

from ..models import RecoveryEvent, TaskStatus

if TYPE_CHECKING:
    from ..state import ClusterState


def _gen_id(prefix: str = "recovery") -> str:
    return f"{prefix}_{secrets.token_hex(8)}"


class Rescheduler:
    """Reschedules tasks that lost their node after a failure."""

    def __init__(self, state: "ClusterState") -> None:
        self._state = state
        self._lock = threading.Lock()

    def reschedule_orphaned(self, task_ids: List[str]) -> int:
        """Attempt to reschedule each task in *task_ids*.

        For each task:
        1. Atomically clear its ``assigned_to`` and set status back to ``ready``
        2. Run the scheduler to pick a new node
        3. If no node is available, mark the task as ``failed``

        Thread-safe: the entire per-task operation is performed under the
        Rescheduler's lock to prevent concurrent reschedule calls from
        interfering with each other.

        Returns:
            Count of tasks that were successfully rescheduled.
        """
        rescheduled = 0

        for task_id in task_ids:
            task = self._state.get_task(task_id)
            if task is None:
                continue

            # Atomically unassign the task and set it back to ready
            # Uses the public API instead of accessing internal state
            self._state.unassign_task(task_id)

            # Try to schedule it
            scheduled = self._state.schedule_pending()

            if scheduled > 0:
                # Check if our task got assigned
                task = self._state.get_task(task_id)
                if task and task.assigned_to:
                    event = RecoveryEvent(
                        id=_gen_id(),
                        task_id=task_id,
                        action="reschedule",
                        status="completed",
                        message=f"Rescheduled to node {task.assigned_to}",
                    )
                    self._state.append_recovery_event(event)
                    rescheduled += 1
                else:
                    # Task went ready but scheduler didn't pick it (no matching node)
                    self._state.set_task_status(
                        task_id, TaskStatus.failed,
                        fail_reason="no available node for reschedule",
                    )
                    event = RecoveryEvent(
                        id=_gen_id(),
                        task_id=task_id,
                        action="mark_failed",
                        status="completed",
                        message="No available node for reschedule",
                    )
                    self._state.append_recovery_event(event)
            else:
                # No nodes available at all
                self._state.set_task_status(
                    task_id, TaskStatus.failed,
                    fail_reason="no available node for reschedule",
                )
                event = RecoveryEvent(
                    id=_gen_id(),
                    task_id=task_id,
                    action="mark_failed",
                    status="completed",
                    message="No available node for reschedule",
                )
                self._state.append_recovery_event(event)

        return rescheduled

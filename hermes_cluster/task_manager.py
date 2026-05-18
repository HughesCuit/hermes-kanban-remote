"""Task Manager — full CRUD + auto-schedule + stats for hermes-agent-cluster.

High-level manager that wraps ClusterStore with enhanced task lifecycle:
  - Full CRUD (create, read, update, delete)
  - Auto-schedule (trigger + assign in one call)
  - Stats & analytics (counts, throughput, latency, history)
  - Filtering & pagination

Design rationale:
  - ClusterStore handles raw persistence; TaskManager adds business logic
  - TaskManager depends only on ClusterStore's public API, not internals
  - Stateless operations (no background threads) — caller drives scheduling
"""

from __future__ import annotations

import logging
import secrets
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from hermes_cluster.models import (
    LeaseStatus,
    NodeStatus,
    Task,
    TaskStatus,
)

logger = logging.getLogger(__name__)


def _generate_id(prefix: str = "") -> str:
    if prefix:
        return f"{prefix}_{secrets.token_hex(8)}"
    return secrets.token_hex(8)


# ---------------------------------------------------------------------------
# Enums & dataclasses
# ---------------------------------------------------------------------------

class SortField(str, Enum):
    priority = "priority"
    created_at = "created_at"
    updated_at = "updated_at"
    status = "status"
    title = "title"


class SortOrder(str, Enum):
    asc = "asc"
    desc = "desc"


@dataclass
class TaskFilter:
    """Filter/sort/pagination params for task queries."""
    status: Optional[str] = None
    assigned_to: Optional[str] = None
    requires: Optional[str] = None  # capability filter
    priority_min: Optional[int] = None
    priority_max: Optional[int] = None
    search: Optional[str] = None  # title substring search
    sort_by: SortField = SortField.created_at
    sort_order: SortOrder = SortOrder.desc
    offset: int = 0
    limit: int = 100


@dataclass
class TaskStats:
    """Aggregated task statistics."""
    total: int = 0
    by_status: Dict[str, int] = field(default_factory=dict)
    by_priority: Dict[int, int] = field(default_factory=dict)
    by_node: Dict[str, int] = field(default_factory=dict)
    avg_priority: float = 0.0
    completion_rate: float = 0.0  # completed / (completed + failed)
    failure_rate: float = 0.0    # failed / (completed + failed)
    oldest_pending_seconds: Optional[float] = None
    newest_task_seconds: Optional[float] = None
    active_tasks: int = 0  # running + assigned
    blocked_tasks: int = 0
    total_leases_active: int = 0


@dataclass
class ThroughputStats:
    """Task throughput metrics."""
    completed_count: int = 0
    failed_count: int = 0
    created_count: int = 0
    window_seconds: int = 0
    completions_per_minute: float = 0.0
    failures_per_minute: float = 0.0


@dataclass
class ScheduleResult:
    """Result of an auto-schedule operation."""
    promoted: int = 0
    scheduled: int = 0
    total_ready: int = 0
    total_pending: int = 0
    task_ids_scheduled: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# TaskManager
# ---------------------------------------------------------------------------

class TaskManager:
    """High-level task management with CRUD, auto-schedule, and stats.

    Wraps ClusterStore to provide a cleaner API for task lifecycle operations.
    Does not manage background threads — scheduling is triggered explicitly.
    """

    def __init__(self, store: Any):
        self._store = store

    # -------------------------------------------------------------------
    # CRUD — Create
    # -------------------------------------------------------------------

    def create_task(
        self,
        title: str,
        requires: Optional[List[str]] = None,
        priority: int = 3,
        depends_on: Optional[List[str]] = None,
        assign_to: Optional[str] = None,
    ) -> Task:
        """Create a new task with optional dependencies and immediate assignment.

        Args:
            title: Task title (required).
            requires: Node capabilities needed (empty = any node).
            priority: 1=highest, 5=lowest (default 3).
            depends_on: Task IDs that must complete first.
            assign_to: Immediately assign to a specific node (skips auto-schedule).

        Returns:
            Created Task object.
        """
        if not title or not title.strip():
            raise ValueError("Task title cannot be empty")

        if priority < 1 or priority > 5:
            raise ValueError(f"Priority must be 1-5, got {priority}")

        task_id = _generate_id("task")
        task = self._store.create_task(
            task_id, title, requires or [], priority
        )

        # Set dependencies if provided
        if depends_on:
            self._store.set_dependencies(task_id, depends_on)

        # Immediate assignment if requested
        if assign_to:
            node = self._store.get_node(assign_to)
            if not node:
                raise ValueError(f"Node {assign_to} not found")
            self._store.set_task_status(task_id, TaskStatus.running)
            # Update assigned_to via SQL
            with self._store._tx() as conn:
                conn.execute(
                    "UPDATE tasks SET assigned_to = ? WHERE id = ?",
                    (assign_to, task_id),
                )

        logger.info(
            "task created: id=%s title=%s priority=%s deps=%s assign=%s",
            task_id, title, priority, depends_on or [], assign_to,
        )
        return self._store.get_task(task_id) or task

    def duplicate_task(self, task_id: str, title_suffix: str = " (copy)") -> Optional[Task]:
        """Clone an existing task with a new ID.

        Preserves: title (with suffix), requires, priority, depends_on.
        Resets: status, assigned_to, version.
        """
        original = self._store.get_task(task_id)
        if not original:
            return None

        new_task = self.create_task(
            title=original.title + title_suffix,
            requires=list(original.requires),
            priority=original.priority,
            depends_on=list(original.depends_on),
        )
        logger.info("task duplicated: %s -> %s", task_id, new_task.id)
        return new_task

    # -------------------------------------------------------------------
    # CRUD — Read
    # -------------------------------------------------------------------

    def get_task(self, task_id: str) -> Optional[Task]:
        """Get a task by ID."""
        return self._store.get_task(task_id)

    def list_tasks(self, task_filter: Optional[TaskFilter] = None) -> List[Task]:
        """List tasks with filtering, sorting, and pagination.

        Args:
            task_filter: Optional filter params. None returns all tasks.
        """
        tasks = self._store.get_all_tasks()

        if task_filter:
            tasks = self._apply_filters(tasks, task_filter)
            tasks = self._apply_sort(tasks, task_filter.sort_by, task_filter.sort_order)
            tasks = tasks[task_filter.offset:task_filter.offset + task_filter.limit]

        return tasks

    def count_tasks(self, status: Optional[str] = None) -> int:
        """Count tasks, optionally filtered by status."""
        if status:
            counts = self._store.task_counts()
            return counts.get(status, 0)
        return self._store.task_counts()["total"]

    # -------------------------------------------------------------------
    # CRUD — Update
    # -------------------------------------------------------------------

    def update_task(
        self,
        task_id: str,
        title: Optional[str] = None,
        priority: Optional[int] = None,
        requires: Optional[List[str]] = None,
        depends_on: Optional[List[str]] = None,
    ) -> Optional[Task]:
        """Update task attributes.

        Only provided (non-None) fields are updated.
        Returns the updated task, or None if not found.
        """
        task = self._store.get_task(task_id)
        if not task:
            return None

        with self._store._tx() as conn:
            updates = []
            params = []

            if title is not None:
                if not title.strip():
                    raise ValueError("Task title cannot be empty")
                updates.append("title = ?")
                params.append(title)

            if priority is not None:
                if priority < 1 or priority > 5:
                    raise ValueError(f"Priority must be 1-5, got {priority}")
                updates.append("priority = ?")
                params.append(priority)

            if requires is not None:
                import json
                updates.append("requires = ?")
                params.append(json.dumps(requires))

            if depends_on is not None:
                import json
                updates.append("depends_on = ?")
                params.append(json.dumps(depends_on))
                # Demote to pending if deps added and task was ready
                conn.execute(
                    """UPDATE tasks SET status = ?
                       WHERE id = ? AND status = ? AND ? != '[]'""",
                    (TaskStatus.pending.value, task_id, TaskStatus.ready.value,
                     json.dumps(depends_on)),
                )

            if updates:
                updates.append("updated_at = ?")
                params.append(datetime.utcnow().isoformat())
                updates.append("version = version + 1")
                params.append(task_id)

                conn.execute(
                    f"UPDATE tasks SET {', '.join(updates)} WHERE id = ?",
                    params,
                )

        logger.info("task updated: id=%s fields=%s", task_id, [k for k in ["title", "priority", "requires", "depends_on"] if locals().get(k) is not None])
        return self._store.get_task(task_id)

    def cancel_task(self, task_id: str) -> bool:
        """Cancel a task — set to failed with cancel reason."""
        task = self._store.get_task(task_id)
        if not task:
            return False
        if task.status in (TaskStatus.completed, TaskStatus.failed):
            return False

        self._store.set_task_status(task_id, TaskStatus.failed, fail_reason="cancelled")
        logger.info("task cancelled: id=%s", task_id)
        return True

    def retry_task(self, task_id: str) -> Optional[Task]:
        """Retry a failed task — reset to pending."""
        task = self._store.get_task(task_id)
        if not task:
            return None
        if task.status != TaskStatus.failed:
            return None

        self._store.set_task_status(task_id, TaskStatus.pending)
        self._store.trigger_pending_tasks()
        logger.info("task retried: id=%s", task_id)
        return self._store.get_task(task_id)

    # -------------------------------------------------------------------
    # CRUD — Delete
    # -------------------------------------------------------------------

    def delete_task(self, task_id: str) -> bool:
        """Delete a task and its associated leases.

        Only allowed for terminal tasks (completed/failed/blocked/pending/ready).
        Running tasks must be cancelled first.
        """
        task = self._store.get_task(task_id)
        if not task:
            return False

        if task.status in (TaskStatus.assigned, TaskStatus.running):
            logger.warning("cannot delete running/assigned task %s — cancel first", task_id)
            return False

        with self._store._tx() as conn:
            # Remove associated leases
            conn.execute("DELETE FROM leases WHERE task_id = ?", (task_id,))
            # Remove associated scheduling decisions
            conn.execute("DELETE FROM scheduling_decisions WHERE task_id = ?", (task_id,))
            # Remove the task
            conn.execute("DELETE FROM tasks WHERE id = ?", (task_id,))

        logger.info("task deleted: id=%s", task_id)
        return True

    def purge_completed(self, older_than_seconds: int = 86400) -> int:
        """Delete completed/failed tasks older than N seconds.

        Returns count of deleted tasks.
        """
        cutoff = datetime.utcnow() - timedelta(seconds=older_than_seconds)
        cutoff_str = cutoff.isoformat()

        with self._store._tx() as conn:
            # Find matching task IDs
            rows = conn.execute(
                """SELECT id FROM tasks
                   WHERE status IN (?, ?) AND updated_at < ?""",
                (TaskStatus.completed.value, TaskStatus.failed.value, cutoff_str),
            ).fetchall()
            task_ids = [r["id"] for r in rows]

            if not task_ids:
                return 0

            placeholders = ",".join("?" for _ in task_ids)
            conn.execute(
                f"DELETE FROM leases WHERE task_id IN ({placeholders})",
                task_ids,
            )
            conn.execute(
                f"DELETE FROM scheduling_decisions WHERE task_id IN ({placeholders})",
                task_ids,
            )
            conn.execute(
                f"DELETE FROM tasks WHERE id IN ({placeholders})",
                task_ids,
            )

        logger.info("purged %d old completed/failed tasks", len(task_ids))
        return len(task_ids)

    # -------------------------------------------------------------------
    # Auto-schedule
    # -------------------------------------------------------------------

    def auto_schedule(self) -> ScheduleResult:
        """Trigger pending tasks and assign to nodes in one call.

        Returns ScheduleResult with counts of promoted/scheduled tasks.
        """
        promoted = self._store.trigger_pending_tasks()
        scheduled = self._store.schedule_pending()

        task_counts = self._store.task_counts()
        ready_tasks = [
            t for t in self._store.get_all_tasks()
            if t.status == TaskStatus.ready
        ]
        pending_tasks = [
            t for t in self._store.get_all_tasks()
            if t.status == TaskStatus.pending
        ]

        # Collect IDs of newly scheduled tasks
        newly_scheduled = [
            t.id for t in self._store.get_all_tasks()
            if t.status == TaskStatus.running and t.assigned_to
        ]

        result = ScheduleResult(
            promoted=promoted,
            scheduled=scheduled,
            total_ready=len(ready_tasks),
            total_pending=len(pending_tasks),
            task_ids_scheduled=newly_scheduled,
        )

        logger.info(
            "auto_schedule: promoted=%d scheduled=%d ready=%d pending=%d",
            promoted, scheduled, len(ready_tasks), len(pending_tasks),
        )
        return result

    def schedule_task_to_node(self, task_id: str, node_id: str) -> bool:
        """Manually assign a specific task to a specific node.

        Returns False if task or node not found, or task is not in a schedulable state.
        """
        task = self._store.get_task(task_id)
        if not task:
            return False

        if task.status not in (TaskStatus.ready, TaskStatus.pending):
            return False

        node = self._store.get_node(node_id)
        if not node or node.status != NodeStatus.online:
            return False

        # Check capability match
        if task.requires and not all(
            cap in node.capabilities for cap in task.requires
        ):
            logger.warning(
                "capability mismatch: task %s requires %s, node %s has %s",
                task_id, task.requires, node_id, node.capabilities,
            )
            return False

        with self._store._tx() as conn:
            conn.execute(
                """UPDATE tasks SET status = ?, assigned_to = ?,
                   updated_at = ?, version = version + 1
                   WHERE id = ?""",
                (TaskStatus.running.value, node_id,
                 datetime.utcnow().isoformat(), task_id),
            )

        logger.info("manually scheduled task %s to node %s", task_id, node_id)
        return True

    # -------------------------------------------------------------------
    # Stats
    # -------------------------------------------------------------------

    def get_stats(self) -> TaskStats:
        """Get comprehensive task statistics."""
        task_counts = self._store.task_counts()
        all_tasks = self._store.get_all_tasks()

        # By status
        by_status: Dict[str, int] = {}
        for t in all_tasks:
            s = t.status.value if hasattr(t.status, "value") else t.status
            by_status[s] = by_status.get(s, 0) + 1

        # By priority
        by_priority: Dict[int, int] = {}
        for t in all_tasks:
            by_priority[t.priority] = by_priority.get(t.priority, 0) + 1

        # By node
        by_node: Dict[str, int] = {}
        for t in all_tasks:
            if t.assigned_to:
                by_node[t.assigned_to] = by_node.get(t.assigned_to, 0) + 1

        # Average priority
        priorities = [t.priority for t in all_tasks]
        avg_priority = sum(priorities) / len(priorities) if priorities else 0.0

        # Completion / failure rates
        completed = task_counts.get("completed", 0)
        failed = task_counts.get("failed", 0)
        terminal = completed + failed
        completion_rate = completed / terminal if terminal > 0 else 0.0
        failure_rate = failed / terminal if terminal > 0 else 0.0

        # Pending age
        now = datetime.utcnow()
        oldest_pending_seconds = None
        pending_tasks = [t for t in all_tasks if t.status == TaskStatus.pending]
        if pending_tasks:
            oldest = min(pending_tasks, key=lambda t: t.created_at)
            oldest_pending_seconds = (now - oldest.created_at).total_seconds()

        # Newest task age
        newest_task_seconds = None
        if all_tasks:
            newest = max(all_tasks, key=lambda t: t.created_at)
            newest_task_seconds = (now - newest.created_at).total_seconds()

        # Active leases
        active_leases = self._store.get_active_leases()

        return TaskStats(
            total=task_counts["total"],
            by_status=by_status,
            by_priority=by_priority,
            by_node=by_node,
            avg_priority=round(avg_priority, 2),
            completion_rate=round(completion_rate, 4),
            failure_rate=round(failure_rate, 4),
            oldest_pending_seconds=oldest_pending_seconds,
            newest_task_seconds=newest_task_seconds,
            active_tasks=by_status.get("running", 0) + by_status.get("assigned", 0),
            blocked_tasks=by_status.get("blocked", 0),
            total_leases_active=len(active_leases),
        )

    def get_throughput(self, window_seconds: int = 3600) -> ThroughputStats:
        """Get task throughput for the last N seconds."""
        cutoff = datetime.utcnow() - timedelta(seconds=window_seconds)
        all_tasks = self._store.get_all_tasks()

        completed_count = 0
        failed_count = 0
        created_count = 0

        for t in all_tasks:
            if t.updated_at and t.updated_at >= cutoff:
                if t.status == TaskStatus.completed:
                    completed_count += 1
                elif t.status == TaskStatus.failed:
                    failed_count += 1
            if t.created_at and t.created_at >= cutoff:
                created_count += 1

        minutes = window_seconds / 60.0
        return ThroughputStats(
            completed_count=completed_count,
            failed_count=failed_count,
            created_count=created_count,
            window_seconds=window_seconds,
            completions_per_minute=round(completed_count / minutes, 2) if minutes > 0 else 0.0,
            failures_per_minute=round(failed_count / minutes, 2) if minutes > 0 else 0.0,
        )

    def get_node_load(self) -> Dict[str, Dict[str, Any]]:
        """Get per-node load summary."""
        all_nodes = self._store.get_all_nodes()
        all_tasks = self._store.get_all_tasks()

        node_tasks: Dict[str, List[str]] = {}
        for t in all_tasks:
            if t.assigned_to and t.status in (TaskStatus.assigned, TaskStatus.running):
                node_tasks.setdefault(t.assigned_to, []).append(t.id)

        result = {}
        for node in all_nodes:
            assigned = node_tasks.get(node.id, [])
            result[node.id] = {
                "name": node.name,
                "status": node.status.value if hasattr(node.status, "value") else node.status,
                "capabilities": node.capabilities,
                "load": node.load,
                "active_tasks": len(assigned),
                "task_ids": assigned,
            }

        return result

    def get_schedule_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recent scheduling decisions."""
        decisions = self._store.get_decisions()
        recent = decisions[-limit:] if len(decisions) > limit else decisions
        return [
            {
                "task_id": d.task_id,
                "task_title": d.task_title,
                "priority": d.priority,
                "node_id": d.node_id,
                "score": d.score,
                "reason": d.reason,
                "timestamp": d.timestamp.isoformat() if d.timestamp else "",
            }
            for d in recent
        ]

    # -------------------------------------------------------------------
    # Bulk operations
    # -------------------------------------------------------------------

    def bulk_create(
        self,
        tasks: List[Dict[str, Any]],
    ) -> List[Task]:
        """Create multiple tasks in batch.

        Each dict should have: title (required), plus optional:
        requires, priority, depends_on, assign_to.
        """
        created = []
        for task_spec in tasks:
            title = task_spec.get("title", "")
            if not title:
                continue
            task = self.create_task(
                title=title,
                requires=task_spec.get("requires"),
                priority=task_spec.get("priority", 3),
                depends_on=task_spec.get("depends_on"),
                assign_to=task_spec.get("assign_to"),
            )
            created.append(task)
        return created

    def bulk_cancel(self, task_ids: List[str]) -> int:
        """Cancel multiple tasks. Returns count cancelled."""
        count = 0
        for tid in task_ids:
            if self.cancel_task(tid):
                count += 1
        return count

    def bulk_delete(self, task_ids: List[str]) -> int:
        """Delete multiple tasks. Returns count deleted."""
        count = 0
        for tid in task_ids:
            if self.delete_task(tid):
                count += 1
        return count

    # -------------------------------------------------------------------
    # Internal helpers
    # -------------------------------------------------------------------

    def _apply_filters(self, tasks: List[Task], f: TaskFilter) -> List[Task]:
        """Apply filter criteria to a list of tasks."""
        result = tasks

        if f.status:
            result = [t for t in result
                      if (t.status.value if hasattr(t.status, "value") else t.status) == f.status]

        if f.assigned_to:
            result = [t for t in result if t.assigned_to == f.assigned_to]

        if f.requires:
            result = [t for t in result if f.requires in t.requires]

        if f.priority_min is not None:
            result = [t for t in result if t.priority >= f.priority_min]

        if f.priority_max is not None:
            result = [t for t in result if t.priority <= f.priority_max]

        if f.search:
            search_lower = f.search.lower()
            result = [t for t in result if search_lower in t.title.lower()]

        return result

    @staticmethod
    def _apply_sort(
        tasks: List[Task],
        sort_by: SortField,
        sort_order: SortOrder,
    ) -> List[Task]:
        """Sort tasks by the given field and order."""
        reverse = (sort_order == SortOrder.desc)

        def _key(t: Task):
            if sort_by == SortField.priority:
                return t.priority
            elif sort_by == SortField.created_at:
                return t.created_at or datetime.min
            elif sort_by == SortField.updated_at:
                return t.updated_at or datetime.min
            elif sort_by == SortField.status:
                return (t.status.value if hasattr(t.status, "value") else t.status)
            elif sort_by == SortField.title:
                return t.title.lower()
            return t.created_at or datetime.min

        return sorted(tasks, key=_key, reverse=reverse)

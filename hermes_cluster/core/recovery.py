"""DEPRECATED: Recovery subsystem — detector, revoker, rescheduler.

.. deprecated::
    This module is deprecated and will be removed in a future version.
    Use ``hermes_cluster.recovery`` package instead, which contains the
    same components (Revoker, Rescheduler, Detector, RecoveryManager) with
    proper thread safety and public API compliance.

    Migration guide:
        - ``from hermes_cluster.core.recovery import Revoker``
          → ``from hermes_cluster.recovery import Revoker``
        - ``from hermes_cluster.core.recovery import Rescheduler``
          → ``from hermes_cluster.recovery import Rescheduler``
        - ``from hermes_cluster.core.recovery import RecoveryDetector``
          → ``from hermes_cluster.recovery import Detector``

Python port of Go's internal/recovery/ package.
Handles node failure recovery: revoke leases → reschedule tasks → log events.

Components:
  - Revoker: revokes all leases for a failed node
  - Rescheduler: reassigns orphaned tasks to other nodes
  - Detector: orchestrates the recovery sequence
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime
from typing import Any, List, Optional

from ..models import NodeStatus, RecoveryEvent, TaskStatus

logger = logging.getLogger(__name__)


def _generate_id(prefix: str = "") -> str:
    import secrets
    if prefix:
        return f"{prefix}_{secrets.token_hex(8)}"
    return secrets.token_hex(8)


# ---------------------------------------------------------------------------
# Revoker
# ---------------------------------------------------------------------------

class Revoker:
    """Revokes all leases for a failed node and logs events."""

    def __init__(self, store: Any):
        self._store = store

    def revoke_all_for_node(self, node_id: str) -> List[str]:
        """Revoke all active leases for a node. Returns list of affected task IDs."""
        active_leases = self._store.get_active_leases()
        revoked_tasks: List[str] = []

        for lease in active_leases:
            if lease.node_id != node_id:
                continue
            self._store.revoke_lease(lease.id)
            revoked_tasks.append(lease.task_id)
            self._store.append_recovery_event(
                RecoveryEvent(
                    id=_generate_id("recovery"),
                    task_id=lease.task_id,
                    node_id=node_id,
                    action="revoke_lease",
                    status="completed",
                    message=f"Revoked lease for task {lease.task_id} on failed node {node_id}",
                )
            )

        if revoked_tasks:
            logger.info("revoked %d leases for node %s", len(revoked_tasks), node_id)

        return revoked_tasks


# ---------------------------------------------------------------------------
# Rescheduler
# ---------------------------------------------------------------------------

class Rescheduler:
    """Reassigns orphaned tasks after node failure."""

    def __init__(self, store: Any):
        self._store = store

    def reschedule_orphaned(self, task_ids: List[str]) -> int:
        """Try to reschedule tasks. Returns count successfully rescheduled."""
        rescheduled = 0

        for task_id in task_ids:
            task = self._store.get_task(task_id)
            if not task:
                continue

            # Get online nodes from the store
            all_nodes = self._store.get_all_nodes()
            online_nodes = [n for n in all_nodes if n.status == NodeStatus.online]

            assigned = False
            for node in online_nodes:
                if not task.requires or all(
                    cap in node.capabilities for cap in task.requires
                ):
                    # Assign to this node
                    self._store.set_task_status(task_id, TaskStatus.running)
                    # Update assigned_to directly on the task object
                    task.assigned_to = node.id
                    assigned = True
                    self._store.append_recovery_event(
                        RecoveryEvent(
                            id=_generate_id("recovery"),
                            task_id=task_id,
                            node_id=node.id,
                            action="reschedule",
                            status="completed",
                            message=f"Rescheduled to node {node.id}",
                        )
                    )
                    rescheduled += 1
                    break

            if not assigned:
                # No available node — mark as failed
                self._store.set_task_status(
                    task_id, TaskStatus.failed, fail_reason="no available node for reschedule"
                )
                self._store.append_recovery_event(
                    RecoveryEvent(
                        id=_generate_id("recovery"),
                        task_id=task_id,
                        action="mark_failed",
                        status="completed",
                        message="No available node for reschedule",
                    )
                )

        return rescheduled


# ---------------------------------------------------------------------------
# Detector
# ---------------------------------------------------------------------------

class RecoveryDetector:
    """Watches for offline events and triggers recovery.

    Runs as a background thread consuming offline events from a queue.
    """

    def __init__(self, revoker: Revoker, rescheduler: Rescheduler, store: Any):
        self._revoker = revoker
        self._rescheduler = rescheduler
        self._store = store
        self._event_queue: List[str] = []  # node_ids
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._running = False
        self._thread: Optional[threading.Thread] = None

    @property
    def is_running(self) -> bool:
        return self._running

    def start(self) -> None:
        """Start the detector loop."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="cluster-recovery-detector"
        )
        self._thread.start()
        logger.info("recovery detector started")

    def stop(self) -> None:
        """Stop the detector."""
        if not self._running:
            return
        self._running = False
        with self._condition:
            self._condition.notify_all()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)
        logger.info("recovery detector stopped")

    def notify_offline(self, node_id: str) -> None:
        """Send an offline event to the detector."""
        with self._condition:
            self._event_queue.append(node_id)
            self._condition.notify()

    def handle_offline_sync(self, node_id: str) -> None:
        """Handle offline event synchronously (for testing or inline use)."""
        self._recover_node(node_id)

    def _loop(self) -> None:
        while self._running:
            node_id = None
            with self._condition:
                while not self._event_queue and self._running:
                    self._condition.wait(timeout=1.0)
                if self._event_queue:
                    node_id = self._event_queue.pop(0)

            if node_id:
                self._recover_node(node_id)

    def _recover_node(self, node_id: str) -> None:
        """Execute the full recovery sequence for a failed node."""
        logger.info("recovery triggered for node %s", node_id)

        # 1. Revoke all leases for the failed node
        revoked_task_ids = self._revoker.revoke_all_for_node(node_id)

        # 2. Try to reschedule orphaned tasks
        if revoked_task_ids:
            rescheduled = self._rescheduler.reschedule_orphaned(revoked_task_ids)
            status = "completed" if rescheduled == len(revoked_task_ids) else "partial"
            self._store.append_recovery_event(
                RecoveryEvent(
                    id=_generate_id("recovery"),
                    node_id=node_id,
                    action="full_recovery",
                    status=status,
                    message=f"Revoked {len(revoked_task_ids)} leases, rescheduled {rescheduled} tasks",
                )
            )
            logger.info(
                "recovery for node %s: revoked=%d rescheduled=%d/%d",
                node_id,
                len(revoked_task_ids),
                rescheduled,
                len(revoked_task_ids),
            )

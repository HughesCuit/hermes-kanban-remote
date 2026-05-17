"""Core cluster orchestration — the Python equivalent of Go's cmdServe.

Central class that wires up all subsystems and manages their lifecycle:
  - State management (ClusterStore with SQLite or ClusterState in-memory)
  - Scheduler (task assignment with capability matching)
  - Recovery (detector → revoker → rescheduler pipeline)
  - Heartbeat watchdog (node health monitoring)
  - Background services (lease expiry scanning, federation)

Architecture mirrors Go's main.go cmdServe:
  1. Initialize state
  2. Register self-node
  3. Wire callbacks (node online → trigger pending → schedule)
  4. Start background services (watchdog, recovery, lease scanner)
  5. Expose subsystems for API layer

Usage:
    core = ClusterCore(cluster_id="my-cluster", node_id="node-1", db_path="cluster.db")
    core.start()
    # ... API handlers use core.store, core.scheduler, etc. ...
    core.stop()
"""

from __future__ import annotations

import logging
import secrets
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional

from ..models import (
    EventType,
    Hook,
    LeaseStatus,
    Node,
    NodeStatus,
    RecoveryEvent,
    SchedulingDecision,
    Task,
    TaskStatus,
)

logger = logging.getLogger(__name__)


def _generate_id(prefix: str = "") -> str:
    if prefix:
        return f"{prefix}_{secrets.token_hex(8)}"
    return secrets.token_hex(8)


# ---------------------------------------------------------------------------
# Scheduler (capability-aware task assignment)
# ---------------------------------------------------------------------------

class ClusterScheduler:
    """Assigns ready tasks to online nodes based on capability matching.

    Mirrors Go's internal/scheduler/scheduler.go.
    """

    def __init__(self, store: Any):
        self._store = store

    def trigger_pending_tasks(self) -> List[str]:
        """Promote pending tasks with all dependencies met to ready.

        Returns list of promoted task IDs.
        """
        return self._store.trigger_pending_tasks()

    def schedule_pending(self) -> int:
        """Assign ready tasks to online nodes. Returns count scheduled."""
        return self._store.schedule_pending()

    def reschedule_task(self, task_id: str) -> Optional[str]:
        """Release a task and reassign it. Returns new node_id or None."""
        task = self._store.get_task(task_id)
        if not task:
            return None

        # Revoke existing lease if any
        if task.assigned_to:
            active_leases = self._store.get_active_leases()
            for lease in active_leases:
                if lease.task_id == task_id:
                    self._store.revoke_lease(lease.id)
                    break

        # Unassign and try to reschedule
        self._store.set_task_status(task_id, TaskStatus.pending)
        self._store.schedule_pending()

        # Check if it got assigned
        task = self._store.get_task(task_id)
        if task and task.assigned_to:
            return task.assigned_to
        return None


# ---------------------------------------------------------------------------
# Workflow Resolver (dependency resolution)
# ---------------------------------------------------------------------------

class WorkflowResolver:
    """Handles task dependency resolution and trigger chains.

    Mirrors Go's internal/workflow/resolver.go.
    """

    MAX_TRIGGER_CHAIN_DEPTH = 10

    def __init__(self, store: Any):
        self._store = store

    def resolve_dependencies(self, task_id: str) -> bool:
        """Check if all dependencies of a task are met. Promote to ready if so."""
        task = self._store.get_task(task_id)
        if not task or task.status != TaskStatus.pending:
            return False

        if not task.depends_on:
            self._store.set_task_status(task_id, TaskStatus.ready)
            return True

        all_done = True
        any_failed = False
        for dep_id in task.depends_on:
            dep = self._store.get_task(dep_id)
            if not dep:
                all_done = False
                continue
            if dep.status == TaskStatus.failed:
                any_failed = True
                break
            if dep.status not in (TaskStatus.completed,):
                all_done = False

        if any_failed:
            self._store.set_task_status(
                task_id, TaskStatus.blocked, fail_reason="dependency failed"
            )
            return False

        if all_done:
            self._store.set_task_status(task_id, TaskStatus.ready)
            return True

        return False

    def on_dependency_complete(self, completed_task_id: str) -> List[str]:
        """When a task completes, check if any dependents can now be promoted."""
        return self._propagate(completed_task_id, 0)

    def on_dependency_failed(self, failed_task_id: str) -> List[str]:
        """When a task fails, block all downstream tasks."""
        blocked: List[str] = []
        all_tasks = self._store.get_all_tasks()
        for task in all_tasks:
            if task.status != TaskStatus.pending:
                continue
            if failed_task_id in task.depends_on:
                self._store.set_task_status(
                    task.id, TaskStatus.blocked,
                    fail_reason=f"dependency {failed_task_id} failed"
                )
                blocked.append(task.id)
        return blocked

    def get_dependents(self, task_id: str) -> List[str]:
        """Get all task IDs that depend on the given task."""
        return self._store.get_dependents(task_id)

    def get_trigger_chain(self, task_id: str) -> List[str]:
        """Get the chain of tasks triggered by completing the given task."""
        return self._store.get_trigger_chain(task_id, max_depth=self.MAX_TRIGGER_CHAIN_DEPTH)

    def get_dependency_graph(self) -> Dict[str, Any]:
        """Build the full dependency graph."""
        return self._store.get_workflow_graph()

    def _propagate(self, completed_task_id: str, depth: int) -> List[str]:
        if depth >= self.MAX_TRIGGER_CHAIN_DEPTH:
            return []

        transitioned: List[str] = []
        all_tasks = self._store.get_all_tasks()

        for task in all_tasks:
            if task.status != TaskStatus.pending:
                continue
            if completed_task_id not in task.depends_on:
                continue
            if self.resolve_dependencies(task.id):
                transitioned.append(task.id)

        # Also check blocked tasks that might become unblocked
        for task in all_tasks:
            if task.status != TaskStatus.blocked:
                continue
            if completed_task_id not in task.depends_on:
                continue
            if self.resolve_dependencies(task.id):
                transitioned.append(task.id)

        # Cascade
        for tid in transitioned:
            inner = self._propagate(tid, depth + 1)
            transitioned.extend(inner)

        return transitioned


# ---------------------------------------------------------------------------
# ClusterCore — main orchestration
# ---------------------------------------------------------------------------

class ClusterCore:
    """Central orchestration class for the Hermes Agent Cluster.

    Wires up all subsystems and manages their lifecycle. This is the
    Python equivalent of Go's cmdServe function.

    Subsystems:
      - store: ClusterStore (SQLite) or ClusterState (in-memory)
      - scheduler: ClusterScheduler
      - workflow: WorkflowResolver
      - recovery_detector: RecoveryDetector
      - watchdog: Watchdog
    """

    def __init__(
        self,
        cluster_id: str = "cluster_default",
        node_id: str = "node_main",
        node_name: str = "main-node",
        node_role: str = "main",
        capabilities: Optional[List[str]] = None,
        db_path: str = ":memory:",
        config_path: str = "",
        # Watchdog timing
        watchdog_check_interval: float = 5.0,
        watchdog_degraded_after: float = 15.0,
        watchdog_offline_after: float = 30.0,
        # Lease timing
        lease_ttl_seconds: int = 60,
        lease_scan_rate_seconds: float = 10.0,
    ):
        self.cluster_id = cluster_id
        self.node_id = node_id
        self.node_name = node_name
        self.node_role = node_role
        self.capabilities = capabilities or ["planning", "reviewing", "scheduling"]
        self.config_path = config_path
        self.started_at = datetime.utcnow()

        # --- Initialize state ---
        # Try to import ClusterStore (SQLite) first, fall back to ClusterState
        try:
            from ..state.cluster_store import ClusterStore
            self.store = ClusterStore(db_path=db_path)
            logger.info("initialized ClusterStore with db_path=%s", db_path)
        except ImportError:
            from ..state import ClusterState
            self.store = ClusterState()
            logger.info("initialized in-memory ClusterState")

        self.store.cluster_id = cluster_id
        self.store.node_id = node_id
        self.store.node_role = node_role
        if config_path:
            self.store.set_config_path(config_path)

        # --- Initialize subsystems ---
        self.scheduler = ClusterScheduler(self.store)
        self.workflow = WorkflowResolver(self.store)

        # Recovery subsystem
        from .recovery import Revoker, Rescheduler, RecoveryDetector
        self._revoker = Revoker(self.store)
        self._rescheduler = Rescheduler(self.store)
        self.recovery_detector = RecoveryDetector(
            self._revoker, self._rescheduler, self.store
        )

        # Watchdog
        from .watchdog import Watchdog, WatchdogRegistry, HeartbeatNode

        class _WatchdogAdapter(WatchdogRegistry):
            """Adapts ClusterStore/ClusterState to WatchdogRegistry interface."""

            def __init__(self, store: Any):
                self._store = store

            def get_all_heartbeat_nodes(self) -> List[HeartbeatNode]:
                nodes = self._store.get_all_nodes()
                return [
                    HeartbeatNode(
                        node_id=n.id,
                        last_heartbeat=n.last_heartbeat,
                        status=n.status.value if isinstance(n.status, NodeStatus) else n.status,
                    )
                    for n in nodes
                ]

            def update_node_status(self, node_id: str, status: str) -> None:
                # Access internal dict directly to avoid deadlock with non-reentrant Lock.
                # get_node() acquires _nodes_lock, so we must not hold it here.
                if hasattr(self._store, '_nodes'):
                    with self._store._nodes_lock:
                        if node_id in self._store._nodes:
                            self._store._nodes[node_id].status = NodeStatus(status)
                elif hasattr(self._store, '_conn'):
                    # ClusterStore: use direct SQL update
                    from datetime import datetime
                    with self._store._lock:
                        self._store._conn.execute(
                            "UPDATE nodes SET status = ? WHERE id = ?",
                            (status, node_id),
                        )

        self._watchdog_adapter = _WatchdogAdapter(self.store)

        def _watchdog_callback(evt: Any) -> None:
            """Handle watchdog events — trigger recovery on offline."""
            logger.info("watchdog event: node=%s status=%s", evt.node_id, evt.event_type)
            if evt.event_type == "offline":
                self.recovery_detector.notify_offline(evt.node_id)

        self.watchdog = Watchdog(
            registry=self._watchdog_adapter,
            check_interval=watchdog_check_interval,
            degraded_after=watchdog_degraded_after,
            offline_after=watchdog_offline_after,
            callback=_watchdog_callback,
        )

        # Lease timing
        self._lease_ttl = timedelta(seconds=lease_ttl_seconds)
        self._lease_scan_rate = lease_scan_rate_seconds

        # Background services
        self._stop_event = threading.Event()
        self._lease_scanner_thread: Optional[threading.Thread] = None

        # --- Wire callbacks ---
        self._wire_callbacks()

        # Register self-node
        self._register_self()

        logger.info(
            "ClusterCore initialized: cluster=%s node=%s role=%s capabilities=%s",
            cluster_id, node_id, node_role, self.capabilities,
        )

    def _wire_callbacks(self) -> None:
        """Wire up event callbacks between subsystems.

        Mirrors Go's callback wiring in cmdServe:
          - node online → trigger pending → schedule
          - capability change → trigger pending → schedule
          - lease expiry → mark node offline → recovery
        """

        def _on_node_online(node_id: str) -> None:
            """When a node comes online, promote pending tasks and schedule."""
            promoted = self.scheduler.trigger_pending_tasks()
            if promoted:
                logger.info("node_online trigger: promoted %d tasks for node %s", len(promoted), node_id)
                self.scheduler.schedule_pending()

        def _on_capability_change(node_id: str, old_caps: List[str], new_caps: List[str]) -> None:
            """When capabilities change, re-evaluate pending tasks."""
            logger.info("capability change: node=%s old=%s new=%s", node_id, old_caps, new_caps)
            promoted = self.scheduler.trigger_pending_tasks()
            if promoted:
                logger.info("capability-change trigger: promoted %d tasks for node %s", len(promoted), node_id)
                self.scheduler.schedule_pending()

        self.store.set_on_node_online(_on_node_online)
        self.store.set_on_capability_change(_on_capability_change)

        def _on_lease_expiry(task_id: str, node_id: str) -> None:
            """When a lease expires, trigger recovery."""
            logger.info("lease expired: task=%s node=%s", task_id, node_id)
            # Mark node offline and trigger recovery
            self.recovery_detector.notify_offline(node_id)

        self.store.set_lease_callback(_on_lease_expiry)

    def _register_self(self) -> None:
        """Register this node in the cluster."""
        self.store.register_node(
            Node(
                id=self.node_id,
                name=self.node_name,
                capabilities=self.capabilities,
                status=NodeStatus.online,
            )
        )
        logger.info("registered node: %s capabilities=%s", self.node_id, self.capabilities)

    # -------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------

    def start(self) -> None:
        """Start all background services."""
        if self._stop_event.is_set():
            self._stop_event.clear()

        # Start watchdog
        self.watchdog.start()

        # Start recovery detector
        self.recovery_detector.start()

        # Start lease expiry scanner
        self._lease_scanner_thread = threading.Thread(
            target=self._lease_scanner_loop, daemon=True, name="cluster-lease-scanner"
        )
        self._lease_scanner_thread.start()

        logger.info("ClusterCore started — all background services running")

    def stop(self) -> None:
        """Stop all background services gracefully."""
        logger.info("ClusterCore stopping...")
        self._stop_event.set()

        self.watchdog.stop()
        self.recovery_detector.stop()

        if self._lease_scanner_thread and self._lease_scanner_thread.is_alive():
            self._lease_scanner_thread.join(timeout=3.0)

        logger.info("ClusterCore stopped")

    # -------------------------------------------------------------------
    # Background services
    # -------------------------------------------------------------------

    def _lease_scanner_loop(self) -> None:
        """Periodically scan for expired leases and trigger callbacks."""
        while not self._stop_event.is_set():
            try:
                active = self.store.get_active_leases()
                # get_active_leases already handles expiry + callbacks
            except Exception:
                logger.exception("lease scanner error")
            self._stop_event.wait(timeout=self._lease_scan_rate)

    # -------------------------------------------------------------------
    # Convenience API methods
    # -------------------------------------------------------------------

    def submit_task(
        self, title: str, requires: Optional[List[str]] = None, priority: int = 3
    ) -> Task:
        """Submit a task to the cluster."""
        task_id = _generate_id("task")
        task = self.store.create_task(
            task_id, title, requires or [], priority
        )
        self.scheduler.trigger_pending_tasks()
        return task

    def complete_task(self, task_id: str) -> bool:
        """Mark a task as completed and trigger downstream tasks."""
        task = self.store.get_task(task_id)
        if not task:
            return False
        self.store.set_task_status(task_id, TaskStatus.completed)
        # Propagate dependency resolution
        self.workflow.on_dependency_complete(task_id)
        # Schedule any newly ready tasks
        self.scheduler.schedule_pending()
        return True

    def fail_task(self, task_id: str, reason: str = "failed") -> bool:
        """Mark a task as failed and block downstream tasks."""
        task = self.store.get_task(task_id)
        if not task:
            return False
        self.store.set_task_status(task_id, TaskStatus.failed, fail_reason=reason)
        self.workflow.on_dependency_failed(task_id)
        return True

    def get_summary(self) -> Dict[str, Any]:
        """Get cluster status summary."""
        summary = self.store.get_summary()
        summary["subsystems"] = {
            "watchdog_running": self.watchdog.is_running,
            "recovery_detector_running": self.recovery_detector._running,
            "lease_scanner_running": (
                self._lease_scanner_thread is not None
                and self._lease_scanner_thread.is_alive()
            ),
        }
        return summary

    def get_dependency_graph(self) -> Dict[str, Any]:
        """Get the full dependency graph."""
        return self.workflow.get_dependency_graph()

    def get_trigger_chain(self, task_id: str) -> List[str]:
        """Get the trigger chain for a task."""
        return self.workflow.get_trigger_chain(task_id)

    def get_recovery_events(self) -> List[RecoveryEvent]:
        """Get all recovery events."""
        return self.store.get_recovery_events()

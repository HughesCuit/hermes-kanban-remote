"""In-memory state management — replaces Go's concurrent maps + mutexes.

Thread-safe via threading.Lock (GIL helps, but we're explicit).
"""

from __future__ import annotations

import hashlib
import secrets
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple

from ..models import (
    BatchSyncMessage,
    Delivery,
    Delivery,
    Hook,
    Lease,
    LeaseStatus,
    Node,
    NodeStatus,
    RecoveryEvent,
    RemoteCluster,
    SchedulingDecision,
    SchedulingStats,
    SyncMessage,
    SyncEventType,
    Task,
    TaskStatus,
    EventType,
    FederationClusterStatus,
)


def _generate_id(prefix: str = "") -> str:
    """Generate a random hex ID, similar to Go's generateID."""
    if prefix:
        return f"{prefix}_{secrets.token_hex(8)}"
    return secrets.token_hex(8)


class ClusterState:
    """Central in-memory state for the cluster.

    Each sub-store is protected by its own lock for fine-grained concurrency.
    """

    def __init__(self):
        import threading

        # Node registry
        self._nodes_lock = threading.Lock()
        self._nodes: Dict[str, Node] = {}
        self._on_node_online: Optional[Callable[[str], None]] = None
        self._on_capability_change: Optional[Callable[[str, List[str], List[str]], None]] = None

        # Task store
        self._tasks_lock = threading.Lock()
        self._tasks: Dict[str, Task] = {}

        # Lease manager
        self._leases_lock = threading.Lock()
        self._leases: Dict[str, Lease] = {}
        self._task_lease_index: Dict[str, str] = {}  # task_id -> lease_id
        self._lease_callback: Optional[Callable[[str, str], None]] = None

        # Sync state
        self._sync_lock = threading.Lock()
        self._sync_version: int = 0

        # Recovery log
        self._recovery_lock = threading.Lock()
        self._recovery_events: List[RecoveryEvent] = []

        # Scheduling decisions
        self._schedule_lock = threading.Lock()
        self._decisions: List[SchedulingDecision] = []
        self._max_decisions: int = 200

        # Federation registry
        self._federation_lock = threading.Lock()
        self._clusters: Dict[str, RemoteCluster] = {}

        # Hook manager
        self._hooks_lock = threading.Lock()
        self._hooks: Dict[str, Hook] = {}
        self._deliveries: List[Delivery] = []
        self._max_deliveries: int = 1000

        # Config
        self._config_lock = threading.Lock()
        self._config: Optional[Dict[str, Any]] = None
        self._config_path: str = ""

        # Server info
        self.started_at: datetime = datetime.utcnow()
        self.cluster_id: str = "cluster_default"
        self.node_id: str = "node_main"
        self.node_role: str = "main"

    # -----------------------------------------------------------------------
    # Node registry
    # -----------------------------------------------------------------------

    def register_node(self, node: Node) -> None:
        with self._nodes_lock:
            self._nodes[node.id] = node
        if self._on_node_online:
            try:
                self._on_node_online(node.id)
            except Exception:
                pass

    def get_node(self, node_id: str) -> Optional[Node]:
        with self._nodes_lock:
            return self._nodes.get(node_id)

    def get_all_nodes(self) -> List[Node]:
        with self._nodes_lock:
            return list(self._nodes.values())

    def update_heartbeat(self, node_id: str) -> None:
        with self._nodes_lock:
            if node_id in self._nodes:
                self._nodes[node_id].last_heartbeat = datetime.utcnow()
                self._nodes[node_id].status = NodeStatus.online

    def update_capabilities(self, node_id: str, caps: List[str]) -> None:
        with self._nodes_lock:
            if node_id in self._nodes:
                old_caps = self._nodes[node_id].capabilities
                self._nodes[node_id].capabilities = caps
                if self._on_capability_change:
                    try:
                        self._on_capability_change(node_id, old_caps, caps)
                    except Exception:
                        pass

    def node_count(self) -> int:
        with self._nodes_lock:
            return len(self._nodes)

    def online_count(self) -> int:
        with self._nodes_lock:
            return sum(1 for n in self._nodes.values() if n.status == NodeStatus.online)

    def set_on_node_online(self, fn: Callable[[str], None]) -> None:
        self._on_node_online = fn

    def set_on_capability_change(self, fn: Callable[[str, List[str], List[str]], None]) -> None:
        self._on_capability_change = fn

    # -----------------------------------------------------------------------
    # Task store
    # -----------------------------------------------------------------------

    def create_task(self, task_id: str, title: str, requires: List[str], priority: int = 3) -> Task:
        now = datetime.utcnow()
        task = Task(
            id=task_id,
            title=title,
            requires=requires,
            priority=priority,
            status=TaskStatus.pending,
            created_at=now,
            updated_at=now,
            version=1,
        )
        with self._tasks_lock:
            self._tasks[task_id] = task
        return task

    def get_task(self, task_id: str) -> Optional[Task]:
        with self._tasks_lock:
            return self._tasks.get(task_id)

    def get_all_tasks(self) -> List[Task]:
        with self._tasks_lock:
            return list(self._tasks.values())

    def set_task_status(self, task_id: str, status: TaskStatus, fail_reason: str = "") -> bool:
        with self._tasks_lock:
            if task_id not in self._tasks:
                return False
            task = self._tasks[task_id]
            task.status = status
            task.updated_at = datetime.utcnow()
            task.version += 1
            if fail_reason:
                task.fail_reason = fail_reason
            return True

    def unblock_task(self, task_id: str) -> bool:
        with self._tasks_lock:
            if task_id not in self._tasks:
                return False
            task = self._tasks[task_id]
            if task.status == TaskStatus.blocked:
                task.status = TaskStatus.pending
                task.updated_at = datetime.utcnow()
                task.version += 1
                return True
            return False

    def set_dependencies(self, task_id: str, depends_on: List[str]) -> bool:
        with self._tasks_lock:
            if task_id not in self._tasks:
                return False
            self._tasks[task_id].depends_on = depends_on
            self._tasks[task_id].updated_at = datetime.utcnow()
            self._tasks[task_id].version += 1
            return True

    def get_dependents(self, task_id: str) -> List[str]:
        """Get all task IDs that depend on the given task."""
        with self._tasks_lock:
            return [
                tid for tid, t in self._tasks.items()
                if task_id in t.depends_on
            ]

    def get_trigger_chain(self, task_id: str, max_depth: int = 10) -> List[str]:
        """Get the chain of tasks triggered by completing the given task."""
        chain: List[str] = []
        visited: set = set()

        def _traverse(tid: str, depth: int):
            if depth >= max_depth or tid in visited:
                return
            visited.add(tid)
            dependents = self.get_dependents(tid)
            for dep_id in dependents:
                chain.append(dep_id)
                _traverse(dep_id, depth + 1)

        _traverse(task_id, 0)
        return chain

    def get_workflow_graph(self) -> Dict[str, Any]:
        """Build a dependency graph from all tasks."""
        with self._tasks_lock:
            nodes = []
            edges = []
            for tid, task in self._tasks.items():
                nodes.append({
                    "id": tid,
                    "title": task.title,
                    "status": task.status.value if isinstance(task.status, TaskStatus) else task.status,
                    "priority": task.priority,
                })
                for dep_id in task.depends_on:
                    edges.append({"from": dep_id, "to": tid})
            return {"nodes": nodes, "edges": edges}

    def task_counts(self) -> Dict[str, int]:
        """Count tasks by status."""
        counts = {
            "total": 0, "ready": 0, "running": 0,
            "completed": 0, "failed": 0, "pending": 0,
        }
        with self._tasks_lock:
            for task in self._tasks.values():
                counts["total"] += 1
                status_val = task.status.value if isinstance(task.status, TaskStatus) else task.status
                if status_val in counts:
                    counts[status_val] += 1
        return counts

    # -----------------------------------------------------------------------
    # Lease manager
    # -----------------------------------------------------------------------

    def create_lease(self, task_id: str, node_id: str, ttl: timedelta) -> Optional[Lease]:
        lease_id = _generate_id("lease")
        now = datetime.utcnow()
        lease = Lease(
            id=lease_id,
            task_id=task_id,
            node_id=node_id,
            created_at=now,
            expires_at=now + ttl,
            status=LeaseStatus.active,
        )
        with self._leases_lock:
            self._leases[lease_id] = lease
            self._task_lease_index[task_id] = lease_id
        return lease

    def revoke_lease(self, lease_id: str) -> bool:
        with self._leases_lock:
            if lease_id not in self._leases:
                return False
            lease = self._leases[lease_id]
            lease.status = LeaseStatus.revoked
            # Remove from task index
            if lease.task_id in self._task_lease_index:
                del self._task_lease_index[lease.task_id]
            return True

    def get_active_leases(self) -> List[Lease]:
        now = datetime.utcnow()
        with self._leases_lock:
            active = []
            for lease in self._leases.values():
                if lease.status == LeaseStatus.active and lease.expires_at > now:
                    active.append(lease)
                elif lease.status == LeaseStatus.active and lease.expires_at <= now:
                    lease.status = LeaseStatus.expired
                    # Trigger expiry callback
                    if self._lease_callback:
                        try:
                            self._lease_callback(lease.task_id, lease.node_id)
                        except Exception:
                            pass
            return active

    def set_lease_callback(self, fn: Callable[[str, str], None]) -> None:
        self._lease_callback = fn

    # -----------------------------------------------------------------------
    # Sync state
    # -----------------------------------------------------------------------

    def handle_sync_message(self, msg: SyncMessage) -> bool:
        """Apply a sync message. Returns True if applied."""
        with self._sync_lock:
            if msg.version <= self._sync_version:
                return False
            self._sync_version = msg.version

        # Apply task state if present
        if msg.task_state:
            with self._tasks_lock:
                task_id = msg.task_state.task_id
                if task_id in self._tasks:
                    task = self._tasks[task_id]
                    task.status = TaskStatus(msg.task_state.status) if msg.task_state.status in TaskStatus.__members__.values() else task.status
                    if msg.task_state.assigned_to:
                        task.assigned_to = msg.task_state.assigned_to
                    task.version = msg.task_state.version
                    task.updated_at = datetime.utcnow()
                else:
                    # Create new task from sync
                    self._tasks[task_id] = Task(
                        id=task_id,
                        title=msg.task_state.title,
                        status=TaskStatus(msg.task_state.status) if msg.task_state.status in TaskStatus.__members__.values() else TaskStatus.pending,
                        assigned_to=msg.task_state.assigned_to,
                        version=msg.task_state.version,
                    )
        return True

    def handle_batch_sync(self, batch: BatchSyncMessage) -> int:
        """Apply a batch of sync messages. Returns count applied."""
        count = 0
        for msg in batch.messages:
            if self.handle_sync_message(msg):
                count += 1
        return count

    def sync_version(self) -> int:
        with self._sync_lock:
            return self._sync_version

    # -----------------------------------------------------------------------
    # Recovery log
    # -----------------------------------------------------------------------

    def append_recovery_event(self, event: RecoveryEvent) -> None:
        with self._recovery_lock:
            if not event.id:
                event.id = _generate_id("recovery")
            event.timestamp = datetime.utcnow()
            self._recovery_events.append(event)

    def get_recovery_events(self) -> List[RecoveryEvent]:
        with self._recovery_lock:
            return list(self._recovery_events)

    def recovery_stats(self) -> Dict[str, Any]:
        with self._recovery_lock:
            total = len(self._recovery_events)
            by_action: Dict[str, int] = {}
            for e in self._recovery_events:
                by_action[e.action] = by_action.get(e.action, 0) + 1
            return {"total": total, "by_action": by_action}

    def trigger_recovery(self, node_id: str) -> None:
        """Notify that a node went offline — trigger recovery."""
        event = RecoveryEvent(
            id=_generate_id("recovery"),
            node_id=node_id,
            action="reschedule",
            status="completed",
            message=f"Node {node_id} went offline, rescheduling tasks",
        )
        self.append_recovery_event(event)

    # -----------------------------------------------------------------------
    # Scheduling decisions
    # -----------------------------------------------------------------------

    def record_decision(self, decision: SchedulingDecision) -> None:
        with self._schedule_lock:
            self._decisions.append(decision)
            if len(self._decisions) > self._max_decisions:
                self._decisions = self._decisions[-self._max_decisions:]

    def get_decisions(self) -> List[SchedulingDecision]:
        with self._schedule_lock:
            return list(self._decisions)

    def get_schedule_stats(self) -> SchedulingStats:
        with self._schedule_lock:
            total = len(self._decisions)
            by_priority: Dict[int, int] = {}
            for d in self._decisions:
                by_priority[d.priority] = by_priority.get(d.priority, 0) + 1
            return SchedulingStats(
                total_decisions=total,
                decisions_by_priority=by_priority,
                last_decisions=self._decisions[-10:] if self._decisions else [],
            )

    def trigger_pending_tasks(self) -> int:
        """Promote pending tasks with all dependencies met to ready. Returns count promoted."""
        promoted = 0
        with self._tasks_lock:
            for task in self._tasks.values():
                if task.status != TaskStatus.pending:
                    continue
                if not task.depends_on:
                    task.status = TaskStatus.ready
                    task.updated_at = datetime.utcnow()
                    promoted += 1
                else:
                    all_done = all(
                        self._tasks.get(dep_id) is not None
                        and self._tasks[dep_id].status == TaskStatus.completed
                        for dep_id in task.depends_on
                    )
                    if all_done:
                        task.status = TaskStatus.ready
                        task.updated_at = datetime.utcnow()
                        promoted += 1
        return promoted

    def schedule_pending(self) -> int:
        """Try to assign ready tasks to available nodes. Returns count scheduled."""
        scheduled = 0
        # Get available nodes
        with self._nodes_lock:
            online_nodes = [n for n in self._nodes.values() if n.status == NodeStatus.online]

        if not online_nodes:
            return 0

        # Sort ready tasks by priority (1=highest first), then by creation time
        with self._tasks_lock:
            ready_tasks = sorted(
                [t for t in self._tasks.values() if t.status == TaskStatus.ready],
                key=lambda t: (t.priority, t.created_at),
            )
            for task in ready_tasks:
                # Simple round-robin: assign to first available node with matching capabilities
                for node in online_nodes:
                    if not task.requires or all(cap in node.capabilities for cap in task.requires):
                        task.status = TaskStatus.running
                        task.assigned_to = node.id
                        task.updated_at = datetime.utcnow()
                        task.version += 1
                        # Record decision
                        decision = SchedulingDecision(
                            task_id=task.id,
                            task_title=task.title,
                            priority=task.priority,
                            node_id=node.id,
                            score=1.0,
                            reason="capability_match",
                        )
                        self.record_decision(decision)
                        scheduled += 1
                        break
        return scheduled

    # -----------------------------------------------------------------------
    # Federation registry
    # -----------------------------------------------------------------------

    def register_federation_cluster(self, cluster_id: str, name: str, endpoint: str) -> RemoteCluster:
        now = datetime.utcnow()
        cluster = RemoteCluster(
            id=cluster_id,
            name=name,
            endpoint=endpoint,
            status=FederationClusterStatus.available,
            registered_at=now,
            last_ping=now,
        )
        with self._federation_lock:
            self._clusters[cluster_id] = cluster
        return cluster

    def remove_federation_cluster(self, cluster_id: str) -> bool:
        with self._federation_lock:
            if cluster_id in self._clusters:
                del self._clusters[cluster_id]
                return True
            return False

    def get_federation_clusters(self) -> List[RemoteCluster]:
        with self._federation_lock:
            return list(self._clusters.values())

    def get_federation_cluster(self, cluster_id: str) -> Optional[RemoteCluster]:
        with self._federation_lock:
            return self._clusters.get(cluster_id)

    # -----------------------------------------------------------------------
    # Hook manager
    # -----------------------------------------------------------------------

    def register_hook(self, url: str, events: List[EventType], secret: str = "") -> Hook:
        hook_id = _generate_id("hook")
        now = datetime.utcnow()
        hook = Hook(
            id=hook_id,
            url=url,
            events=events,
            secret=secret or None,
            active=True,
            created_at=now,
            updated_at=now,
        )
        with self._hooks_lock:
            self._hooks[hook_id] = hook
        return hook

    def deregister_hook(self, hook_id: str) -> bool:
        with self._hooks_lock:
            if hook_id in self._hooks:
                del self._hooks[hook_id]
                return True
            return False

    def list_hooks(self) -> List[Hook]:
        with self._hooks_lock:
            # Return hooks without secrets
            return [h.model_copy(update={"secret": None}) for h in self._hooks.values()]

    def get_hook_deliveries(self, hook_id: str) -> List[Delivery]:
        with self._hooks_lock:
            return [d for d in self._deliveries if d.hook_id == hook_id]

    def add_delivery(self, delivery: Delivery) -> None:
        with self._hooks_lock:
            self._deliveries.append(delivery)
            if len(self._deliveries) > self._max_deliveries:
                self._deliveries = self._deliveries[-self._max_deliveries:]

    # -----------------------------------------------------------------------
    # Config
    # -----------------------------------------------------------------------

    def get_config(self) -> Optional[Dict[str, Any]]:
        with self._config_lock:
            return self._config

    def set_config(self, config: Dict[str, Any]) -> None:
        with self._config_lock:
            self._config = config

    def get_config_path(self) -> str:
        return self._config_path

    def set_config_path(self, path: str) -> None:
        self._config_path = path

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------

    def get_summary(self) -> Dict[str, Any]:
        task_counts = self.task_counts()
        return {
            "cluster_id": self.cluster_id,
            "node_id": self.node_id,
            "role": self.node_role,
            "nodes": {"total": self.node_count(), "online": self.online_count()},
            "tasks": task_counts,
            "leases": {"active": len(self.get_active_leases())},
            "sync_version": self.sync_version(),
            "uptime_seconds": int((datetime.utcnow() - self.started_at).total_seconds()),
        }

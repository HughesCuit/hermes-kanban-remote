"""Persistent SQLite storage layer — drop-in replacement for in-memory ClusterState.

Uses WAL mode for concurrent reads. Thread-safe via sqlite3's serialized
access and explicit threading.Lock for compound operations.

Schema mirrors the in-memory ClusterState but persists to a single .db file.
"""

from __future__ import annotations

import json
import secrets
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from ..models import (
    BatchSyncMessage,
    Delivery,
    EventType,
    FederationClusterStatus,
    Hook,
    Lease,
    LeaseStatus,
    Node,
    NodeStatus,
    RecoveryEvent,
    RemoteCluster,
    SchedulingDecision,
    SchedulingStats,
    SyncEventType,
    SyncMessage,
    Task,
    TaskStatus,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _generate_id(prefix: str = "") -> str:
    if prefix:
        return f"{prefix}_{secrets.token_hex(8)}"
    return secrets.token_hex(8)


def _dt_to_str(dt: datetime) -> str:
    """ISO format string for SQLite storage."""
    if dt is None:
        return ""
    return dt.isoformat()


def _str_to_dt(s: str) -> datetime:
    """Parse ISO string back to datetime."""
    if not s:
        return datetime.utcnow()
    try:
        return datetime.fromisoformat(s)
    except (ValueError, TypeError):
        return datetime.utcnow()


def _json_dumps(obj: Any) -> str:
    """Serialize to JSON string for SQLite TEXT columns."""
    if obj is None:
        return "[]"
    return json.dumps(obj, default=str)


def _json_loads(s: str) -> Any:
    """Deserialize JSON string."""
    if not s:
        return []
    try:
        return json.loads(s)
    except (json.JSONDecodeError, TypeError):
        return []


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS nodes (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    capabilities TEXT DEFAULT '[]',
    status TEXT DEFAULT 'online',
    last_heartbeat TEXT,
    load REAL DEFAULT 0.0
);

CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    requires TEXT DEFAULT '[]',
    depends_on TEXT DEFAULT '[]',
    priority INTEGER DEFAULT 3,
    status TEXT DEFAULT 'pending',
    assigned_to TEXT,
    created_at TEXT,
    updated_at TEXT,
    version INTEGER DEFAULT 0,
    fail_reason TEXT
);

CREATE TABLE IF NOT EXISTS leases (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    created_at TEXT,
    expires_at TEXT,
    status TEXT DEFAULT 'active'
);

CREATE TABLE IF NOT EXISTS sync_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version INTEGER NOT NULL,
    sender_node TEXT DEFAULT '',
    task_state TEXT,
    event_type TEXT DEFAULT 'task_created',
    timestamp INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS recovery_events (
    id TEXT PRIMARY KEY,
    task_id TEXT DEFAULT '',
    node_id TEXT DEFAULT '',
    action TEXT DEFAULT '',
    status TEXT DEFAULT '',
    message TEXT,
    timestamp TEXT
);

CREATE TABLE IF NOT EXISTS scheduling_decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    task_title TEXT NOT NULL,
    priority INTEGER DEFAULT 3,
    node_id TEXT NOT NULL,
    score REAL DEFAULT 0.0,
    reason TEXT DEFAULT '',
    timestamp TEXT
);

CREATE TABLE IF NOT EXISTS federation_clusters (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    status TEXT DEFAULT 'available',
    registered_at TEXT,
    last_ping TEXT,
    ping_latency REAL DEFAULT 0.0
);

CREATE TABLE IF NOT EXISTS hooks (
    id TEXT PRIMARY KEY,
    url TEXT NOT NULL,
    events TEXT DEFAULT '[]',
    secret TEXT,
    active INTEGER DEFAULT 1,
    created_at TEXT,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS deliveries (
    id TEXT PRIMARY KEY,
    hook_id TEXT NOT NULL,
    event_type TEXT DEFAULT '',
    payload TEXT DEFAULT '{}',
    status TEXT DEFAULT 'delivered',
    created_at TEXT
);

CREATE TABLE IF NOT EXISTS kv_store (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_priority ON tasks(priority);
CREATE INDEX IF NOT EXISTS idx_tasks_assigned ON tasks(assigned_to);
CREATE INDEX IF NOT EXISTS idx_leases_task ON leases(task_id);
CREATE INDEX IF NOT EXISTS idx_leases_status ON leases(status);
CREATE INDEX IF NOT EXISTS idx_leases_expires ON leases(expires_at);
CREATE INDEX IF NOT EXISTS idx_deliveries_hook ON deliveries(hook_id);
CREATE INDEX IF NOT EXISTS idx_recovery_node ON recovery_events(node_id);
"""


# ---------------------------------------------------------------------------
# ClusterStore
# ---------------------------------------------------------------------------

class ClusterStore:
    """Persistent SQLite-backed store for cluster state.

    Drop-in replacement for ClusterState with the same public API.
    All writes go through a lock; reads use the WAL-mode connection
    which allows concurrent readers.
    """

    def __init__(self, db_path: str = ":memory:"):
        """Initialize the store.

        Args:
            db_path: Path to SQLite database file. Use ":memory:" for
                     in-memory (useful for tests).
        """
        self._db_path = db_path
        self._lock = threading.RLock()
        self._conn: Optional[sqlite3.Connection] = None

        # Callbacks (same as ClusterState)
        self._on_node_online: Optional[Callable[[str], None]] = None
        self._on_capability_change: Optional[Callable[[str, List[str], List[str]], None]] = None
        self._lease_callback: Optional[Callable[[str, str], None]] = None

        # Config cache (loaded from kv_store)
        self._config: Optional[Dict[str, Any]] = None
        self._config_path: str = ""

        # Server info
        self.started_at: datetime = datetime.utcnow()
        self.cluster_id: str = "cluster_default"
        self.node_id: str = "node_main"
        self.node_role: str = "main"

        # Max limits (matching ClusterState)
        self._max_decisions: int = 200
        self._max_deliveries: int = 1000

        self._init_db()

    def _init_db(self) -> None:
        """Initialize database connection and schema."""
        self._conn = sqlite3.connect(
            self._db_path,
            check_same_thread=False,
            timeout=10.0,
        )
        self._conn.row_factory = sqlite3.Row
        # Enable WAL for concurrent reads
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(_SCHEMA_SQL)
        self._conn.commit()

    @contextmanager
    def _tx(self):
        """Transaction context manager — wraps operations in BEGIN/COMMIT."""
        with self._lock:
            try:
                yield self._conn
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def close(self) -> None:
        """Close the database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    # -------------------------------------------------------------------
    # Node registry
    # -------------------------------------------------------------------

    def register_node(self, node: Node) -> None:
        now = datetime.utcnow()
        with self._tx() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO nodes (id, name, capabilities, status, last_heartbeat, load)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (node.id, node.name, _json_dumps(node.capabilities),
                 node.status.value, _dt_to_str(node.last_heartbeat), node.load),
            )
        if self._on_node_online:
            try:
                self._on_node_online(node.id)
            except Exception:
                pass

    def get_node(self, node_id: str) -> Optional[Node]:
        with self._lock:
            row = self._conn.execute("SELECT * FROM nodes WHERE id = ?", (node_id,)).fetchone()
        if row is None:
            return None
        return self._row_to_node(row)

    def get_all_nodes(self) -> List[Node]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM nodes").fetchall()
        return [self._row_to_node(r) for r in rows]

    def update_heartbeat(self, node_id: str) -> None:
        now = datetime.utcnow()
        with self._tx() as conn:
            conn.execute(
                "UPDATE nodes SET last_heartbeat = ?, status = ? WHERE id = ?",
                (_dt_to_str(now), NodeStatus.online.value, node_id),
            )

    def update_capabilities(self, node_id: str, caps: List[str]) -> None:
        with self._lock:
            row = self._conn.execute(
                "SELECT capabilities FROM nodes WHERE id = ?", (node_id,)
            ).fetchone()
            old_caps = _json_loads(row["capabilities"]) if row else []

        with self._tx() as conn:
            conn.execute(
                "UPDATE nodes SET capabilities = ? WHERE id = ?",
                (_json_dumps(caps), node_id),
            )

        if self._on_capability_change:
            try:
                self._on_capability_change(node_id, old_caps, caps)
            except Exception:
                pass

    def node_count(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT COUNT(*) as c FROM nodes").fetchone()
            return row["c"]

    def online_count(self) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) as c FROM nodes WHERE status = ?",
                (NodeStatus.online.value,),
            ).fetchone()
            return row["c"]

    def set_on_node_online(self, fn: Callable[[str], None]) -> None:
        self._on_node_online = fn

    def set_on_capability_change(self, fn: Callable[[str, List[str], List[str]], None]) -> None:
        self._on_capability_change = fn

    def _row_to_node(self, row: sqlite3.Row) -> Node:
        return Node(
            id=row["id"],
            name=row["name"],
            capabilities=_json_loads(row["capabilities"]),
            status=NodeStatus(row["status"]),
            last_heartbeat=_str_to_dt(row["last_heartbeat"]),
            load=row["load"],
        )

    # -------------------------------------------------------------------
    # Task store
    # -------------------------------------------------------------------

    def create_task(
        self, task_id: str, title: str, requires: List[str], priority: int = 3
    ) -> Task:
        now = datetime.utcnow()
        with self._tx() as conn:
            conn.execute(
                """INSERT OR IGNORE INTO tasks
                   (id, title, requires, depends_on, priority, status, created_at, updated_at, version)
                   VALUES (?, ?, ?, '[]', ?, ?, ?, ?, 1)""",
                (task_id, title, _json_dumps(requires), priority,
                 TaskStatus.pending.value, _dt_to_str(now), _dt_to_str(now)),
            )
        # Promote to ready immediately if no dependencies (matching ClusterState behavior)
        # The original in-memory store returns by reference so the caller
        # sees the promotion after trigger_pending_tasks(). SQLite returns
        # a copy, so we must promote here for API compatibility.
        # create_task always creates with empty depends_on, so promote immediately
        with self._tx() as conn:
            conn.execute(
                """UPDATE tasks SET status = ?, updated_at = ?
                   WHERE id = ? AND status = ?""",
                (TaskStatus.ready.value, _dt_to_str(datetime.utcnow()),
                 task_id, TaskStatus.pending.value),
            )
        return self.get_task(task_id) or Task(
            id=task_id, title=title, requires=requires, priority=priority,
            status=TaskStatus.pending, created_at=now, updated_at=now, version=1,
        )

    def get_task(self, task_id: str) -> Optional[Task]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
        if row is None:
            return None
        return self._row_to_task(row)

    def get_all_tasks(self) -> List[Task]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM tasks").fetchall()
        return [self._row_to_task(r) for r in rows]

    def set_task_status(
        self, task_id: str, status: TaskStatus, fail_reason: str = ""
    ) -> bool:
        now = datetime.utcnow()
        with self._tx() as conn:
            if fail_reason:
                result = conn.execute(
                    """UPDATE tasks SET status = ?, updated_at = ?, version = version + 1,
                       fail_reason = ? WHERE id = ?""",
                    (status.value, _dt_to_str(now), fail_reason, task_id),
                )
            else:
                result = conn.execute(
                    """UPDATE tasks SET status = ?, updated_at = ?, version = version + 1
                       WHERE id = ?""",
                    (status.value, _dt_to_str(now), task_id),
                )
            return result.rowcount > 0

    def unblock_task(self, task_id: str) -> bool:
        now = datetime.utcnow()
        with self._tx() as conn:
            result = conn.execute(
                """UPDATE tasks SET status = ?, updated_at = ?, version = version + 1
                   WHERE id = ? AND status = ?""",
                (TaskStatus.pending.value, _dt_to_str(now), task_id, TaskStatus.blocked.value),
            )
            return result.rowcount > 0

    def set_dependencies(self, task_id: str, depends_on: List[str]) -> bool:
        now = datetime.utcnow()
        with self._tx() as conn:
            result = conn.execute(
                """UPDATE tasks SET depends_on = ?, updated_at = ?, version = version + 1
                   WHERE id = ?""",
                (_json_dumps(depends_on), _dt_to_str(now), task_id),
            )
            # If deps added and task was ready, demote to pending until deps are met
            if depends_on and result.rowcount > 0:
                conn.execute(
                    """UPDATE tasks SET status = ? WHERE id = ? AND status = ?""",
                    (TaskStatus.pending.value, task_id, TaskStatus.ready.value),
                )
            return result.rowcount > 0

    def get_dependents(self, task_id: str) -> List[str]:
        """Get all task IDs that depend on the given task."""
        with self._lock:
            rows = self._conn.execute("SELECT id, depends_on FROM tasks").fetchall()
        return [
            row["id"]
            for row in rows
            if task_id in _json_loads(row["depends_on"])
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
        with self._lock:
            rows = self._conn.execute("SELECT * FROM tasks").fetchall()
        nodes = []
        edges = []
        for row in rows:
            nodes.append({
                "id": row["id"],
                "title": row["title"],
                "status": row["status"],
                "priority": row["priority"],
            })
            for dep_id in _json_loads(row["depends_on"]):
                edges.append({"from": dep_id, "to": row["id"]})
        return {"nodes": nodes, "edges": edges}

    def task_counts(self) -> Dict[str, int]:
        """Count tasks by status."""
        counts = {
            "total": 0, "ready": 0, "running": 0,
            "completed": 0, "failed": 0, "pending": 0,
        }
        with self._lock:
            rows = self._conn.execute(
                "SELECT status, COUNT(*) as c FROM tasks GROUP BY status"
            ).fetchall()
        for row in rows:
            counts["total"] += row["c"]
            status_val = row["status"]
            if status_val in counts:
                counts[status_val] += row["c"]
        return counts

    def _row_to_task(self, row: sqlite3.Row) -> Task:
        return Task(
            id=row["id"],
            title=row["title"],
            requires=_json_loads(row["requires"]),
            depends_on=_json_loads(row["depends_on"]),
            priority=row["priority"],
            status=TaskStatus(row["status"]),
            assigned_to=row["assigned_to"],
            created_at=_str_to_dt(row["created_at"]),
            updated_at=_str_to_dt(row["updated_at"]),
            version=row["version"],
            fail_reason=row["fail_reason"],
        )

    # -------------------------------------------------------------------
    # Lease manager
    # -------------------------------------------------------------------

    def create_lease(self, task_id: str, node_id: str, ttl: timedelta) -> Optional[Lease]:
        lease_id = _generate_id("lease")
        now = datetime.utcnow()
        expires = now + ttl
        lease = Lease(
            id=lease_id,
            task_id=task_id,
            node_id=node_id,
            created_at=now,
            expires_at=expires,
            status=LeaseStatus.active,
        )
        with self._tx() as conn:
            conn.execute(
                """INSERT INTO leases (id, task_id, node_id, created_at, expires_at, status)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (lease_id, task_id, node_id, _dt_to_str(now),
                 _dt_to_str(expires), LeaseStatus.active.value),
            )
        return lease

    def revoke_lease(self, lease_id: str) -> bool:
        with self._tx() as conn:
            result = conn.execute(
                "UPDATE leases SET status = ? WHERE id = ?",
                (LeaseStatus.revoked.value, lease_id),
            )
            return result.rowcount > 0

    def get_active_leases(self) -> List[Lease]:
        now = datetime.utcnow()
        with self._tx() as conn:
            rows = conn.execute(
                "SELECT * FROM leases WHERE status = ?",
                (LeaseStatus.active.value,),
            ).fetchall()

        active = []
        expired_ids = []
        for row in rows:
            lease = self._row_to_lease(row)
            if lease.expires_at > now:
                active.append(lease)
            else:
                lease.status = LeaseStatus.expired
                expired_ids.append(lease.id)
                if self._lease_callback:
                    try:
                        self._lease_callback(lease.task_id, lease.node_id)
                    except Exception:
                        pass

        # Mark expired leases
        if expired_ids:
            with self._tx() as conn:
                placeholders = ",".join("?" for _ in expired_ids)
                conn.execute(
                    f"UPDATE leases SET status = ? WHERE id IN ({placeholders})",
                    [LeaseStatus.expired.value] + expired_ids,
                )

        return active

    def set_lease_callback(self, fn: Callable[[str, str], None]) -> None:
        self._lease_callback = fn

    def _row_to_lease(self, row: sqlite3.Row) -> Lease:
        return Lease(
            id=row["id"],
            task_id=row["task_id"],
            node_id=row["node_id"],
            created_at=_str_to_dt(row["created_at"]),
            expires_at=_str_to_dt(row["expires_at"]),
            status=LeaseStatus(row["status"]),
        )

    # -------------------------------------------------------------------
    # Sync state
    # -------------------------------------------------------------------

    def _sync_version(self) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT MAX(version) as v FROM sync_log"
            ).fetchone()
            return row["v"] if row and row["v"] else 0

    def sync_version(self) -> int:
        return self._sync_version()

    def handle_sync_message(self, msg: SyncMessage) -> bool:
        with self._lock:
            current = self._sync_version()
            if msg.version <= current:
                return False

        with self._tx() as conn:
            # Record sync message
            task_state_json = msg.task_state.model_dump_json() if msg.task_state else None
            conn.execute(
                """INSERT INTO sync_log (version, sender_node, task_state, event_type, timestamp)
                   VALUES (?, ?, ?, ?, ?)""",
                (msg.version, msg.sender_node, task_state_json,
                 msg.event_type.value, msg.timestamp),
            )

            # Apply task state if present
            if msg.task_state:
                existing = conn.execute(
                    "SELECT id FROM tasks WHERE id = ?", (msg.task_state.task_id,)
                ).fetchone()
                if existing:
                    conn.execute(
                        """UPDATE tasks SET status = ?, assigned_to = ?, version = ?,
                           updated_at = ? WHERE id = ?""",
                        (msg.task_state.status, msg.task_state.assigned_to,
                         msg.task_state.version, _dt_to_str(datetime.utcnow()),
                         msg.task_state.task_id),
                    )
                else:
                    conn.execute(
                        """INSERT INTO tasks
                           (id, title, status, assigned_to, version, created_at, updated_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?)""",
                        (msg.task_state.task_id, msg.task_state.title,
                         msg.task_state.status, msg.task_state.assigned_to,
                         msg.task_state.version,
                         _dt_to_str(datetime.utcnow()), _dt_to_str(datetime.utcnow())),
                    )
        return True

    def handle_batch_sync(self, batch: BatchSyncMessage) -> int:
        count = 0
        for msg in batch.messages:
            if self.handle_sync_message(msg):
                count += 1
        return count

    # -------------------------------------------------------------------
    # Recovery log
    # -------------------------------------------------------------------

    def append_recovery_event(self, event: RecoveryEvent) -> None:
        if not event.id:
            event.id = _generate_id("recovery")
        now = datetime.utcnow()
        event.timestamp = now
        with self._tx() as conn:
            conn.execute(
                """INSERT INTO recovery_events (id, task_id, node_id, action, status, message, timestamp)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (event.id, event.task_id, event.node_id, event.action,
                 event.status, event.message, _dt_to_str(now)),
            )

    def get_recovery_events(self) -> List[RecoveryEvent]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM recovery_events ORDER BY timestamp"
            ).fetchall()
        return [self._row_to_recovery_event(r) for r in rows]

    def recovery_stats(self) -> Dict[str, Any]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT action, COUNT(*) as c FROM recovery_events GROUP BY action"
            ).fetchall()
            total = sum(r["c"] for r in rows)
        by_action = {r["action"]: r["c"] for r in rows}
        return {"total": total, "by_action": by_action}

    def trigger_recovery(self, node_id: str) -> None:
        event = RecoveryEvent(
            id=_generate_id("recovery"),
            node_id=node_id,
            action="reschedule",
            status="completed",
            message=f"Node {node_id} went offline, rescheduling tasks",
        )
        self.append_recovery_event(event)

    def _row_to_recovery_event(self, row: sqlite3.Row) -> RecoveryEvent:
        return RecoveryEvent(
            id=row["id"],
            task_id=row["task_id"],
            node_id=row["node_id"],
            action=row["action"],
            status=row["status"],
            message=row["message"],
            timestamp=_str_to_dt(row["timestamp"]),
        )

    # -------------------------------------------------------------------
    # Scheduling decisions
    # -------------------------------------------------------------------

    def record_decision(self, decision: SchedulingDecision) -> None:
        with self._tx() as conn:
            conn.execute(
                """INSERT INTO scheduling_decisions
                   (task_id, task_title, priority, node_id, score, reason, timestamp)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (decision.task_id, decision.task_title, decision.priority,
                 decision.node_id, decision.score, decision.reason,
                 _dt_to_str(decision.timestamp)),
            )
            # Trim old decisions
            count = conn.execute(
                "SELECT COUNT(*) as c FROM scheduling_decisions"
            ).fetchone()["c"]
            if count > self._max_decisions:
                conn.execute(
                    """DELETE FROM scheduling_decisions WHERE id NOT IN
                       (SELECT id FROM scheduling_decisions ORDER BY id DESC LIMIT ?)""",
                    (self._max_decisions,),
                )

    def get_decisions(self) -> List[SchedulingDecision]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM scheduling_decisions ORDER BY id"
            ).fetchall()
        return [self._row_to_decision(r) for r in rows]

    def get_schedule_stats(self) -> SchedulingStats:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM scheduling_decisions"
            ).fetchall()
        total = len(rows)
        by_priority: Dict[int, int] = {}
        decisions = []
        for row in rows:
            d = self._row_to_decision(row)
            decisions.append(d)
            by_priority[d.priority] = by_priority.get(d.priority, 0) + 1

        return SchedulingStats(
            total_decisions=total,
            decisions_by_priority=by_priority,
            last_decisions=decisions[-10:] if decisions else [],
        )

    def trigger_pending_tasks(self) -> int:
        """Promote pending tasks with all dependencies met to ready."""
        promoted = 0
        with self._tx() as conn:
            rows = conn.execute(
                "SELECT id, depends_on FROM tasks WHERE status = ?",
                (TaskStatus.pending.value,),
            ).fetchall()

            for row in rows:
                depends = _json_loads(row["depends_on"])
                if not depends:
                    conn.execute(
                        """UPDATE tasks SET status = ?, updated_at = ?
                           WHERE id = ? AND status = ?""",
                        (TaskStatus.ready.value, _dt_to_str(datetime.utcnow()),
                         row["id"], TaskStatus.pending.value),
                    )
                    promoted += 1
                else:
                    all_done = True
                    for dep_id in depends:
                        dep_row = conn.execute(
                            "SELECT status FROM tasks WHERE id = ?", (dep_id,)
                        ).fetchone()
                        if dep_row is None or dep_row["status"] != TaskStatus.completed.value:
                            all_done = False
                            break
                    if all_done:
                        conn.execute(
                            """UPDATE tasks SET status = ?, updated_at = ?
                               WHERE id = ? AND status = ?""",
                            (TaskStatus.ready.value, _dt_to_str(datetime.utcnow()),
                             row["id"], TaskStatus.pending.value),
                        )
                        promoted += 1
        return promoted

    def schedule_pending(self) -> int:
        """Try to assign ready tasks to available nodes."""
        scheduled = 0
        with self._tx() as conn:
            online_nodes = [
                self._row_to_node(r)
                for r in conn.execute(
                    "SELECT * FROM nodes WHERE status = ?",
                    (NodeStatus.online.value,),
                ).fetchall()
            ]
            if not online_nodes:
                return 0

            ready_tasks = [
                self._row_to_task(r)
                for r in conn.execute(
                    """SELECT * FROM tasks WHERE status = ?
                       ORDER BY priority, created_at""",
                    (TaskStatus.ready.value,),
                ).fetchall()
            ]

            for task in ready_tasks:
                for node in online_nodes:
                    if not task.requires or all(
                        cap in node.capabilities for cap in task.requires
                    ):
                        conn.execute(
                            """UPDATE tasks SET status = ?, assigned_to = ?,
                               updated_at = ?, version = version + 1
                               WHERE id = ?""",
                            (TaskStatus.running.value, node.id,
                             _dt_to_str(datetime.utcnow()), task.id),
                        )
                        decision = SchedulingDecision(
                            task_id=task.id,
                            task_title=task.title,
                            priority=task.priority,
                            node_id=node.id,
                            score=1.0,
                            reason="capability_match",
                        )
                        # Inline record_decision to avoid nested _tx()
                        conn.execute(
                            """INSERT INTO scheduling_decisions
                               (task_id, task_title, priority, node_id, score, reason, timestamp)
                               VALUES (?, ?, ?, ?, ?, ?, ?)""",
                            (decision.task_id, decision.task_title, decision.priority,
                             decision.node_id, decision.score, decision.reason,
                             _dt_to_str(decision.timestamp)),
                        )
                        # Trim old decisions within same transaction
                        count = conn.execute(
                            "SELECT COUNT(*) as c FROM scheduling_decisions"
                        ).fetchone()["c"]
                        if count > self._max_decisions:
                            conn.execute(
                                """DELETE FROM scheduling_decisions WHERE id NOT IN
                                   (SELECT id FROM scheduling_decisions ORDER BY id DESC LIMIT ?)""",
                                (self._max_decisions,),
                            )
                        scheduled += 1
                        break
        return scheduled

    def _row_to_decision(self, row: sqlite3.Row) -> SchedulingDecision:
        return SchedulingDecision(
            task_id=row["task_id"],
            task_title=row["task_title"],
            priority=row["priority"],
            node_id=row["node_id"],
            score=row["score"],
            reason=row["reason"],
            timestamp=_str_to_dt(row["timestamp"]),
        )

    # -------------------------------------------------------------------
    # Federation registry
    # -------------------------------------------------------------------

    def register_federation_cluster(
        self, cluster_id: str, name: str, endpoint: str
    ) -> RemoteCluster:
        now = datetime.utcnow()
        cluster = RemoteCluster(
            id=cluster_id,
            name=name,
            endpoint=endpoint,
            status=FederationClusterStatus.available,
            registered_at=now,
            last_ping=now,
        )
        with self._tx() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO federation_clusters
                   (id, name, endpoint, status, registered_at, last_ping, ping_latency)
                   VALUES (?, ?, ?, ?, ?, ?, 0.0)""",
                (cluster_id, name, endpoint, FederationClusterStatus.available.value,
                 _dt_to_str(now), _dt_to_str(now)),
            )
        return cluster

    def remove_federation_cluster(self, cluster_id: str) -> bool:
        with self._tx() as conn:
            result = conn.execute(
                "DELETE FROM federation_clusters WHERE id = ?", (cluster_id,)
            )
            return result.rowcount > 0

    def get_federation_clusters(self) -> List[RemoteCluster]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM federation_clusters").fetchall()
        return [self._row_to_cluster(r) for r in rows]

    def get_federation_cluster(self, cluster_id: str) -> Optional[RemoteCluster]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM federation_clusters WHERE id = ?", (cluster_id,)
            ).fetchone()
        if row is None:
            return None
        return self._row_to_cluster(row)

    def _row_to_cluster(self, row: sqlite3.Row) -> RemoteCluster:
        return RemoteCluster(
            id=row["id"],
            name=row["name"],
            endpoint=row["endpoint"],
            status=FederationClusterStatus(row["status"]),
            registered_at=_str_to_dt(row["registered_at"]),
            last_ping=_str_to_dt(row["last_ping"]),
            ping_latency=row["ping_latency"],
        )

    # -------------------------------------------------------------------
    # Hook manager
    # -------------------------------------------------------------------

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
        with self._tx() as conn:
            conn.execute(
                """INSERT INTO hooks (id, url, events, secret, active, created_at, updated_at)
                   VALUES (?, ?, ?, ?, 1, ?, ?)""",
                (hook_id, url, _json_dumps([e.value for e in events]),
                 secret or None, _dt_to_str(now), _dt_to_str(now)),
            )
        return hook

    def deregister_hook(self, hook_id: str) -> bool:
        with self._tx() as conn:
            result = conn.execute(
                "DELETE FROM hooks WHERE id = ?", (hook_id,)
            )
            return result.rowcount > 0

    def list_hooks(self) -> List[Hook]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM hooks").fetchall()
        hooks = []
        for row in rows:
            h = self._row_to_hook(row)
            # Hide secrets (matching ClusterState behavior)
            hooks.append(h.model_copy(update={"secret": None}))
        return hooks

    def get_hook_deliveries(self, hook_id: str) -> List[Delivery]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM deliveries WHERE hook_id = ?", (hook_id,)
            ).fetchall()
        return [self._row_to_delivery(r) for r in rows]

    def add_delivery(self, delivery: Delivery) -> None:
        with self._tx() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO deliveries
                   (id, hook_id, event_type, payload, status, created_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (delivery.id, delivery.hook_id, delivery.event_type,
                 _json_dumps(delivery.payload), delivery.status,
                 _dt_to_str(delivery.created_at)),
            )
            # Trim old deliveries
            count = conn.execute(
                "SELECT COUNT(*) as c FROM deliveries"
            ).fetchone()["c"]
            if count > self._max_deliveries:
                conn.execute(
                    """DELETE FROM deliveries WHERE id NOT IN
                       (SELECT id FROM deliveries ORDER BY created_at DESC LIMIT ?)""",
                    (self._max_deliveries,),
                )

    def _row_to_hook(self, row: sqlite3.Row) -> Hook:
        return Hook(
            id=row["id"],
            url=row["url"],
            events=[EventType(e) for e in _json_loads(row["events"])],
            secret=row["secret"],
            active=bool(row["active"]),
            created_at=_str_to_dt(row["created_at"]),
            updated_at=_str_to_dt(row["updated_at"]),
        )

    def _row_to_delivery(self, row: sqlite3.Row) -> Delivery:
        return Delivery(
            id=row["id"],
            hook_id=row["hook_id"],
            event_type=row["event_type"],
            payload=_json_loads(row["payload"]),
            status=row["status"],
            created_at=_str_to_dt(row["created_at"]),
        )

    # -------------------------------------------------------------------
    # Config
    # -------------------------------------------------------------------

    def get_config(self) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM kv_store WHERE key = 'cluster_config'"
            ).fetchone()
        if row is None:
            return self._config
        self._config = _json_loads(row["value"])
        return self._config

    def set_config(self, config: Dict[str, Any]) -> None:
        self._config = config
        with self._tx() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO kv_store (key, value) VALUES ('cluster_config', ?)",
                (_json_dumps(config),),
            )

    def get_config_path(self) -> str:
        return self._config_path

    def set_config_path(self, path: str) -> None:
        self._config_path = path

    # -------------------------------------------------------------------
    # Summary
    # -------------------------------------------------------------------

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

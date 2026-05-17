"""State Sync — HTTP client, receiver, LWW conflict resolution, and coordinator.

This module implements the Go backend's sync/protocol.go with:

1. SyncClient — sends BatchSyncMessage to remote cluster nodes
2. SyncRouter — FastAPI router that accepts incoming sync messages
3. LWWResolver — Last-Writer-Wins conflict resolution (version + timestamp)
4. SyncCoordinator — background loop: periodic delta sync + full-state sync

Architecture:
  Node A                          Node B
    |  POST /api/v1/sync/batch -->  |
    |                               | LWW: reject if version <= local
    |                               | Apply: update tasks, merge state
    | <-- 200 OK {accepted: N} ---  |
"""

from __future__ import annotations

import logging
import secrets
import threading
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from pydantic import BaseModel

from ..models import (
    BatchSyncMessage,
    Node,
    NodeStatus,
    SyncMessage,
    SyncEventType,
    Task,
    TaskSync,
    TaskStatus,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _generate_id(prefix: str = "") -> str:
    if prefix:
        return f"{prefix}_{secrets.token_hex(8)}"
    return secrets.token_hex(8)


def _dt_to_str(dt: Optional[datetime]) -> str:
    if dt is None:
        return ""
    return dt.isoformat()


def _str_to_dt(s: str) -> datetime:
    if not s:
        return datetime.utcnow()
    try:
        return datetime.fromisoformat(s)
    except (ValueError, TypeError):
        return datetime.utcnow()


# ---------------------------------------------------------------------------
# LWW Conflict Resolver
# ---------------------------------------------------------------------------

class LWWResolver:
    """Last-Writer-Wins conflict resolution for state sync.

    Rules:
      1. Higher version always wins.
      2. Same version → higher timestamp wins.
      3. Same version + same timestamp → sender_node ID wins (deterministic).

    This is a pure logic class — no I/O, easy to test.
    """

    @staticmethod
    def should_apply(
        remote_version: int,
        remote_timestamp: int,
        remote_sender: str,
        local_version: int,
        local_timestamp: int,
        local_sender: str,
    ) -> bool:
        """Determine if a remote update should override local state.

        Returns True if the remote update wins the LWW comparison.
        """
        if remote_version > local_version:
            return True
        if remote_version < local_version:
            return False
        # Same version — tie-break on timestamp
        if remote_timestamp > local_timestamp:
            return True
        if remote_timestamp < local_timestamp:
            return False
        # Same version + same timestamp — tie-break on sender node ID
        return remote_sender > local_sender

    @staticmethod
    def should_apply_task(
        remote: TaskSync,
        local_version: int,
    ) -> bool:
        """Check if a remote TaskSync should override local task state."""
        return LWWResolver.should_apply(
            remote_version=remote.version,
            remote_timestamp=0,  # TaskSync has no timestamp; version is primary key
            remote_sender="",
            local_version=local_version,
            local_timestamp=0,
            local_sender="",
        )


# ---------------------------------------------------------------------------
# Sync Client — sends state to remote nodes
# ---------------------------------------------------------------------------

class SyncClient:
    """HTTP client that sends BatchSyncMessages to remote cluster nodes.

    Uses httpx for async-capable HTTP. Falls back gracefully on connection
    failures (logs warning, returns False — never crashes the sync loop).

    Usage:
        client = SyncClient(timeout=5.0)
        ok = client.send_batch("http://node-b:8787", batch_msg)
        ok = client.send_full_state("http://node-b:8787", state_data)
    """

    def __init__(
        self,
        timeout: float = 5.0,
        max_retries: int = 2,
        retry_delay: float = 1.0,
    ):
        self._timeout = timeout
        self._max_retries = max_retries
        self._retry_delay = retry_delay
        self._stats = {"sent": 0, "failed": 0, "retries": 0}

    @property
    def stats(self) -> Dict[str, int]:
        return dict(self._stats)

    def send_batch(self, endpoint: str, batch: BatchSyncMessage) -> bool:
        """Send a batch sync message to a remote node.

        Args:
            endpoint: Base URL of remote node (e.g. "http://node-b:8787")
            batch: BatchSyncMessage to send

        Returns:
            True if successfully sent and accepted (HTTP 2xx).
        """
        import httpx

        url = f"{endpoint.rstrip('/')}/api/v1/sync/batch"
        payload = batch.model_dump(mode="json")

        for attempt in range(1, self._max_retries + 1):
            try:
                with httpx.Client(timeout=self._timeout) as client:
                    resp = client.post(url, json=payload)
                    if resp.status_code < 300:
                        self._stats["sent"] += 1
                        logger.debug(
                            "sync sent to %s: %d messages (HTTP %d)",
                            endpoint, len(batch.messages), resp.status_code,
                        )
                        return True
                    else:
                        logger.warning(
                            "sync send to %s failed: HTTP %d %s",
                            endpoint, resp.status_code, resp.text[:200],
                        )
            except Exception as e:
                logger.warning(
                    "sync send to %s attempt %d/%d failed: %s",
                    endpoint, attempt, self._max_retries, e,
                )
                if attempt < self._max_retries:
                    self._stats["retries"] += 1
                    time.sleep(self._retry_delay * attempt)

        self._stats["failed"] += 1
        return False

    def send_single(
        self, endpoint: str, msg: SyncMessage
    ) -> bool:
        """Send a single sync message to a remote node."""
        batch = BatchSyncMessage(messages=[msg])
        return self.send_batch(endpoint, batch)

    def send_full_state(
        self, endpoint: str, state_data: Dict[str, Any]
    ) -> bool:
        """Send full cluster state snapshot to a remote node.

        Args:
            endpoint: Base URL of remote node
            state_data: Dict with keys 'nodes', 'tasks', 'leases'
        """
        import httpx

        url = f"{endpoint.rstrip('/')}/api/v1/sync/full"
        for attempt in range(1, self._max_retries + 1):
            try:
                with httpx.Client(timeout=self._timeout * 3) as client:
                    resp = client.post(url, json=state_data)
                    if resp.status_code < 300:
                        self._stats["sent"] += 1
                        logger.debug("full state sent to %s (HTTP %d)", endpoint, resp.status_code)
                        return True
                    else:
                        logger.warning(
                            "full state send to %s failed: HTTP %d",
                            endpoint, resp.status_code,
                        )
            except Exception as e:
                logger.warning(
                    "full state send to %s attempt %d/%d failed: %s",
                    endpoint, attempt, self._max_retries, e,
                )
                if attempt < self._max_retries:
                    self._stats["retries"] += 1
                    time.sleep(self._retry_delay * attempt)

        self._stats["failed"] += 1
        return False

    def health_check(self, endpoint: str) -> bool:
        """Check if a remote node is reachable."""
        import httpx

        url = f"{endpoint.rstrip('/')}/api/v1/health"
        try:
            with httpx.Client(timeout=self._timeout) as client:
                resp = client.get(url)
                return resp.status_code < 300
        except Exception:
            return False


# ---------------------------------------------------------------------------
# Sync Receiver — processes incoming sync messages with LWW
# ---------------------------------------------------------------------------

class SyncReceiver:
    """Processes incoming sync messages and applies them with LWW resolution.

    Wraps ClusterStore's handle_sync_message() with additional LWW checks
    and event logging.

    Usage:
        receiver = SyncReceiver(store, node_id="node-a")
        result = receiver.receive_batch(batch_msg)
    """

    def __init__(self, store: Any, node_id: str = "node_main"):
        self._store = store
        self._node_id = node_id
        self._lww = LWWResolver()
        self._stats = {"received": 0, "applied": 0, "rejected_stale": 0}

    @property
    def stats(self) -> Dict[str, int]:
        return dict(self._stats)

    def receive_batch(self, batch: BatchSyncMessage) -> Dict[str, Any]:
        """Process a batch of sync messages with LWW conflict resolution.

        Returns:
            Dict with 'accepted' count, 'rejected' count, and 'details'.
        """
        accepted = 0
        rejected = 0
        details = []

        for msg in batch.messages:
            result = self.receive_single(msg)
            if result["applied"]:
                accepted += 1
            else:
                rejected += 1
            details.append(result)

        self._stats["received"] += len(batch.messages)

        return {
            "accepted": accepted,
            "rejected": rejected,
            "total": len(batch.messages),
            "details": details,
        }

    def receive_single(self, msg: SyncMessage) -> Dict[str, Any]:
        """Process a single sync message with LWW conflict resolution."""
        self._stats["received"] += 1

        # Get local sync version
        local_version = self._store.sync_version()

        # LWW check: reject if remote version is stale
        if msg.version <= local_version:
            self._stats["rejected_stale"] += 1
            logger.debug(
                "sync rejected: remote version %d <= local %d (sender=%s)",
                msg.version, local_version, msg.sender_node,
            )
            return {
                "applied": False,
                "reason": "stale_version",
                "remote_version": msg.version,
                "local_version": local_version,
            }

        # Apply the sync message via store
        applied = self._store.handle_sync_message(msg)
        if applied:
            self._stats["applied"] += 1
            logger.debug(
                "sync applied: version=%d event=%s sender=%s",
                msg.version, msg.event_type.value, msg.sender_node,
            )
            return {
                "applied": True,
                "reason": "applied",
                "version": msg.version,
                "event_type": msg.event_type.value,
            }
        else:
            self._stats["rejected_stale"] += 1
            return {
                "applied": False,
                "reason": "store_rejected",
                "remote_version": msg.version,
            }

    def receive_full_state(
        self, state_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Apply a full state snapshot from a remote node.

        This is used for initial sync or recovery. Merges all nodes and
        tasks, applying LWW per-entity.
        """
        nodes_applied = 0
        tasks_applied = 0
        nodes_rejected = 0
        tasks_rejected = 0

        # Apply nodes
        for node_data in state_data.get("nodes", []):
            try:
                node = Node(**node_data)
                existing = self._store.get_node(node.id)
                if existing is None:
                    self._store.register_node(node)
                    nodes_applied += 1
                else:
                    # LWW on node: use load as version proxy
                    # (higher load = more recent in full state)
                    if node.load >= existing.load:
                        self._store.register_node(node)
                        nodes_applied += 1
                    else:
                        nodes_rejected += 1
            except Exception as e:
                logger.warning("full state node merge failed: %s", e)
                nodes_rejected += 1

        # Apply tasks
        for task_data in state_data.get("tasks", []):
            try:
                task = Task(**task_data)
                existing = self._store.get_task(task.id)
                if existing is None:
                    # Create new task
                    self._store.create_task(
                        task.id, task.title, task.requires, task.priority
                    )
                    # Set status and assigned_to if different from default
                    if task.status != TaskStatus.pending:
                        self._store.set_task_status(task.id, task.status)
                    tasks_applied += 1
                else:
                    # LWW: use task version
                    if task.version > existing.version:
                        self._store.set_task_status(
                            task.id, task.status, task.fail_reason or ""
                        )
                        tasks_applied += 1
                    else:
                        tasks_rejected += 1
            except Exception as e:
                logger.warning("full state task merge failed: %s", e)
                tasks_rejected += 1

        return {
            "nodes_applied": nodes_applied,
            "nodes_rejected": nodes_rejected,
            "tasks_applied": tasks_applied,
            "tasks_rejected": tasks_rejected,
        }


# ---------------------------------------------------------------------------
# Sync Coordinator — background sync loop
# ---------------------------------------------------------------------------

class SyncCoordinator:
    """Orchestrates periodic state sync between cluster nodes.

    Responsibilities:
      1. Delta sync: send pending SyncMessages to all registered peers
      2. Full state sync: periodically snapshot and push full state
      3. Peer health monitoring: skip unreachable peers
      4. Background thread with graceful shutdown

    Usage:
        coordinator = SyncCoordinator(
            store=store,
            node_id="node-a",
            peers=["http://node-b:8787", "http://node-c:8787"],
            delta_interval=5.0,
            full_sync_interval=60.0,
        )
        coordinator.start()
        # ... later ...
        coordinator.stop()
    """

    def __init__(
        self,
        store: Any,
        node_id: str = "node_main",
        peers: Optional[List[str]] = None,
        delta_interval: float = 5.0,
        full_sync_interval: float = 60.0,
        timeout: float = 5.0,
        on_sync_complete: Optional[Callable[[Dict[str, Any]], None]] = None,
    ):
        self._store = store
        self._node_id = node_id
        self._peers: List[str] = peers or []
        self._delta_interval = delta_interval
        self._full_sync_interval = full_sync_interval
        self._on_sync_complete = on_sync_complete

        self._client = SyncClient(timeout=timeout)
        self._receiver = SyncReceiver(store, node_id)

        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._last_full_sync: float = 0.0
        self._pending_messages: List[SyncMessage] = []
        self._pending_lock = threading.Lock()

    @property
    def client(self) -> SyncClient:
        return self._client

    @property
    def receiver(self) -> SyncReceiver:
        return self._receiver

    @property
    def peers(self) -> List[str]:
        return list(self._peers)

    def add_peer(self, endpoint: str) -> None:
        """Add a peer node for sync."""
        if endpoint not in self._peers:
            self._peers.append(endpoint)
            logger.info("sync peer added: %s", endpoint)

    def remove_peer(self, endpoint: str) -> None:
        """Remove a peer node from sync."""
        if endpoint in self._peers:
            self._peers.remove(endpoint)
            logger.info("sync peer removed: %s", endpoint)

    def queue_sync_message(self, msg: SyncMessage) -> None:
        """Queue a sync message for the next delta sync cycle."""
        with self._pending_lock:
            self._pending_messages.append(msg)

    def queue_task_event(
        self,
        task_id: str,
        title: str,
        status: str,
        event_type: SyncEventType,
        assigned_to: Optional[str] = None,
        version: int = 1,
    ) -> None:
        """Convenience: queue a task sync event."""
        msg = SyncMessage(
            version=self._store.sync_version() + 1,
            sender_node=self._node_id,
            task_state=TaskSync(
                task_id=task_id,
                title=title,
                status=status,
                assigned_to=assigned_to,
                version=version,
            ),
            event_type=event_type,
            timestamp=int(time.time()),
        )
        self.queue_sync_message(msg)

    def start(self) -> None:
        """Start the background sync loop."""
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._sync_loop,
            daemon=True,
            name="sync-coordinator",
        )
        self._thread.start()
        logger.info(
            "SyncCoordinator started: peers=%d delta_interval=%.1fs full_sync_interval=%.1fs",
            len(self._peers), self._delta_interval, self._full_sync_interval,
        )

    def stop(self) -> None:
        """Stop the background sync loop gracefully."""
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5.0)
        logger.info("SyncCoordinator stopped")

    def _sync_loop(self) -> None:
        """Main sync loop — runs in background thread."""
        while not self._stop_event.is_set():
            try:
                self._do_delta_sync()

                now = time.time()
                if now - self._last_full_sync >= self._full_sync_interval:
                    self._do_full_sync()
                    self._last_full_sync = now

            except Exception:
                logger.exception("sync loop error")

            self._stop_event.wait(timeout=self._delta_interval)

    def _do_delta_sync(self) -> Dict[str, Any]:
        """Send pending sync messages to all peers."""
        # Drain pending queue
        with self._pending_lock:
            messages = list(self._pending_messages)
            self._pending_messages.clear()

        if not messages:
            return {"sent_to": 0, "messages": 0}

        batch = BatchSyncMessage(messages=messages)
        results = {}

        for peer in self._peers:
            try:
                ok = self._client.send_batch(peer, batch)
                results[peer] = {"success": ok, "messages": len(messages)}
            except Exception as e:
                results[peer] = {"success": False, "error": str(e)}

        summary = {
            "sent_to": sum(1 for r in results.values() if r.get("success")),
            "messages": len(messages),
            "peers": results,
        }

        if self._on_sync_complete:
            try:
                self._on_sync_complete(summary)
            except Exception:
                logger.exception("on_sync_complete callback error")

        return summary

    def _do_full_sync(self) -> Dict[str, Any]:
        """Send full state snapshot to all peers."""
        state_data = self._build_full_state()
        results = {}

        for peer in self._peers:
            try:
                ok = self._client.send_full_state(peer, state_data)
                results[peer] = {"success": ok}
            except Exception as e:
                results[peer] = {"success": False, "error": str(e)}

        return {
            "type": "full_sync",
            "sent_to": sum(1 for r in results.values() if r.get("success")),
            "peers": results,
        }

    def _build_full_state(self) -> Dict[str, Any]:
        """Build full state snapshot from store."""
        nodes = self._store.get_all_nodes()
        tasks = self._store.get_all_tasks()

        return {
            "node_id": self._node_id,
            "sync_version": self._store.sync_version(),
            "timestamp": int(time.time()),
            "nodes": [
                {
                    "id": n.id,
                    "name": n.name,
                    "capabilities": n.capabilities,
                    "status": n.status.value if isinstance(n.status, NodeStatus) else n.status,
                    "last_heartbeat": _dt_to_str(n.last_heartbeat),
                    "load": n.load,
                }
                for n in nodes
            ],
            "tasks": [
                {
                    "id": t.id,
                    "title": t.title,
                    "requires": t.requires,
                    "depends_on": t.depends_on,
                    "priority": t.priority,
                    "status": t.status.value if isinstance(t.status, TaskStatus) else t.status,
                    "assigned_to": t.assigned_to,
                    "created_at": _dt_to_str(t.created_at),
                    "updated_at": _dt_to_str(t.updated_at),
                    "version": t.version,
                    "fail_reason": t.fail_reason,
                }
                for t in tasks
            ],
        }

    def get_stats(self) -> Dict[str, Any]:
        """Get combined sync statistics."""
        return {
            "client": self._client.stats,
            "receiver": self._receiver.stats,
            "peers": len(self._peers),
            "pending_messages": len(self._pending_messages),
            "running": self._thread is not None and self._thread.is_alive(),
        }

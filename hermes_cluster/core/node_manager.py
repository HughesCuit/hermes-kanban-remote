"""Node Manager — join/heartbeat/capabilities lifecycle.

Port of Go's cluster/node.go + heartbeat/sender.go.

Responsibilities:
  1. Node join: validate config, register in ClusterStore, emit events
  2. Heartbeat: periodic updates that refresh last_heartbeat timestamp
  3. Capabilities: report and track capability changes per node
  4. Status transitions: online → degraded → offline (via Watchdog)

Thread-safe: all public methods are safe to call from any thread.
"""

from __future__ import annotations

import logging
import secrets
import threading
import time
from datetime import datetime, timedelta
from typing import Callable, Dict, List, Optional, Set

from ..models import (
    Node,
    NodeStatus,
    WatchdogConfig,
    HeartbeatConfig,
    NodeConfig,
    TimelineEvent,
)
from ..state.cluster_store import ClusterStore


# ---------------------------------------------------------------------------
# Lightweight config wrappers that accept float seconds (for convenience)
# and convert to timedelta for the Pydantic models.
# ---------------------------------------------------------------------------

class _HeartbeatConfig:
    """Wrapper accepting float seconds, converting to timedelta."""
    def __init__(self, interval: float = 30.0, lease_timeout: float = 120.0):
        self.interval = interval
        self.lease_timeout = lease_timeout


class _WatchdogConfig:
    """Wrapper accepting float seconds, converting to timedelta."""
    def __init__(self, check_interval: float = 5.0, degraded_after: float = 15.0, offline_after: float = 30.0):
        self.check_interval = check_interval
        self.degraded_after = degraded_after
        self.offline_after = offline_after

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Events emitted by NodeManager
# ---------------------------------------------------------------------------

class NodeEvent:
    """Lightweight event for node lifecycle changes."""

    __slots__ = ("node_id", "event_type", "timestamp", "detail")

    def __init__(self, node_id: str, event_type: str, detail: str = ""):
        self.node_id = node_id
        self.event_type = event_type  # "joined", "left", "heartbeat", "cap_changed", "status_changed"
        self.timestamp = datetime.utcnow()
        self.detail = detail

    def __repr__(self) -> str:
        return f"NodeEvent({self.node_id!r}, {self.event_type!r}, {self.detail!r})"


# ---------------------------------------------------------------------------
# NodeManager
# ---------------------------------------------------------------------------

class NodeManager:
    """Manages the full lifecycle of cluster nodes.

    Args:
        store: persistent cluster state
        heartbeat_config: heartbeat timing (interval, lease_timeout)
        watchdog_config: watchdog timing (check_interval, degraded/offline thresholds)
    """

    def __init__(
        self,
        store: ClusterStore,
        heartbeat_config: Optional[_HeartbeatConfig] = None,
        watchdog_config: Optional[_WatchdogConfig] = None,
    ):
        self._store = store
        self._heartbeat_cfg = heartbeat_config or _HeartbeatConfig()
        self._watchdog_cfg = watchdog_config or _WatchdogConfig()

        # Event listeners
        self._listeners: List[Callable[[NodeEvent], None]] = []

        # Heartbeat sender state
        self._heartbeat_running = False
        self._heartbeat_stop = threading.Event()
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._local_node_id: Optional[str] = None

        # Capabilities tracking (node_id → last known caps)
        self._cap_cache: Dict[str, List[str]] = {}

    # ------------------------------------------------------------------
    # Event system
    # ------------------------------------------------------------------

    def add_listener(self, fn: Callable[[NodeEvent], None]) -> None:
        """Register a callback for node events."""
        self._listeners.append(fn)

    def remove_listener(self, fn: Callable[[NodeEvent], None]) -> None:
        """Unregister a callback."""
        self._listeners = [l for l in self._listeners if l is not fn]

    def _emit(self, event: NodeEvent) -> None:
        """Fire event to all listeners, log to timeline."""
        logger.debug("node event: %s", event)
        for fn in self._listeners:
            try:
                fn(event)
            except Exception:
                logger.exception("listener error for %s", event.event_type)

        # Persist to timeline
        try:
            self._store.append_timeline(TimelineEvent(
                type=f"node_{event.event_type}",
                timestamp=event.timestamp,
                node_id=event.node_id,
                description=event.detail or f"node {event.event_type}",
            ))
        except Exception:
            pass  # non-critical

    # ------------------------------------------------------------------
    # 1. Node join
    # ------------------------------------------------------------------

    def join(
        self,
        node_id: str,
        name: str = "",
        capabilities: Optional[List[str]] = None,
        load: float = 0.0,
    ) -> Node:
        """Register a new node in the cluster.

        If the node already exists, updates its status to online and
        refreshes the heartbeat. This is idempotent.

        Args:
            node_id: unique node identifier
            name: human-readable name (defaults to node_id)
            capabilities: list of capability strings
            load: initial load value (0.0 - 1.0)

        Returns:
            The registered Node object.
        """
        if not node_id:
            raise ValueError("node_id is required")

        caps = capabilities or []
        now = datetime.utcnow()

        existing = self._store.get_node(node_id)
        if existing is not None:
            # Re-join: update status + heartbeat + capabilities
            self._store.update_heartbeat(node_id)
            if caps:
                self._store.update_capabilities(node_id, caps)
            node = self._store.get_node(node_id)
            logger.info("node re-joined: %s", node_id)
            self._emit(NodeEvent(node_id, "joined", f"re-join, caps={caps}"))
            return node  # type: ignore

        node = Node(
            id=node_id,
            name=name or node_id,
            capabilities=caps,
            status=NodeStatus.online,
            last_heartbeat=now,
            load=load,
        )
        self._store.register_node(node)
        self._cap_cache[node_id] = list(caps)

        logger.info("node joined: %s (name=%s, caps=%s)", node_id, name, caps)
        self._emit(NodeEvent(node_id, "joined", f"name={name}, caps={caps}"))
        return node

    def leave(self, node_id: str) -> bool:
        """Mark a node as offline and remove it from the cluster.

        Returns True if the node existed.
        """
        node = self._store.get_node(node_id)
        if node is None:
            return False

        self._store.set_node_status(node_id, NodeStatus.offline)
        self._cap_cache.pop(node_id, None)
        logger.info("node left: %s", node_id)
        self._emit(NodeEvent(node_id, "left", "node left cluster"))
        return True

    def get_node(self, node_id: str) -> Optional[Node]:
        """Get a node by ID."""
        return self._store.get_node(node_id)

    def get_all_nodes(self) -> List[Node]:
        """Get all registered nodes."""
        return self._store.get_all_nodes()

    def online_nodes(self) -> List[Node]:
        """Get only online nodes."""
        return [n for n in self._store.get_all_nodes() if n.status == NodeStatus.online]

    def node_count(self) -> int:
        """Total number of registered nodes."""
        return self._store.node_count()

    def online_count(self) -> int:
        """Number of online nodes."""
        return self._store.online_count()

    # ------------------------------------------------------------------
    # 2. Heartbeat
    # ------------------------------------------------------------------

    def send_heartbeat(self, node_id: str, load: float = 0.0) -> None:
        """Send a single heartbeat for a node.

        Updates the last_heartbeat timestamp and optionally the load.
        This is the core heartbeat primitive — both manual and periodic
        heartbeats use this.

        Args:
            node_id: the node sending the heartbeat
            load: current load (0.0 - 1.0), clamped
        """
        node = self._store.get_node(node_id)
        if node is None:
            logger.warning("heartbeat for unknown node: %s", node_id)
            return

        # Clamp load
        load = max(0.0, min(1.0, load))

        self._store.update_heartbeat(node_id, load=load)
        self._emit(NodeEvent(
            node_id, "heartbeat",
            f"load={load:.2f}",
        ))

    def start_heartbeat_sender(
        self,
        node_id: str,
        load_fn: Optional[Callable[[], float]] = None,
    ) -> None:
        """Start periodic heartbeat sending for the local node.

        Args:
            node_id: this node's ID
            load_fn: optional callable returning current load (0.0-1.0).
                     If None, load is always 0.0.
        """
        if self._heartbeat_running:
            logger.warning("heartbeat sender already running")
            return

        self._local_node_id = node_id
        self._heartbeat_running = True
        self._heartbeat_stop.clear()

        def _loop():
            logger.info(
                "heartbeat sender started: node=%s interval=%.1fs",
                node_id, self._heartbeat_cfg.interval,
            )
            while not self._heartbeat_stop.is_set():
                try:
                    load = load_fn() if load_fn else 0.0
                    self.send_heartbeat(node_id, load=load)
                except Exception:
                    logger.exception("heartbeat send failed for %s", node_id)
                self._heartbeat_stop.wait(timeout=self._heartbeat_cfg.interval)
            logger.info("heartbeat sender stopped: node=%s", node_id)

        self._heartbeat_thread = threading.Thread(
            target=_loop, daemon=True, name=f"heartbeat-{node_id}",
        )
        self._heartbeat_thread.start()

    def stop_heartbeat_sender(self) -> None:
        """Stop the periodic heartbeat sender."""
        if not self._heartbeat_running:
            return
        self._heartbeat_running = False
        self._heartbeat_stop.set()
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join(timeout=3.0)
        self._heartbeat_thread = None
        self._local_node_id = None

    @property
    def heartbeat_running(self) -> bool:
        return self._heartbeat_running

    # ------------------------------------------------------------------
    # 3. Capabilities
    # ------------------------------------------------------------------

    def update_capabilities(self, node_id: str, capabilities: List[str]) -> bool:
        """Update a node's capabilities.

        Only emits a cap_changed event if capabilities actually changed.

        Returns True if the node exists and capabilities were updated.
        """
        node = self._store.get_node(node_id)
        if node is None:
            logger.warning("update_capabilities for unknown node: %s", node_id)
            return False

        old_caps = self._cap_cache.get(node_id, [])
        new_caps = sorted(capabilities)  # normalize order

        if sorted(old_caps) == new_caps:
            return True  # no change

        self._store.update_capabilities(node_id, new_caps)
        self._cap_cache[node_id] = list(new_caps)

        added = set(new_caps) - set(old_caps)
        removed = set(old_caps) - set(new_caps)
        detail_parts = []
        if added:
            detail_parts.append(f"added={sorted(added)}")
        if removed:
            detail_parts.append(f"removed={sorted(removed)}")

        self._emit(NodeEvent(
            node_id, "cap_changed",
            "; ".join(detail_parts) or "reordered",
        ))
        return True

    def get_capabilities(self, node_id: str) -> List[str]:
        """Get a node's current capabilities."""
        node = self._store.get_node(node_id)
        if node is None:
            return []
        return list(node.capabilities)

    def nodes_with_capability(self, capability: str) -> List[Node]:
        """Find all online nodes that have a specific capability."""
        return [
            n for n in self._store.get_all_nodes()
            if n.status == NodeStatus.online and capability in n.capabilities
        ]

    # ------------------------------------------------------------------
    # 4. Status management
    # ------------------------------------------------------------------

    def set_status(self, node_id: str, status: NodeStatus) -> bool:
        """Directly set a node's status.

        Returns True if the node existed.
        """
        node = self._store.get_node(node_id)
        if node is None:
            return False

        old_status = node.status
        if old_status == status:
            return True

        self._store.set_node_status(node_id, status)
        self._emit(NodeEvent(
            node_id, "status_changed",
            f"{old_status.value} → {status.value}",
        ))
        return True

    def get_stale_nodes(
        self,
        threshold_seconds: float = 0.0,
    ) -> List[Node]:
        """Find nodes whose heartbeat is older than threshold.

        If threshold_seconds is 0, uses the watchdog's degraded_after config.
        """
        if threshold_seconds <= 0:
            threshold_seconds = self._watchdog_cfg.degraded_after

        now = datetime.utcnow()
        cutoff = now - timedelta(seconds=threshold_seconds)
        return [
            n for n in self._store.get_all_nodes()
            if n.last_heartbeat < cutoff
        ]

    # ------------------------------------------------------------------
    # Watchdog adapter (bridges NodeManager ↔ Watchdog)
    # ------------------------------------------------------------------

    def create_watchdog_registry(self):
        """Create a WatchdogRegistry adapter for the HeartbeatWatchdog."""
        store = self._store

        class _Registry:
            def get_all_heartbeat_nodes(self_inner):
                from ..core.watchdog import HeartbeatNode
                nodes = store.get_all_nodes()
                return [
                    HeartbeatNode(
                        node_id=n.id,
                        last_heartbeat=n.last_heartbeat,
                        status=n.status.value,
                    )
                    for n in nodes
                ]

            def update_node_status(self_inner, node_id: str, status: str):
                try:
                    ns = NodeStatus(status)
                except ValueError:
                    ns = NodeStatus.offline
                store.set_node_status(node_id, ns)

        return _Registry()

    def start_watchdog(self) -> None:
        """Start the heartbeat watchdog in a background thread."""
        from ..core.watchdog import Watchdog

        registry = self.create_watchdog_registry()
        self._watchdog = Watchdog(
            registry=registry,
            check_interval=self._watchdog_cfg.check_interval,
            degraded_after=self._watchdog_cfg.degraded_after,
            offline_after=self._watchdog_cfg.offline_after,
            callback=self._on_watchdog_event,
        )
        self._watchdog.start()

    def stop_watchdog(self) -> None:
        """Stop the heartbeat watchdog."""
        if hasattr(self, "_watchdog") and self._watchdog:
            self._watchdog.stop()
            self._watchdog = None

    def _on_watchdog_event(self, event) -> None:
        """Handle watchdog status change events."""
        try:
            ns = NodeStatus(event.event_type)
        except ValueError:
            ns = NodeStatus.offline

        self._emit(NodeEvent(
            event.node_id, "status_changed",
            f"watchdog → {event.event_type}",
        ))

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------

    def summary(self) -> Dict:
        """Return a summary dict of node manager state."""
        nodes = self._store.get_all_nodes()
        return {
            "total_nodes": len(nodes),
            "online_nodes": sum(1 for n in nodes if n.status == NodeStatus.online),
            "degraded_nodes": sum(1 for n in nodes if n.status == NodeStatus.degraded),
            "offline_nodes": sum(1 for n in nodes if n.status == NodeStatus.offline),
            "heartbeat_running": self._heartbeat_running,
            "capabilities": {
                n.id: list(n.capabilities) for n in nodes
            },
        }

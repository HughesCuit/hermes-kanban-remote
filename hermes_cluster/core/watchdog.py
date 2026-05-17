"""Heartbeat watchdog — monitors node health via heartbeat staleness.

Python port of Go's internal/heartbeat/watchdog.go.
Runs as a background thread, checks heartbeat age periodically,
and emits status change events (online → degraded → offline).

Designed to work with NodeManager's create_watchdog_registry() adapter.
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime
from typing import Callable, List, Optional

logger = logging.getLogger(__name__)


class WatchdogEvent:
    """Event emitted when a node's status changes."""

    __slots__ = ("node_id", "event_type", "timestamp")

    def __init__(self, node_id: str, event_type: str):
        self.node_id = node_id
        self.event_type = event_type  # "online", "degraded", "offline"
        self.timestamp = datetime.utcnow()

    def __repr__(self) -> str:
        return f"WatchdogEvent({self.node_id!r}, {self.event_type!r})"


class HeartbeatNode:
    """Minimal node info needed by the watchdog."""

    __slots__ = ("node_id", "last_heartbeat", "status")

    def __init__(self, node_id: str, last_heartbeat: datetime, status: str):
        self.node_id = node_id
        self.last_heartbeat = last_heartbeat
        self.status = status


class WatchdogRegistry:
    """Interface the watchdog needs from the cluster state."""

    def get_all_heartbeat_nodes(self) -> List[HeartbeatNode]:
        raise NotImplementedError

    def update_node_status(self, node_id: str, status: str) -> None:
        raise NotImplementedError


class Watchdog:
    """Monitors node heartbeats and emits status change events.

    Parameters:
        registry: provides node heartbeat info and status updates
        check_interval: how often to check (seconds)
        degraded_after: seconds without heartbeat to mark degraded
        offline_after: seconds without heartbeat to mark offline
        callback: called with WatchdogEvent on status change
    """

    def __init__(
        self,
        registry: WatchdogRegistry,
        check_interval: float = 5.0,
        degraded_after: float = 15.0,
        offline_after: float = 30.0,
        callback: Optional[Callable[[WatchdogEvent], None]] = None,
    ):
        self._registry = registry
        self._check_interval = check_interval
        self._degraded_after = degraded_after
        self._offline_after = offline_after
        self._callback = callback
        self._running = False
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    @property
    def is_running(self) -> bool:
        return self._running

    def start(self) -> None:
        """Start the watchdog loop in a background thread."""
        if self._running:
            return
        self._running = True
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="cluster-watchdog"
        )
        self._thread.start()
        logger.info(
            "watchdog started: check_interval=%.1fs degraded_after=%.1fs offline_after=%.1fs",
            self._check_interval,
            self._degraded_after,
            self._offline_after,
        )

    def stop(self) -> None:
        """Stop the watchdog loop."""
        if not self._running:
            return
        self._running = False
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)
        logger.info("watchdog stopped")

    def update_intervals(
        self,
        check_interval: float,
        degraded_after: float,
        offline_after: float,
    ) -> None:
        """Dynamically update timing parameters."""
        self._check_interval = check_interval
        self._degraded_after = degraded_after
        self._offline_after = offline_after

    def check_now(self) -> List[WatchdogEvent]:
        """Run a single check cycle and return events. Useful for testing."""
        return self._check()

    def _loop(self) -> None:
        while self._running:
            self._check()
            self._stop_event.wait(timeout=self._check_interval)

    def _check(self) -> List[WatchdogEvent]:
        now = datetime.utcnow()
        nodes = self._registry.get_all_heartbeat_nodes()
        events: List[WatchdogEvent] = []

        for node in nodes:
            elapsed = (now - node.last_heartbeat).total_seconds()
            if elapsed >= self._offline_after:
                new_status = "offline"
            elif elapsed >= self._degraded_after:
                new_status = "degraded"
            else:
                new_status = "online"

            if new_status != node.status:
                self._registry.update_node_status(node.node_id, new_status)
                evt = WatchdogEvent(node.node_id, new_status)
                events.append(evt)
                if self._callback:
                    try:
                        self._callback(evt)
                    except Exception:
                        logger.exception("watchdog callback error for node %s", node.node_id)

        return events

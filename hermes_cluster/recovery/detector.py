"""Detector — watches for offline events and triggers the full recovery sequence.

Mirrors Go's internal/recovery/detector.go:
  - Start() / Stop() lifecycle
  - NotifyOffline(node_id) sends an event to the detector channel
  - handleOffline performs: revoke → reschedule → log
"""

from __future__ import annotations

import logging
import queue
import secrets
import threading
import time
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from .revoker import Revoker
    from .rescheduler import Rescheduler

logger = logging.getLogger(__name__)


def _gen_id(prefix: str = "recovery") -> str:
    return f"{prefix}_{secrets.token_hex(8)}"


class OfflineEvent:
    """A node-going-offline event."""

    __slots__ = ("node_id", "timestamp")

    def __init__(self, node_id: str, timestamp: float) -> None:
        self.node_id = node_id
        self.timestamp = timestamp


class Detector:
    """Watches for offline events and triggers the full recovery pipeline."""

    def __init__(
        self,
        revoker: "Revoker",
        rescheduler: "Rescheduler",
        state: "ClusterState",
    ) -> None:
        # Import here to avoid circular imports
        from ..state import ClusterState

        self._revoker = revoker
        self._rescheduler = rescheduler
        self._state = state
        self._running = False
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._event_queue: queue.Queue[Optional[OfflineEvent]] = queue.Queue(maxsize=100)
        self._lock = threading.Lock()

    def start(self) -> None:
        """Start the detector background loop."""
        with self._lock:
            if self._running:
                return
            self._running = True
            self._stop_event.clear()

        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="recovery-detector"
        )
        self._thread.start()
        logger.info("Recovery detector started")

    def stop(self) -> None:
        """Stop the detector background loop."""
        with self._lock:
            if not self._running:
                return
            self._running = False
            self._stop_event.set()

        if self._thread and self._thread.is_alive():
            self._event_queue.put(None)  # Unblock get()
            self._thread.join(timeout=5.0)
        logger.info("Recovery detector stopped")

    @property
    def is_running(self) -> bool:
        with self._lock:
            return self._running

    def notify_offline(self, node_id: str) -> None:
        """Send a node-offline event to the detector queue."""
        event = OfflineEvent(node_id=node_id, timestamp=time.time())
        try:
            self._event_queue.put_nowait(event)
        except Exception:
            logger.warning("Recovery event queue full, dropping event for %s", node_id)

    def _loop(self) -> None:
        """Main detector loop — blocks on queue until stopped."""
        while True:
            try:
                event = self._event_queue.get(timeout=1.0)
            except Exception:
                # Queue empty or timeout — check stop flag
                if self._stop_event.is_set():
                    return
                continue

            if event is None:
                # Poison pill
                return

            self._handle_offline(event)

    def _handle_offline(self, evt: OfflineEvent) -> None:
        """Perform the full recovery sequence for a failed node."""
        logger.info("Handling offline event for node %s", evt.node_id)

        # Step 1: Revoke all leases for the failed node
        revoked_task_ids = self._revoker.revoke_all_for_node(evt.node_id)

        # Step 2: Try to reschedule orphaned tasks
        if revoked_task_ids:
            rescheduled = self._rescheduler.reschedule_orphaned(revoked_task_ids)
            status = "completed" if rescheduled == len(revoked_task_ids) else "partial"
            message = (
                f"Revoked {len(revoked_task_ids)} leases, "
                f"rescheduled {rescheduled}/{len(revoked_task_ids)} tasks"
            )
        else:
            status = "completed"
            message = "No active leases to revoke"

        # Step 3: Log the full recovery event
        from ..models import RecoveryEvent
        event = RecoveryEvent(
            id=_gen_id(),
            node_id=evt.node_id,
            action="full_recovery",
            status=status,
            message=message,
        )
        self._state.append_recovery_event(event)
        logger.info("Recovery for node %s: %s — %s", evt.node_id, status, message)

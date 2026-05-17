"""RecoveryManager — orchestrator for the full recovery pipeline.

Mirrors Go's recovery package wiring:
  - Creates Revoker, Rescheduler, Detector
  - Provides trigger_recovery() for manual invocation
  - Manages auto-recovery background thread
  - Exposes stats and log queries
"""

from __future__ import annotations

import logging
import secrets
import threading
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ..models import RecoveryEvent, TaskStatus
from .detector import Detector
from .revoker import Revoker
from .rescheduler import Rescheduler

if TYPE_CHECKING:
    from ..state import ClusterState

logger = logging.getLogger(__name__)


def _gen_id(prefix: str = "recovery") -> str:
    return f"{prefix}_{secrets.token_hex(8)}"


class RecoveryManager:
    """Full recovery pipeline: trigger → revoke → reschedule → log.

    Usage::

        manager = RecoveryManager(state)
        manager.start_auto_recovery()          # background lease expiry scanner
        manager.trigger_recovery("node-1")     # manual trigger
        manager.stop_auto_recovery()           # cleanup
    """

    def __init__(self, state: "ClusterState") -> None:
        self._state = state
        self._revoker = Revoker(state)
        self._rescheduler = Rescheduler(state)
        self._detector = Detector(self._revoker, self._rescheduler, state)

        # Auto-recovery config
        self._auto_recovery_enabled = False
        self._scan_interval: float = 30.0  # seconds between lease expiry scans
        self._auto_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()

        # Stats
        self._total_recoveries: int = 0
        self._total_revoked: int = 0
        self._total_rescheduled: int = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def trigger_recovery(self, node_id: str) -> Dict[str, Any]:
        """Manually trigger recovery for a node.

        This runs the full pipeline synchronously:
        1. Revoke all leases for the node
        2. Reschedule orphaned tasks
        3. Log all events

        Returns:
            Summary dict with counts and status.
        """
        with self._lock:
            self._total_recoveries += 1

        # Step 1: Revoke leases
        revoked_task_ids = self._revoker.revoke_all_for_node(node_id)
        with self._lock:
            self._total_revoked += len(revoked_task_ids)

        # Step 2: Reschedule
        if revoked_task_ids:
            rescheduled = self._rescheduler.reschedule_orphaned(revoked_task_ids)
            with self._lock:
                self._total_rescheduled += rescheduled
            status = "completed" if rescheduled == len(revoked_task_ids) else "partial"
            message = (
                f"Revoked {len(revoked_task_ids)} leases, "
                f"rescheduled {rescheduled}/{len(revoked_task_ids)} tasks"
            )
        else:
            rescheduled = 0
            status = "completed"
            message = "No active leases to revoke"

        # Step 3: Log the full recovery event
        event = RecoveryEvent(
            id=_gen_id(),
            node_id=node_id,
            action="full_recovery",
            status=status,
            message=message,
        )
        self._state.append_recovery_event(event)

        return {
            "status": "accepted",
            "node_id": node_id,
            "revoked": len(revoked_task_ids),
            "rescheduled": rescheduled,
            "recovery_status": status,
            "message": message,
        }

    def trigger_recovery_async(self, node_id: str) -> None:
        """Trigger recovery in the background (non-blocking).

        Uses the Detector's event queue for async processing.
        """
        self._detector.notify_offline(node_id)

    def get_recovery_events(self) -> List[RecoveryEvent]:
        """Get all recovery events from the log."""
        return self._state.get_recovery_events()

    def get_recovery_stats(self) -> Dict[str, Any]:
        """Get recovery statistics.

        Returns:
            Dict with total counts, by_action breakdown, and manager stats.
        """
        base_stats = self._state.recovery_stats()
        with self._lock:
            base_stats.update({
                "total_triggers": self._total_recoveries,
                "total_revoked": self._total_revoked,
                "total_rescheduled": self._total_rescheduled,
                "auto_recovery_enabled": self._auto_recovery_enabled,
                "scan_interval_seconds": self._scan_interval,
            })
        return base_stats

    # ------------------------------------------------------------------
    # Auto-recovery (background lease expiry scanner)
    # ------------------------------------------------------------------

    def start_auto_recovery(self, scan_interval: float = 30.0) -> None:
        """Start the auto-recovery background thread.

        Periodically scans for expired leases and triggers recovery
        for nodes that lost their leases.

        Args:
            scan_interval: Seconds between scans (default 30s).
        """
        with self._lock:
            if self._auto_recovery_enabled:
                return
            self._auto_recovery_enabled = True
            self._scan_interval = scan_interval
            self._stop_event.clear()

        self._auto_thread = threading.Thread(
            target=self._auto_recovery_loop,
            daemon=True,
            name="recovery-auto",
        )
        self._auto_thread.start()
        logger.info("Auto-recovery started (interval=%ss)", scan_interval)

    def stop_auto_recovery(self) -> None:
        """Stop the auto-recovery background thread."""
        with self._lock:
            if not self._auto_recovery_enabled:
                return
            self._auto_recovery_enabled = False
            self._stop_event.set()

        if self._auto_thread and self._auto_thread.is_alive():
            self._auto_thread.join(timeout=5.0)
        logger.info("Auto-recovery stopped")

    @property
    def is_auto_recovery_enabled(self) -> bool:
        with self._lock:
            return self._auto_recovery_enabled

    def detect_expired_leases(self) -> Dict[str, Any]:
        """Manually trigger expired lease detection.

        Scans all active leases, marks expired ones, and triggers
        recovery for affected nodes.

        Returns:
            Summary of expired leases and recovery actions.
        """
        # This triggers the lease manager's expiry check
        active_leases = self._state.get_active_leases()

        # Check for tasks that were assigned but their leases expired
        expired_nodes = set()
        with self._state._leases_lock:
            for lease in self._state._leases.values():
                if lease.status.value == "expired":
                    expired_nodes.add(lease.node_id)

        recovered_nodes = []
        for node_id in expired_nodes:
            self.trigger_recovery(node_id)
            recovered_nodes.append(node_id)

        return {
            "expired_nodes": list(expired_nodes),
            "recovered_nodes": recovered_nodes,
            "active_leases": len(active_leases),
        }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _auto_recovery_loop(self) -> None:
        """Background loop that scans for expired leases periodically."""
        while not self._stop_event.is_set():
            try:
                self.detect_expired_leases()
            except Exception as e:
                logger.error("Auto-recovery scan error: %s", e)

            # Wait for stop or interval
            self._stop_event.wait(timeout=self._scan_interval)

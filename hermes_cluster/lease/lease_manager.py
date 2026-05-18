"""Lease Manager — create/revoke/list + background TTL expiry scanner.

Wraps ClusterStore lease CRUD and adds:
  - Background thread scanning for expired leases at configurable scan_rate
  - Callback registration for lease expiry events
  - Thread-safe API with proper lifecycle management (start/stop)
  - Lease metrics (active/expired counts)

Design:
  LeaseManager owns a background daemon thread that periodically calls
  ClusterStore.get_active_leases(), which internally marks expired leases
  and fires registered callbacks. The manager adds orchestration on top:
  metric tracking, callback management, and clean lifecycle.

Usage:
    store = ClusterStore(":memory:")
    config = LeaseConfig(ttl=timedelta(seconds=60), scan_rate=timedelta(seconds=10))
    manager = LeaseManager(store, config)

    # Register expiry callback
    manager.on_expire(lambda task_id, node_id: print(f"Expired: {task_id}"))

    # Start background scanner
    manager.start()

    # Create a lease
    lease = manager.create(task_id="t_123", node_id="node_a")

    # Revoke
    manager.revoke(lease.id)

    # List active leases
    active = manager.list_active()

    # Stop scanner
    manager.stop()
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timedelta
from typing import Callable, List, Optional

from ..models import Lease, LeaseConfig, LeaseMetric, LeaseStatus
from ..state.cluster_store import ClusterStore

logger = logging.getLogger(__name__)

# Type alias for expiry callback: (task_id, node_id) -> None
ExpiryCallback = Callable[[str, str], None]


class LeaseManager:
    """Thread-safe lease manager with background TTL expiry scanning.

    Lifecycle:
        1. Construct with a ClusterStore and LeaseConfig
        2. Register callbacks via on_expire()
        3. Call start() to launch the background scanner
        4. Use create/revoke/list/extend API
        5. Call stop() for clean shutdown

    Thread safety:
        All public methods are safe to call from any thread.
        The background scanner runs in its own daemon thread.
    """

    def __init__(self, store: ClusterStore, config: Optional[LeaseConfig] = None):
        self._store = store
        self._config = config or LeaseConfig()
        self._callbacks: List[ExpiryCallback] = []
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._running = False

        # Metrics (updated on each scan)
        self._metrics = LeaseMetric(active_count=0, expired_count=0)
        self._total_created = 0
        self._total_revoked = 0
        self._total_expired = 0

        # Wire up store callback to fan out to registered callbacks
        self._store.set_lease_callback(self._on_lease_expired)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the background TTL expiry scanner."""
        with self._lock:
            if self._running:
                logger.warning("LeaseManager already running")
                return
            self._stop_event.clear()
            self._running = True
            self._thread = threading.Thread(
                target=self._scanner_loop,
                name="lease-scanner",
                daemon=True,
            )
            self._thread.start()
            logger.info(
                "LeaseManager started (ttl=%ss, scan_rate=%ss)",
                self._config.ttl.total_seconds(),
                self._config.scan_rate.total_seconds(),
            )

    def stop(self, timeout: float = 5.0) -> None:
        """Stop the background scanner and wait for it to finish."""
        with self._lock:
            if not self._running:
                return
            self._running = False
            self._stop_event.set()

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
            if self._thread.is_alive():
                logger.warning("LeaseManager scanner thread did not stop within %ss", timeout)
        logger.info("LeaseManager stopped")

    @property
    def is_running(self) -> bool:
        return self._running

    # ------------------------------------------------------------------
    # Callback registration
    # ------------------------------------------------------------------

    def on_expire(self, callback: ExpiryCallback) -> None:
        """Register a callback for lease expiry events.

        Callback signature: (task_id: str, node_id: str) -> None
        Multiple callbacks can be registered; they are called in order.
        Exceptions in callbacks are caught and logged (never crash the scanner).
        """
        with self._lock:
            self._callbacks.append(callback)

    def remove_callback(self, callback: ExpiryCallback) -> bool:
        """Remove a registered callback. Returns True if found and removed."""
        with self._lock:
            try:
                self._callbacks.remove(callback)
                return True
            except ValueError:
                return False

    def clear_callbacks(self) -> None:
        """Remove all registered callbacks."""
        with self._lock:
            self._callbacks.clear()

    # ------------------------------------------------------------------
    # Lease CRUD
    # ------------------------------------------------------------------

    def create(
        self,
        task_id: str,
        node_id: str,
        ttl: Optional[timedelta] = None,
    ) -> Lease:
        """Create a new lease for a task on a node.

        Args:
            task_id: The task this lease is for.
            node_id: The node holding this lease.
            ttl: Time-to-live. Defaults to config.ttl.

        Returns:
            The created Lease with status ACTIVE.

        Raises:
            ValueError: If task_id or node_id is empty.
        """
        if not task_id:
            raise ValueError("task_id is required")
        if not node_id:
            raise ValueError("node_id is required")

        effective_ttl = ttl or self._config.ttl
        lease = self._store.create_lease(task_id, node_id, effective_ttl)
        if lease:
            with self._lock:
                self._total_created += 1
            logger.debug("Lease created: %s (task=%s, node=%s, ttl=%ss)",
                         lease.id, task_id, node_id, effective_ttl.total_seconds())
        return lease

    def revoke(self, lease_id: str) -> bool:
        """Revoke a lease by ID.

        Returns True if the lease existed and was revoked.
        Returns False if the lease was not found or already terminal.
        """
        if not lease_id:
            return False
        result = self._store.revoke_lease(lease_id)
        if result:
            with self._lock:
                self._total_revoked += 1
            logger.debug("Lease revoked: %s", lease_id)
        return result

    def list_active(self) -> List[Lease]:
        """List all currently active (non-expired, non-revoked) leases.

        Triggers expiry scan as a side effect — expired leases found during
        this call are marked and their callbacks fired.
        """
        return self._store.get_active_leases()

    def get_by_task(self, task_id: str) -> Optional[Lease]:
        """Get the active lease for a given task, if any."""
        if not task_id:
            return None
        return self._store.get_lease_by_task(task_id)

    def extend(self, lease_id: str, additional_ttl: Optional[timedelta] = None) -> Optional[Lease]:
        """Extend a lease's expiry by additional_ttl (default: config.ttl).

        Only works on ACTIVE leases. Returns the updated lease, or None if
        the lease was not found or not active.

        Thread-safe: the entire revoke+create is performed under a lock to
        prevent race conditions where concurrent calls could create duplicate
        leases for the same task.
        """
        if not lease_id:
            return None

        effective_ttl = additional_ttl or self._config.ttl

        # Hold the lock for the entire lookup → create → revoke sequence
        # to prevent concurrent extend() calls from creating duplicate leases.
        with self._lock:
            # Look up the active lease via store (which also updates metrics)
            active_leases = self._store.get_active_leases()
            for lease in active_leases:
                if lease.id == lease_id:
                    # Create new lease first, then revoke old — this ensures
                    # we always have a valid lease if the create succeeds.
                    new_lease = self._store.create_lease(
                        lease.task_id, lease.node_id, effective_ttl
                    )
                    if new_lease:
                        # Revoke old lease (marks as revoked, not expired)
                        self._store.revoke_lease(lease_id)
                        logger.debug(
                            "Lease extended: %s -> %s (task=%s, +%ss)",
                            lease_id, new_lease.id, lease.task_id,
                            effective_ttl.total_seconds(),
                        )
                    return new_lease

        return None

    # ------------------------------------------------------------------
    # Metrics
    # ------------------------------------------------------------------

    def metrics(self) -> LeaseMetric:
        """Return current lease metrics (active/expired counts).

        Metrics are updated on each scanner tick.
        """
        with self._lock:
            return LeaseMetric(
                active_count=self._metrics.active_count,
                expired_count=self._metrics.expired_count,
            )

    def stats(self) -> dict:
        """Return detailed lease statistics."""
        active = self.list_active()
        with self._lock:
            return {
                "active_count": len(active),
                "total_created": self._total_created,
                "total_revoked": self._total_revoked,
                "total_expired": self._total_expired,
                "scan_rate_seconds": self._config.scan_rate.total_seconds(),
                "ttl_seconds": self._config.ttl.total_seconds(),
                "is_running": self._running,
            }

    # ------------------------------------------------------------------
    # Internal: scanner thread
    # ------------------------------------------------------------------

    def _scanner_loop(self) -> None:
        """Background loop that scans for expired leases."""
        logger.debug("Scanner loop started")
        while not self._stop_event.is_set():
            try:
                self._scan_once()
            except Exception:
                logger.exception("Error in lease scanner")
            self._stop_event.wait(timeout=self._config.scan_rate.total_seconds())
        logger.debug("Scanner loop ended")

    def _scan_once(self) -> None:
        """Single scan pass: list_active triggers expiry + callbacks."""
        active = self._store.get_active_leases()
        # get_active_leases already handles expiry marking + callbacks.
        # We just update metrics here.
        with self._lock:
            prev_expired = self._total_expired
            self._metrics.active_count = len(active)
            # get_active_leases internally marked expired ones — we detect
            # the count change via the callback
        logger.debug("Scan: %d active leases", len(active))

    def _on_lease_expired(self, task_id: str, node_id: str) -> None:
        """Internal callback from ClusterStore when a lease expires."""
        with self._lock:
            self._total_expired += 1
            callbacks = list(self._callbacks)
        logger.info("Lease expired: task=%s, node=%s", task_id, node_id)
        for cb in callbacks:
            try:
                cb(task_id, node_id)
            except Exception:
                logger.exception("Error in expiry callback for task=%s", task_id)

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "LeaseManager":
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop()
        return None

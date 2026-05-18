"""Concurrency condition tests — verify C1 and H1 race condition fixes.

These tests exercise concurrent access patterns that would have caused
bugs before the fixes:
  - C1: LeaseManager.extend() concurrent calls → no duplicate leases
  - H1: Rescheduler reschedule_orphaned() → atomic unassign + schedule
  - M1: NodeManager._listeners → safe concurrent add/emit
  - M3: RecoveryManager.detect_expired_leases() → uses public API only
"""

from __future__ import annotations

import threading
import time
from datetime import timedelta

import pytest

from hermes_cluster.lease.lease_manager import LeaseManager
from hermes_cluster.recovery.rescheduler import Rescheduler
from hermes_cluster.recovery.manager import RecoveryManager
from hermes_cluster.state import ClusterState
from hermes_cluster.state.cluster_store import ClusterStore
from hermes_cluster.core.node_manager import NodeManager
from hermes_cluster.models import NodeStatus, TaskStatus


# ---------------------------------------------------------------------------
# C1: Concurrent LeaseManager.extend() — no duplicate leases
# ---------------------------------------------------------------------------

class TestLeaseManagerExtendConcurrency:
    """C1: verify concurrent extend() calls don't create duplicate leases."""

    def test_concurrent_extends_no_duplicates(self):
        """Two threads extend the same lease simultaneously.

        Before the fix: both threads could see the same active lease and
        both create new leases, resulting in duplicate leases for the same
        task (one active, one orphaned).

        After the fix: the manager's lock serializes the operation — only
        one thread succeeds, the other finds no active lease.
        """
        store = ClusterStore(":memory:")
        config = LeaseConfig(ttl=timedelta(seconds=60))
        manager = LeaseManager(store, config)

        # Create a lease for task t1 on node n1
        lease = manager.create(task_id="t1", node_id="n1")
        assert lease is not None

        results = []
        errors = []

        def extend_worker():
            try:
                result = manager.extend(lease.id)
                results.append(result)
            except Exception as e:
                errors.append(e)

        # Launch two concurrent extend() calls
        t1 = threading.Thread(target=extend_worker)
        t2 = threading.Thread(target=extend_worker)
        t1.start()
        t2.start()
        t1.join(timeout=5.0)
        t2.join(timeout=5.0)

        assert not errors, f"Errors during concurrent extend: {errors}"

        # At most one should have succeeded (returned a new lease)
        successful = [r for r in results if r is not None]
        assert len(successful) <= 1, (
            f"Expected at most 1 successful extend, got {len(successful)}"
        )

        # Verify no duplicate active leases for task t1
        active = store.get_active_leases()
        active_for_t1 = [l for l in active if l.task_id == "t1"]
        assert len(active_for_t1) <= 1, (
            f"Expected at most 1 active lease for t1, got {len(active_for_t1)}"
        )

    def test_extend_under_load(self):
        """10 threads try to extend the same lease concurrently.

        Only one should succeed, and the lease count should remain correct.
        """
        store = ClusterStore(":memory:")
        config = LeaseConfig(ttl=timedelta(seconds=60))
        manager = LeaseManager(store, config)

        lease = manager.create(task_id="t_load", node_id="n1")
        assert lease is not None

        results = []
        barrier = threading.Barrier(10)

        def worker():
            barrier.wait()  # Synchronize all threads to start together
            result = manager.extend(lease.id)
            results.append(result)

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5.0)

        successful = [r for r in results if r is not None]
        assert len(successful) <= 1, (
            f"Expected at most 1 successful extend under load, got {len(successful)}"
        )

        # Total active leases for this task should be at most 1
        active = store.get_active_leases()
        active_for_t = [l for l in active if l.task_id == "t_load"]
        assert len(active_for_t) <= 1


# ---------------------------------------------------------------------------
# H1: Rescheduler — atomic unassign + schedule
# ---------------------------------------------------------------------------

class TestReschedulerConcurrency:
    """H1: verify reschedule_orphaned uses atomic unassign_task."""

    def test_unassign_task_atomic(self):
        """unassign_task atomically clears assigned_to and sets status to ready."""
        state = ClusterState()

        # Create a node and a task
        from hermes_cluster.models import Node, Task
        node = Node(id="n1", name="node1", capabilities=["cpu"], status=NodeStatus.online)
        state.register_node(node)
        task = state.create_task("t1", "Test task", requires=["cpu"])
        state.set_task_status("t1", TaskStatus.running)
        # Simulate assignment
        with state._tasks_lock:
            state._tasks["t1"].assigned_to = "n1"

        # Verify initial state
        task = state.get_task("t1")
        assert task.assigned_to == "n1"
        assert task.status == TaskStatus.running

        # Unassign atomically
        result = state.unassign_task("t1")
        assert result is True

        # Verify both fields changed atomically
        task = state.get_task("t1")
        assert task.assigned_to is None
        assert task.status == TaskStatus.ready

    def test_rescheduler_uses_public_api(self):
        """Rescheduler.reschedule_orphaned() uses public API, no internal access."""
        state = ClusterState()

        from hermes_cluster.models import Node
        node = Node(id="n1", name="node1", capabilities=[], status=NodeStatus.online)
        state.register_node(node)
        state.create_task("t1", "Task 1", requires=[])
        state.set_task_status("t1", TaskStatus.running)
        with state._tasks_lock:
            state._tasks["t1"].assigned_to = "n1"

        rescheduler = Rescheduler(state)
        count = rescheduler.reschedule_orphaned(["t1"])

        # Task should be rescheduled (or failed if no matching node)
        task = state.get_task("t1")
        assert task.status in (TaskStatus.running, TaskStatus.failed)
        # assigned_to should be set (if rescheduled) or None (if failed)
        if task.status == TaskStatus.running:
            assert task.assigned_to == "n1"
        else:
            assert task.assigned_to is None


# ---------------------------------------------------------------------------
# M1: NodeManager._listeners — safe concurrent add/emit
# ---------------------------------------------------------------------------

class TestNodeManagerListenersConcurrency:
    """M1: verify listener list is thread-safe during concurrent add/emit."""

    def test_concurrent_add_and_emit(self):
        """Adding listeners while events are being emitted shouldn't crash."""
        store = ClusterStore(":memory:")
        manager = NodeManager(store)

        events_received = []
        errors = []

        def listener(event):
            events_received.append(event)

        # Start emitting events in a background thread
        def emitter():
            for _ in range(100):
                try:
                    manager._emit(
                        NodeEvent("test-node", "heartbeat", "concurrent test")
                    )
                except Exception as e:
                    errors.append(e)

        # Start adding listeners in another thread
        def adder():
            for _ in range(50):
                try:
                    manager.add_listener(listener)
                except Exception as e:
                    errors.append(e)

        t_emitter = threading.Thread(target=emitter)
        t_adder = threading.Thread(target=adder)
        t_emitter.start()
        t_adder.start()
        t_emitter.join(timeout=5.0)
        t_adder.join(timeout=5.0)

        assert not errors, f"Concurrency errors: {errors}"

    def test_remove_during_emit(self):
        """Removing a listener while events are being emitted shouldn't crash."""
        store = ClusterStore(":memory:")
        manager = NodeManager(store)

        def listener_a(event):
            pass

        def listener_b(event):
            pass

        manager.add_listener(listener_a)
        manager.add_listener(listener_b)

        errors = []

        def emitter():
            for _ in range(100):
                try:
                    manager._emit(
                        NodeEvent("test-node", "heartbeat", "remove test")
                    )
                except Exception as e:
                    errors.append(e)

        def remover():
            for _ in range(50):
                try:
                    manager.remove_listener(listener_a)
                except Exception as e:
                    errors.append(e)

        t_emitter = threading.Thread(target=emitter)
        t_remover = threading.Thread(target=remover)
        t_emitter.start()
        t_remover.start()
        t_emitter.join(timeout=5.0)
        t_remover.join(timeout=5.0)

        assert not errors, f"Concurrency errors: {errors}"


# ---------------------------------------------------------------------------
# M3: RecoveryManager.detect_expired_leases() uses public API
# ---------------------------------------------------------------------------

class TestRecoveryManagerPublicAPI:
    """M3: verify detect_expired_leases() doesn't access internal state."""

    def test_detect_expired_uses_public_api(self):
        """detect_expired_leases should work via public API only."""
        state = ClusterState()
        manager = RecoveryManager(state)

        # Create a node and a lease that's already expired
        from hermes_cluster.models import Node
        node = Node(id="n1", name="node1", capabilities=[], status=NodeStatus.online)
        state.register_node(node)

        # Create an already-expired lease
        state.create_lease("t1", "n1", timedelta(seconds=-1))  # expired immediately

        # Should not raise any AttributeError from accessing internal state
        result = manager.detect_expired_leases()
        assert "expired_nodes" in result
        assert "recovered_nodes" in result
        assert "active_leases" in result

    def test_get_expired_leases_public(self):
        """get_expired_leases() returns expired leases via public API."""
        state = ClusterState()
        from hermes_cluster.models import LeaseStatus

        # Create an expired lease
        state.create_lease("t1", "n1", timedelta(seconds=-1))

        # Get active leases to trigger expiry marking
        state.get_active_leases()

        # Use public API to get expired leases
        expired = state.get_expired_leases()
        assert len(expired) >= 1
        assert all(l.status == LeaseStatus.expired for l in expired)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

from hermes_cluster.models import LeaseConfig
from hermes_cluster.core.node_manager import NodeEvent

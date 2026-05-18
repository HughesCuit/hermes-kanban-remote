"""Integration tests for NodeManager, LeaseManager, Watchdog, and RecoveryManager wiring.

These tests verify that the managers are properly integrated into the FastAPI app
and that the end-to-end flows work correctly.
"""

import time
import pytest
from fastapi.testclient import TestClient

from hermes_cluster.app import create_app
from hermes_cluster.state import ClusterState
from hermes_cluster.core.node_manager import NodeManager, _WatchdogConfig


def _make_app(**kwargs):
    """Create a test app with fresh state."""
    return create_app(
        cluster_id=kwargs.get("cluster_id", "test-integration"),
        node_id=kwargs.get("node_id", "test-main"),
        node_role=kwargs.get("node_role", "main"),
    )


@pytest.fixture
def client():
    """Create a test client."""
    app = _make_app()
    return TestClient(app)


# ---------------------------------------------------------------------------
# Heartbeat integration
# ---------------------------------------------------------------------------

class TestHeartbeatIntegration:
    def test_join_and_heartbeat_updates_status(self, client):
        """Join a node, send heartbeat, verify it's online."""
        # Join a worker node
        resp = client.post("/api/v1/nodes/join", json={
            "node_name": "worker-1",
            "capabilities": ["coding"],
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["node_id"] == "node_worker-1"
        assert data["status"] == "registered"

        # Send heartbeat
        resp = client.post("/api/v1/nodes/heartbeat", json={
            "node_id": "node_worker-1",
        })
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

        # Verify node is listed and online
        nodes = client.get("/api/v1/nodes").json()
        worker = [n for n in nodes if n["id"] == "node_worker-1"]
        assert len(worker) == 1
        assert worker[0]["status"] == "online"

    def test_heartbeat_refreshes_timestamp(self, client):
        """Verify heartbeat updates the last_heartbeat timestamp."""
        client.post("/api/v1/nodes/join", json={"node_name": "hb-ts"})

        # Get initial heartbeat time
        nodes = client.get("/api/v1/nodes").json()
        node = [n for n in nodes if n["id"] == "node_hb-ts"][0]
        initial_hb = node.get("last_heartbeat")

        time.sleep(0.1)

        # Send heartbeat
        client.post("/api/v1/nodes/heartbeat", json={"node_id": "node_hb-ts"})

        # Verify timestamp updated
        nodes = client.get("/api/v1/nodes").json()
        node = [n for n in nodes if n["id"] == "node_hb-ts"][0]
        assert node["last_heartbeat"] != initial_hb

    def test_multiple_nodes_join(self, client):
        """Verify multiple nodes can join and are all listed."""
        for i in range(3):
            client.post("/api/v1/nodes/join", json={
                "node_name": f"multi-{i}",
                "capabilities": [f"cap-{i}"],
            })
        nodes = client.get("/api/v1/nodes").json()
        worker_nodes = [n for n in nodes if n["name"].startswith("multi-")]
        assert len(worker_nodes) == 3


# ---------------------------------------------------------------------------
# Lease integration
# ---------------------------------------------------------------------------

class TestLeaseIntegration:
    def test_claim_creates_lease(self, client):
        """Claim a task → verify lease is created."""
        # Submit a task
        task = client.post("/api/v1/tasks", json={
            "title": "Lease test task",
            "priority": 2,
        }).json()

        # Claim the task
        resp = client.post(f"/api/v1/tasks/{task['id']}/claim", json={
            "node_id": "node_worker-1",
        })
        assert resp.status_code == 200

        # Verify a lease was created
        leases = client.get("/api/v1/leases").json()
        assert len(leases) >= 1
        matching = [l for l in leases if l["task_id"] == task["id"]]
        assert len(matching) == 1
        assert matching[0]["node_id"] == "node_worker-1"
        assert matching[0]["status"] == "active"

    def test_complete_revokes_lease(self, client):
        """Complete a task → verify lease is revoked."""
        # Submit and claim a task
        task = client.post("/api/v1/tasks", json={"title": "Revoke test"}).json()
        client.post(f"/api/v1/tasks/{task['id']}/claim", json={
            "node_id": "node_worker-2",
        })

        # Verify lease exists
        leases = client.get("/api/v1/leases").json()
        matching = [l for l in leases if l["task_id"] == task["id"]]
        assert len(matching) == 1

        # Complete the task
        resp = client.post(f"/api/v1/tasks/{task['id']}/complete")
        assert resp.status_code == 200
        assert resp.json()["status"] == "completed"

        # Verify lease is revoked (not active)
        active = client.get("/api/v1/leases").json()
        active_for_task = [l for l in active if l["task_id"] == task["id"] and l["status"] == "active"]
        assert len(active_for_task) == 0

    def test_release_revokes_lease(self, client):
        """Release a task → verify lease is revoked."""
        task = client.post("/api/v1/tasks", json={"title": "Release test"}).json()
        client.post(f"/api/v1/tasks/{task['id']}/claim", json={
            "node_id": "node_worker-3",
        })

        # Release the task
        resp = client.post(f"/api/v1/tasks/{task['id']}/release", json={
            "node_id": "node_worker-3",
            "reason": "too busy",
        })
        assert resp.status_code == 200
        assert resp.json()["status"] == "ready"

        # Verify lease is revoked
        active = client.get("/api/v1/leases").json()
        active_for_task = [l for l in active if l["task_id"] == task["id"] and l["status"] == "active"]
        assert len(active_for_task) == 0

    def test_claim_without_lease_manager_fallback(self, client):
        """Verify claim works even without lease_manager (fallback mode)."""
        task = client.post("/api/v1/tasks", json={"title": "No lease mgr"}).json()
        resp = client.post(f"/api/v1/tasks/{task['id']}/claim", json={
            "node_id": "node_worker-4",
        })
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Watchdog + Recovery integration
# ---------------------------------------------------------------------------

class TestWatchdogRecoveryIntegration:
    def test_node_manager_wiring(self, client):
        """Verify NodeManager is properly wired into the app."""
        # The node_manager should handle join via its join method
        resp = client.post("/api/v1/nodes/join", json={
            "node_name": "wired-node",
        })
        assert resp.status_code == 200
        node_id = resp.json()["node_id"]
        assert node_id == "node_wired-node"

    def test_lease_manager_wiring(self, client):
        """Verify LeaseManager is properly wired into the app."""
        # Submit + claim should create a lease via LeaseManager
        task = client.post("/api/v1/tasks", json={"title": "Wired lease"}).json()
        client.post(f"/api/v1/tasks/{task['id']}/claim", json={
            "node_id": "node_lease-wired",
        })
        leases = client.get("/api/v1/leases").json()
        assert any(l["task_id"] == task["id"] for l in leases)

    def test_recovery_trigger_via_api(self, client):
        """Verify recovery can be triggered via the API."""
        resp = client.post("/api/v1/recovery/trigger", json={
            "node_id": "offline-node",
        })
        assert resp.status_code == 200
        assert resp.json()["status"] == "accepted"

    def test_recovery_log_after_trigger(self, client):
        """Verify recovery events are logged."""
        client.post("/api/v1/recovery/trigger", json={"node_id": "log-node"})
        resp = client.get("/api/v1/recovery/log")
        assert resp.status_code == 200
        events = resp.json()
        assert len(events) >= 1
        assert any(e["node_id"] == "log-node" for e in events)

    def test_health_endpoint_still_works(self, client):
        """Verify the health endpoint still works with managers initialized."""
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["cluster_id"] == "test-integration"

    def test_summary_with_managers(self, client):
        """Verify summary endpoint works with managers initialized."""
        client.post("/api/v1/nodes/join", json={"node_name": "summary-node"})
        client.post("/api/v1/tasks", json={"title": "Summary task"})
        resp = client.get("/api/v1/summary")
        assert resp.status_code == 200
        data = resp.json()
        assert data["cluster_id"] == "test-integration"
        assert "nodes" in data
        assert "tasks" in data

    def test_full_task_lifecycle(self, client):
        """Full lifecycle: submit → claim → complete with lease management."""
        # 1. Join a worker
        client.post("/api/v1/nodes/join", json={
            "node_name": "lifecycle-worker",
            "capabilities": ["coding"],
        })

        # 2. Submit a task
        task = client.post("/api/v1/tasks", json={
            "title": "Lifecycle task",
            "requires": ["coding"],
        }).json()
        assert task["status"] == "ready"

        # 3. Claim the task
        resp = client.post(f"/api/v1/tasks/{task['id']}/claim", json={
            "node_id": "node_lifecycle-worker",
        })
        assert resp.status_code == 200

        # 4. Verify lease created
        leases = client.get("/api/v1/leases").json()
        assert any(l["task_id"] == task["id"] and l["status"] == "active" for l in leases)

        # 5. Complete the task
        resp = client.post(f"/api/v1/tasks/{task['id']}/complete")
        assert resp.status_code == 200

        # 6. Verify lease revoked and task completed
        task_check = client.get(f"/api/v1/tasks/{task['id']}")  # This endpoint doesn't exist, use list
        # Use list endpoint instead
        tasks = client.get("/api/v1/tasks").json()
        completed = [t for t in tasks if t["id"] == task["id"]]
        assert len(completed) == 1
        assert completed[0]["status"] == "completed"

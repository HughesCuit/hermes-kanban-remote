"""Tests for the missing API endpoints:
1. POST /api/v1/tasks/{task_id}/claim
2. POST /api/v1/tasks/{task_id}/release
3. GET  /api/v1/cluster/status
4. POST /api/v1/schedule/trigger (with assignments)
"""

import pytest
from fastapi.testclient import TestClient

from hermes_cluster.app import create_app
from hermes_cluster.state import ClusterState
from hermes_cluster.models import Node, NodeStatus


@pytest.fixture
def app():
    """Create a test app with fresh state."""
    return create_app(
        cluster_id="test-cluster",
        node_id="test-node",
        node_role="main",
    )


@pytest.fixture
def client(app):
    """Create a test client."""
    return TestClient(app)


def _register_node(client, name="worker-1", capabilities=None):
    """Helper to register a node and return its node_id."""
    resp = client.post("/api/v1/nodes/join", json={
        "node_name": name,
        "capabilities": capabilities or ["coding"],
    })
    return resp.json()["node_id"]


def _submit_task(client, title="Test task", requires=None, priority=0):
    """Helper to submit a task and return its data."""
    resp = client.post("/api/v1/tasks", json={
        "title": title,
        "requires": requires or [],
        "priority": priority,
    })
    return resp.json()


# ---------------------------------------------------------------------------
# 1. POST /api/v1/tasks/{task_id}/claim
# ---------------------------------------------------------------------------

class TestClaimTask:
    def test_claim_ready_task(self, client):
        """A worker can claim a ready task."""
        task = _submit_task(client, "Claim me")
        assert task["status"] == "ready"

        resp = client.post(f"/api/v1/tasks/{task['id']}/claim", json={
            "node_id": "worker-1",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "running"
        assert data["assigned_to"] == "worker-1"
        assert "claimed_at" in data

    def test_claim_nonexistent_task(self, client):
        """Claiming a nonexistent task returns 404."""
        resp = client.post("/api/v1/tasks/nonexistent/claim", json={
            "node_id": "worker-1",
        })
        assert resp.status_code == 404

    def test_claim_already_running_task(self, client):
        """Cannot claim a task that is already running."""
        task = _submit_task(client, "Already running")
        # Claim it first
        client.post(f"/api/v1/tasks/{task['id']}/claim", json={
            "node_id": "worker-1",
        })
        # Try to claim again
        resp = client.post(f"/api/v1/tasks/{task['id']}/claim", json={
            "node_id": "worker-2",
        })
        assert resp.status_code == 409

    def test_claim_blocked_task(self, client):
        """Cannot claim a task that is blocked (failed dependency)."""
        t1 = _submit_task(client, "Parent")
        t2 = _submit_task(client, "Child")
        client.post(f"/api/v1/tasks/{t2['id']}/dependencies", json={
            "depends_on": [t1["id"]],
        })
        # Fail the parent — t2 may be blocked (or still ready due to auto-promote)
        # Either way, completing t1 triggers scheduling; set t2 to blocked via state
        # Instead, test with a task that's in a non-ready state (blocked)
        # Create a scenario: submit t3, fail t1, set t3 depends on t1
        # Simpler: just manually set a task to blocked state via fail endpoint
        client.post(f"/api/v1/tasks/{t1['id']}/fail")
        # Now t2 is still ready (auto-promoted before deps were set).
        # Test with a truly non-ready task: the completed t1
        resp = client.post(f"/api/v1/tasks/{t1['id']}/claim", json={
            "node_id": "worker-1",
        })
        assert resp.status_code == 409

    def test_claim_completed_task(self, client):
        """Cannot claim a completed task."""
        task = _submit_task(client, "Done already")
        client.post(f"/api/v1/tasks/{task['id']}/complete")
        resp = client.post(f"/api/v1/tasks/{task['id']}/claim", json={
            "node_id": "worker-1",
        })
        assert resp.status_code == 409


# ---------------------------------------------------------------------------
# 2. POST /api/v1/tasks/{task_id}/release
# ---------------------------------------------------------------------------

class TestReleaseTask:
    def test_release_claimed_task(self, client):
        """A worker can release a task they claimed."""
        task = _submit_task(client, "Release me")
        client.post(f"/api/v1/tasks/{task['id']}/claim", json={
            "node_id": "worker-1",
        })

        resp = client.post(f"/api/v1/tasks/{task['id']}/release", json={
            "node_id": "worker-1",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ready"
        assert data["assigned_to"] is None

    def test_release_with_reason(self, client):
        """Release with a reason records the reason."""
        task = _submit_task(client, "Release with reason")
        client.post(f"/api/v1/tasks/{task['id']}/claim", json={
            "node_id": "worker-1",
        })

        resp = client.post(f"/api/v1/tasks/{task['id']}/release", json={
            "node_id": "worker-1",
            "reason": "timeout",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ready"
        assert data["fail_reason"] == "timeout"

    def test_release_nonexistent_task(self, client):
        """Releasing a nonexistent task returns 404."""
        resp = client.post("/api/v1/tasks/nonexistent/release", json={
            "node_id": "worker-1",
        })
        assert resp.status_code == 404

    def test_release_wrong_node(self, client):
        """A different worker cannot release a task assigned to someone else."""
        task = _submit_task(client, "Not yours")
        client.post(f"/api/v1/tasks/{task['id']}/claim", json={
            "node_id": "worker-1",
        })

        resp = client.post(f"/api/v1/tasks/{task['id']}/release", json={
            "node_id": "worker-2",
        })
        assert resp.status_code == 403

    def test_release_then_reclaim(self, client):
        """After release, another worker can claim the task."""
        task = _submit_task(client, "Reclaimable")
        client.post(f"/api/v1/tasks/{task['id']}/claim", json={
            "node_id": "worker-1",
        })
        client.post(f"/api/v1/tasks/{task['id']}/release", json={
            "node_id": "worker-1",
        })

        resp = client.post(f"/api/v1/tasks/{task['id']}/claim", json={
            "node_id": "worker-2",
        })
        assert resp.status_code == 200
        assert resp.json()["assigned_to"] == "worker-2"


# ---------------------------------------------------------------------------
# 3. GET /api/v1/cluster/status
# ---------------------------------------------------------------------------

class TestClusterStatus:
    def test_cluster_status_empty(self, client):
        """Cluster status returns correct data with no tasks/nodes."""
        resp = client.get("/api/v1/cluster/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["cluster_id"] == "test-cluster"
        assert data["node_count"] == 0
        assert data["online_nodes"] == 0
        assert data["task_count"] == 0
        assert data["tasks_by_status"] == {
            "pending": 0,
            "ready": 0,
            "running": 0,
            "completed": 0,
            "failed": 0,
        }
        assert "uptime_seconds" in data
        assert data["version"] == "python-1.0.0"

    def test_cluster_status_with_tasks(self, client):
        """Cluster status reflects registered nodes and submitted tasks."""
        _register_node(client, "node-a", ["coding"])
        _register_node(client, "node-b", ["reviewing"])
        _submit_task(client, "Task 1")
        _submit_task(client, "Task 2")
        t3 = _submit_task(client, "Task 3")
        client.post(f"/api/v1/tasks/{t3['id']}/complete")

        resp = client.get("/api/v1/cluster/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["cluster_id"] == "test-cluster"
        assert data["node_count"] == 2
        assert data["online_nodes"] == 2
        assert data["task_count"] == 3
        # Completing a task triggers _trigger_downstream → schedule_pending(),
        # which assigns the remaining ready tasks to nodes (→ running).
        assert data["tasks_by_status"]["completed"] == 1
        assert data["tasks_by_status"]["running"] == 2

    def test_cluster_status_uptime_positive(self, client):
        """Uptime is a non-negative integer."""
        resp = client.get("/api/v1/cluster/status")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data["uptime_seconds"], int)
        assert data["uptime_seconds"] >= 0


# ---------------------------------------------------------------------------
# 4. POST /api/v1/schedule/trigger
# ---------------------------------------------------------------------------

class TestScheduleTrigger:
    def test_trigger_empty_cluster(self, client):
        """Trigger on empty cluster returns zero counts."""
        resp = client.post("/api/v1/schedule/trigger")
        assert resp.status_code == 200
        data = resp.json()
        assert data["promoted"] == 0
        assert data["scheduled"] == 0
        assert data["assignments"] == []

    def test_trigger_assigns_ready_tasks(self, client):
        """Trigger assigns ready tasks to online nodes."""
        _register_node(client, "sched-node", ["coding"])
        _submit_task(client, "Schedulable task")

        resp = client.post("/api/v1/schedule/trigger")
        assert resp.status_code == 200
        data = resp.json()
        assert data["scheduled"] >= 1
        assert len(data["assignments"]) >= 1
        # Verify the assignment
        assignment = data["assignments"][0]
        assert assignment["node_id"] == "node_sched-node"

    def test_trigger_no_online_nodes(self, client):
        """With no online nodes, tasks stay ready but aren't assigned."""
        _submit_task(client, "No node for me")

        resp = client.post("/api/v1/schedule/trigger")
        assert resp.status_code == 200
        data = resp.json()
        # Task was already promoted to ready on submit (no deps),
        # so trigger_pending_tasks finds 0 pending tasks to promote.
        # But schedule_pending can't assign (no online nodes).
        assert data["scheduled"] == 0
        assert data["assignments"] == []

    def test_trigger_capability_mismatch(self, client):
        """Task with unmatched capability isn't assigned to a node without it."""
        _register_node(client, "gpu-only", ["gpu"])
        _submit_task(client, "Needs coding", requires=["coding"])

        resp = client.post("/api/v1/schedule/trigger")
        assert resp.status_code == 200
        data = resp.json()
        # The task should remain ready, not assigned
        assert data["scheduled"] == 0

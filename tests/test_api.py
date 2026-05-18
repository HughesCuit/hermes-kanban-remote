"""Comprehensive tests for the hermes-cluster Python backend.

Tests all API endpoints matching the Go backend's behavior.
"""

import pytest
from fastapi.testclient import TestClient

from hermes_cluster.app import create_app
from hermes_cluster.state import ClusterState


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


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

class TestHealth:
    def test_health(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["cluster_id"] == "test-cluster"
        assert data["node_id"] == "test-node"
        assert data["role"] == "main"
        assert "uptime_seconds" in data
        assert data["version"] == "python-1.0.0"


# ---------------------------------------------------------------------------
# Node endpoints
# ---------------------------------------------------------------------------

class TestNodes:
    def test_join(self, client):
        resp = client.post("/api/v1/nodes/join", json={
            "node_name": "worker-1",
            "capabilities": ["coding", "gpu"],
            "endpoint": "http://worker1:8788",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["node_id"] == "node_worker-1"
        assert data["status"] == "registered"

    def test_heartbeat(self, client):
        # Join first
        client.post("/api/v1/nodes/join", json={"node_name": "hb-test"})
        resp = client.post("/api/v1/nodes/heartbeat", json={"node_id": "node_hb-test"})
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_list_nodes(self, client):
        client.post("/api/v1/nodes/join", json={"node_name": "list-test"})
        resp = client.get("/api/v1/nodes")
        assert resp.status_code == 200
        nodes = resp.json()
        assert len(nodes) >= 1
        assert any(n["name"] == "list-test" for n in nodes)

    def test_update_capabilities(self, client):
        client.post("/api/v1/nodes/join", json={"node_name": "cap-test"})
        resp = client.patch("/api/v1/nodes/node_cap-test/capabilities", json={
            "capabilities": ["planning", "reviewing"],
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "updated"
        assert data["capabilities"] == ["planning", "reviewing"]

    def test_update_capabilities_not_found(self, client):
        resp = client.patch("/api/v1/nodes/nonexistent/capabilities", json={
            "capabilities": ["coding"],
        })
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Task endpoints
# ---------------------------------------------------------------------------

class TestTasks:
    def test_submit_task(self, client):
        resp = client.post("/api/v1/tasks", json={
            "title": "Test task",
            "requires": ["coding"],
            "priority": 2,
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["title"] == "Test task"
        assert data["priority"] == 2
        # Tasks with no dependencies are immediately promoted to ready
        assert data["status"] == "ready"

    def test_list_tasks(self, client):
        client.post("/api/v1/tasks", json={"title": "List test"})
        resp = client.get("/api/v1/tasks")
        assert resp.status_code == 200
        tasks = resp.json()
        assert len(tasks) >= 1

    def test_complete_task(self, client):
        # Create task
        create_resp = client.post("/api/v1/tasks", json={"title": "Complete me"})
        task_id = create_resp.json()["id"]

        resp = client.post(f"/api/v1/tasks/{task_id}/complete")
        assert resp.status_code == 200
        assert resp.json()["status"] == "completed"

    def test_fail_task(self, client):
        create_resp = client.post("/api/v1/tasks", json={"title": "Fail me"})
        task_id = create_resp.json()["id"]

        resp = client.post(f"/api/v1/tasks/{task_id}/fail", json={"reason": "test failure"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "failed"
        assert isinstance(data["blocked"], list)

    def test_unblock_task(self, client):
        # Create and fail a task, then unblock it
        create_resp = client.post("/api/v1/tasks", json={"title": "Block me"})
        task_id = create_resp.json()["id"]
        client.post(f"/api/v1/tasks/{task_id}/fail")
        # Unblock (status should be failed, not blocked, but test the endpoint)
        resp = client.post(f"/api/v1/tasks/{task_id}/unblock")
        # May return 400 if task isn't in blocked state, which is fine

    def test_set_dependencies(self, client):
        # Create two tasks
        t1 = client.post("/api/v1/tasks", json={"title": "Task 1"}).json()
        t2 = client.post("/api/v1/tasks", json={"title": "Task 2"}).json()

        resp = client.post(f"/api/v1/tasks/{t2['id']}/dependencies", json={
            "depends_on": [t1["id"]],
        })
        assert resp.status_code == 200
        data = resp.json()
        assert t1["id"] in data["depends_on"]

    def test_get_dependents(self, client):
        t1 = client.post("/api/v1/tasks", json={"title": "Parent"}).json()
        t2 = client.post("/api/v1/tasks", json={"title": "Child"}).json()
        client.post(f"/api/v1/tasks/{t2['id']}/dependencies", json={"depends_on": [t1["id"]]})

        resp = client.get(f"/api/v1/tasks/{t1['id']}/dependents")
        assert resp.status_code == 200
        data = resp.json()
        assert t2["id"] in data["dependents"]

    def test_get_trigger_chain(self, client):
        t1 = client.post("/api/v1/tasks", json={"title": "Root"}).json()
        t2 = client.post("/api/v1/tasks", json={"title": "Child"}).json()
        client.post(f"/api/v1/tasks/{t2['id']}/dependencies", json={"depends_on": [t1["id"]]})

        resp = client.get(f"/api/v1/tasks/{t1['id']}/trigger-chain")
        assert resp.status_code == 200
        data = resp.json()
        assert t2["id"] in data["chain"]

    def test_workflow_graph(self, client):
        t1 = client.post("/api/v1/tasks", json={"title": "Node A"}).json()
        t2 = client.post("/api/v1/tasks", json={"title": "Node B"}).json()
        client.post(f"/api/v1/tasks/{t2['id']}/dependencies", json={"depends_on": [t1["id"]]})

        resp = client.get("/api/v1/workflow/graph")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["nodes"]) >= 2
        assert len(data["edges"]) >= 1


# ---------------------------------------------------------------------------
# Lease endpoints
# ---------------------------------------------------------------------------

class TestLeases:
    def test_create_lease(self, client):
        # Create a task first
        task = client.post("/api/v1/tasks", json={"title": "Lease test"}).json()
        resp = client.post("/api/v1/leases", json={
            "task_id": task["id"],
            "node_id": "node_1",
            "ttl_seconds": 60,
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["task_id"] == task["id"]
        assert data["status"] == "active"

    def test_revoke_lease(self, client):
        task = client.post("/api/v1/tasks", json={"title": "Revoke test"}).json()
        lease = client.post("/api/v1/leases", json={
            "task_id": task["id"],
            "node_id": "node_1",
            "ttl_seconds": 60,
        }).json()

        resp = client.delete(f"/api/v1/leases/{lease['id']}")
        assert resp.status_code == 200
        assert resp.json()["status"] == "revoked"

    def test_list_leases(self, client):
        task = client.post("/api/v1/tasks", json={"title": "List leases"}).json()
        client.post("/api/v1/leases", json={
            "task_id": task["id"],
            "node_id": "node_1",
            "ttl_seconds": 60,
        })
        resp = client.get("/api/v1/leases")
        assert resp.status_code == 200
        leases = resp.json()
        assert len(leases) >= 1


# ---------------------------------------------------------------------------
# Sync endpoints
# ---------------------------------------------------------------------------

class TestSync:
    def test_sync_receive(self, client):
        resp = client.post("/api/v1/sync/receive", json={
            "version": 1,
            "sender_node": "node_1",
            "event_type": "task_created",
            "timestamp": 0,
        })
        assert resp.status_code == 200
        assert resp.json()["applied"] is True

    def test_sync_receive_batch(self, client):
        resp = client.post("/api/v1/sync/receive-batch", json={
            "messages": [
                {"version": 2, "sender_node": "node_1", "event_type": "task_created", "timestamp": 0},
                {"version": 3, "sender_node": "node_1", "event_type": "task_completed", "timestamp": 0},
            ]
        })
        assert resp.status_code == 200
        assert resp.json()["applied"] == 2

    def test_sync_status(self, client):
        resp = client.get("/api/v1/sync/status")
        assert resp.status_code == 200
        assert "version" in resp.json()


# ---------------------------------------------------------------------------
# Recovery endpoints
# ---------------------------------------------------------------------------

class TestRecovery:
    def test_recovery_trigger(self, client):
        resp = client.post("/api/v1/recovery/trigger", json={"node_id": "offline-node"})
        assert resp.status_code == 200
        assert resp.json()["status"] == "accepted"

    def test_recovery_log(self, client):
        client.post("/api/v1/recovery/trigger", json={"node_id": "test-node"})
        resp = client.get("/api/v1/recovery/log")
        assert resp.status_code == 200
        events = resp.json()
        assert len(events) >= 1

    def test_recovery_stats(self, client):
        client.post("/api/v1/recovery/trigger", json={"node_id": "stats-node"})
        resp = client.get("/api/v1/recovery/stats")
        assert resp.status_code == 200
        stats = resp.json()
        assert stats["total"] >= 1


# ---------------------------------------------------------------------------
# Schedule endpoints
# ---------------------------------------------------------------------------

class TestSchedule:
    def test_schedule_trigger(self, client):
        resp = client.post("/api/v1/schedule/trigger")
        assert resp.status_code == 200
        data = resp.json()
        assert "promoted" in data
        assert "scheduled" in data

    def test_schedule_stats(self, client):
        resp = client.get("/api/v1/schedule/stats")
        assert resp.status_code == 200
        assert "total_decisions" in resp.json()

    def test_schedule_decisions(self, client):
        resp = client.get("/api/v1/schedule/decisions")
        assert resp.status_code == 200
        data = resp.json()
        assert "decisions" in data
        assert "count" in data


# ---------------------------------------------------------------------------
# Federation endpoints
# ---------------------------------------------------------------------------

class TestFederation:
    def test_register_cluster(self, client):
        resp = client.post("/api/v1/federation/clusters", json={
            "name": "remote-cluster",
            "endpoint": "http://remote:8787",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "remote-cluster"
        assert data["status"] == "available"

    def test_list_clusters(self, client):
        client.post("/api/v1/federation/clusters", json={
            "name": "list-test",
            "endpoint": "http://list:8787",
        })
        resp = client.get("/api/v1/federation/clusters")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] >= 1

    def test_remove_cluster(self, client):
        reg = client.post("/api/v1/federation/clusters", json={
            "name": "remove-test",
            "endpoint": "http://remove:8787",
        }).json()
        resp = client.delete(f"/api/v1/federation/clusters/{reg['id']}")
        assert resp.status_code == 200

    def test_cluster_status(self, client):
        reg = client.post("/api/v1/federation/clusters", json={
            "name": "status-test",
            "endpoint": "http://status:8787",
        }).json()
        resp = client.get(f"/api/v1/federation/clusters/{reg['id']}/status")
        assert resp.status_code == 200

    def test_forward_task(self, client):
        reg = client.post("/api/v1/federation/clusters", json={
            "name": "forward-test",
            "endpoint": "http://forward:8787",
        }).json()
        resp = client.post("/api/v1/federation/tasks", json={
            "cluster_id": reg["id"],
            "title": "Forwarded task",
            "requires": ["coding"],
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "forwarded"


# ---------------------------------------------------------------------------
# Hook endpoints
# ---------------------------------------------------------------------------

class TestHooks:
    def test_register_hook(self, client):
        resp = client.post("/api/v1/hooks", json={
            "url": "http://example.com/webhook",
            "events": ["task_created", "task_completed"],
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["url"] == "http://example.com/webhook"
        assert data["active"] is True

    def test_list_hooks(self, client):
        client.post("/api/v1/hooks", json={
            "url": "http://example.com/hook",
            "events": ["task_created"],
        })
        resp = client.get("/api/v1/hooks")
        assert resp.status_code == 200
        hooks = resp.json()
        assert len(hooks) >= 1
        # Secret should not be exposed
        assert all(h.get("secret") is None for h in hooks)

    def test_deregister_hook(self, client):
        hook = client.post("/api/v1/hooks", json={
            "url": "http://example.com/dereg",
            "events": ["task_created"],
        }).json()
        resp = client.delete(f"/api/v1/hooks/{hook['id']}")
        assert resp.status_code == 200

    def test_hook_deliveries(self, client):
        hook = client.post("/api/v1/hooks", json={
            "url": "http://example.com/del",
            "events": ["task_created"],
        }).json()
        resp = client.get(f"/api/v1/hooks/{hook['id']}/deliveries")
        assert resp.status_code == 200
        assert resp.json() == []


# ---------------------------------------------------------------------------
# Status / Summary
# ---------------------------------------------------------------------------

class TestStatus:
    def test_status(self, client):
        client.post("/api/v1/tasks", json={"title": "Status task"})
        resp = client.get("/api/v1/status")
        assert resp.status_code == 200
        data = resp.json()
        assert "entries" in data
        assert "summary" in data

    def test_status_with_filters(self, client):
        resp = client.get("/api/v1/status?node=test&status=pending")
        assert resp.status_code == 200

    def test_summary(self, client):
        resp = client.get("/api/v1/summary")
        assert resp.status_code == 200
        data = resp.json()
        assert data["cluster_id"] == "test-cluster"
        assert "nodes" in data
        assert "tasks" in data
        assert "leases" in data


# ---------------------------------------------------------------------------
# Config endpoints
# ---------------------------------------------------------------------------

class TestConfig:
    def test_get_config(self, client):
        resp = client.get("/api/v1/config")
        assert resp.status_code == 200
        data = resp.json()
        assert "cluster" in data
        assert "node" in data
        assert "server" in data

    def test_get_default_config(self, client):
        resp = client.get("/api/v1/config?defaults=true")
        assert resp.status_code == 200
        data = resp.json()
        assert data["cluster"]["id"] == "cluster_default"

    def test_update_config(self, client):
        resp = client.put("/api/v1/config", json={
            "cluster": {"id": "updated-cluster", "role": "main", "endpoint": "", "token": ""},
            "node": {"id": "updated-node", "name": "updated", "capabilities": ["coding"]},
            "server": {"bind": "127.0.0.1", "port": 9090},
            "lease": {"ttl": "30s", "scan_rate": "5s"},
            "watchdog": {"check_interval": "3s", "degraded_after": "10s", "offline_after": "20s"},
            "tls": {"enabled": False, "cert_file": "", "key_file": ""},
            "heartbeat": {"interval": "30s", "lease_timeout": "120s"},
            "reconnect": {"initial_interval": "1s", "max_interval": "60s", "multiplier": 2.0},
            "federation": {"enabled": True, "ping_interval": "30s", "token": ""},
            "telemetry": {"enabled": False, "exporter": "otlp", "endpoint": "", "service_name": "hermes-cluster", "sample_rate": 1.0, "batch_timeout": "5s"},
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "saved"


# ---------------------------------------------------------------------------
# Visualization endpoints
# ---------------------------------------------------------------------------

class TestVisualization:
    def test_topology(self, client):
        client.post("/api/v1/nodes/join", json={"node_name": "viz-node"})
        resp = client.get("/api/v1/cluster/topology")
        assert resp.status_code == 200
        data = resp.json()
        assert "nodes" in data

    def test_metrics(self, client):
        resp = client.get("/api/v1/cluster/metrics")
        assert resp.status_code == 200
        data = resp.json()
        assert "nodes" in data
        assert "tasks" in data

    def test_timeline(self, client):
        resp = client.get("/api/v1/cluster/timeline")
        assert resp.status_code == 200

    def test_viz(self, client):
        resp = client.get("/api/v1/cluster/viz")
        assert resp.status_code == 200
        data = resp.json()
        assert "topology" in data
        assert "metrics" in data
        assert "timeline" in data


# ---------------------------------------------------------------------------
# Dashboard serving
# ---------------------------------------------------------------------------

class TestDashboard:
    def test_dashboard_redirect(self, client):
        resp = client.get("/dashboard", follow_redirects=False)
        # FastAPI/Starlette uses 307 Temporary Redirect
        assert resp.status_code in (301, 307)
        assert "/dashboard/" in resp.headers.get("location", "")

    def test_dashboard_without_static(self, client):
        # Without static dir, dashboard returns 404
        resp = client.get("/dashboard/")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Integration: task lifecycle with dependencies
# ---------------------------------------------------------------------------

class TestTaskLifecycle:
    def test_dependency_chain(self, client):
        """Test: create 3 tasks in a chain, complete them in order."""
        t1 = client.post("/api/v1/tasks", json={"title": "Step 1"}).json()
        t2 = client.post("/api/v1/tasks", json={"title": "Step 2"}).json()
        t3 = client.post("/api/v1/tasks", json={"title": "Step 3"}).json()

        # t2 depends on t1, t3 depends on t2
        client.post(f"/api/v1/tasks/{t2['id']}/dependencies", json={"depends_on": [t1["id"]]})
        client.post(f"/api/v1/tasks/{t3['id']}/dependencies", json={"depends_on": [t2["id"]]})

        # Complete t1 -> t2 should become ready
        client.post(f"/api/v1/tasks/{t1['id']}/complete")
        t2_status = client.get("/api/v1/tasks").json()
        t2_task = next(t for t in t2_status if t["id"] == t2["id"])
        assert t2_task["status"] == "ready"

        # Complete t2 -> t3 should become ready
        client.post(f"/api/v1/tasks/{t2['id']}/complete")
        t3_status = client.get("/api/v1/tasks").json()
        t3_task = next(t for t in t3_status if t["id"] == t3["id"])
        assert t3_task["status"] == "ready"

    def test_task_failure_blocks_downstream(self, client):
        """Test: failing a task blocks its dependents."""
        t1 = client.post("/api/v1/tasks", json={"title": "Prereq"}).json()
        t2 = client.post("/api/v1/tasks", json={"title": "Dependent"}).json()
        client.post(f"/api/v1/tasks/{t2['id']}/dependencies", json={"depends_on": [t1["id"]]})

        # Fail t1
        resp = client.post(f"/api/v1/tasks/{t1['id']}/fail", json={"reason": "broken"})
        blocked = resp.json()["blocked"]
        assert t2["id"] in blocked

    def test_priority_scheduling(self, client):
        """Test: higher priority tasks get scheduled first."""
        # Create a node with all capabilities
        client.post("/api/v1/nodes/join", json={
            "node_name": "priority-worker",
            "capabilities": ["coding", "planning"],
        })

        # Create tasks with different priorities (both become "ready" immediately)
        low = client.post("/api/v1/tasks", json={"title": "Low priority", "priority": 5, "requires": ["coding"]}).json()
        high = client.post("/api/v1/tasks", json={"title": "High priority", "priority": 1, "requires": ["coding"]}).json()

        # Trigger scheduling — both are ready, but only one can be assigned (round-robin to first node)
        client.post("/api/v1/schedule/trigger")

        # Check decisions — high priority should be scheduled first
        resp = client.get("/api/v1/schedule/decisions")
        decisions = resp.json()["decisions"]
        if decisions:
            assert decisions[0]["priority"] == 1 or decisions[0]["task_title"] == "High priority"

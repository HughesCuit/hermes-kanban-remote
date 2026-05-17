"""Integration test — verify ClusterStore works as drop-in for ClusterState in all routers."""

import sys
import time as _time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.testclient import TestClient

from hermes_cluster.state.cluster_store import ClusterStore
from hermes_cluster.routers import (
    nodes_router, tasks_router, leases_router, sync_router,
    recovery_router, schedule_router, federation_router,
    hooks_router, workflow_router, status_router, config_router,
    visualization_router,
)
from hermes_cluster.routers import (
    nodes as nodes_mod, tasks as tasks_mod, leases as leases_mod,
    sync as sync_mod, recovery as recovery_mod, schedule as schedule_mod,
    federation as federation_mod, hooks as hooks_mod,
    workflow as workflow_mod, status as status_mod,
    config as config_mod, visualization as visualization_mod,
)


def create_app_with_store(**kwargs):
    app = FastAPI(title="test", version="1.0.0")
    app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

    state = ClusterStore(db_path=":memory:")
    state.cluster_id = kwargs.get("cluster_id", "test-cluster")
    state.node_id = kwargs.get("node_id", "test-node")
    state.node_role = kwargs.get("node_role", "main")

    nodes_mod.init(state)
    tasks_mod.init(state)
    leases_mod.init(state)
    sync_mod.init(state)
    recovery_mod.init(state)
    schedule_mod.init(state)
    federation_mod.init(state, kwargs.get("fed_token", ""))
    hooks_mod.init(state)
    workflow_mod.init(state)
    status_mod.init(state)
    config_mod.init(state)
    visualization_mod.init(state)

    app.include_router(nodes_router)
    app.include_router(tasks_router)
    app.include_router(leases_router)
    app.include_router(sync_router)
    app.include_router(recovery_router)
    app.include_router(schedule_router)
    app.include_router(federation_router)
    app.include_router(hooks_router)
    app.include_router(workflow_router)
    app.include_router(status_router)
    app.include_router(config_router)
    app.include_router(visualization_router)

    @app.get("/health")
    async def health():
        uptime = int(_time.time() - state.started_at.timestamp())
        return {"status": "ok", "cluster_id": state.cluster_id, "node_id": state.node_id,
                "role": state.node_role, "uptime_seconds": uptime, "version": "python-1.0.0"}
    return app


@pytest.fixture
def client():
    return TestClient(create_app_with_store())


class TestDropInHealth:
    def test_health(self, client):
        r = client.get("/health")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"
        assert r.json()["cluster_id"] == "test-cluster"

class TestDropInNodes:
    def test_join(self, client):
        r = client.post("/api/v1/nodes/join", json={"node_name": "w1", "capabilities": ["coding"]})
        assert r.status_code == 200
        assert r.json()["node_id"] == "node_w1"
    def test_heartbeat(self, client):
        client.post("/api/v1/nodes/join", json={"node_name": "hb"})
        r = client.post("/api/v1/nodes/heartbeat", json={"node_id": "node_hb"})
        assert r.status_code == 200
    def test_list(self, client):
        client.post("/api/v1/nodes/join", json={"node_name": "n1"})
        r = client.get("/api/v1/nodes")
        assert r.status_code == 200 and len(r.json()) >= 1

class TestDropInTasks:
    def test_submit(self, client):
        r = client.post("/api/v1/tasks", json={"title": "T1", "priority": 2})
        assert r.status_code == 200 and r.json()["status"] == "ready"
    def test_complete(self, client):
        tid = client.post("/api/v1/tasks", json={"title": "C1"}).json()["id"]
        r = client.post(f"/api/v1/tasks/{tid}/complete")
        assert r.status_code == 200 and r.json()["status"] == "completed"
    def test_fail(self, client):
        tid = client.post("/api/v1/tasks", json={"title": "F1"}).json()["id"]
        r = client.post(f"/api/v1/tasks/{tid}/fail", json={"reason": "boom"})
        assert r.status_code == 200 and r.json()["status"] == "failed"
    def test_dependencies(self, client):
        t1 = client.post("/api/v1/tasks", json={"title": "P"}).json()
        t2 = client.post("/api/v1/tasks", json={"title": "C"}).json()
        r = client.post(f"/api/v1/tasks/{t2['id']}/dependencies", json={"depends_on": [t1["id"]]})
        assert r.status_code == 200 and t1["id"] in r.json()["depends_on"]

class TestDropInLeases:
    def test_create_revoke(self, client):
        tid = client.post("/api/v1/tasks", json={"title": "L"}).json()["id"]
        lease = client.post("/api/v1/leases", json={"task_id": tid, "node_id": "n1", "ttl_seconds": 60}).json()
        assert lease["status"] == "active"
        r = client.delete(f"/api/v1/leases/{lease['id']}")
        assert r.status_code == 200 and r.json()["status"] == "revoked"

class TestDropInSync:
    def test_receive(self, client):
        r = client.post("/api/v1/sync/receive", json={"version": 1, "sender_node": "n1", "event_type": "task_created", "timestamp": 0})
        assert r.status_code == 200 and r.json()["applied"] is True
    def test_status(self, client):
        r = client.get("/api/v1/sync/status")
        assert r.status_code == 200 and "version" in r.json()

class TestDropInRecovery:
    def test_trigger_log(self, client):
        client.post("/api/v1/recovery/trigger", json={"node_id": "off"})
        r = client.get("/api/v1/recovery/log")
        assert r.status_code == 200 and len(r.json()) >= 1

class TestDropInSchedule:
    def test_trigger(self, client):
        r = client.post("/api/v1/schedule/trigger")
        assert r.status_code == 200 and "promoted" in r.json()

class TestDropInFederation:
    def test_register(self, client):
        r = client.post("/api/v1/federation/clusters", json={"name": "rc", "endpoint": "http://r:8787"})
        assert r.status_code == 200 and r.json()["name"] == "rc"

class TestDropInHooks:
    def test_register_list(self, client):
        client.post("/api/v1/hooks", json={"url": "http://e.com/h", "events": ["task_created"]})
        r = client.get("/api/v1/hooks")
        assert r.status_code == 200 and len(r.json()) >= 1

class TestDropInSummary:
    def test_summary(self, client):
        r = client.get("/api/v1/summary")
        assert r.status_code == 200 and r.json()["cluster_id"] == "test-cluster"

class TestDropInConfig:
    def test_get(self, client):
        r = client.get("/api/v1/config")
        assert r.status_code == 200 and "cluster" in r.json()

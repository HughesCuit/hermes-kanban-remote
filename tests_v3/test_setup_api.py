"""Tests for the Setup Wizard API endpoints.

Covers:
  - GET  /api/v1/setup/status
  - GET  /api/v1/setup/config
  - POST /api/v1/setup/config
  - POST /api/v1/setup/test-connection
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

# Ensure the plugin root is on sys.path
_plugin_root = str(Path(__file__).resolve().parent.parent)
if _plugin_root not in sys.path:
    sys.path.insert(0, _plugin_root)

import setup_wizard as sw
from hermes_cluster.app import create_app
from hermes_cluster.routers import setup as setup_router_mod
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _isolated_config(tmp_path, monkeypatch):
    """Redirect CONFIG_DIR / CONFIG_PATH to a temp dir for every test."""
    fake_dir = tmp_path / "config"
    fake_dir.mkdir()
    fake_path = fake_dir / "config.json"
    monkeypatch.setattr(sw, "CONFIG_DIR", fake_dir)
    monkeypatch.setattr(sw, "CONFIG_PATH", fake_path)
    # Patch the router's lazy-loaded module reference
    monkeypatch.setattr(setup_router_mod, "_get_setup_wizard", lambda: sw)
    yield fake_path


@pytest.fixture()
def client(tmp_path):
    """Create a test client for the FastAPI app."""
    static_dir = tmp_path / "static"
    static_dir.mkdir()
    app = create_app(static_dir=str(static_dir))
    with TestClient(app) as c:
        yield c


# ---------------------------------------------------------------------------
# GET /api/v1/setup/status
# ---------------------------------------------------------------------------

class TestSetupStatus:
    def test_no_config(self, client):
        """When no config file exists, setup_complete should be False."""
        resp = client.get("/api/v1/setup/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["setup_complete"] is False
        assert data["has_config"] is False

    def test_incomplete_config(self, client, _isolated_config):
        """When config exists but setup_complete is False."""
        _isolated_config.write_text(json.dumps({
            "role": "main",
            "setup_complete": False,
        }))
        resp = client.get("/api/v1/setup/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["setup_complete"] is False
        assert data["has_config"] is True

    def test_complete_config(self, client, _isolated_config):
        """When config exists and setup_complete is True."""
        cfg = sw.build_main_config(cluster_id="test-cluster", token="abcdef1234567890abcdef1234567890")
        _isolated_config.write_text(json.dumps(cfg))
        resp = client.get("/api/v1/setup/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["setup_complete"] is True
        assert data["has_config"] is True

    def test_corrupt_json(self, client, _isolated_config):
        """When config file has invalid JSON."""
        _isolated_config.write_text("not valid json {{{")
        resp = client.get("/api/v1/setup/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["setup_complete"] is False
        assert data["has_config"] is True


# ---------------------------------------------------------------------------
# GET /api/v1/setup/config
# ---------------------------------------------------------------------------

class TestGetSetupConfig:
    def test_no_config_returns_404(self, client):
        resp = client.get("/api/v1/setup/config")
        assert resp.status_code == 404

    def test_returns_config_with_masked_token(self, client, _isolated_config):
        token = "abcdefgh12345678ijklmnop"
        cfg = sw.build_main_config(cluster_id="my-cluster", token=token)
        _isolated_config.write_text(json.dumps(cfg))

        resp = client.get("/api/v1/setup/config")
        assert resp.status_code == 200
        data = resp.json()
        assert data["cluster_id"] == "my-cluster"
        assert data["role"] == "main"
        # Token should be masked
        masked = data["token"]
        assert masked.startswith("abcdefgh")
        assert masked.endswith("mnop")
        assert "*" in masked

    def test_mask_token_short(self):
        """Short tokens should not be masked."""
        from hermes_cluster.routers.setup import _mask_token
        short = "abc123"
        assert _mask_token(short) == "abc123"

    def test_mask_token_exact_12(self):
        """12-char token should not be masked (boundary)."""
        from hermes_cluster.routers.setup import _mask_token
        token = "a" * 12
        assert _mask_token(token) == token

    def test_mask_token_long(self):
        """16+ char token should be masked."""
        from hermes_cluster.routers.setup import _mask_token
        token = "a" * 32
        masked = _mask_token(token)
        assert masked.startswith("aaaaaaaa")
        assert masked.endswith("aaaa")  # last 4 chars
        assert "*" in masked


# ---------------------------------------------------------------------------
# POST /api/v1/setup/config
# ---------------------------------------------------------------------------

class TestSaveSetupConfig:
    def test_save_main_config(self, client):
        resp = client.post("/api/v1/setup/config", json={
            "cluster_id": "test-main",
            "role": "main",
            "node_id": "n1",
            "node_name": "Node One",
            "capabilities": ["coding"],
            "port": 9090,
            "auto_start": False,
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "saved"
        assert data["config"]["cluster_id"] == "test-main"
        assert data["config"]["role"] == "main"
        assert data["config"]["setup_complete"] is True
        assert data["config"]["port"] == 9090
        assert data["config"]["token"]  # should have generated a token
        assert data["server_started"] is False

    def test_save_worker_config(self, client):
        resp = client.post("/api/v1/setup/config", json={
            "role": "worker",
            "endpoint": "http://10.0.0.1:8787",
            "token": "worker-token-123",
            "node_id": "w1",
            "node_name": "Worker 1",
            "capabilities": ["testing"],
            "auto_start": False,
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "saved"
        assert data["config"]["role"] == "worker"
        assert data["config"]["endpoint"] == "http://10.0.0.1:8787"
        assert data["config"]["token"] == "worker-token-123"
        assert data["config"]["setup_complete"] is True

    def test_worker_requires_endpoint(self, client):
        resp = client.post("/api/v1/setup/config", json={
            "role": "worker",
            "token": "tok",
        })
        assert resp.status_code == 422

    def test_worker_requires_token(self, client):
        resp = client.post("/api/v1/setup/config", json={
            "role": "worker",
            "endpoint": "http://x:1",
        })
        assert resp.status_code == 422

    def test_config_persisted_to_disk(self, client, _isolated_config):
        client.post("/api/v1/setup/config", json={
            "cluster_id": "persist-test",
            "role": "main",
            "node_id": "n1",
            "auto_start": False,
        })
        assert _isolated_config.exists()
        data = json.loads(_isolated_config.read_text())
        assert data["cluster_id"] == "persist-test"
        assert data["setup_complete"] is True

    def test_auto_start_flag(self, client):
        resp = client.post("/api/v1/setup/config", json={
            "cluster_id": "auto-test",
            "role": "main",
            "node_id": "n1",
            "auto_start": True,
        })
        assert resp.status_code == 200
        data = resp.json()
        # server_started may be True or False depending on serve module availability
        assert "server_started" in data


# ---------------------------------------------------------------------------
# POST /api/v1/setup/test-connection
# ---------------------------------------------------------------------------

class TestTestConnection:
    def test_invalid_endpoint_returns_502(self, client):
        resp = client.post("/api/v1/setup/test-connection", json={
            "endpoint": "http://127.0.0.1:19999",
        })
        # Should fail since nothing is listening
        assert resp.status_code == 502

    def test_missing_endpoint_field(self, client):
        resp = client.post("/api/v1/setup/test-connection", json={})
        assert resp.status_code == 422

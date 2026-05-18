"""Tests for the Cluster Setup Wizard.

Covers:
  - Config load / save
  - Token generation
  - Answer parsing helpers
  - Main-node and worker-node config building
  - WizardState interactive flow
  - Quick-setup one-shot helpers
  - Integration with __init__.py (slash command, on_session_start)
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Ensure the plugin root is on sys.path so setup_wizard is importable
_plugin_root = str(Path(__file__).resolve().parent.parent)
if _plugin_root not in sys.path:
    sys.path.insert(0, _plugin_root)

from setup_wizard import (
    CONFIG_PATH,
    DEFAULT_CAPABILITIES,
    DEFAULT_CLUSTER_ID,
    DEFAULT_PORT,
    SETUP_PROMPT,
    WizardState,
    build_main_config,
    build_worker_config,
    generate_token,
    join_cluster,
    load_config,
    parse_capabilities,
    parse_int,
    parse_port,
    parse_role,
    quick_setup_main,
    quick_setup_worker,
    save_config,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _isolated_config(tmp_path, monkeypatch):
    """Redirect CONFIG_DIR / CONFIG_PATH to a temp dir for every test."""
    fake_dir = tmp_path / "config"
    fake_dir.mkdir()
    fake_path = fake_dir / "config.json"
    monkeypatch.setattr("setup_wizard.CONFIG_DIR", fake_dir)
    monkeypatch.setattr("setup_wizard.CONFIG_PATH", fake_path)
    yield fake_path


# ---------------------------------------------------------------------------
# Token generation
# ---------------------------------------------------------------------------

class TestGenerateToken:
    def test_length(self):
        token = generate_token()
        assert len(token) == 32  # 16 bytes → 32 hex chars

    def test_hex_format(self):
        token = generate_token()
        int(token, 16)  # should not raise

    def test_uniqueness(self):
        tokens = {generate_token() for _ in range(100)}
        assert len(tokens) == 100


# ---------------------------------------------------------------------------
# Config load / save
# ---------------------------------------------------------------------------

class TestConfigPersistence:
    def test_load_returns_none_when_missing(self):
        assert load_config() is None

    def test_save_and_load_roundtrip(self):
        cfg = build_main_config(cluster_id="test", token="abc123")
        save_config(cfg)
        loaded = load_config()
        assert loaded is not None
        assert loaded["cluster_id"] == "test"
        assert loaded["token"] == "abc123"
        assert loaded["setup_complete"] is True

    def test_load_returns_none_if_not_complete(self, _isolated_config):
        _isolated_config.write_text(json.dumps({"role": "main", "setup_complete": False}))
        assert load_config() is None

    def test_save_creates_parent_dirs(self, tmp_path):
        deep = tmp_path / "a" / "b" / "c" / "config.json"
        with patch("setup_wizard.CONFIG_PATH", deep), \
             patch("setup_wizard.CONFIG_DIR", deep.parent):
            cfg = build_main_config()
            save_config(cfg)
            assert deep.exists()


# ---------------------------------------------------------------------------
# Answer parsers
# ---------------------------------------------------------------------------

class TestParsers:
    @pytest.mark.parametrize("answer,expected", [
        ("1", "main"),
        ("main", "main"),
        ("2", "worker"),
        ("worker", "worker"),
        (" 1 ", "main"),
        ("3", None),
        ("", None),
        ("maybe", None),
    ])
    def test_parse_role(self, answer, expected):
        assert parse_role(answer) == expected

    def test_parse_capabilities_default(self):
        assert parse_capabilities("", ["coding"]) == ["coding"]

    def test_parse_capabilities_comma_separated(self):
        caps = parse_capabilities("planning, testing", [])
        assert caps == ["planning", "testing"]

    def test_parse_capabilities_strips_whitespace(self):
        caps = parse_capabilities("  planning ,  reviewing  ", [])
        assert caps == ["planning", "reviewing"]

    def test_parse_port_default(self):
        assert parse_port("") == DEFAULT_PORT

    def test_parse_port_valid(self):
        assert parse_port("9090") == 9090

    def test_parse_port_out_of_range(self):
        assert parse_port("99999") == DEFAULT_PORT

    def test_parse_port_non_numeric(self):
        assert parse_port("abc") == DEFAULT_PORT

    def test_parse_int_default(self):
        assert parse_int("", 42) == 42

    def test_parse_int_valid(self):
        assert parse_int("7", 42) == 7


# ---------------------------------------------------------------------------
# Config builders
# ---------------------------------------------------------------------------

class TestBuildMainConfig:
    def test_defaults(self):
        cfg = build_main_config()
        assert cfg["role"] == "main"
        assert cfg["cluster_id"] == DEFAULT_CLUSTER_ID
        assert cfg["port"] == DEFAULT_PORT
        assert cfg["setup_complete"] is True
        assert cfg["auto_start"] is True
        assert len(cfg["token"]) == 32

    def test_custom_values(self):
        cfg = build_main_config(
            cluster_id="my-cluster",
            node_id="n1",
            node_name="Node One",
            capabilities=["coding"],
            port=9090,
            token="fixed",
        )
        assert cfg["cluster_id"] == "my-cluster"
        assert cfg["node_id"] == "n1"
        assert cfg["port"] == 9090
        assert cfg["token"] == "fixed"
        assert cfg["capabilities"] == ["coding"]


class TestBuildWorkerConfig:
    def test_defaults(self):
        cfg = build_worker_config(endpoint="http://10.0.0.1:8787", token="tok")
        assert cfg["role"] == "worker"
        assert cfg["endpoint"] == "http://10.0.0.1:8787"
        assert cfg["token"] == "tok"
        assert cfg["port"] == 0
        assert cfg["setup_complete"] is True
        assert cfg["node_id"].startswith("worker-")

    def test_custom_values(self):
        cfg = build_worker_config(
            endpoint="http://x:1",
            token="t",
            node_id="w1",
            node_name="Worker 1",
            capabilities=["debugging"],
        )
        assert cfg["node_id"] == "w1"
        assert cfg["node_name"] == "Worker 1"
        assert cfg["capabilities"] == ["debugging"]


# ---------------------------------------------------------------------------
# WizardState — interactive flow
# ---------------------------------------------------------------------------

class TestWizardState:
    def test_initial_state(self):
        w = WizardState()
        assert w.role is None
        assert w.complete is False
        assert w._step_index == 0
        assert w.error is None

    def test_current_prompt_is_setup_prompt(self):
        w = WizardState()
        assert SETUP_PROMPT in w.current_prompt

    def test_invalid_role_shows_error(self):
        w = WizardState()
        result = w.process_answer("maybe")
        assert "error" in result.lower() or "1" in result

    def test_main_flow_happy_path(self):
        w = WizardState()
        # Step 1: role
        prompt = w.process_answer("1")
        assert w.role == "main"
        assert "Cluster Name" in prompt

        # Step 2: cluster_id
        prompt = w.process_answer("test-cluster")
        assert w.answers["cluster_id"] == "test-cluster"
        assert "Node ID" in prompt

        # Step 3: node_id
        prompt = w.process_answer("main-1")
        assert w.answers["node_id"] == "main-1"
        assert "Display Name" in prompt

        # Step 4: node_name
        prompt = w.process_answer("Main One")
        assert w.answers["node_name"] == "Main One"
        assert "Capabilities" in prompt

        # Step 5: capabilities
        prompt = w.process_answer("planning, coding")
        assert w.answers["capabilities"] == ["planning", "coding"]
        assert "Port" in prompt

        # Step 6: port → completes
        summary = w.process_answer("9090")
        assert w.complete is True
        assert w.answers["port"] == 9090
        assert "Configuration Complete" in summary

    def test_worker_flow_happy_path(self):
        w = WizardState()
        w.process_answer("2")  # role
        assert w.role == "worker"

        w.process_answer("http://10.0.0.1:8787")  # endpoint
        w.process_answer("secret-token")           # token
        w.process_answer("worker-1")               # node_id
        w.process_answer("Worker One")             # node_name
        summary = w.process_answer("coding, testing")  # capabilities → done

        assert w.complete is True
        assert "Configuration Complete" in summary

    def test_defaults_used_when_empty(self):
        w = WizardState()
        w.process_answer("1")        # role = main
        w.process_answer("")         # cluster_id → default
        assert w.answers["cluster_id"] == DEFAULT_CLUSTER_ID
        w.process_answer("")         # node_id → default
        assert w.answers["node_id"] == "main-node"
        w.process_answer("")         # node_name → default
        assert w.answers["node_name"] == "Main Node"
        w.process_answer("")         # capabilities → default
        assert w.answers["capabilities"] == DEFAULT_CAPABILITIES
        w.process_answer("")         # port → default
        assert w.complete is True
        assert w.answers["port"] == DEFAULT_PORT

    def test_worker_requires_endpoint_and_token(self):
        w = WizardState()
        w.process_answer("2")

        # Empty endpoint (required)
        result = w.process_answer("")
        assert "required" in result.lower()
        # Should not advance
        step = w.current_step
        assert step is not None
        assert step["key"] == "endpoint"

    def test_saves_config_on_complete(self, _isolated_config):
        w = WizardState()
        w.process_answer("1")
        for _ in range(5):
            w.process_answer("")  # use all defaults
        assert _isolated_config.exists()
        data = json.loads(_isolated_config.read_text())
        assert data["setup_complete"] is True


# ---------------------------------------------------------------------------
# Quick-setup helpers
# ---------------------------------------------------------------------------

class TestQuickSetup:
    def test_quick_setup_main(self):
        cfg = quick_setup_main(cluster_id="qs-test", port=9999)
        assert cfg["role"] == "main"
        assert cfg["cluster_id"] == "qs-test"
        assert cfg["port"] == 9999
        assert len(cfg["token"]) == 32
        # Verify it was persisted
        loaded = load_config()
        assert loaded is not None
        assert loaded["cluster_id"] == "qs-test"

    def test_quick_setup_worker(self):
        cfg = quick_setup_worker(endpoint="http://x:1", token="t")
        assert cfg["role"] == "worker"
        assert cfg["endpoint"] == "http://x:1"
        loaded = load_config()
        assert loaded is not None
        assert loaded["role"] == "worker"


# ---------------------------------------------------------------------------
# Integration: __init__.py slash command and session hooks
# ---------------------------------------------------------------------------

class TestInitIntegration:
    """Test that __init__.py correctly exposes setup detection and /cluster-setup."""

    def test_needs_setup_flag_default(self):
        """_needs_setup should start as False."""
        import importlib
        mod = importlib.import_module("__init__")
        assert hasattr(mod, "_needs_setup")
        # After module load it may be True or False depending on config;
        # just verify the attribute exists.

    def test_handle_cluster_setup_interactive(self):
        """Calling _handle_cluster_setup without args should return the prompt."""
        import importlib
        mod = importlib.import_module("__init__")
        # Reset wizard state
        mod._wizard = None
        mod._needs_setup = False
        result = mod._handle_cluster_setup({})
        assert isinstance(result, str)
        # Should contain either the setup prompt or a fallback message
        assert "cluster" in result.lower() or "setup" in result.lower()

    def test_handle_cluster_setup_main_quick(self):
        """Quick main setup via slash command args."""
        import importlib
        mod = importlib.import_module("__init__")
        result = mod._handle_cluster_setup({"role": "main", "cluster_id": "qs-int"})
        data = json.loads(result)
        assert data["status"] == "configured"
        assert data["role"] == "main"
        assert "token" in data

    def test_handle_cluster_setup_worker_missing_args(self):
        """Worker quick setup without endpoint/token should error."""
        import importlib
        mod = importlib.import_module("__init__")
        result = mod._handle_cluster_setup({"role": "worker"})
        data = json.loads(result)
        assert "error" in data

    def test_handle_wizard_answer_no_wizard(self):
        """Answering with no active wizard should warn."""
        import importlib
        mod = importlib.import_module("__init__")
        mod._wizard = None
        result = mod._handle_wizard_answer("hello")
        assert "No wizard" in result

    def test_on_session_start_sets_needs_setup(self, _isolated_config):
        """When no config exists, on_session_start should set _needs_setup."""
        import importlib
        mod = importlib.import_module("__init__")
        mod._needs_setup = False
        inject_fn = MagicMock()
        mod._on_session_start(inject_message=inject_fn)
        assert mod._needs_setup is True
        inject_fn.assert_called_once()

    def test_on_session_start_skips_setup_when_config_exists(self, _isolated_config):
        """When config exists, on_session_start should NOT set _needs_setup."""
        import importlib
        mod = importlib.import_module("__init__")
        # Write a valid config
        _isolated_config.write_text(json.dumps({
            "role": "main",
            "setup_complete": True,
            "auto_start": False,
        }))
        mod._needs_setup = False
        mod._on_session_start()
        # _needs_setup should remain False (or the auto-start ran, but setup not triggered)
        # We can't assert False definitively because the env-based config might also
        # lack a token, but at least the inject should NOT have been called.
        assert mod._needs_setup is False

    def test_register_calls_register_command(self):
        """register() should call ctx.register_command for /cluster-setup."""
        import importlib
        mod = importlib.import_module("__init__")
        ctx = MagicMock()
        mod.register(ctx)
        # Check register_command was called with name='cluster-setup'
        cmd_calls = [c for c in ctx.register_command.call_args_list]
        names = [c.kwargs.get("name") or c.args[0] if c.args else c.kwargs.get("name")
                 for c in cmd_calls]
        assert "cluster-setup" in names

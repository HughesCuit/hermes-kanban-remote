"""Tests for hermes-agent-cluster plugin auto-start lifecycle."""

import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Add the plugin directory to the path
sys.path.insert(0, str(Path(__file__).parent))

from __init__ import (
    _auto_start_process,
    _auto_start_lock,
    _cluster_config,
    _check_cluster_health,
    _wait_for_cluster,
    _find_binary,
    _generate_default_config,
    _start_cluster_auto,
    _stop_cluster_auto,
    _get_plugin_config,
    _on_session_start,
    _on_session_end,
    _api_call,
    DEFAULT_PORT,
    DEFAULT_CLUSTER_ID,
    DEFAULT_NODE_ID,
)


class TestPluginConfig:
    """Test configuration loading."""

    def test_default_config(self):
        """Test default configuration values."""
        config = _get_plugin_config()
        assert config["auto_start"] is True
        assert config["port"] == DEFAULT_PORT
        assert config["cluster_id"] == DEFAULT_CLUSTER_ID
        assert config["node_id"] == DEFAULT_NODE_ID
        assert config["capabilities"] == ["planning", "reviewing", "scheduling"]

    def test_env_overrides(self):
        """Test environment variable overrides."""
        with patch.dict(os.environ, {
            "HERMES_CLUSTER_AUTO_START": "false",
            "HERMES_CLUSTER_PORT": "9999",
            "HERMES_CLUSTER_ID": "test-cluster",
            "HERMES_CLUSTER_NODE_ID": "test-node",
        }):
            config = _get_plugin_config()
            assert config["auto_start"] is False
            assert config["port"] == 9999
            assert config["cluster_id"] == "test-cluster"
            assert config["node_id"] == "test-node"

    def test_env_invalid_port(self):
        """Test invalid environment variable handling."""
        with patch.dict(os.environ, {"HERMES_CLUSTER_PORT": "invalid"}):
            config = _get_plugin_config()
            # Should keep default
            assert config["port"] == DEFAULT_PORT


class TestHealthCheck:
    """Test cluster health checking."""

    def test_check_health_success(self):
        """Test successful health check."""
        with patch("__init__.urlopen") as mock_urlopen:
            mock_response = MagicMock()
            mock_response.read.return_value = json.dumps({"summary": {"total_nodes": 1}}).encode()
            mock_response.__enter__ = MagicMock(return_value=mock_response)
            mock_response.__exit__ = MagicMock(return_value=False)
            mock_urlopen.return_value = mock_response
            
            result = _check_cluster_health("http://127.0.0.1:8787")
            assert result is True

    def test_check_health_failure(self):
        """Test failed health check."""
        with patch("__init__.urlopen") as mock_urlopen:
            mock_urlopen.side_effect = Exception("Connection refused")
            
            result = _check_cluster_health("http://127.0.0.1:8787")
            assert result is False

    def test_check_health_error_response(self):
        """Test health check with error response."""
        with patch("__init__.urlopen") as mock_urlopen:
            mock_response = MagicMock()
            mock_response.read.return_value = json.dumps({"error": "unhealthy"}).encode()
            mock_response.__enter__ = MagicMock(return_value=mock_response)
            mock_response.__exit__ = MagicMock(return_value=False)
            mock_urlopen.return_value = mock_response
            
            result = _check_cluster_health("http://127.0.0.1:8787")
            assert result is False


class TestWaitForCluster:
    """Test waiting for cluster to become healthy."""

    def test_wait_success(self):
        """Test successful wait."""
        with patch("__init__._check_cluster_health") as mock_health:
            mock_health.side_effect = [False, False, True]
            
            result = _wait_for_cluster("http://127.0.0.1:8787", timeout=1.0)
            assert result is True
            assert mock_health.call_count == 3

    def test_wait_timeout(self):
        """Test wait timeout."""
        with patch("__init__._check_cluster_health", return_value=False):
            result = _wait_for_cluster("http://127.0.0.1:8787", timeout=0.1)
            assert result is False


class TestBinaryFinder:
    """Test binary discovery."""

    def test_find_binary_absolute_path(self):
        """Test finding binary with absolute path."""
        with patch("os.path.isfile", return_value=True):
            result = _find_binary({"binary_path": "/usr/local/bin/hermes-cluster"})
            assert result == "/usr/local/bin/hermes-cluster"

    def test_find_binary_not_found(self):
        """Test binary not found."""
        with patch("os.path.isfile", return_value=False):
            result = _find_binary({"binary_path": "/nonexistent/hermes-cluster"})
            assert result is None

    def test_find_binary_in_path(self):
        """Test finding binary in PATH."""
        with patch("shutil.which", return_value="/usr/bin/hermes-cluster"):
            result = _find_binary({"binary_path": "hermes-cluster"})
            assert result == "/usr/bin/hermes-cluster"


class TestConfigGeneration:
    """Test config file generation."""

    def test_generate_default_config(self, tmp_path):
        """Test generating default config."""
        with patch.dict(os.environ, {"HERMES_HOME": str(tmp_path)}):
            config = {
                "cluster_id": "test-cluster",
                "node_id": "test-node",
                "node_name": "test-name",
                "capabilities": ["planning", "reviewing"],
                "token": "test-token",
                "port": 8787,
            }
            
            config_path = _generate_default_config(config)
            assert config_path.exists()
            
            content = config_path.read_text()
            assert "test-cluster" in content
            assert "test-node" in content
            assert "test-token" in content

    def test_generate_default_config_no_overwrite(self, tmp_path):
        """Test that existing config is not overwritten."""
        with patch.dict(os.environ, {"HERMES_HOME": str(tmp_path)}):
            config_dir = tmp_path / "agent-cluster"
            config_dir.mkdir(parents=True)
            config_file = config_dir / "cluster.yaml"
            config_file.write_text("existing content")
            
            config = {
                "cluster_id": "test",
                "node_id": "test",
                "node_name": "test",
                "capabilities": ["test"],
                "token": "",
                "port": 8787,
            }
            
            config_path = _generate_default_config(config)
            assert config_path.read_text() == "existing content"


class TestClusterLifecycle:
    """Test cluster start/stop lifecycle."""

    def test_start_cluster_already_running(self):
        """Test starting cluster when already running."""
        with patch("__init__._check_cluster_health", return_value=True):
            config = {"port": 8787, "node_id": "test-node"}
            result = _start_cluster_auto(config)
            
            assert result is True
            assert _cluster_config["base_url"] == "http://127.0.0.1:8787"
            assert _cluster_config["node_id"] == "test-node"

    def test_start_cluster_binary_not_found(self):
        """Test starting cluster when binary not found."""
        with patch("__init__._check_cluster_health", return_value=False), \
             patch("__init__._find_binary", return_value=None):
            config = {"port": 8787}
            result = _start_cluster_auto(config)
            
            assert result is False

    def test_start_cluster_success(self):
        """Test successful cluster start."""
        mock_process = MagicMock()
        mock_process.pid = 12345
        
        with patch("__init__._check_cluster_health", side_effect=[False, True]), \
             patch("__init__._find_binary", return_value="/usr/bin/hermes-cluster"), \
             patch("__init__._generate_default_config", return_value=Path("/tmp/config.yaml")), \
             patch("subprocess.Popen", return_value=mock_process):
            config = {"port": 8787, "node_id": "test-node"}
            result = _start_cluster_auto(config)
            
            assert result is True
            assert _cluster_config["base_url"] == "http://127.0.0.1:8787"

    def test_stop_cluster(self):
        """Test stopping cluster."""
        import __init__ as plugin
        
        mock_process = MagicMock()
        mock_process.poll.return_value = None
        mock_process.wait.return_value = None
        plugin._auto_start_process = mock_process
        
        _stop_cluster_auto()
        
        mock_process.terminate.assert_called_once()
        assert plugin._auto_start_process is None


class TestHooks:
    """Test lifecycle hooks."""

    def test_on_session_start_disabled(self):
        """Test on_session_start when auto-start disabled."""
        with patch("__init__._get_plugin_config", return_value={"auto_start": False}):
            _on_session_start()
            # Should not raise, just skip

    def test_on_session_start_enabled(self):
        """Test on_session_start when auto-start enabled."""
        with patch("__init__._get_plugin_config", return_value={"auto_start": True}), \
             patch("__init__._start_cluster_auto", return_value=True) as mock_start:
            _on_session_start()
            # Should start in background thread
            time.sleep(0.1)  # Give thread time to start

    def test_on_session_end(self):
        """Test on_session_end cleanup."""
        import __init__ as plugin
        
        mock_process = MagicMock()
        mock_process.poll.return_value = None
        mock_process.wait.return_value = None
        plugin._auto_start_process = mock_process
        
        _on_session_end()
        
        assert plugin._auto_start_process is None


class TestAPIHelper:
    """Test API call helper."""

    def test_api_call_success(self):
        """Test successful API call."""
        with patch("__init__.urlopen") as mock_urlopen:
            mock_response = MagicMock()
            mock_response.read.return_value = json.dumps({"result": "ok"}).encode()
            mock_response.__enter__ = MagicMock(return_value=mock_response)
            mock_response.__exit__ = MagicMock(return_value=False)
            mock_urlopen.return_value = mock_response
            
            result = _api_call("http://127.0.0.1:8787", "GET", "/health")
            assert result == {"result": "ok"}

    def test_api_call_with_data(self):
        """Test API call with request body."""
        with patch("__init__.urlopen") as mock_urlopen:
            mock_response = MagicMock()
            mock_response.read.return_value = json.dumps({"created": True}).encode()
            mock_response.__enter__ = MagicMock(return_value=mock_response)
            mock_response.__exit__ = MagicMock(return_value=False)
            mock_urlopen.return_value = mock_response
            
            result = _api_call("http://127.0.0.1:8787", "POST", "/api/v1/tasks", {"title": "test"})
            assert result == {"created": True}

    def test_api_call_error(self):
        """Test API call error handling."""
        with patch("__init__.urlopen") as mock_urlopen:
            mock_urlopen.side_effect = Exception("Connection refused")
            
            result = _api_call("http://127.0.0.1:8787", "GET", "/health")
            assert "error" in result


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

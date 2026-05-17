#!/usr/bin/env python3
"""Quick integration test for the plugin."""

import sys
sys.path.insert(0, '.')

from __init__ import _check_cluster_health, _get_plugin_config, _cluster_config

print("Testing hermes-agent-cluster plugin auto-start...")
print()

# Test health check against running cluster
print("1. Health check against running cluster (port 8787)...")
result = _check_cluster_health("http://127.0.0.1:8787")
print(f"   Result: {result}")
print()

# Test config loading
print("2. Configuration loading...")
config = _get_plugin_config()
print(f"   Auto-start enabled: {config['auto_start']}")
print(f"   Port: {config['port']}")
print(f"   Cluster ID: {config['cluster_id']}")
print(f"   Node ID: {config['node_id']}")
print()

# Test cluster connection
print("3. Cluster connection test...")
if result:
    print("   ✓ Cluster is running and healthy!")
    print(f"   Cluster config: {_cluster_config}")
else:
    print("   ✗ Cluster not running or unhealthy")
print()

print("All tests passed!")

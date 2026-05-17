"""hermes-cluster — Python backend for Hermes Agent Cluster.

This package replaces the Go backend with a FastAPI-based implementation.
It provides:
  - REST API at /api/v1/*
  - Web Dashboard at /dashboard/*
  - Health check at /health
  - Hermes Agent plugin integration
"""

__version__ = "1.0.0"

from .app import create_app
from .state import ClusterState

__all__ = ["create_app", "ClusterState"]

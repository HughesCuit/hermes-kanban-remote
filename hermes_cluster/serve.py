#!/usr/bin/env python3
"""Entry point for the hermes-cluster Python server.

Usage:
    python -m hermes_cluster.serve [--port 8787] [--config cluster.yaml]
"""

import argparse
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Hermes Agent Cluster — Python backend")
    parser.add_argument("--port", type=int, default=8787, help="Port to listen on")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--config", default="", help="Path to cluster.yaml config file")
    parser.add_argument("--static-dir", default="", help="Path to dashboard static files")
    parser.add_argument("--cluster-id", default="cluster_default", help="Cluster identifier")
    parser.add_argument("--node-id", default="node_main", help="Node identifier")
    parser.add_argument("--node-role", default="main", choices=["main", "worker"], help="Node role")
    parser.add_argument("--fed-token", default="", help="Federation auth token")
    args = parser.parse_args()

    # Load config from YAML if provided
    config_path = args.config
    if config_path and Path(config_path).exists():
        try:
            import yaml
            with open(config_path) as f:
                cfg = yaml.safe_load(f)
            # Override CLI args from config
            if "cluster" in cfg:
                args.cluster_id = cfg["cluster"].get("id", args.cluster_id)
                args.node_role = cfg["cluster"].get("role", args.node_role)
                args.fed_token = cfg["cluster"].get("token", args.fed_token)
            if "node" in cfg:
                args.node_id = cfg["node"].get("id", args.node_id)
            if "server" in cfg:
                args.port = cfg["server"].get("port", args.port)
                args.host = cfg["server"].get("bind", args.host)
        except ImportError:
            print("Warning: PyYAML not installed, ignoring config file", file=sys.stderr)
        except Exception as e:
            print(f"Warning: Failed to load config: {e}", file=sys.stderr)

    # Auto-detect static directory
    static_dir = args.static_dir
    if not static_dir:
        # Look for static files in the Go project's dashboard directory
        go_dashboard = Path(__file__).parent.parent.parent / "internal" / "dashboard" / "static"
        if go_dashboard.exists():
            static_dir = str(go_dashboard)

    # Create and run app
    from .app import create_app

    app = create_app(
        cluster_id=args.cluster_id,
        node_id=args.node_id,
        node_role=args.node_role,
        config_path=config_path,
        fed_token=args.fed_token,
        static_dir=static_dir if static_dir else None,
    )

    print(f"Starting hermes-cluster (Python) on {args.host}:{args.port}")
    print(f"Cluster: {args.cluster_id} | Node: {args.node_id} | Role: {args.node_role}")
    if static_dir:
        print(f"Dashboard: http://{args.host}:{args.port}/dashboard/")
    print(f"API docs:  http://{args.host}:{args.port}/docs")
    print(f"Health:    http://{args.host}:{args.port}/health")

    import uvicorn
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()

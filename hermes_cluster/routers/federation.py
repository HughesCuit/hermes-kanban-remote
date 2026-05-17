"""Federation endpoints — /api/v1/federation"""

import hashlib
import secrets

from fastapi import APIRouter, HTTPException, Request

from ..models import (
    FederationRegisterRequest,
    FederationForwardRequest,
    RemoteCluster,
)
from ..state import ClusterState

router = APIRouter(prefix="/api/v1/federation", tags=["federation"])

_state: ClusterState = None
_fed_token: str = ""


def init(state: ClusterState, fed_token: str = ""):
    global _state, _fed_token
    _state = state
    _fed_token = fed_token


@router.post("/clusters")
async def register_cluster(req: FederationRegisterRequest):
    if not req.name or not req.endpoint:
        raise HTTPException(status_code=400, detail="name and endpoint are required")
    # Generate stable ID from endpoint (matching Go's SHA256 approach)
    endpoint_hash = hashlib.sha256(req.endpoint.encode()).hexdigest()[:16]
    cluster_id = "fed_" + endpoint_hash
    return _state.register_federation_cluster(cluster_id, req.name, req.endpoint)


@router.delete("/clusters/{cluster_id}")
async def remove_cluster(cluster_id: str):
    if not _state.remove_federation_cluster(cluster_id):
        raise HTTPException(status_code=404, detail="cluster not found")
    return {"status": "removed"}


@router.get("/clusters")
async def list_clusters():
    clusters = _state.get_federation_clusters()
    return {"clusters": clusters, "total": len(clusters)}


@router.get("/clusters/{cluster_id}/status")
async def cluster_status(cluster_id: str):
    cluster = _state.get_federation_cluster(cluster_id)
    if not cluster:
        raise HTTPException(status_code=404, detail="cluster not found")
    return cluster


@router.post("/tasks")
async def forward_task(req: FederationForwardRequest):
    if not req.cluster_id or not req.title:
        raise HTTPException(status_code=400, detail="cluster_id and title are required")
    cluster = _state.get_federation_cluster(req.cluster_id)
    if not cluster:
        raise HTTPException(status_code=404, detail="cluster not found")
    # In a full implementation, this would forward the task via HTTP
    # For now, return a placeholder
    remote_id = "remote_" + secrets.token_hex(8)
    return {
        "status": "forwarded",
        "cluster_id": req.cluster_id,
        "remote_task_id": remote_id,
    }

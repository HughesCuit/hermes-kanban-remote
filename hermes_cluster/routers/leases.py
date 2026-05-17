"""Lease management endpoints — /api/v1/leases"""

from datetime import timedelta

from fastapi import APIRouter, HTTPException

from ..models import CreateLeaseRequest, Lease
from ..state import ClusterState

router = APIRouter(prefix="/api/v1/leases", tags=["leases"])

_state: ClusterState = None


def init(state: ClusterState):
    global _state
    _state = state


@router.post("")
async def create_lease(req: CreateLeaseRequest):
    lease = _state.create_lease(req.task_id, req.node_id, timedelta(seconds=req.ttl_seconds))
    if not lease:
        raise HTTPException(status_code=409, detail="lease already exists")
    return lease


@router.delete("/{lease_id}")
async def revoke_lease(lease_id: str):
    if not _state.revoke_lease(lease_id):
        raise HTTPException(status_code=404, detail="lease not found")
    return {"status": "revoked"}


@router.get("")
async def list_leases():
    return _state.get_active_leases()

"""Recovery endpoints — /api/v1/recovery

Full API surface matching Go's recovery HTTP handlers:
  POST /trigger          — trigger recovery for a node
  GET  /log              — list all recovery events
  GET  /stats            — recovery statistics
  POST /auto-recovery/enable   — start background auto-recovery
  POST /auto-recovery/disable  — stop background auto-recovery
  GET  /auto-recovery/status   — check auto-recovery state
  POST /detect-expired         — manually scan for expired leases
"""

from __future__ import annotations

import threading
from typing import Any, Dict

from fastapi import APIRouter
from pydantic import BaseModel

from ..models import RecoveryTriggerRequest
from ..state import ClusterState

router = APIRouter(prefix="/api/v1/recovery", tags=["recovery"])

_state: ClusterState = None  # type: ignore[assignment]
_recovery_manager = None  # Lazy init after state is set


def init(state: ClusterState):
    """Wire up the recovery router with shared cluster state."""
    global _state, _recovery_manager
    _state = state
    # Import here to avoid circular at module level
    from ..recovery.manager import RecoveryManager
    _recovery_manager = RecoveryManager(state)


def _get_manager():
    """Get or create the RecoveryManager (fallback if not wired via init)."""
    global _recovery_manager
    if _recovery_manager is None:
        from ..recovery.manager import RecoveryManager
        _recovery_manager = RecoveryManager(_state)
    return _recovery_manager


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class AutoRecoveryConfig(BaseModel):
    scan_interval: float = 30.0


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/trigger")
async def recovery_trigger(req: RecoveryTriggerRequest):
    """Trigger full recovery pipeline for a node.

    1. Revokes all active leases for the node
    2. Reschedules orphaned tasks to available nodes
    3. Logs all recovery events
    """
    manager = _get_manager()
    # Run async to match Go's `go s.Recovery.NotifyOffline(req.NodeID)`
    result = manager.trigger_recovery(req.node_id)
    return result


@router.get("/log")
async def recovery_log():
    """Get all recovery events."""
    manager = _get_manager()
    events = manager.get_recovery_events()
    # Serialize to dicts for JSON response
    return [e.model_dump(mode="json") for e in events]


@router.get("/stats")
async def recovery_stats():
    """Get recovery statistics."""
    manager = _get_manager()
    return manager.get_recovery_stats()


@router.post("/auto-recovery/enable")
async def recovery_auto_enable(config: AutoRecoveryConfig = None):
    """Enable auto-recovery background scanner."""
    if config is None:
        config = AutoRecoveryConfig()
    manager = _get_manager()
    manager.start_auto_recovery(scan_interval=config.scan_interval)
    return {
        "status": "enabled",
        "scan_interval": config.scan_interval,
    }


@router.post("/auto-recovery/disable")
async def recovery_auto_disable():
    """Disable auto-recovery background scanner."""
    manager = _get_manager()
    manager.stop_auto_recovery()
    return {"status": "disabled"}


@router.get("/auto-recovery/status")
async def recovery_auto_status():
    """Check auto-recovery status."""
    manager = _get_manager()
    return {
        "enabled": manager.is_auto_recovery_enabled,
        "scan_interval": manager._scan_interval,
    }


@router.post("/detect-expired")
async def recovery_detect_expired():
    """Manually trigger expired lease detection and recovery."""
    manager = _get_manager()
    return manager.detect_expired_leases()

"""Webhook API router — register/deregister/list/deliveries (from api/api.go hooks section)."""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from hermes_cluster.hooks.manager import HookManager
from hermes_cluster.hooks.payload import HookEventType

router = APIRouter(prefix="/api/v1/hooks", tags=["hooks"])

# Global HookManager instance (set during app startup)
_hook_manager: Optional[HookManager] = None


def set_hook_manager(manager: HookManager) -> None:
    """Set the global HookManager for this router."""
    global _hook_manager
    _hook_manager = manager


def get_hook_manager() -> HookManager:
    """Get the global HookManager. Raises if not configured."""
    if _hook_manager is None:
        raise HTTPException(
            status_code=503,
            detail="webhook system not configured",
        )
    return _hook_manager


# ---------------------------------------------------------------------------
# Request/Response models
# ---------------------------------------------------------------------------

class RegisterHookRequest(BaseModel):
    """Register webhook request body (from api/api.go registerHookRequest)."""
    url: str = ""
    events: list[HookEventType] = Field(default_factory=list)
    secret: str = ""


class RegisterHookResponse(BaseModel):
    """Response for hook registration."""
    id: str
    url: str
    events: list[str]
    active: bool
    created_at: str
    updated_at: str


class DeregisterHookResponse(BaseModel):
    """Response for hook deregistration."""
    status: str = "deregistered"


class HookListItem(BaseModel):
    """Hook list item (secret omitted)."""
    id: str
    url: str
    events: list[str]
    active: bool
    created_at: str
    updated_at: str
    secret: str = ""


class DeliveryItem(BaseModel):
    """Delivery record in API response."""
    id: str
    hook_id: str
    event_type: str
    url: str
    status: str
    status_code: int
    error: str
    attempts: int
    max_attempts: int
    created_at: float
    updated_at: float


# ---------------------------------------------------------------------------
# Endpoints (mirrors Go api/api.go hooks section)
# ---------------------------------------------------------------------------

@router.post("", status_code=201, response_model=RegisterHookResponse)
async def register_hook(req: RegisterHookRequest):
    """POST /hooks — Register a new webhook subscription.

    Mirrors Go handleRegisterHook.
    """
    mgr = get_hook_manager()

    if not req.url:
        raise HTTPException(status_code=400, detail="url is required")
    if not req.events:
        raise HTTPException(status_code=400, detail="at least one event type is required")

    try:
        hook = mgr.register(url=req.url, events=req.events, secret=req.secret)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return RegisterHookResponse(
        id=hook.id,
        url=hook.url,
        events=[e.value for e in hook.events],
        active=hook.active,
        created_at=hook.created_at.isoformat(),
        updated_at=hook.updated_at.isoformat(),
    )


@router.delete("/{hook_id}", response_model=DeregisterHookResponse)
async def deregister_hook(hook_id: str):
    """DELETE /hooks/{id} — Remove a webhook by ID.

    Mirrors Go handleDeregisterHook.
    """
    mgr = get_hook_manager()

    try:
        mgr.deregister(hook_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"hook {hook_id} not found")

    return DeregisterHookResponse(status="deregistered")


@router.get("", response_model=list[HookListItem])
async def list_hooks():
    """GET /hooks — List all registered hooks (secrets omitted).

    Mirrors Go handleListHooks.
    """
    mgr = get_hook_manager()
    hooks = mgr.list_hooks()

    return [
        HookListItem(
            id=h.id,
            url=h.url,
            events=[e.value for e in h.events],
            active=h.active,
            created_at=h.created_at.isoformat(),
            updated_at=h.updated_at.isoformat(),
        )
        for h in hooks
    ]


@router.get("/{hook_id}/deliveries", response_model=list[DeliveryItem])
async def get_hook_deliveries(hook_id: str):
    """GET /hooks/{id}/deliveries — Get delivery history for a hook.

    Mirrors Go handleHookDeliveries.
    """
    mgr = get_hook_manager()

    # Verify hook exists
    hook = mgr.get(hook_id)
    if hook is None:
        raise HTTPException(status_code=404, detail=f"hook {hook_id} not found")

    deliveries = mgr.get_deliveries(hook_id)

    return [
        DeliveryItem(
            id=d.id,
            hook_id=d.hook_id,
            event_type=d.event_type,
            url=d.url,
            status=d.status,
            status_code=d.status_code,
            error=d.error,
            attempts=d.attempts,
            max_attempts=d.max_attempts,
            created_at=d.created_at,
            updated_at=d.updated_at,
        )
        for d in deliveries
    ]

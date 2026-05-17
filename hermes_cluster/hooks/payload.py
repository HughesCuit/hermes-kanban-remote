"""Webhook event types, payloads, and HMAC signing (from hooks/payload.go)."""

from __future__ import annotations

import hashlib
import hmac
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class HookEventType(str, Enum):
    """Cluster lifecycle events that trigger webhooks."""
    TASK_CREATED = "task_created"
    TASK_COMPLETED = "task_completed"
    TASK_FAILED = "task_failed"
    NODE_JOINED = "node_joined"
    NODE_LEFT = "node_left"
    LEASE_CREATED = "lease_created"
    LEASE_EXPIRED = "lease_expired"


# All valid event types for quick lookup
_ALL_EVENTS: frozenset[HookEventType] = frozenset(HookEventType)


def all_event_types() -> list[HookEventType]:
    """Return complete list of supported event types."""
    return list(HookEventType)


def is_valid_event(event_type: str) -> bool:
    """Check if a string is a valid event type."""
    return event_type in _ALL_EVENTS


class DeliveryStatus(str, Enum):
    """Webhook delivery states."""
    SUCCESS = "success"
    FAILED = "failed"
    PENDING = "pending"
    RETRY = "retry"


class Payload(BaseModel):
    """JSON body sent to webhook endpoints."""
    event_type: HookEventType = Field(default=HookEventType.TASK_CREATED, alias="event_type")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    data: Any = None


def sign_payload(body: bytes, secret: str) -> str:
    """Compute HMAC-SHA256 over raw body bytes.

    Returns hex-encoded signature string suitable for X-Hub-Signature-256 header.
    Format: sha256=<hex>
    """
    mac = hmac.new(secret.encode("utf-8"), body, hashlib.sha256)
    return f"sha256={mac.hexdigest()}"


def verify_signature(body: bytes, secret: str, signature: str) -> bool:
    """Verify HMAC-SHA256 signature of raw body bytes.

    Uses constant-time comparison to prevent timing attacks.
    """
    expected = sign_payload(body, secret)
    return hmac.compare_digest(expected, signature)

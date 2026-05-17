"""hermes_cluster.hooks — Webhook subsystem for Hermes Agent Cluster.

Provides:
- EventType / DeliveryStatus enums
- Payload model
- HMAC-SHA256 signing/verification
- HookManager (register, deregister, list, get, emit)
- Dispatcher (async delivery with exponential backoff retries)
"""

from __future__ import annotations

from hermes_cluster.hooks.payload import (
    DeliveryStatus,
    HookEventType,
    Payload,
    all_event_types,
    is_valid_event,
    sign_payload,
    verify_signature,
)

__all__ = [
    "HookEventType",
    "DeliveryStatus",
    "Payload",
    "all_event_types",
    "is_valid_event",
    "sign_payload",
    "verify_signature",
]

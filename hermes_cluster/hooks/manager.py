"""HookManager — manages webhook registrations and dispatches events (from hooks/manager.go)."""

from __future__ import annotations

import asyncio
import logging
import secrets
import time
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from hermes_cluster.hooks.dispatcher import DeliveryCallback, DeliveryRecord, Dispatcher
from hermes_cluster.hooks.payload import (
    DeliveryStatus,
    HookEventType,
    Payload,
    all_event_types,
    is_valid_event,
)

logger = logging.getLogger(__name__)


def _generate_id(prefix: str) -> str:
    """Generate a random hex ID for hooks and deliveries."""
    return f"{prefix}_{secrets.token_hex(8)}"


class Hook:
    """A registered webhook subscription.

    Mirrors the Go Hook struct from hooks/manager.go.
    """

    def __init__(
        self,
        id: str,
        url: str,
        events: list[HookEventType],
        secret: str = "",
        active: bool = True,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None,
    ):
        self.id = id
        self.url = url
        self.events = events
        self.secret = secret
        self.active = active
        self.created_at = created_at or datetime.now(timezone.utc)
        self.updated_at = updated_at or datetime.now(timezone.utc)

    def to_dict(self, include_secret: bool = False) -> dict:
        """Serialize to dict. Secret is omitted by default (matching Go List behavior)."""
        d = {
            "id": self.id,
            "url": self.url,
            "events": [e.value for e in self.events],
            "active": self.active,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }
        if include_secret:
            d["secret"] = self.secret
        else:
            d["secret"] = ""
        return d


class HookManager:
    """Manages webhook registrations and dispatches events to subscribers.

    Thread-safe (using asyncio Lock for async context).
    Mirrors Go hooks/manager.go.
    """

    def __init__(
        self,
        dispatcher: Optional[Dispatcher] = None,
        max_history: int = 1000,
    ):
        self._hooks: dict[str, Hook] = {}
        self._deliveries: list[DeliveryRecord] = []
        self._max_history = max_history
        self._dispatcher = dispatcher or Dispatcher()
        self._lock = asyncio.Lock()

    def register(self, url: str, events: list[HookEventType], secret: str = "") -> Hook:
        """Register a new webhook subscription.

        Args:
            url: The webhook endpoint URL.
            events: List of event types to subscribe to.
            secret: Optional HMAC-SHA256 secret for signing.

        Returns:
            The created Hook.

        Raises:
            ValueError: If url is empty or no events provided.
        """
        if not url:
            raise ValueError("url is required")
        if not events:
            raise ValueError("at least one event type is required")
        for event in events:
            if not is_valid_event(event.value):
                raise ValueError(f"invalid event type: {event}")

        hook = Hook(
            id=_generate_id("hook"),
            url=url,
            events=list(events),
            secret=secret,
        )
        self._hooks[hook.id] = hook
        return hook

    def deregister(self, hook_id: str) -> None:
        """Remove a webhook by ID.

        Raises:
            KeyError: If hook not found.
        """
        if hook_id not in self._hooks:
            raise KeyError(f"hook {hook_id} not found")
        del self._hooks[hook_id]

    def get(self, hook_id: str) -> Optional[Hook]:
        """Return a hook by ID, or None."""
        return self._hooks.get(hook_id)

    def list_hooks(self) -> list[Hook]:
        """Return all registered hooks (without secrets)."""
        result = []
        for hook in self._hooks.values():
            # Create a copy without secret for list responses
            h = Hook(
                id=hook.id,
                url=hook.url,
                events=list(hook.events),
                secret="",  # Omit secret in list responses
                active=hook.active,
                created_at=hook.created_at,
                updated_at=hook.updated_at,
            )
            result.append(h)
        return result

    def get_hooks_for_event(self, event_type: HookEventType) -> list[Hook]:
        """Return all active hooks that subscribe to the given event type."""
        result = []
        for hook in self._hooks.values():
            if not hook.active:
                continue
            if event_type in hook.events:
                result.append(hook)
        return result

    def emit(self, event_type: HookEventType, data: Any) -> int:
        """Dispatch an event to all matching hooks asynchronously.

        Returns the number of hooks triggered.
        """
        hooks = self.get_hooks_for_event(event_type)
        if not hooks:
            return 0

        payload = Payload(
            event_type=event_type,
            timestamp=datetime.now(timezone.utc),
            data=data,
        )

        for hook in hooks:
            asyncio.get_event_loop().create_task(
                self._dispatcher.deliver(
                    hook_id=hook.id,
                    hook_url=hook.url,
                    hook_secret=hook.secret,
                    payload=payload,
                    callback=self._record_delivery,
                )
            )
        return len(hooks)

    def _record_delivery(self, record: DeliveryRecord) -> None:
        """Record a delivery attempt to history (thread-safe)."""
        # Cap history to max_history
        if len(self._deliveries) >= self._max_history:
            self._deliveries.pop(0)  # Remove oldest (FIFO)
        self._deliveries.append(record)

    def get_deliveries(self, hook_id: str) -> list[DeliveryRecord]:
        """Return delivery history for a specific hook."""
        return [d for d in self._deliveries if d.hook_id == hook_id]

    def get_deliveries_all(self) -> list[DeliveryRecord]:
        """Return all delivery history (capped by max_history)."""
        return list(self._deliveries)

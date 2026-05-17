"""Dispatcher — async webhook delivery with exponential backoff retries (from hooks/dispatcher.go)."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

from hermes_cluster.hooks.payload import (
    DeliveryStatus,
    HookEventType,
    Payload,
    sign_payload,
)

logger = logging.getLogger(__name__)

# Defaults from Go dispatcher.go
DEFAULT_MAX_RETRIES = 3
DEFAULT_BASE_DELAY = 1.0  # seconds
DEFAULT_MAX_DELAY = 30.0  # seconds
DEFAULT_HTTP_TIMEOUT = 10  # seconds
DEFAULT_WORKER_COUNT = 4


@dataclass
class DeliveryRecord:
    """A record of a single delivery attempt."""
    id: str
    hook_id: str
    event_type: str
    url: str
    status: str  # DeliveryStatus value
    status_code: int = 0
    error: str = ""
    attempts: int = 0
    max_attempts: int = 0
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)


# Type for delivery callbacks
DeliveryCallback = Callable[[DeliveryRecord], None]


class Dispatcher:
    """Handles async webhook delivery with exponential backoff retries.

    Mirrors Go's dispatcher.go implementation:
    - Worker pool for concurrent deliveries
    - Exponential backoff with jitter on failure
    - HMAC-SHA256 signing if secret configured
    - Delivery callback for recording results
    """

    def __init__(
        self,
        max_retries: int = DEFAULT_MAX_RETRIES,
        base_delay: float = DEFAULT_BASE_DELAY,
        max_delay: float = DEFAULT_MAX_DELAY,
        http_timeout: int = DEFAULT_HTTP_TIMEOUT,
        worker_count: int = DEFAULT_WORKER_COUNT,
    ):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.http_timeout = http_timeout
        self.worker_count = worker_count
        self._running = False
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._tasks: list[asyncio.Task] = []

    def start(self) -> None:
        """Mark dispatcher as ready. Workers are launched on-demand."""
        self._running = True
        self._semaphore = asyncio.Semaphore(self.worker_count)
        logger.info(
            "hooks dispatcher started: workers=%d max_retries=%d",
            self.worker_count,
            self.max_retries,
        )

    def stop(self) -> None:
        """Signal workers to stop."""
        self._running = False
        logger.info("hooks dispatcher stopped")

    async def deliver(
        self,
        hook_id: str,
        hook_url: str,
        hook_secret: str,
        payload: Payload,
        callback: Optional[DeliveryCallback] = None,
    ) -> DeliveryRecord:
        """Deliver payload to a webhook endpoint with retries.

        Returns the final DeliveryRecord.
        """
        body = json.dumps(payload.model_dump(), default=str).encode("utf-8")
        delivery_id = _generate_id("deliv")
        max_attempts = self.max_retries + 1

        for attempt in range(1, max_attempts + 1):
            if not self._running:
                record = DeliveryRecord(
                    id=delivery_id,
                    hook_id=hook_id,
                    event_type=payload.event_type.value,
                    url=hook_url,
                    status=DeliveryStatus.PENDING.value,
                    attempts=attempt,
                    max_attempts=max_attempts,
                    error="dispatcher stopped",
                )
                if callback:
                    callback(record)
                return record

            status_code, error = await self._send_request(
                hook_url, hook_secret, body
            )

            err_msg = f"status {status_code}" if error is None else str(error)

            if error is None and 200 <= status_code < 300:
                record = DeliveryRecord(
                    id=delivery_id,
                    hook_id=hook_id,
                    event_type=payload.event_type.value,
                    url=hook_url,
                    status=DeliveryStatus.SUCCESS.value,
                    status_code=status_code,
                    attempts=attempt,
                    max_attempts=max_attempts,
                )
                if callback:
                    callback(record)
                return record

            # If this was the last attempt, record failure
            if attempt >= max_attempts:
                record = DeliveryRecord(
                    id=delivery_id,
                    hook_id=hook_id,
                    event_type=payload.event_type.value,
                    url=hook_url,
                    status=DeliveryStatus.FAILED.value,
                    status_code=status_code,
                    error=err_msg,
                    attempts=attempt,
                    max_attempts=max_attempts,
                )
                if callback:
                    callback(record)
                return record

            # Record retry attempt
            if callback:
                callback(DeliveryRecord(
                    id=delivery_id,
                    hook_id=hook_id,
                    event_type=payload.event_type.value,
                    url=hook_url,
                    status=DeliveryStatus.RETRY.value,
                    status_code=status_code,
                    error=err_msg,
                    attempts=attempt,
                    max_attempts=max_attempts,
                ))

            # Exponential backoff before next attempt
            delay = self._compute_backoff(attempt)
            await asyncio.sleep(delay)

        # Should not reach here, but safety fallback
        record = DeliveryRecord(
            id=delivery_id,
            hook_id=hook_id,
            event_type=payload.event_type.value,
            url=hook_url,
            status=DeliveryStatus.FAILED.value,
            error="exhausted retries",
            attempts=max_attempts,
            max_attempts=max_attempts,
        )
        if callback:
            callback(record)
        return record

    async def _send_request(
        self, url: str, secret: str, body: bytes
    ) -> tuple[int, Optional[str]]:
        """Perform HTTP POST to webhook endpoint.

        Returns (status_code, error_message).
        """
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "hermes-agent-cluster/1.0",
        }

        # HMAC-SHA256 signature if secret configured
        if secret:
            signature = sign_payload(body, secret)
            headers["X-Hub-Signature-256"] = signature

        try:
            req = Request(url, data=body, headers=headers, method="POST")
            with urlopen(req, timeout=self.http_timeout) as resp:
                return resp.status, None
        except HTTPError as e:
            return e.code, str(e)
        except URLError as e:
            return 0, str(e)
        except Exception as e:
            return 0, str(e)

    def _compute_backoff(self, attempt: int) -> float:
        """Calculate delay with exponential backoff, capped at max_delay."""
        delay = self.base_delay * math.pow(2, attempt - 1)
        return min(delay, self.max_delay)


def _generate_id(prefix: str) -> str:
    """Generate a random hex ID."""
    import secrets
    return f"{prefix}_{secrets.token_hex(8)}"

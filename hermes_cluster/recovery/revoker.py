"""Revoker — revokes all leases for a failed node.

Mirrors Go's internal/recovery/revoker.go:
  - RevokeAllForNode(node_id) → list of task_ids whose leases were revoked
  - Logs each revocation as a RecoveryEvent
"""

from __future__ import annotations

import secrets
import threading
from typing import TYPE_CHECKING, List

from ..models import LeaseStatus, RecoveryEvent

if TYPE_CHECKING:
    from ..state import ClusterState


def _gen_id(prefix: str = "recovery") -> str:
    return f"{prefix}_{secrets.token_hex(8)}"


class Revoker:
    """Revokes all active leases belonging to a failed node."""

    def __init__(self, state: "ClusterState") -> None:
        self._state = state
        self._lock = threading.Lock()

    def revoke_all_for_node(self, node_id: str) -> List[str]:
        """Revoke all active leases for *node_id* and log each revocation.

        Returns:
            List of task_ids whose leases were revoked.
        """
        revoked_task_ids: List[str] = []

        # Get all active leases and filter by node
        active_leases = self._state.get_active_leases()
        for lease in active_leases:
            if lease.node_id == node_id:
                # Revoke the lease
                success = self._state.revoke_lease(lease.id)
                if success:
                    revoked_task_ids.append(lease.task_id)
                    # Log the revocation event
                    event = RecoveryEvent(
                        id=_gen_id(),
                        task_id=lease.task_id,
                        node_id=node_id,
                        action="revoke_lease",
                        status="completed",
                        message=f"Revoked lease {lease.id} for task {lease.task_id}",
                    )
                    self._state.append_recovery_event(event)

        return revoked_task_ids

"""Recovery module — trigger/log/stats + auto-recovery pipeline.

Mirrors Go's internal/recovery package:
  - Revoker: revokes all leases for a failed node
  - Rescheduler: reschedules orphaned tasks
  - Detector: watches for offline events, triggers full recovery
  - RecoveryManager: orchestrator with background auto-recovery
"""

from .manager import RecoveryManager
from .revoker import Revoker
from .rescheduler import Rescheduler
from .detector import Detector

__all__ = ["RecoveryManager", "Revoker", "Rescheduler", "Detector"]

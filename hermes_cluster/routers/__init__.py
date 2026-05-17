"""Router package — aggregates all API routers."""

from .nodes import router as nodes_router
from .tasks import router as tasks_router
from .leases import router as leases_router
from .sync import router as sync_router
from .recovery import router as recovery_router
from .schedule import router as schedule_router
from .federation import router as federation_router
from .hooks import router as hooks_router
from .workflow import router as workflow_router
from .status import router as status_router
from .config import router as config_router
from .visualization import router as visualization_router

__all__ = [
    "nodes_router",
    "tasks_router",
    "leases_router",
    "sync_router",
    "recovery_router",
    "schedule_router",
    "federation_router",
    "hooks_router",
    "workflow_router",
    "status_router",
    "config_router",
    "visualization_router",
]

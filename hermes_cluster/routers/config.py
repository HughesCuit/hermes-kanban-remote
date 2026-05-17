"""Config management endpoints — /api/v1/config"""

from fastapi import APIRouter, HTTPException, Query

from ..models import ConfigJSON
from ..state import ClusterState

router = APIRouter(prefix="/api/v1/config", tags=["config"])

_state: ClusterState = None


def init(state: ClusterState):
    global _state
    _state = state


@router.get("")
async def get_config(defaults: bool = Query(False, description="Return default config")):
    if defaults:
        return ConfigJSON()
    config = _state.get_config()
    if config is None:
        # Return defaults
        return ConfigJSON()
    return config


@router.put("")
async def update_config(cfg: ConfigJSON):
    config_dict = cfg.model_dump()
    _state.set_config(config_dict)

    # Try to save to file if path is configured
    config_path = _state.get_config_path()
    if config_path:
        try:
            import yaml
            with open(config_path, "w") as f:
                yaml.dump(config_dict, f, default_flow_style=False)
        except ImportError:
            pass  # yaml not available, skip file save
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"failed to save config: {str(e)}"
            )

    return {"status": "saved", "config": config_dict}

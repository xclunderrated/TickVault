"""Backward-compatible settings imports; implementation lives in core."""

from .core.config import (
    CONFIG,
    Config,
    get_config,
    reload_config,
    snapshot_config,
    use_config,
)

__all__ = [
    "CONFIG",
    "Config",
    "reload_config",
    "get_config",
    "snapshot_config",
    "use_config",
]

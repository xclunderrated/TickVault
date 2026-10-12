"""Download APIs, progress models, and transport implementation."""

from typing import TYPE_CHECKING, Any

from .models import DownloadProgress, DownloadReport

if TYPE_CHECKING:
    from .api import download_range

__all__ = ["DownloadProgress", "DownloadReport", "download_range"]


def __getattr__(name: str) -> Any:
    if name == "download_range":
        from .api import download_range

        return download_range
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

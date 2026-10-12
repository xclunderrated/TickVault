"""Compatibility entry point for the download package."""

from .download.api import download_range
from .download.models import DownloadProgress, DownloadReport

__all__ = ["download_range", "DownloadProgress", "DownloadReport"]

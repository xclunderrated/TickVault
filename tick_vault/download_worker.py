"""Compatibility entry point for queue download workers."""

from .download.worker import download_worker

__all__ = ["download_worker"]

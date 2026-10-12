"""Compatibility entry point for the durable metadata writer."""

from .download.metadata_worker import metadata_worker

__all__ = ["metadata_worker"]

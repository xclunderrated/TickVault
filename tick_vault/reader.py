"""Compatibility imports; reading lives in :mod:`tick_vault.read`."""

from .read.reader import DecodeBackend, empty_tick_frame, iter_tick_data, read_tick_data

__all__ = ["DecodeBackend", "empty_tick_frame", "iter_tick_data", "read_tick_data"]

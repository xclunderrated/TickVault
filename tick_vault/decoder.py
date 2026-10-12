"""Compatibility imports for the validated BI5 decoder."""

from .read.decoder import RAW_DTYPE, TICK_DTYPE, decode_chunk

__all__ = ["RAW_DTYPE", "TICK_DTYPE", "decode_chunk"]

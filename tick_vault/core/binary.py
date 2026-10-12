"""Validate legacy hourly .bi5 records before publishing them to the cache."""

import lzma

import numpy as np

RAW_DTYPE = np.dtype(
    [
        ("time", ">u4"),
        ("ask", ">u4"),
        ("bid", ">u4"),
        ("ask_volume", ">f4"),
        ("bid_volume", ">f4"),
    ]
)
MAX_RAW_BYTES = 64 * 1024 * 1024


def validate_payload(content: bytes) -> bytes:
    """Return decompressed records, rejecting truncated or non-tick responses.

    An LZMA stream containing no records is a legitimate empty hour. This decoder
    accepts only the legacy hourly format; daily S3 files have another timebase.
    """
    if not isinstance(content, bytes):
        raise TypeError("payload must be bytes")
    if not content:
        raise ValueError("empty compressed payload")
    try:
        decoder = lzma.LZMADecompressor(memlimit=256 * 1024 * 1024)
        raw = decoder.decompress(content, max_length=MAX_RAW_BYTES + 1)
    except lzma.LZMAError as exc:
        raise ValueError("invalid LZMA tick payload") from exc
    if len(raw) > MAX_RAW_BYTES:
        raise ValueError("hourly tick payload exceeds decompression limit")
    if not decoder.eof or decoder.unused_data:
        raise ValueError("truncated LZMA stream or unexpected trailing data")
    if len(raw) % RAW_DTYPE.itemsize:
        raise ValueError("tick payload length must be a multiple of 20 bytes")
    ticks = np.frombuffer(raw, dtype=RAW_DTYPE)
    if np.any(ticks["time"] >= 3_600_000):
        raise ValueError("tick timestamp is outside its hour")
    if np.any(ticks["time"][1:] < ticks["time"][:-1]):
        raise ValueError("tick timestamps are out of order")
    for side in ("ask_volume", "bid_volume"):
        if np.any(~np.isfinite(ticks[side])) or np.any(ticks[side] < 0):
            raise ValueError("tick volumes must be finite and non-negative")
        if np.any(ticks[side].astype(np.float64) * 1e6 >= 2**63):
            raise ValueError("tick volumes overflow int64 millionths")
    return raw

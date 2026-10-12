"""Validated Dukascopy BI5 decoding with contextual corruption diagnostics."""

from pathlib import Path

import numpy as np

from ..chunk import TickChunk
from ..constants import PIPET_SIZE_REGISTRY
from ..core.binary import validate_payload

TICK_DTYPE = np.dtype(
    [
        ("time", "datetime64[ms]"),
        ("ask", "float64"),
        ("bid", "float64"),
        ("ask_volume", "int64"),
        ("bid_volume", "int64"),
    ]
)
RAW_DTYPE = np.dtype(
    [
        ("time", ">u4"),
        ("ask", ">u4"),
        ("bid", ">u4"),
        ("ask_volume", ">f4"),
        ("bid_volume", ">f4"),
    ]
)


def decode_chunk(
    chunk: TickChunk,
    pipet_scale: float | None = None,
    *,
    base: str | Path | None = None,
) -> np.ndarray:
    """Decode an hour to a sorted structured array of UTC-naive timestamps.

    Volumes retain the existing API's integer millionths representation. BI5
    floats are validated before conversion, including overflow prevention.
    """
    if pipet_scale is None:
        try:
            pipet_scale = PIPET_SIZE_REGISTRY[chunk.symbol]
        except KeyError as exc:
            raise ValueError(
                f"Pipet scale is not registered for {chunk.symbol}; pass pipet_scale."
            ) from exc
    if not np.isfinite(pipet_scale) or pipet_scale <= 0:
        raise ValueError("pipet_scale must be a finite positive number")
    content = chunk.load(base)
    try:
        buffer = validate_payload(content)
    except ValueError as exc:
        raise ValueError(
            f"Corrupt chunk {chunk.symbol} at {chunk.time.isoformat()} "
            f"({chunk.path(base)}): {exc}"
        ) from exc
    raw_data = np.frombuffer(buffer, dtype=RAW_DTYPE)
    result = np.empty(len(raw_data), dtype=TICK_DTYPE)
    start = np.datetime64(chunk.time.replace(tzinfo=None), "ms")
    result["time"] = start + raw_data["time"].astype("timedelta64[ms]")
    result["ask"] = raw_data["ask"].astype(np.float64) * pipet_scale
    result["bid"] = raw_data["bid"].astype(np.float64) * pipet_scale
    for field in ("ask_volume", "bid_volume"):
        volumes = np.round(raw_data[field].astype(np.float64) * 1e6)
        if np.any(volumes >= np.iinfo(np.int64).max):
            raise ValueError(
                f"Corrupt chunk {chunk.path(base)}: {field} overflows int64"
            )
        result[field] = volumes.astype(np.int64)
    if len(result) > 1 and np.any(result["time"][1:] < result["time"][:-1]):
        result = result[np.argsort(result["time"], kind="stable")]
    return result

"""Exact-range tick queries and bounded serial, thread or process decoding."""

from collections import deque
from collections.abc import Generator, Iterator
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd
from tqdm import tqdm

from ..chunk import TickChunk
from ..config import get_config
from ..constants import PIPET_SIZE_REGISTRY
from ..metadata import MetadataDB
from ..utils import normalize_datetime, normalize_symbol
from .decoder import TICK_DTYPE, decode_chunk

DecodeBackend = Literal["serial", "thread", "process"]


def empty_tick_frame() -> pd.DataFrame:
    """Consistent schema, including for empty ranges and empty markets."""
    return pd.DataFrame(np.empty(0, dtype=TICK_DTYPE))


def _decode_file(chunk: TickChunk, scale: float, base: Path) -> np.ndarray:
    # Explicit paths ensure spawned process workers use their parent's storage.
    return decode_chunk(chunk, pipet_scale=scale, base=base)


def _decoded_chunks(
    chunks: Iterator[TickChunk],
    scale: float,
    backend: DecodeBackend,
    max_workers: int | None,
    max_pending: int | None,
) -> Generator[np.ndarray, None, None]:
    base = get_config().save_directory
    if backend == "serial":
        for chunk in chunks:
            yield _decode_file(chunk, scale, base)
        return
    executor_cls = ThreadPoolExecutor if backend == "thread" else ProcessPoolExecutor
    with executor_cls(max_workers=max_workers) as executor:
        # Bounded ordered futures avoid Executor.map's eager consumption before
        # Python 3.14 and cap both submitted work and decoded memory.
        limit = max_pending or max(1, (max_workers or 4) * 2)
        pending: deque[Future[np.ndarray]] = deque()
        try:
            for _ in range(limit):
                next_chunk = next(chunks, None)
                if next_chunk is None:
                    break
                pending.append(executor.submit(_decode_file, next_chunk, scale, base))
            while pending:
                yield pending.popleft().result()
                next_chunk = next(chunks, None)
                if next_chunk is not None:
                    pending.append(
                        executor.submit(_decode_file, next_chunk, scale, base)
                    )
        finally:
            for future in pending:
                future.cancel()


def iter_tick_data(
    symbol: str,
    start: datetime | None = None,
    end: datetime | None = None,
    pipet_scale: float | None = None,
    strict: bool = True,
    show_progress: bool = True,
    *,
    backend: DecodeBackend = "serial",
    max_workers: int | None = None,
    max_pending: int | None = None,
    batch_size: int = 100_000,
) -> Generator[pd.DataFrame, None, None]:
    """Yield ordered tick frames for exact UTC ``[start, end)`` bounds.

    Naive bounds mean UTC; aware bounds are converted to UTC. Timestamps preserve
    the existing UTC-naive pandas representation. Frames have at most batch_size
    rows. Memory holds at most one decoded hour per pending worker, independent
    of the length of the requested date range.

    Strict mode requires each overlapping hour downloaded or confirmed no-data.
    Non-strict mode clips to known coverage and skips unresolved hours. Both
    modes report corrupt or missing files claimed as downloaded.
    """
    symbol = normalize_symbol(symbol)
    if backend not in ("serial", "thread", "process"):
        raise ValueError("backend must be 'serial', 'thread' or 'process'")
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    if max_workers is not None and max_workers < 1:
        raise ValueError("max_workers must be positive")
    if max_pending is not None and max_pending < 1:
        raise ValueError("max_pending must be positive")
    start = normalize_datetime(start) if start is not None else None
    end = normalize_datetime(end) if end is not None else None
    if start is not None and end is not None:
        if end < start:
            raise ValueError("end must not be before start")
        if end == start:
            return
    if pipet_scale is None:
        try:
            pipet_scale = PIPET_SIZE_REGISTRY[symbol]
        except KeyError as exc:
            raise ValueError(
                f"Pipet scale is not registered for {symbol}; pass pipet_scale."
            ) from exc
    if not np.isfinite(pipet_scale) or pipet_scale <= 0:
        raise ValueError("pipet_scale must be a finite positive number")
    with MetadataDB() as db:
        first, last = db.first_chunk(symbol), db.last_chunk(symbol)
        if first is None or last is None:
            if strict:
                raise ValueError(
                    f"No data available for symbol {symbol} in the database"
                )
            return
        coverage_start, coverage_end = first.time, last.time + timedelta(hours=1)
        start = coverage_start if start is None else start
        end = coverage_end if end is None else end
        if strict:
            if start < coverage_start:
                raise ValueError(
                    f"Requested start {start.isoformat()} is before first available "
                    f"hour {coverage_start.isoformat()}; use strict=False to clip."
                )
            if end > coverage_end:
                raise ValueError(
                    f"Requested end {end.isoformat()} is after last available "
                    f"hour {coverage_end.isoformat()}; use strict=False to clip."
                )
        else:
            start, end = max(start, coverage_start), min(end, coverage_end)
        if end <= start:
            return
        if strict:
            db.check_for_gaps(symbol, start, end)
        lower = np.datetime64(start.replace(tzinfo=None), "us")
        upper = np.datetime64(end.replace(tzinfo=None), "us")
        chunks = db.iter_available_chunks(symbol, start, end)
        arrays = _decoded_chunks(chunks, pipet_scale, backend, max_workers, max_pending)
        with tqdm(
            desc=f"Reading {symbol}", unit="chunk", disable=not show_progress
        ) as progress:
            try:
                for array in arrays:
                    progress.update(1)
                    array = array[(array["time"] >= lower) & (array["time"] < upper)]
                    for offset in range(0, len(array), batch_size):
                        yield pd.DataFrame(array[offset : offset + batch_size])
            finally:
                arrays.close()


def read_tick_data(
    symbol: str,
    start: datetime | None = None,
    end: datetime | None = None,
    pipet_scale: float | None = None,
    strict: bool = True,
    show_progress: bool = True,
    *,
    backend: DecodeBackend = "serial",
    max_workers: int | None = None,
    max_pending: int | None = None,
    batch_size: int = 100_000,
) -> pd.DataFrame:
    """Materialize iter_tick_data; use the iterator for memory-bounded queries."""
    frames = list(
        iter_tick_data(
            symbol,
            start,
            end,
            pipet_scale,
            strict,
            show_progress,
            backend=backend,
            max_workers=max_workers,
            max_pending=max_pending,
            batch_size=batch_size,
        )
    )
    return pd.concat(frames, ignore_index=True) if frames else empty_tick_frame()

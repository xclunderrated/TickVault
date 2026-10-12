"""Convenience operations composed from the download and reader APIs."""

from __future__ import annotations

import asyncio
import math
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from .config import Config, snapshot_config, use_config
from .constants import PIPET_SIZE_REGISTRY
from .downloader import download_range
from .reader import DecodeBackend, read_tick_data
from .utils import normalize_datetime, normalize_symbol

if TYPE_CHECKING:
    from .downloader import DownloadProgress


def _utc(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{name} must be a datetime")
    return normalize_datetime(value)


def _complete_hour_now() -> datetime:
    return datetime.now(UTC).replace(minute=0, second=0, microsecond=0)


async def download_and_read(
    symbol: str,
    start: datetime,
    end: datetime | None = None,
    proxies: list[str] | None = None,
    *,
    pipet_scale: float | None = None,
    strict: bool = True,
    show_progress: bool = True,
    backend: DecodeBackend = "serial",
    max_workers: int | None = None,
    max_pending: int | None = None,
    batch_size: int = 100_000,
    stop_event: asyncio.Event | None = None,
    progress_callback: (
        Callable[[DownloadProgress], Awaitable[None] | None] | None
    ) = None,
    retry_no_data: bool = False,
    settings: Config | None = None,
    state_path: str | Path | None = None,
) -> pd.DataFrame:
    """Download missing complete hours and return ticks in exact UTC ``[start, end)``.

    Naive datetimes mean UTC. Historical sub-hour bounds are retained when reading;
    the downloader fetches their enclosing hours. An omitted or future ``end`` is
    capped at the start of the current UTC hour, because that hour is incomplete.
    ``max_workers`` controls decoding; configure download concurrency with
    :func:`tick_vault.reload_config`. A cooperative stop raises ``CancelledError``
    after the downloader has saved completed work, so partial data is not returned
    as if the requested range were complete.

    This convenience function materializes a DataFrame. For larger datasets, use
    ``download_range`` followed by ``iter_tick_data`` or a streaming export.
    """
    symbol = normalize_symbol(symbol)
    if pipet_scale is None and symbol not in PIPET_SIZE_REGISTRY:
        raise ValueError(
            f"Pipet scale is not registered for {symbol}; supply pipet_scale"
        )
    if pipet_scale is not None and (not math.isfinite(pipet_scale) or pipet_scale <= 0):
        raise ValueError("pipet_scale must be positive and finite")
    if backend not in {"serial", "thread", "process"}:
        raise ValueError("backend must be serial, thread, or process")
    for name, value in (
        ("max_workers", max_workers),
        ("max_pending", max_pending),
        ("batch_size", batch_size),
    ):
        if value is not None and (
            not isinstance(value, int) or isinstance(value, bool) or value < 1
        ):
            raise ValueError(f"{name} must be a positive integer")
    start = _utc(start, "start")
    complete_hour = _complete_hour_now()
    if end is not None:
        end = _utc(end, "end")
        if end <= start:
            raise ValueError("end must be after start")
    end = complete_hour if end is None else min(end, complete_hour)
    if end <= start:
        raise ValueError("The requested range contains no complete historical hour")

    captured = (settings or snapshot_config()).model_copy(deep=True)
    with use_config(captured):
        report = await download_range(
            symbol,
            start,
            end,
            proxies,
            stop_event=stop_event,
            progress_callback=progress_callback,
            show_progress=show_progress,
            retry_no_data=retry_no_data,
            settings=captured,
            state_path=state_path,
        )
        if report.cancelled:
            raise asyncio.CancelledError("Download stopped; completed hours are saved")
        # The context snapshot is propagated to the reader thread, preserving
        # this vault even when another operation reloads the global settings.
        return await asyncio.to_thread(
            read_tick_data,
            symbol,
            start,
            end,
            pipet_scale=pipet_scale,
            strict=strict,
            show_progress=show_progress,
            backend=backend,
            max_workers=max_workers,
            max_pending=max_pending,
            batch_size=batch_size,
        )

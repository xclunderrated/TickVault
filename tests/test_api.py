import asyncio
from datetime import UTC, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pandas as pd
import pytest

from tick_vault import api


@pytest.mark.asyncio
async def test_download_and_read_preserves_same_hour_exact_utc_bounds(monkeypatch):
    download = AsyncMock(return_value=SimpleNamespace(cancelled=False))
    expected = pd.DataFrame({"time": [datetime(2024, 1, 2, 10, 20, tzinfo=UTC)]})
    read = Mock(return_value=expected)
    monkeypatch.setattr(api, "download_range", download)
    monkeypatch.setattr(api, "read_tick_data", read)
    start = datetime(2024, 1, 2, 12, 15, tzinfo=timezone(timedelta(hours=2)))
    end = datetime(2024, 1, 2, 10, 45)  # Naive datetimes mean UTC.
    result = await api.download_and_read(
        "EURUSD",
        start,
        end,
        pipet_scale=0.00001,
        backend="thread",
        max_workers=2,
        show_progress=False,
    )
    assert result is expected
    download_args = download.await_args.args
    read_args = read.call_args.args
    assert (
        download_args[:3]
        == read_args[:3]
        == (
            "EURUSD",
            datetime(2024, 1, 2, 10, 15, tzinfo=UTC),
            end.replace(tzinfo=UTC),
        )
    )
    assert read.call_args.kwargs["backend"] == "thread"
    assert read.call_args.kwargs["max_workers"] == 2
    assert read.call_args.kwargs["pipet_scale"] == 0.00001


@pytest.mark.asyncio
async def test_default_end_is_resolved_at_call_and_excludes_current_hour(monkeypatch):
    now = datetime(2024, 1, 2, 14, 39, tzinfo=UTC)

    monkeypatch.setattr(api, "_complete_hour_now", lambda: now.replace(minute=0))
    download = AsyncMock(return_value=SimpleNamespace(cancelled=False))
    monkeypatch.setattr(api, "download_range", download)
    monkeypatch.setattr(api, "read_tick_data", Mock(return_value=pd.DataFrame()))
    start = datetime(2024, 1, 2, 10)
    await api.download_and_read("EURUSD", start, show_progress=False)
    assert download.await_args.args[2] == now.replace(minute=0)
    now += timedelta(hours=1)
    await api.download_and_read("EURUSD", start, show_progress=False)
    assert download.await_args.args[2] == now.replace(minute=0)


@pytest.mark.asyncio
async def test_cooperative_cancellation_does_not_read_partial_download(monkeypatch):
    download = AsyncMock(return_value=SimpleNamespace(cancelled=True))
    read = Mock()
    monkeypatch.setattr(api, "download_range", download)
    monkeypatch.setattr(api, "read_tick_data", read)
    with pytest.raises(asyncio.CancelledError):
        await api.download_and_read(
            "EURUSD", datetime(2024, 1, 2), datetime(2024, 1, 3)
        )
    read.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("end", [datetime(2024, 1, 2), datetime(2024, 1, 1)])
async def test_invalid_range_never_downloads(monkeypatch, end):
    download = AsyncMock()
    monkeypatch.setattr(api, "download_range", download)
    with pytest.raises(ValueError, match="end must be after start"):
        await api.download_and_read("EURUSD", datetime(2024, 1, 2), end)
    download.assert_not_called()


@pytest.mark.asyncio
async def test_download_failure_propagates_without_reading(monkeypatch):
    download = AsyncMock(side_effect=RuntimeError("feed unavailable"))
    read = Mock()
    monkeypatch.setattr(api, "download_range", download)
    monkeypatch.setattr(api, "read_tick_data", read)
    with pytest.raises(RuntimeError, match="feed unavailable"):
        await api.download_and_read(
            "EURUSD", datetime(2024, 1, 2), datetime(2024, 1, 3)
        )
    read.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "options",
    [
        {"backend": "unsupported"},
        {"max_workers": 0},
        {"max_pending": -1},
        {"batch_size": 0},
        {"pipet_scale": float("nan")},
    ],
)
async def test_invalid_read_options_fail_before_download(monkeypatch, options):
    download = AsyncMock()
    monkeypatch.setattr(api, "download_range", download)
    with pytest.raises(ValueError):
        await api.download_and_read(
            "EURUSD", datetime(2024, 1, 2), datetime(2024, 1, 3), **options
        )
    download.assert_not_called()

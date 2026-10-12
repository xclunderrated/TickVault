"""Offline end-to-end pipeline checks against an HTTP mock transport."""

import json
import lzma
import struct
from datetime import UTC, datetime, timedelta

import httpx
import pandas as pd
import pytest

from tick_vault import api, cli, config
from tick_vault.downloader import download_range
from tick_vault.metadata import MetadataDB
from tick_vault.read.storage import read_hdf5, read_sqlite
from tick_vault.reader import read_tick_data


@pytest.fixture
def archive(tmp_path):
    original = config.CONFIG.model_dump()
    config.reload_config(
        base_directory=tmp_path,
        worker_per_proxy=2,
        adaptive_workers=False,
        fetch_max_retry_attempts=0,
        datafeed_base_url="https://mock.invalid/datafeed/",
        console_log_level="ERROR",
        env_file=None,
    )
    yield tmp_path
    config.reload_config(**original)


def _payload():
    records = b"".join(
        struct.pack(">IIIff", offset, 110020 + index, 110000 + index, 1.25, 2.5)
        for index, offset in enumerate((0, 30 * 60_000, 40 * 60_000))
    )
    return lzma.compress(records, format=lzma.FORMAT_ALONE)


def _mock_feed(monkeypatch, responses):
    # The transport accepts only explicitly declared paths. No test uses sockets.
    from tick_vault.download import api as downloader

    calls = []

    def handle(request):
        assert request.url.host == "mock.invalid"
        path = request.url.path
        assert path in responses, f"Unexpected feed URL: {request.url}"
        calls.append(path)
        status, content = responses[path]
        return httpx.Response(status, content=content, request=request)

    transport = httpx.MockTransport(handle)
    client_type = httpx.AsyncClient

    def client(*args, **kwargs):
        kwargs["transport"] = transport
        return client_type(*args, **kwargs)

    monkeypatch.setattr(downloader, "AsyncClient", client)
    return calls


@pytest.mark.asyncio
async def test_same_hour_download_and_read_exact_bounds_and_resume(
    archive, monkeypatch
):
    calls = _mock_feed(
        monkeypatch, {"/datafeed/EURUSD/2024/00/02/21h_ticks.bi5": (200, _payload())}
    )
    start = datetime(2024, 1, 2, 21, 30, tzinfo=UTC)
    end = datetime(2024, 1, 2, 21, 40, tzinfo=UTC)
    data = await api.download_and_read(
        "eurusd", start, end, show_progress=False, backend="thread", max_workers=2
    )
    assert len(data) == 1
    assert data.time.iloc[0] == pd.Timestamp("2024-01-02T21:30:00")
    assert data.ask.iloc[0] == pytest.approx(1.10021)
    assert data.bid.iloc[0] == pytest.approx(1.10001)
    assert data.ask_volume.iloc[0] == 1_250_000
    assert data.bid_volume.iloc[0] == 2_500_000
    again = await api.download_and_read("EURUSD", start, end, show_progress=False)
    pd.testing.assert_frame_equal(data, again)
    report = await download_range("EURUSD", start, end, show_progress=False)
    assert report.cached == 1
    assert report.attempted == 0
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_download_no_data_status_real_cli_and_exports(
    archive, monkeypatch, capsys
):
    responses = {
        "/datafeed/EURUSD/2024/00/02/21h_ticks.bi5": (200, _payload()),
        "/datafeed/EURUSD/2024/00/02/22h_ticks.bi5": (404, b""),
        "/datafeed/EURUSD/2024/00/02/23h_ticks.bi5": (200, b""),
    }
    calls = _mock_feed(monkeypatch, responses)
    start, end = datetime(2024, 1, 2, 21, tzinfo=UTC), datetime(2024, 1, 3, tzinfo=UTC)
    report = await download_range("EURUSD", start, end, show_progress=False)
    assert report.total == 3
    assert report.downloaded == 1
    assert report.no_data == 2
    data = read_tick_data("EURUSD", start, end, show_progress=False)
    assert len(data) == 3
    # All attempted no-data hours still form verified strict coverage.
    empty = read_tick_data(
        "EURUSD", start + timedelta(hours=1), end, show_progress=False
    )
    assert empty.empty
    assert list(empty.columns) == ["time", "ask", "bid", "ask_volume", "bid_volume"]
    with MetadataDB() as database:
        status = database.status("EURUSD", start, end)
    assert status["downloaded"] == 1
    assert status["no_data"] == 2
    assert status["failed"] == 0
    assert (
        cli.main(
            ["status", "EURUSD", "--start", start.isoformat(), "--end", end.isoformat()]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["no_data"] == 2

    csv = archive / "ticks.csv"
    assert cli.main(["read", "EURUSD", "--output", str(csv), "--batch-size", "1"]) == 0
    pd.testing.assert_frame_equal(
        pd.read_csv(csv, parse_dates=["time"]).astype({"time": "datetime64[ns]"}),
        data.astype({"time": "datetime64[ns]"}),
        check_dtype=False,
    )
    sqlite = archive / "ticks.sqlite"
    assert cli.main(["export", "EURUSD", str(sqlite), "--batch-size", "1"]) == 0
    pd.testing.assert_frame_equal(
        read_sqlite(sqlite, "EURUSD").astype({"time": "datetime64[ns]"}),
        data.astype({"time": "datetime64[ns]"}),
        check_dtype=False,
    )
    cached = await download_range("EURUSD", start, end, show_progress=False)
    assert cached.cached == 3
    assert len(calls) == 3  # Local status, reads, exports, and resume send no request.


@pytest.mark.asyncio
async def test_real_cli_hdf5_export(archive, monkeypatch):
    pytest.importorskip("tables")
    _mock_feed(
        monkeypatch, {"/datafeed/EURUSD/2024/00/02/21h_ticks.bi5": (200, _payload())}
    )
    start, end = datetime(2024, 1, 2, 21, tzinfo=UTC), datetime(
        2024, 1, 2, 22, tzinfo=UTC
    )
    await download_range("EURUSD", start, end, show_progress=False)
    output = archive / "ticks.h5"
    assert cli.main(["export", "EURUSD", str(output), "--batch-size", "1"]) == 0
    expected = read_tick_data("EURUSD", start, end, show_progress=False)
    pd.testing.assert_frame_equal(
        read_hdf5(output, "EURUSD").astype({"time": "datetime64[ns]"}),
        expected.astype({"time": "datetime64[ns]"}),
        check_dtype=False,
    )

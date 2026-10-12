"""Regression cases identified by an independent cancellation/cache review."""

import asyncio
import json
import lzma
import struct
import subprocess
import sys
from datetime import datetime, timedelta

import httpx
import pytest

from tick_vault import cli, config
from tick_vault.chunk import TickChunk
from tick_vault.download import api as download_api
from tick_vault.metadata import MetadataDB
from tick_vault.reader import iter_tick_data


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
        queue_size=1,
        env_file=None,
    )
    yield tmp_path
    config.reload_config(**original)


def _payload():
    return lzma.compress(
        struct.pack(">IIIff", 0, 110020, 110000, 1.25, 2.5), format=lzma.FORMAT_ALONE
    )


def _transport(monkeypatch, handler):
    client_type = httpx.AsyncClient

    def factory(*args, **kwargs):
        return client_type(*args, **kwargs, transport=httpx.MockTransport(handler))

    monkeypatch.setattr(download_api, "AsyncClient", factory)


@pytest.mark.asyncio
async def test_cooperative_stop_interrupts_awaited_progress_callback(
    archive, monkeypatch
):
    stop, entered = asyncio.Event(), asyncio.Event()
    _transport(
        monkeypatch,
        lambda request: httpx.Response(200, content=_payload(), request=request),
    )

    async def progress(snapshot):
        if snapshot.downloaded:
            entered.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(
        download_api.download_range(
            "EURUSD",
            datetime(2024, 1, 2),
            datetime(2024, 1, 2, 1),
            progress_callback=progress,
            stop_event=stop,
            show_progress=False,
        )
    )
    await asyncio.wait_for(entered.wait(), 1)
    stop.set()
    report = await asyncio.wait_for(task, 0.2)
    assert report.cancelled
    assert report.downloaded == 1
    with MetadataDB() as database:
        assert database.status("EURUSD")["downloaded"] == 1


@pytest.mark.asyncio
async def test_rate_limited_retries_obey_reduced_capacity(archive, monkeypatch):
    config.reload_config(
        **(
            config.CONFIG.model_dump()
            | {
                "adaptive_workers": True,
                "fetch_max_retry_attempts": 1,
                "adaptive_adjustment_interval": 100,
            }
        )
    )
    initial_ready = asyncio.Event()
    initial_count = retry_active = maximum_retry_active = 0
    attempts = {}

    async def handle(request):
        nonlocal initial_count, retry_active, maximum_retry_active
        path = request.url.path
        attempts[path] = attempts.get(path, 0) + 1
        if attempts[path] == 1:
            initial_count += 1
            if initial_count == 2:
                initial_ready.set()
            await initial_ready.wait()
            return httpx.Response(429, headers={"Retry-After": "0.02"}, request=request)
        retry_active += 1
        maximum_retry_active = max(maximum_retry_active, retry_active)
        await asyncio.sleep(0.03)
        retry_active -= 1
        return httpx.Response(200, content=_payload(), request=request)

    _transport(monkeypatch, handle)
    report = await asyncio.wait_for(
        download_api.download_range(
            "EURUSD",
            datetime(2024, 1, 2),
            datetime(2024, 1, 2, 2),
            show_progress=False,
        ),
        1,
    )
    assert report.downloaded == 2
    assert maximum_retry_active <= 1  # First rate limit reduces capacity 2 -> 1.


@pytest.mark.parametrize(
    "target", ["metadata", "raw", "log", "-wal", "-shm", "-journal"]
)
def test_csv_export_cannot_overwrite_vault_state(archive, target):
    chunk = TickChunk(symbol="EURUSD", time=datetime(2024, 1, 2))
    chunk.save(_payload())
    with MetadataDB() as database:
        database.insert_rows([chunk])
    if target.startswith("-"):
        metadata = config.CONFIG.metadata_db_path
        destination = metadata.with_name(metadata.name + target)
        assert not destination.exists()  # Metadata connection is closed above.
        destination.write_bytes(b"original test sidecar; no active SQLite connection")
    else:
        destination = {
            "metadata": config.CONFIG.metadata_db_path,
            "raw": chunk.path(),
            "log": config.CONFIG.log_file_path,
        }[target]
    original = destination.read_bytes()
    assert cli.main(["read", "EURUSD", "--output", str(destination)]) == 1
    assert destination.read_bytes() == original


def test_real_download_cli_stdout_contains_one_json_document(
    archive, monkeypatch, capsys
):
    _transport(
        monkeypatch,
        lambda request: httpx.Response(200, content=_payload(), request=request),
    )
    assert (
        cli.main(
            [
                "download",
                "EURUSD",
                "2024-01-02",
                "--end",
                "2024-01-02T01:00:00Z",
                "--json",
            ]
        )
        == 0
    )
    output = capsys.readouterr()
    report = json.loads(output.out)
    assert report["downloaded"] == 1
    assert output.out.count("\n") == 1


def test_csv_pipe_consumer_can_close_early_without_shutdown_error(archive):
    chunk = TickChunk(symbol="EURUSD", time=datetime(2024, 1, 2))
    records = struct.pack(">IIIff", 0, 110020, 110000, 1.25, 2.5) * 20_000
    chunk.save(lzma.compress(records, format=lzma.FORMAT_ALONE))
    with MetadataDB() as database:
        database.insert_rows([chunk])
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "tick_vault",
            "read",
            "EURUSD",
            "--base-directory",
            str(archive),
            "--quiet",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert process.stdout.readline().startswith("time,ask,bid")
        process.stdout.close()
        process.stdout = None
        # A loaded Windows runner may spend several seconds shutting down the
        # interpreter. This checks clean pipe closure, not startup/exit latency.
        _, errors = process.communicate(timeout=20)
        assert process.returncode == 0, errors
        assert "BrokenPipeError" not in errors
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate()


def test_streaming_reader_does_not_lock_concurrent_metadata_writer(archive):
    start = datetime(2024, 1, 2)
    chunks = [
        TickChunk(symbol="EURUSD", time=start + timedelta(hours=hour))
        for hour in range(2)
    ]
    for chunk in chunks:
        chunk.save(_payload())
    with MetadataDB() as database:
        database.insert_rows(chunks)
    batches = iter_tick_data(
        "EURUSD", start, start + timedelta(hours=2), batch_size=1, show_progress=False
    )
    try:
        assert len(next(batches)) == 1  # Hold a live read cursor while processing.
        with MetadataDB() as writer:
            writer.conn.execute("PRAGMA busy_timeout=20")
            writer.mark_no_data(
                TickChunk(symbol="EURUSD", time=start + timedelta(hours=2))
            )
            assert writer.status("EURUSD")["no_data"] == 1
    finally:
        batches.close()

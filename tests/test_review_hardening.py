"""Offline regressions for restart paths, protected exports and loop liveness."""

import asyncio
import lzma
import struct
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from tick_vault import cli
from tick_vault.chunk import TickChunk
from tick_vault.config import Config, use_config
from tick_vault.download.api import download_range
from tick_vault.download.jobs import resume_download_job, submit_download_job
from tick_vault.download.metadata_worker import metadata_worker
from tick_vault.download.models import DownloadResult
from tick_vault.metadata import MetadataDB
from tick_vault.provider._state import state_path
from tick_vault.read import export_hdf5, export_sqlite

START = datetime(2024, 1, 1, tzinfo=UTC)


def payload():
    return lzma.compress(struct.pack(">IIIff", 0, 110020, 110000, 1.0, 2.0))


@pytest.fixture
def settings(tmp_path):
    config = Config(base_directory=tmp_path / "vault", _env_file=None)
    with use_config(config):
        yield config


@pytest.mark.parametrize("kind", ["database", "wal", "shm", "journal", "raw"])
@pytest.mark.parametrize("format", ["sqlite", "hdf5", "csv"])
def test_exports_cannot_overwrite_provider_state(settings, kind, format):
    provider = state_path(settings.provider_state_directory)
    target = {
        "database": provider,
        "wal": Path(str(provider) + "-wal"),
        "shm": Path(str(provider) + "-shm"),
        "journal": Path(str(provider) + "-journal"),
        "raw": provider.parent / (provider.stem + "-raw") / "ab" / "fixture.bi5",
    }[kind]
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(b"preserve provider state")
    with pytest.raises(ValueError, match="provider"):
        if format in ("sqlite", "hdf5"):
            if format == "hdf5":
                pytest.importorskip("tables")
            export = export_sqlite if format == "sqlite" else export_hdf5
            export(target, "EURUSD", START, START, show_progress=False)
        else:
            args = cli.build_parser().parse_args(["read", "EURUSD"])
            cli._validate_csv_destination(args, target)
    assert target.read_bytes() == b"preserve provider state"


def test_default_provider_path_protected_even_with_custom_state(
    settings, tmp_path, monkeypatch
):
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "appdata"))
    target = state_path()
    with pytest.raises(ValueError, match="provider"):
        export_sqlite(target, "EURUSD", START, START, show_progress=False)
    assert not target.parent.exists()


@pytest.mark.asyncio
async def test_queued_job_resumes_original_relative_vault_after_directory_change(
    tmp_path, monkeypatch
):
    first, second = tmp_path / "first", tmp_path / "second"
    first.mkdir()
    second.mkdir()
    monkeypatch.chdir(first)
    config = Config(
        base_directory=Path("vault"),
        provider_state_directory=Path("provider"),
        _env_file=None,
    )
    with use_config(config):
        chunk = TickChunk(symbol="EURUSD", time=START)
        chunk.save(payload())
        with MetadataDB() as db:
            db.record_result(chunk, "downloaded")
        job = await submit_download_job(
            "EURUSD", START, START + timedelta(minutes=2), settings=config, run=False
        )
    monkeypatch.chdir(second)
    resumed = await resume_download_job(job.job_id, state_path=first / "provider")
    assert resumed.complete, resumed.to_dict()
    assert resumed.report["cached"] == 1
    assert Path(job.config["base_directory"]) == first / "vault"
    assert Path(job.config["provider_state_directory"]).is_absolute()
    assert not (second / "vault").exists()


@pytest.mark.asyncio
async def test_cache_discovery_keeps_event_loop_responsive(settings, monkeypatch):
    chunk = TickChunk(symbol="EURUSD", time=START)
    chunk.save(payload())  # An orphan requires decompression during discovery.
    entered, release = threading.Event(), threading.Event()
    original = MetadataDB._validate_local
    main_thread = threading.get_ident()

    def validate(db, item):
        assert (
            threading.get_ident() != main_thread
        ), "cache validation blocks event loop"
        entered.set()
        assert release.wait(5)
        return original(db, item)

    monkeypatch.setattr(MetadataDB, "_validate_local", validate)
    stop = asyncio.Event()
    task = asyncio.create_task(
        download_range(
            "EURUSD",
            START,
            START + timedelta(minutes=2),
            settings=settings,
            stop_event=stop,
            show_progress=False,
        )
    )
    try:
        for _ in range(100):
            if entered.is_set() or task.done():
                break
            await asyncio.sleep(0.01)
        if task.done():
            await task
        assert entered.is_set()
        stop.set()
        await asyncio.sleep(0.02)
        assert not task.done()  # The owned disk operation finishes before exit.
        release.set()
        report = await asyncio.wait_for(task, 3)
        assert report.cancelled
        with MetadataDB() as db:
            assert db.get_status(chunk) == "downloaded"
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_metadata_commit_keeps_loop_responsive_and_survives_cancel(
    settings, monkeypatch
):
    settings.metadata_update_batch_size = 1
    entered, release = threading.Event(), threading.Event()
    original = MetadataDB.insert_rows
    main_thread = threading.get_ident()

    def insert(db, *args, **kwargs):
        assert threading.get_ident() != main_thread, "metadata commit blocks event loop"
        entered.set()
        assert release.wait(5)
        return original(db, *args, **kwargs)

    monkeypatch.setattr(MetadataDB, "insert_rows", insert)
    queue = asyncio.Queue()
    chunk = TickChunk(symbol="EURUSD", time=START)
    queue.put_nowait(DownloadResult(chunk, "no_data"))
    task = asyncio.create_task(metadata_worker(queue))
    try:
        for _ in range(100):
            if entered.is_set() or task.done():
                break
            await asyncio.sleep(0.01)
        if task.done():
            await task
        assert entered.is_set()
        task.cancel()
        await asyncio.sleep(0.02)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        await asyncio.wait_for(queue.join(), 1)
        with MetadataDB() as db:
            assert db.get_status(chunk) == "no_data"
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)

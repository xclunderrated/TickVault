import errno
import lzma
import multiprocessing
import sqlite3
import struct
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from tick_vault.chunk import TickChunk
from tick_vault.config import CONFIG, reload_config
from tick_vault.core.filelock import destination_lock
from tick_vault.metadata import MetadataDB
from tick_vault.read import (
    export_hdf5,
    export_sqlite,
    iter_hdf5,
    iter_sqlite,
    read_hdf5,
    read_sqlite,
    read_tick_data,
)


@pytest.fixture
def vault(tmp_path):
    previous = CONFIG.model_dump()
    reload_config(base_directory=tmp_path / "vault", env_file=None)
    for hour in range(2):
        chunk = TickChunk(symbol="EURUSD", time=datetime(2024, 1, 1, hour, tzinfo=UTC))
        raw = b"".join(
            struct.pack(">IIIff", offset, 123456, 123450, 1, 2)
            for offset in (0, 0, 1000, 2000)
        )
        chunk.save(lzma.compress(raw))
        with MetadataDB() as db:
            db.record_result(chunk, "downloaded")
    yield tmp_path
    reload_config(**previous, env_file=None)


@pytest.mark.parametrize("backend", ["sqlite", "hdf5"])
def test_streaming_storage_roundtrip_and_precise_query(vault, backend):
    if backend == "hdf5":
        pytest.importorskip("tables")
    export = export_sqlite if backend == "sqlite" else export_hdf5
    read = read_sqlite if backend == "sqlite" else read_hdf5
    iterate = iter_sqlite if backend == "sqlite" else iter_hdf5
    path = vault / ("ticks.db" if backend == "sqlite" else "ticks.h5")
    assert export(path, "EURUSD", show_progress=False, batch_size=2) == 8
    pd.testing.assert_frame_equal(
        read(path, "EURUSD").astype({"time": "datetime64[ns]"}),
        read_tick_data("EURUSD", show_progress=False).astype(
            {"time": "datetime64[ns]"}
        ),
        check_dtype=False,
    )
    start = datetime(2024, 1, 1, 0, 0, 0, 1, tzinfo=UTC)
    end = datetime(2024, 1, 1, 1, 0, 1, tzinfo=UTC)
    frames = list(iterate(path, "EURUSD", start, end, batch_size=1))
    assert all(len(frame) == 1 for frame in frames)
    actual = pd.concat(frames, ignore_index=True)
    expected = read_tick_data("EURUSD", start, end, show_progress=False)
    pd.testing.assert_frame_equal(
        actual.astype({"time": "datetime64[ns]"}),
        expected.astype({"time": "datetime64[ns]"}),
        check_dtype=False,
    )
    assert read(path, "GBPUSD").empty
    assert read(path, "EURUSD", start, start).empty
    with pytest.raises(ValueError, match="already exist"):
        export(path, "EURUSD", if_exists="fail", show_progress=False)
    assert export(path, "EURUSD", if_exists="replace", show_progress=False) == 8
    assert len(read(path, "EURUSD")) == 8


@pytest.mark.parametrize("backend", ["sqlite", "hdf5"])
def test_storage_failed_replace_preserves_original(vault, backend):
    if backend == "hdf5":
        pytest.importorskip("tables")
    export = export_sqlite if backend == "sqlite" else export_hdf5
    read = read_sqlite if backend == "sqlite" else read_hdf5
    path = vault / ("ticks.db" if backend == "sqlite" else "ticks.h5")
    export(path, "EURUSD", show_progress=False)
    original = read(path, "EURUSD")
    chunk = TickChunk(symbol="EURUSD", time=datetime(2024, 1, 1, 1))
    chunk.path().write_bytes(b"corrupt later chunk")
    with pytest.raises(ValueError, match="Corrupt chunk"):
        export(path, "EURUSD", show_progress=False)
    pd.testing.assert_frame_equal(read(path, "EURUSD"), original)


def test_sqlite_schema_index_duplicate_times_and_append(vault):
    path = vault / "ticks.db"
    export_sqlite(path, "EURUSD", show_progress=False)
    export_sqlite(path, "EURUSD", if_exists="append", show_progress=False)
    actual = read_sqlite(path, "EURUSD")
    assert len(actual) == 16 and actual.time.is_monotonic_increasing
    with closing(sqlite3.connect(path)) as conn:
        assert (
            conn.execute("SELECT COUNT(*) FROM ticks WHERE symbol='EURUSD'").fetchone()[
                0
            ]
            == 16
        )
        details = conn.execute(
            "EXPLAIN QUERY PLAN SELECT * FROM ticks WHERE symbol=? AND time>=? AND time<?",
            ("EURUSD", 0, 1),
        ).fetchall()
        assert any("ticks_symbol_time" in row[-1] for row in details)


def test_empty_export_and_no_silent_creation_on_read(vault):
    path = vault / "empty.db"
    now = datetime(2024, 1, 1, tzinfo=UTC)
    assert export_sqlite(path, "EURUSD", now, now, show_progress=False) == 0
    assert read_sqlite(path, "EURUSD").empty
    with pytest.raises(sqlite3.OperationalError):
        read_sqlite(vault / "missing.db", "EURUSD")
    assert not (vault / "missing.db").exists()
    with pytest.raises(ValueError, match="separate file"):
        export_sqlite(CONFIG.metadata_db_path, "EURUSD", show_progress=False)


def test_sqlite_query_timezone_microseconds(vault):
    path = vault / "ticks.db"
    export_sqlite(path, "EURUSD", show_progress=False)
    start = datetime(2024, 1, 1, 0, 0, 0, 1, tzinfo=UTC)
    end = start + timedelta(milliseconds=1000)
    assert list(read_sqlite(path, "EURUSD", start, end).time) == [
        pd.Timestamp("2024-01-01T00:00:01")
    ]


@pytest.mark.parametrize("backend", ["sqlite", "hdf5"])
def test_exports_preserve_other_symbols_and_failed_new_destination(vault, backend):
    if backend == "hdf5":
        pytest.importorskip("tables")
    export = export_sqlite if backend == "sqlite" else export_hdf5
    read = read_sqlite if backend == "sqlite" else read_hdf5
    path = vault / ("ticks.db" if backend == "sqlite" else "ticks.h5")
    other = TickChunk(symbol="GBPUSD", time=datetime(2024, 1, 1))
    other.save(lzma.compress(struct.pack(">IIIff", 0, 123456, 123450, 1, 1)))
    with MetadataDB() as db:
        db.record_result(other, "downloaded")
    export(path, "GBPUSD", show_progress=False)
    original = read(path, "GBPUSD")
    export(path, "EURUSD", show_progress=False)
    export(path, "EURUSD", if_exists="replace", show_progress=False)
    pd.testing.assert_frame_equal(read(path, "GBPUSD"), original)
    broken = TickChunk(symbol="EURUSD", time=datetime(2024, 1, 1, 1))
    broken.path().write_bytes(b"corrupt")
    nonexistent = path.with_name("failed" + path.suffix)
    with pytest.raises(ValueError, match="Corrupt chunk"):
        export(nonexistent, "EURUSD", show_progress=False)
    assert not nonexistent.exists()
    assert list(vault.glob("*.tmp")) == []


def test_hdf5_chronological_append_and_rejected_overlap_preserve_store(vault):
    pytest.importorskip("tables")
    path = vault / "ticks.h5"
    start = datetime(2024, 1, 1, tzinfo=UTC)
    middle = start + timedelta(hours=1)
    end = middle + timedelta(hours=1)
    assert export_hdf5(path, "EURUSD", start, middle, show_progress=False) == 4
    assert (
        export_hdf5(
            path, "EURUSD", middle, end, if_exists="append", show_progress=False
        )
        == 4
    )
    original = read_hdf5(path, "EURUSD")
    assert original.time.is_monotonic_increasing
    with pytest.raises(ValueError, match="chronological"):
        export_hdf5(path, "EURUSD", if_exists="append", show_progress=False)
    pd.testing.assert_frame_equal(read_hdf5(path, "EURUSD"), original)


def test_raw_cache_export_protected(vault):
    chunk = TickChunk(symbol="EURUSD", time=datetime(2024, 1, 1))
    original = chunk.load()
    with pytest.raises(ValueError, match="raw download cache"):
        export_sqlite(chunk.path(), "EURUSD", show_progress=False)
    assert chunk.load() == original


@pytest.mark.parametrize("backend", ["sqlite", "hdf5"])
def test_active_log_export_protected(vault, backend):
    if backend == "hdf5":
        pytest.importorskip("tables")
    export = export_sqlite if backend == "sqlite" else export_hdf5
    original = CONFIG.log_file_path.read_bytes()
    with pytest.raises(ValueError, match="active log"):
        export(CONFIG.log_file_path, "EURUSD", show_progress=False)
    assert CONFIG.log_file_path.read_bytes() == original


@pytest.mark.parametrize("backend", ["sqlite", "hdf5"])
@pytest.mark.parametrize("suffix", ["-wal", "-shm", "-journal"])
def test_metadata_sidecar_export_protected(vault, backend, suffix):
    if backend == "hdf5":
        pytest.importorskip("tables")
    export = export_sqlite if backend == "sqlite" else export_hdf5
    sidecar = str(CONFIG.metadata_db_path) + suffix
    with pytest.raises(ValueError, match="separate file"):
        export(sidecar, "EURUSD", show_progress=False)


def _competing_export(path, backend, symbol, mode, ready, entered, release, results):
    from tick_vault.read import storage

    reload_config(base_directory=Path(path).parent / "process-vault", env_file=None)

    def frames(*args, **kwargs):
        entered.set()
        if not release.wait(15):
            raise TimeoutError("test did not release export input")
        yield pd.DataFrame(
            {
                "time": [pd.Timestamp("2024-01-01T02:00:00")],
                "ask": [1.2],
                "bid": [1.1],
                "ask_volume": [1],
                "bid_volume": [2],
            }
        )

    storage.iter_tick_data = frames
    export = storage.export_sqlite if backend == "sqlite" else storage.export_hdf5
    ready.set()
    try:
        count = export(path, symbol, if_exists=mode, lock_timeout=10)
        results.put(("ok", count))
    except Exception as exc:
        results.put(("error", type(exc).__name__, str(exc)))


@pytest.mark.parametrize("backend", ["sqlite", "hdf5"])
@pytest.mark.parametrize("scenario", ["different_symbols", "append", "fail"])
def test_competing_process_exports_preserve_updates(vault, backend, scenario):
    if backend == "hdf5":
        pytest.importorskip("tables")
    export = export_sqlite if backend == "sqlite" else export_hdf5
    read = read_sqlite if backend == "sqlite" else read_hdf5
    path = vault / ("concurrent.db" if backend == "sqlite" else "concurrent.h5")
    mode = "replace" if scenario == "different_symbols" else scenario
    if scenario == "append":
        export(path, "EURUSD", show_progress=False)
    symbols = ["EURUSD", "GBPUSD" if scenario == "different_symbols" else "EURUSD"]
    context = multiprocessing.get_context("spawn")
    ready = [context.Event(), context.Event()]
    entered = [context.Event(), context.Event()]
    release = [context.Event(), context.Event()]
    results = context.Queue()
    processes = [
        context.Process(
            target=_competing_export,
            args=(
                path,
                backend,
                symbols[index],
                mode,
                ready[index],
                entered[index],
                release[index],
                results,
            ),
        )
        for index in range(2)
    ]
    try:
        processes[0].start()
        assert ready[0].wait(15) and entered[0].wait(15)
        processes[1].start()
        assert ready[1].wait(15)
        # The first exporter already holds a snapshot. The second must wait
        # before creating its own snapshot, otherwise either update is lost.
        assert not entered[1].wait(0.3)
        release[0].set()
        assert results.get(timeout=15) == ("ok", 1)
        if scenario == "fail":
            result = results.get(timeout=15)
            assert result[:2] == ("error", "ValueError")
            assert "already exist" in result[2]
        else:
            assert entered[1].wait(15)
            release[1].set()
            assert results.get(timeout=15) == ("ok", 1)
        for process in processes:
            process.join(15)
            assert process.exitcode == 0
        if scenario == "different_symbols":
            assert len(read(path, "EURUSD")) == len(read(path, "GBPUSD")) == 1
        elif scenario == "append":
            assert len(read(path, "EURUSD")) == 10
        else:
            assert len(read(path, "EURUSD")) == 1
        assert list(vault.glob("*.tmp")) == []
    finally:
        for event in release:
            event.set()
        for process in processes:
            if process.pid is not None:
                if process.is_alive():
                    process.terminate()
                process.join(15)
        results.close()


@pytest.mark.parametrize("backend", ["sqlite", "hdf5"])
def test_reader_close_releases_publication_lock(vault, backend):
    if backend == "hdf5":
        pytest.importorskip("tables")
    export = export_sqlite if backend == "sqlite" else export_hdf5
    iterate = iter_sqlite if backend == "sqlite" else iter_hdf5
    path = vault / ("reader.db" if backend == "sqlite" else "reader.h5")
    export(path, "EURUSD", show_progress=False)
    stream = iterate(path, "EURUSD", batch_size=1)
    context = multiprocessing.get_context("spawn")
    ready, entered, release = context.Event(), context.Event(), context.Event()
    results = context.Queue()
    process = context.Process(
        target=_competing_export,
        args=(path, backend, "EURUSD", "replace", ready, entered, release, results),
    )
    try:
        assert len(next(stream)) == 1
        with pytest.raises(TimeoutError, match="destination lock"):
            export(path, "EURUSD", show_progress=False, lock_timeout=0.1)
        process.start()
        assert ready.wait(15)
        assert not entered.wait(0.3)
        stream.close()
        assert entered.wait(15)
        release.set()
        assert results.get(timeout=15) == ("ok", 1)
        process.join(15)
        assert process.exitcode == 0
    finally:
        stream.close()
        release.set()
        if process.pid is not None:
            if process.is_alive():
                process.terminate()
            process.join(15)
        results.close()
    assert export(path, "EURUSD", show_progress=False, lock_timeout=0) == 8


@pytest.mark.parametrize("backend", ["sqlite", "hdf5"])
@pytest.mark.parametrize("error", [errno.ENOSPC, errno.EACCES])
def test_publication_failure_preserves_existing_store(
    vault, backend, error, monkeypatch
):
    if backend == "hdf5":
        pytest.importorskip("tables")
    export = export_sqlite if backend == "sqlite" else export_hdf5
    read = read_sqlite if backend == "sqlite" else read_hdf5
    path = vault / ("original.db" if backend == "sqlite" else "original.h5")
    export(path, "EURUSD", show_progress=False)
    original = path.read_bytes()

    def reject_publication(*args, **kwargs):
        raise OSError(error, "simulated publication failure")

    monkeypatch.setattr("tick_vault.read.storage.os.replace", reject_publication)
    with pytest.raises(OSError, match="simulated publication failure"):
        export(path, "EURUSD", show_progress=False)
    assert path.read_bytes() == original
    assert len(read(path, "EURUSD")) == 8
    assert list(vault.glob("*.tmp")) == []
    with destination_lock(path, timeout=0):
        pass


@pytest.mark.parametrize("backend", ["sqlite", "hdf5"])
def test_interrupted_export_releases_resources_and_preserves_store(
    vault, backend, monkeypatch
):
    if backend == "hdf5":
        pytest.importorskip("tables")
    export = export_sqlite if backend == "sqlite" else export_hdf5
    path = vault / ("cancelled.db" if backend == "sqlite" else "cancelled.h5")
    export(path, "EURUSD", show_progress=False)
    original = path.read_bytes()
    closed = []

    def interrupted(*args, **kwargs):
        try:
            yield read_tick_data("EURUSD", show_progress=False)
            raise KeyboardInterrupt
        finally:
            closed.append(True)

    monkeypatch.setattr("tick_vault.read.storage.iter_tick_data", interrupted)
    with pytest.raises(KeyboardInterrupt):
        export(path, "EURUSD", show_progress=False)
    assert closed == [True]
    assert path.read_bytes() == original
    assert list(vault.glob("*.tmp")) == []
    with destination_lock(path, timeout=0):
        pass

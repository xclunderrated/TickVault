import importlib
import lzma
import struct
from datetime import UTC, datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from tick_vault.chunk import TickChunk
from tick_vault.config import CONFIG, reload_config
from tick_vault.decoder import decode_chunk
from tick_vault.metadata import MetadataDB
from tick_vault.reader import iter_tick_data, read_tick_data


@pytest.fixture
def vault(tmp_path):
    previous = CONFIG.model_dump()
    reload_config(base_directory=tmp_path / "vault", env_file=None)
    yield tmp_path
    reload_config(**previous, env_file=None)


def save_hour(hour=0, offsets=(0, 1000, 2000), symbol="EURUSD"):
    chunk = TickChunk(symbol=symbol, time=datetime(2024, 1, 1, hour, tzinfo=UTC))
    raw = b"".join(
        struct.pack(">IIIff", offset, 123456, 123450, 0.1, 0.2) for offset in offsets
    )
    chunk.save(lzma.compress(raw))
    with MetadataDB() as db:
        db.record_result(chunk, "downloaded")
    return chunk


def test_exact_utc_end_exclusive_subhour_range(vault):
    save_hour(offsets=(0, 1000, 2000, 3599999))
    save_hour(1, offsets=(0, 1000))
    tz = timezone(timedelta(hours=5, minutes=30))
    start = datetime(2024, 1, 1, 5, 30, 1, 1, tzinfo=tz)
    end = datetime(2024, 1, 1, 6, 30, 1, tzinfo=tz)
    frame = read_tick_data("eurusd", start, end, show_progress=False)
    assert list(frame.time) == [
        pd.Timestamp("2024-01-01T00:00:02"),
        pd.Timestamp("2024-01-01T00:59:59.999"),
        pd.Timestamp("2024-01-01T01:00:00"),
    ]
    assert frame.ask.iloc[0] == pytest.approx(1.23456)
    assert frame.ask_volume.iloc[0] == 100_000
    assert frame.bid_volume.iloc[0] == 200_000
    assert frame.time.dt.tz is None


def test_empty_equal_clipped_and_confirmed_no_data_ranges(vault):
    now = datetime(2024, 1, 1, tzinfo=UTC)
    frame = read_tick_data("EURUSD", now, now, show_progress=False)
    assert list(frame.columns) == ["time", "ask", "bid", "ask_volume", "bid_volume"]
    assert frame.empty and np.issubdtype(frame.ask.dtype, np.floating)
    with MetadataDB() as db:
        db.mark_no_data(TickChunk(symbol="EURUSD", time=now))
    assert read_tick_data("EURUSD", show_progress=False).empty
    assert read_tick_data(
        "EURUSD",
        now + timedelta(days=1),
        now + timedelta(days=2),
        strict=False,
        show_progress=False,
    ).empty
    with pytest.raises(ValueError, match="end"):
        read_tick_data("EURUSD", now + timedelta(hours=1), now, show_progress=False)


def test_strict_and_non_strict_gaps(vault):
    save_hour(0)
    save_hour(2)
    with pytest.raises(RuntimeError, match="1 unresolved hours"):
        read_tick_data("EURUSD", show_progress=False)
    assert len(read_tick_data("EURUSD", strict=False, show_progress=False)) == 6
    with MetadataDB() as db:
        db.mark_no_data(TickChunk(symbol="EURUSD", time=datetime(2024, 1, 1, 1)))
    assert len(read_tick_data("EURUSD", show_progress=False)) == 6


@pytest.mark.parametrize("strict", [True, False])
def test_missing_and_corrupt_claimed_data_raise_with_context(vault, strict):
    chunk = save_hour()
    chunk.path().unlink()
    with pytest.raises(FileNotFoundError, match="EURUSD"):
        read_tick_data("EURUSD", strict=strict, show_progress=False)
    chunk.path().write_bytes(b"this is not a tick file")
    with pytest.raises(ValueError, match="Corrupt chunk EURUSD at 2024-01-01"):
        read_tick_data("EURUSD", strict=strict, show_progress=False)


@pytest.mark.parametrize("backend", ["serial", "thread", "process"])
def test_decode_backends_preserve_order_and_frame_bounds(vault, backend):
    save_hour(0, offsets=(0, 0, 1, 2))
    save_hour(1, offsets=(0, 1, 2))
    frames = list(
        iter_tick_data(
            "EURUSD",
            show_progress=False,
            backend=backend,
            max_workers=2,
            max_pending=2,
            batch_size=2,
        )
    )
    assert all(0 < len(frame) <= 2 for frame in frames)
    actual = pd.concat(frames, ignore_index=True)
    expected = read_tick_data("EURUSD", show_progress=False)
    pd.testing.assert_frame_equal(actual, expected)
    assert actual.time.is_monotonic_increasing


def test_parallel_decode_consumes_bounded_work(monkeypatch):
    module = importlib.import_module("tick_vault.read.reader")
    consumed = []

    def chunks():
        for value in range(100):
            consumed.append(value)
            yield value

    monkeypatch.setattr(
        module, "_decode_file", lambda chunk, scale, base: np.array([chunk])
    )
    arrays = module._decoded_chunks(chunks(), 1.0, "thread", 2, 3)
    assert next(arrays).tolist() == [0]
    assert len(consumed) == 3
    assert next(arrays).tolist() == [1]
    assert len(consumed) == 4
    arrays.close()


def test_invalid_scale_and_corrupt_record_shape(vault):
    chunk = save_hour()
    for scale in (0, -1, float("inf"), float("nan")):
        with pytest.raises(ValueError, match="positive"):
            decode_chunk(chunk, scale)
    chunk.path().write_bytes(lzma.compress(b"bad length"))
    with pytest.raises(ValueError, match="multiple of 20"):
        decode_chunk(chunk)

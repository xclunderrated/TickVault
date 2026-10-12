import lzma
import os
import struct
import subprocess
import sys
from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from tick_vault.chunk import TickChunk
from tick_vault.config import CONFIG, Config, reload_config
from tick_vault.constants import PIPET_SIZE_REGISTRY
from tick_vault.core.binary import validate_payload
from tick_vault.utils import generate_hourly_datetimes, hour_ceil, hour_floor


def payload(offset=100, volume=1.5):
    return lzma.compress(
        struct.pack(">IIIff", offset, 110001, 110000, volume, 2.0),
        format=lzma.FORMAT_ALONE,
    )


@pytest.fixture
def config_snapshot():
    original = CONFIG.model_dump()
    yield
    CONFIG.__dict__.update(original)


def test_reload_reaches_existing_imports_and_updates_logs(tmp_path, config_snapshot):
    from tick_vault.core import chunk, logger

    identity = id(CONFIG)
    reload_config(BASE_DIRECTORY=tmp_path / "a", worker_per_proxy=2, env_file=None)
    first = TickChunk(symbol=" eurusd ", time=datetime(2024, 1, 1))
    assert id(CONFIG) == identity == id(chunk.CONFIG)
    assert first.path().is_relative_to(tmp_path / "a")
    reload_config(
        base_directory=tmp_path / "b", console_log_level="ERROR", env_file=None
    )
    assert first.path().is_relative_to(tmp_path / "b")
    managed = [h for h in logger.logger.handlers if getattr(h, "_tick_vault", False)]
    assert len(managed) == 2
    assert sum(hasattr(h, "baseFilename") for h in managed) == 1
    assert (tmp_path / "b" / "logs.log").exists()


def test_reload_validation_is_atomic(config_snapshot):
    before = CONFIG.model_dump()
    with pytest.raises(ValidationError):
        reload_config(worker_per_proxy=0)
    assert CONFIG.model_dump() == before
    with pytest.raises(ValueError, match="Unknown"):
        reload_config(worker_per_proxxy=2)


def test_env_file_and_environment_precedence(tmp_path, monkeypatch, config_snapshot):
    dotenv = tmp_path / "test.env"
    dotenv.write_text("TICK_VAULT_WORKER_PER_PROXY=2\nUNRELATED=ignored\n")
    monkeypatch.setenv("TICK_VAULT_WORKER_PER_PROXY", "3")
    reload_config(base_directory=tmp_path, env_file=dotenv)
    assert CONFIG.worker_per_proxy == 3
    reload_config(base_directory=tmp_path, env_file=dotenv, worker_per_proxy=4)
    assert CONFIG.worker_per_proxy == 4


def test_import_does_not_create_files(tmp_path):
    env = os.environ.copy()
    env["TICK_VAULT_BASE_DIRECTORY"] = str(tmp_path / "no-side-effects")
    result = subprocess.run(
        [sys.executable, "-c", "import tick_vault"],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert not (tmp_path / "no-side-effects").exists()
    assert not result.stdout


def test_utc_conversion_precedes_range_comparison_and_hour_rounding():
    zone = timezone(timedelta(hours=5, minutes=30))
    start = datetime(2024, 1, 1, 5, 45, tzinfo=zone)
    end = datetime(2024, 1, 1, 2, 15)  # UTC naive
    assert generate_hourly_datetimes(start, end) == [
        datetime(2024, 1, 1, 0, tzinfo=UTC),
        datetime(2024, 1, 1, 1, tzinfo=UTC),
    ]
    assert hour_floor(start).hour == 0
    assert hour_ceil(start).hour == 1
    assert TickChunk(
        symbol="EURUSD", time=datetime(2024, 1, 1, 5, 30, tzinfo=zone)
    ).time == datetime(2024, 1, 1, tzinfo=UTC)
    with pytest.raises(ValueError):
        TickChunk(symbol="EURUSD", time=datetime(2024, 1, 1, 5, tzinfo=zone))


@pytest.mark.parametrize(
    "symbol", ["../EURUSD", "EUR/USD", "", "X;DROP TABLE y", "EURÜSD", "C:\\EURUSD"]
)
def test_unsafe_symbols_are_rejected(symbol):
    with pytest.raises(ValueError):
        TickChunk(symbol=symbol, time=datetime(2024, 1, 1))


def test_index_scales():
    assert PIPET_SIZE_REGISTRY["USATECHIDXUSD"] == 0.001
    assert PIPET_SIZE_REGISTRY["USA500IDXUSD"] == 0.001


@pytest.mark.parametrize("symbol", ["USATECHIDXUSD", "USA500IDXUSD"])
def test_index_decoding_uses_registered_scale(symbol, tmp_path):
    from tick_vault.decoder import decode_chunk

    chunk = TickChunk(symbol=symbol, time=datetime(2024, 1, 1))
    chunk.save(payload(), base=tmp_path)
    ticks = decode_chunk(chunk, base=tmp_path)
    assert ticks["ask"][0] == pytest.approx(110.001)
    assert ticks["bid"][0] == pytest.approx(110.0)


def test_valid_payload_and_empty_hour():
    assert len(validate_payload(payload())) == 20
    assert validate_payload(lzma.compress(b"", format=lzma.FORMAT_ALONE)) == b""


@pytest.mark.parametrize(
    "data",
    [
        b"<html>denied</html>",
        b"",
        payload()[:-4],
        lzma.compress(b"a"),
        payload(offset=3600000),
        payload(volume=float("nan")),
        payload(volume=-1.0),
        payload(volume=1e20),
        payload() + b"garbage",
    ],
)
def test_invalid_payload_is_never_accepted(data):
    with pytest.raises(ValueError):
        validate_payload(data)


def test_atomic_save_preserves_old_file_when_replace_fails(tmp_path, monkeypatch):
    from tick_vault.core import chunk as chunk_module

    chunk = TickChunk(symbol="EURUSD", time=datetime(2024, 1, 1))
    first = payload(offset=1)
    chunk.save(first, base=tmp_path)

    def fail_replace(*args):
        raise OSError("disk failure")

    monkeypatch.setattr(chunk_module.os, "replace", fail_replace)
    with pytest.raises(OSError, match="disk failure"):
        chunk.save(payload(offset=2), base=tmp_path)
    assert chunk.load(tmp_path) == first
    assert not list(chunk.path(tmp_path).parent.glob(".tick-*.tmp"))
    with pytest.raises(ValueError):
        chunk.save(b"corrupt", base=tmp_path)
    assert chunk.load(tmp_path) == first


@pytest.mark.parametrize(
    "url",
    [
        "http://datafeed.dukascopy.com/",
        "https://user:pass@example.com/",
        "https://example.com/?x=1",
        "https://example.com/#part",
        "file:///data",
    ],
)
def test_endpoint_configuration_validation(url):
    with pytest.raises(ValidationError):
        Config(datafeed_base_url=url)

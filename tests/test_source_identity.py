"""Source identity prevents local cache bytes being relabelled after URL changes."""

import hashlib
import lzma
import sqlite3
import struct
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from tick_vault.config import Config
from tick_vault.download import api
from tick_vault.download.admission import source_key
from tick_vault.metadata import LEGACY_SOURCE_KEY, MetadataDB, SourceIdentityError


def identity(url):
    return hashlib.sha256(url.encode()).hexdigest()


def test_empty_vault_binding_is_durable_and_rejects_feed_changes(tmp_path):
    path = tmp_path / "metadata.db"
    first = identity("https://one.invalid/datafeed/")
    second = identity("https://two.invalid/datafeed/")
    with MetadataDB(path) as db:
        db.bind_source(first, raw_directory=tmp_path / "downloads")
        db.bind_source(first, raw_directory=tmp_path / "downloads")
    with MetadataDB(path) as restarted:
        with pytest.raises(SourceIdentityError, match="separate base_directory"):
            restarted.bind_source(second, raw_directory=tmp_path / "downloads")
        assert (
            restarted.conn.execute("SELECT source_key FROM vault_source").fetchone()[0]
            == first
        )


@pytest.mark.parametrize(
    "has_raw,has_rows", [(True, False), (False, True), (True, True)]
)
def test_legacy_unbound_vault_only_inherits_standard_feed(tmp_path, has_raw, has_rows):
    raw = tmp_path / "downloads"
    path = tmp_path / "metadata.db"
    if has_raw:
        raw.mkdir()
        (raw / "existing.bi5").write_bytes(b"unproven legacy artifact")
    with MetadataDB(path) as db:
        if has_rows:
            with db.conn:
                db.conn.execute("CREATE TABLE symbol_EURUSD(timestamp INTEGER)")
                db.conn.execute("INSERT INTO symbol_EURUSD VALUES(1)")
        with pytest.raises(SourceIdentityError, match="unbound vault"):
            db.bind_source(identity("https://custom.invalid/"), raw_directory=raw)
        db.bind_source(LEGACY_SOURCE_KEY, raw_directory=raw)
        assert (
            db.conn.execute("SELECT source_key FROM vault_source").fetchone()[0]
            == LEGACY_SOURCE_KEY
        )


def test_concurrent_first_bindings_cannot_mix_sources(tmp_path):
    path = tmp_path / "metadata.db"
    with MetadataDB(path):
        pass
    barrier = threading.Barrier(2)

    def bind(key):
        with MetadataDB(path) as db:
            barrier.wait(timeout=3)
            try:
                db.bind_source(key, raw_directory=tmp_path / "downloads")
                return "bound", key
            except SourceIdentityError:
                return "rejected", key

    keys = [identity("https://one.invalid/"), identity("https://two.invalid/")]
    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(bind, keys))
    assert [state for state, key in outcomes].count("bound") == 1
    with closing(sqlite3.connect(path)) as connection:
        persisted = connection.execute(
            "SELECT source_key FROM vault_source"
        ).fetchone()[0]
    assert persisted == next(key for state, key in outcomes if state == "bound")


@pytest.mark.asyncio
async def test_changing_source_cannot_reuse_old_local_cached_hour(
    tmp_path, monkeypatch
):
    settings = Config(
        _env_file=None,
        base_directory=tmp_path / "vault",
        datafeed_base_url="https://one.invalid/datafeed/",
        worker_per_proxy=1,
        adaptive_workers=False,
        fetch_max_retry_attempts=0,
        console_log_level="ERROR",
    )
    calls = []
    client = httpx.AsyncClient

    async def handler(request):
        calls.append(str(request.url))
        payload = lzma.compress(
            struct.pack(">IIIff", 10, 110020, 110000, 1, 2), format=lzma.FORMAT_ALONE
        )
        return httpx.Response(200, content=payload, request=request)

    monkeypatch.setattr(
        api,
        "AsyncClient",
        lambda *args, **kwargs: client(
            *args, transport=httpx.MockTransport(handler), **kwargs
        ),
    )
    start = datetime(2024, 1, 2, tzinfo=UTC)
    report = await api.download_range(
        "EURUSD",
        start,
        start + timedelta(hours=1),
        settings=settings,
        show_progress=False,
    )
    assert report.complete
    second = settings.model_copy(
        update={"datafeed_base_url": "https://two.invalid/datafeed/"}
    )
    with pytest.raises(SourceIdentityError, match="separate base_directory"):
        await api.download_range(
            "EURUSD",
            start,
            start + timedelta(hours=1),
            settings=second,
            show_progress=False,
        )
    assert len(calls) == 1
    with MetadataDB(settings.metadata_db_path) as db:
        assert db.conn.execute("SELECT source_key FROM vault_source").fetchone()[
            0
        ] == source_key(settings)

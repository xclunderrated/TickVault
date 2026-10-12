import lzma
import os
import sqlite3
import struct
from contextlib import closing
from datetime import UTC, datetime, timedelta

import pytest

from tick_vault.chunk import TickChunk
from tick_vault.config import CONFIG, reload_config
from tick_vault.metadata import MetadataDB


@pytest.fixture
def vault(tmp_path):
    previous = CONFIG.model_dump()
    reload_config(base_directory=tmp_path / "vault", env_file=None)
    yield tmp_path
    reload_config(**previous, env_file=None)


def hour(number=0):
    return TickChunk(symbol="EURUSD", time=datetime(2024, 1, 1, number, tzinfo=UTC))


def test_legacy_migration_retains_rows_and_retries_uncertain_absence(vault):
    path = vault / "old.db"
    with closing(sqlite3.connect(path)) as conn, conn:
        conn.execute(
            "CREATE TABLE symbol_EURUSD (timestamp INTEGER PRIMARY KEY, has_data INTEGER NOT NULL)"
        )
        conn.executemany(
            "INSERT INTO symbol_EURUSD VALUES (?,?)",
            [(int(hour(0).time.timestamp()), 1), (int(hour(1).time.timestamp()), 0)],
        )
    with MetadataDB(path) as db:
        assert db.get_status(hour(0)) == "downloaded"
        assert db.get_status(hour(1)) == "failed"
        assert db.status("EURUSD") == {
            "downloaded": 1,
            "no_data": 0,
            "failed": 1,
            "total": 2,
        }
        assert list(
            db.iter_pending_chunks(
                "EURUSD", hour(0).time, hour(2).time, validate_files=False
            )
        ) == [hour(1)]
    with MetadataDB(path) as db:
        assert db.status("EURUSD")["total"] == 2


def test_explicit_no_data_retry_and_expiration(vault):
    with MetadataDB() as db:
        db.mark_no_data(hour())
        assert list(db.iter_pending_chunks("EURUSD", hour().time, hour(1).time)) == []
        assert list(
            db.iter_pending_chunks(
                "EURUSD", hour().time, hour(1).time, retry_no_data=True
            )
        ) == [hour()]
        assert (
            list(
                db.iter_pending_chunks(
                    "EURUSD", hour().time, hour(1).time, recheck_no_data_after=86_400
                )
            )
            == []
        )
        db.conn.execute("UPDATE symbol_EURUSD SET checked_at=0")
        db.conn.commit()
        assert list(
            db.iter_pending_chunks(
                "EURUSD",
                hour().time,
                hour(1).time,
                recheck_no_data_after=timedelta(days=1),
            )
        ) == [hour()]


def test_missing_corrupt_downloads_reconcile_to_pending(vault):
    good = hour(2)
    good.save(lzma.compress(struct.pack(">IIIff", 0, 100, 90, 1, 1)))
    bad = hour(1)
    bad.path().parent.mkdir(parents=True, exist_ok=True)
    bad.path().write_bytes(b"broken")
    with MetadataDB() as db:
        db.insert_rows([hour(0), bad, good], status="downloaded")
        pending = list(db.iter_pending_chunks("EURUSD", hour(0).time, hour(3).time))
        assert pending == [hour(0), bad]
        assert db.get_status(hour(0)) == "failed"
        assert db.get_status(bad) == "failed"
        assert db.get_status(good) == "downloaded"


def test_pending_large_range_avoids_sql_variable_limit(vault):
    start = datetime(2020, 1, 1, tzinfo=UTC)
    end = start + timedelta(hours=40_000)
    with MetadataDB() as db:
        assert (
            sum(1 for _ in db.iter_pending_chunks("EURUSD", start, end, batch_size=17))
            == 40_000
        )
        assert list(db.iter_pending_chunks("EURUSD", start, start)) == []


def test_failed_hours_are_gaps_but_confirmed_empty_is_coverage(vault):
    with MetadataDB() as db:
        db.mark_no_data(hour(0))
        db.record_result(hour(1), "failed", error="timeout")
        db.mark_no_data(hour(2))
        assert db.first_chunk("EURUSD") == hour(0)
        assert db.last_chunk("EURUSD") == hour(2)
        with pytest.raises(RuntimeError, match="1 unresolved"):
            db.check_for_gaps("EURUSD", hour(0).time, hour(3).time)
        db.mark_no_data(hour(1))
        db.check_for_gaps(
            "EURUSD", hour(0).time, hour(2).time + timedelta(microseconds=1)
        )
        assert db.status("EURUSD", hour(1).time, hour(2).time)["total"] == 1
        assert db.status("EURUSD", hour(0).time, hour(0).time)["total"] == 0


def test_no_file_inference_is_retryable_failure(vault):
    with MetadataDB() as db:
        db.insert_rows([hour()])
        assert db.get_status(hour()) == "failed"
        assert db.first_chunk("EURUSD") is None
        with pytest.raises(ValueError, match="Unknown download status"):
            db.record_result(hour(), "wrong")


def test_cached_batches_yield_for_responsive_async_resume(vault):
    with MetadataDB() as db:
        for number in range(10):
            db.mark_no_data(hour(number))
        assert list(
            db.iter_pending_batches("EURUSD", hour().time, hour(10).time, batch_size=3)
        ) == [[], [], [], []]


def test_legacy_hyphen_table_migrates_without_history_loss(vault):
    path = vault / "legacy.db"
    chunk = TickChunk(symbol="TEST-PAIR", time=hour().time)
    with closing(sqlite3.connect(path)) as conn, conn:
        conn.execute(
            "CREATE TABLE symbol_TEST_PAIR(timestamp INTEGER PRIMARY KEY,has_data INTEGER NOT NULL)"
        )
        conn.execute(
            "INSERT INTO symbol_TEST_PAIR VALUES (?,1)", (int(chunk.time.timestamp()),)
        )
    with MetadataDB(path) as db:
        assert db.get_status(chunk) == "downloaded"
        assert db.status("TEST-PAIR")["total"] == 1


def test_valid_orphan_and_empty_cache_are_recovered(vault):
    chunk = hour()
    chunk.save(lzma.compress(struct.pack(">IIIff", 0, 100, 90, 1, 1)))
    empty = hour(1)
    empty.save(lzma.compress(b""))
    with MetadataDB() as db:
        assert list(db.iter_pending_chunks("EURUSD", hour().time, hour(2).time)) == []
        assert db.get_status(chunk) == "downloaded"
        assert db.get_status(empty) == "no_data"
        assert list(
            db.iter_pending_chunks(
                "EURUSD", hour().time, hour(2).time, retry_no_data=True
            )
        ) == [empty]


def test_unchanged_validated_cache_skips_decompression(vault, monkeypatch):
    chunk = hour()
    chunk.save(lzma.compress(struct.pack(">IIIff", 0, 100, 90, 1, 1)))
    with MetadataDB() as db:
        db.record_result(chunk, "downloaded")

    def unexpected_validation(content):
        pytest.fail("matching size/mtime fingerprint should skip decompression")

    monkeypatch.setattr("tick_vault.metadata.validate_payload", unexpected_validation)
    with MetadataDB() as db:
        assert list(db.iter_pending_chunks("EURUSD", hour().time, hour(1).time)) == []


def test_same_size_changed_mtime_corruption_is_revalidated(vault):
    chunk = hour()
    chunk.save(lzma.compress(struct.pack(">IIIff", 0, 100, 90, 1, 1)))
    with MetadataDB() as db:
        db.record_result(chunk, "downloaded")
    old_stat = chunk.path().stat()
    chunk.path().write_bytes(b"x" * old_stat.st_size)
    os.utime(chunk.path(), ns=(old_stat.st_atime_ns, old_stat.st_mtime_ns + 1_000_000))
    with MetadataDB() as db:
        assert list(db.iter_pending_chunks("EURUSD", hour().time, hour(1).time)) == [
            chunk
        ]
        assert db.get_status(chunk) == "failed"
        row = db.conn.execute(
            "SELECT file_size,file_mtime_ns,error FROM symbol_EURUSD"
        ).fetchone()
        assert row[0] is None and row[1] is None and "LZMA" in row[2]


def test_invalid_downloaded_status_never_stores_trusted_fingerprint(vault):
    bad = hour()
    bad.path().parent.mkdir(parents=True, exist_ok=True)
    bad.path().write_bytes(b"not ticks")
    with MetadataDB() as db:
        db.record_result(bad, "downloaded")
        assert db.get_status(bad) == "failed"
        assert db.conn.execute(
            "SELECT file_size,file_mtime_ns FROM symbol_EURUSD"
        ).fetchone() == (None, None)


@pytest.mark.parametrize("retry_no_data", [False, True])
def test_valid_published_file_supersedes_prior_no_data(vault, retry_no_data):
    chunk = hour()
    with MetadataDB() as db:
        db.mark_no_data(chunk)
        db.conn.execute("UPDATE symbol_EURUSD SET checked_at=1")
        db.conn.commit()
    # Simulate publication followed by a crash before the success receipt.
    chunk.save(lzma.compress(struct.pack(">IIIff", 0, 100, 90, 1, 1)))
    with MetadataDB() as db:
        assert (
            list(
                db.iter_pending_chunks(
                    "EURUSD",
                    chunk.time,
                    hour(1).time,
                    retry_no_data=retry_no_data,
                    recheck_no_data_after=3600,
                )
            )
            == []
        )
        assert db.get_status(chunk) == "downloaded"
        assert list(db.iter_available_chunks("EURUSD", chunk.time, hour(1).time)) == [
            chunk
        ]


@pytest.mark.parametrize("retry_no_data", [False, True])
def test_empty_cache_revalidation_preserves_provider_observation(vault, retry_no_data):
    chunk = hour()
    chunk.save(lzma.compress(b""))
    with MetadataDB() as db:
        db.mark_no_data(chunk)
        db.conn.execute("UPDATE symbol_EURUSD SET checked_at=1")
        db.conn.commit()
        for _ in range(2):
            # Force a fresh decompression even once a fingerprint is recorded.
            previous = chunk.path().stat()
            os.utime(
                chunk.path(),
                ns=(previous.st_atime_ns, previous.st_mtime_ns + 1_000_000),
            )
            assert list(
                db.iter_pending_chunks(
                    "EURUSD",
                    chunk.time,
                    hour(1).time,
                    retry_no_data=retry_no_data,
                    recheck_no_data_after=3600,
                )
            ) == [chunk]
            assert db.get_status(chunk) == "no_data"
            assert db.conn.execute(
                "SELECT checked_at FROM symbol_EURUSD"
            ).fetchone() == (1,)


def test_new_empty_orphan_does_not_satisfy_explicit_retry(vault):
    chunk = hour()
    chunk.save(lzma.compress(b""))
    with MetadataDB() as db:
        assert list(
            db.iter_pending_chunks(
                "EURUSD", chunk.time, hour(1).time, retry_no_data=True
            )
        ) == [chunk]
        assert db.get_status(chunk) == "no_data"

"""Durable download status and bounded, ordered metadata queries."""

import hashlib
import sqlite3
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import TracebackType
from typing import Any, Literal, cast

from .chunk import TickChunk
from .config import get_config
from .core.binary import validate_payload
from .utils import hour_ceil, hour_floor, normalize_datetime, normalize_symbol

DownloadStatus = Literal["downloaded", "no_data", "failed"]
_COMPLETE = ("downloaded", "no_data")
LEGACY_SOURCE_KEY = hashlib.sha256(
    b"https://datafeed.dukascopy.com/datafeed/"
).hexdigest()


class SourceIdentityError(ValueError):
    """A local vault cannot silently combine different configured feeds."""


class MetadataDB:
    """SQLite metadata; open one connection per owning thread.

    Legacy per-symbol tables and ``has_data`` values are retained. The additive
    migration distinguishes successful downloads, confirmed empty hours and
    failures. A legacy absent file cannot prove no data, so it remains retryable.
    """

    def __init__(self, db_path: str | Path | None = None) -> None:
        self.db_path = (
            Path(db_path) if db_path is not None else get_config().metadata_db_path
        )
        if self.db_path.is_dir():
            raise ValueError(f"Expected a database file, got directory: {self.db_path}")
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path, timeout=30)
        try:
            # A reader can retain a bounded streaming cursor while background
            # download results commit on another connection without blocking.
            self.conn.execute("PRAGMA journal_mode = WAL")
            self.conn.execute("PRAGMA foreign_keys = ON")
        except Exception:
            self.conn.close()
            raise
        self._ensured: set[str] = set()

    def bind_source(
        self, source_key: str, *, raw_directory: str | Path | None = None
    ) -> None:
        """Atomically pin a vault to one source before any cache discovery or GET.

        Existing unbound archives can only inherit the standard legacy feed.
        A custom feed needs an empty vault, since existing bytes and negative
        cache rows have no provenance that can establish their custom source.
        """
        if len(source_key) != 64 or any(
            character not in "0123456789abcdef" for character in source_key
        ):
            raise ValueError("source_key must be a SHA256 feed identity")
        if self.conn.in_transaction:
            raise ValueError("Bind the vault source before writing metadata")
        raw = (
            Path(raw_directory)
            if raw_directory is not None
            else get_config().save_directory
        )
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS vault_source ("
                "singleton INTEGER PRIMARY KEY CHECK(singleton=1), "
                "source_key TEXT NOT NULL, bound_at INTEGER NOT NULL)"
            )
            existing = self.conn.execute(
                "SELECT source_key FROM vault_source WHERE singleton=1"
            ).fetchone()
            if existing is not None:
                if existing[0] != source_key:
                    raise SourceIdentityError(
                        "This vault belongs to a different datafeed source; "
                        "use a separate base_directory for this feed"
                    )
            else:
                has_rows = False
                tables = self.conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
                for (name,) in tables:
                    if name.lower().startswith("symbol_"):
                        quoted = '"' + name.replace('"', '""') + '"'
                        if (
                            self.conn.execute(
                                f"SELECT 1 FROM {quoted} LIMIT 1"
                            ).fetchone()
                            is not None
                        ):
                            has_rows = True
                            break
                has_raw = raw.is_file() or any(
                    path.is_file() for path in raw.rglob("*.bi5")
                )
                if source_key != LEGACY_SOURCE_KEY and (has_rows or has_raw):
                    raise SourceIdentityError(
                        "An existing unbound vault can only inherit the standard "
                        "legacy datafeed; use a separate base_directory for this feed"
                    )
                self.conn.execute(
                    "INSERT INTO vault_source VALUES(1,?,?)",
                    (source_key, int(datetime.now(UTC).timestamp())),
                )
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise

    def _get_table_name(self, symbol: str) -> str:
        return "symbol_" + normalize_symbol(symbol)

    def _table(self, symbol: str) -> str:
        return '"' + self._get_table_name(symbol).replace('"', '""') + '"'

    def _ensure_table_exists(self, symbol: str) -> None:
        symbol = normalize_symbol(symbol)
        if symbol in self._ensured:
            return
        table = self._table(symbol)
        with self.conn:
            # Older releases replaced '-' with '_'. Rename that existing table
            # when unambiguous so valid legacy instrument histories stay visible.
            current_name = self._get_table_name(symbol)
            legacy_name = current_name.replace("-", "_")
            if legacy_name != current_name:
                current_exists = self.conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? COLLATE NOCASE",
                    (current_name,),
                ).fetchone()
                legacy_exists = self.conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? COLLATE NOCASE",
                    (legacy_name,),
                ).fetchone()
                if legacy_exists and not current_exists:
                    self.conn.execute(f'ALTER TABLE "{legacy_name}" RENAME TO {table}')
            self.conn.execute(
                f"CREATE TABLE IF NOT EXISTS {table} ("
                "timestamp INTEGER PRIMARY KEY, "
                "has_data INTEGER NOT NULL CHECK(has_data IN (0, 1)), "
                "status TEXT NOT NULL CHECK(status IN ('downloaded','no_data','failed')), "
                "checked_at INTEGER NOT NULL DEFAULT 0, error TEXT, "
                "file_size INTEGER, file_mtime_ns INTEGER)"
            )
            columns = {
                row[1] for row in self.conn.execute(f"PRAGMA table_info({table})")
            }
            if "status" not in columns:
                self.conn.execute(f"ALTER TABLE {table} ADD COLUMN status TEXT")
            self.conn.execute(
                f"UPDATE {table} SET status = CASE WHEN has_data = 1 "
                "THEN 'downloaded' ELSE 'failed' END WHERE status IS NULL"
            )
            if "checked_at" not in columns:
                self.conn.execute(
                    f"ALTER TABLE {table} ADD COLUMN checked_at INTEGER NOT NULL DEFAULT 0"
                )
            if "error" not in columns:
                self.conn.execute(f"ALTER TABLE {table} ADD COLUMN error TEXT")
            for name in ("file_size", "file_mtime_ns"):
                if name not in columns:
                    self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} INTEGER")
        self._ensured.add(symbol)

    def _validate_local(self, chunk: TickChunk) -> tuple[DownloadStatus, int, int]:
        before = chunk.path().stat()
        raw = validate_payload(chunk.load())
        after = chunk.path().stat()
        if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
            raise ValueError(f"Chunk changed while validating: {chunk.path()}")
        return ("downloaded" if raw else "no_data", after.st_size, after.st_mtime_ns)

    def _upsert(
        self,
        chunk: TickChunk,
        status: DownloadStatus,
        error: str | None,
        file_size: int | None = None,
        file_mtime_ns: int | None = None,
        *,
        checked_at: int | None = None,
    ) -> None:
        self.conn.execute(
            f"INSERT INTO {self._table(chunk.symbol)} "
            "(timestamp,has_data,status,checked_at,error,file_size,file_mtime_ns) "
            "VALUES (?,?,?,?,?,?,?) ON CONFLICT(timestamp) DO UPDATE SET "
            "has_data=excluded.has_data,status=excluded.status, "
            "checked_at=excluded.checked_at,error=excluded.error, "
            "file_size=excluded.file_size,file_mtime_ns=excluded.file_mtime_ns",
            (
                int(chunk.time.timestamp()),
                int(status == "downloaded"),
                status,
                (
                    int(datetime.now(UTC).timestamp())
                    if checked_at is None
                    else checked_at
                ),
                error,
                file_size,
                file_mtime_ns,
            ),
        )

    def insert_rows(
        self,
        chunks: Iterable[TickChunk],
        *,
        status: DownloadStatus | None = None,
        error: str | None = None,
    ) -> None:
        """Record a batch; pass a status to distinguish confirmed no-data hours.

        Downloaded files are validated before storing a size/mtime fingerprint
        used by warm resumes. The compatible one-argument form infers downloaded
        from an existing file. An absent file is a failure, never proof of no data.
        """
        if status is not None and status not in (*_COMPLETE, "failed"):
            raise ValueError(f"Unknown download status: {status}")
        with self.conn:
            for chunk in chunks:
                self._ensure_table_exists(chunk.symbol)
                actual_status = status or (
                    "downloaded" if chunk.path().is_file() else "failed"
                )
                file_size, file_mtime_ns = None, None
                actual_error = error
                if actual_status == "downloaded":
                    try:
                        actual_status, file_size, file_mtime_ns = self._validate_local(
                            chunk
                        )
                    except (OSError, ValueError) as exc:
                        actual_status, actual_error = "failed", str(exc)
                self._upsert(
                    chunk, actual_status, actual_error, file_size, file_mtime_ns
                )

    def record_result(
        self, chunk: TickChunk, status: DownloadStatus, error: str | None = None
    ) -> None:
        self.insert_rows([chunk], status=status, error=error)

    def mark_no_data(self, chunk: TickChunk) -> None:
        self.record_result(chunk, "no_data")

    def get_status(self, chunk: TickChunk) -> DownloadStatus | None:
        self._ensure_table_exists(chunk.symbol)
        row = self.conn.execute(
            f"SELECT status FROM {self._table(chunk.symbol)} WHERE timestamp=?",
            (int(chunk.time.timestamp()),),
        ).fetchone()
        return cast(DownloadStatus, row[0]) if row is not None else None

    def status(
        self, symbol: str, start: datetime | None = None, end: datetime | None = None
    ) -> dict[str, int]:
        """Count each recorded status in the overlapping UTC range."""
        self._ensure_table_exists(symbol)
        start = normalize_datetime(start) if start is not None else None
        end = normalize_datetime(end) if end is not None else None
        if start is not None and end is not None and end < start:
            raise ValueError("end must not be before start")
        counts = {"downloaded": 0, "no_data": 0, "failed": 0, "total": 0}
        if start is not None and end == start:
            return counts
        where, parameters = [], []
        if start is not None:
            where.append("timestamp>=?")
            parameters.append(int(hour_floor(start).timestamp()))
        if end is not None:
            where.append("timestamp<?")
            parameters.append(int(hour_ceil(end).timestamp()))
        query = f"SELECT status,COUNT(*) FROM {self._table(symbol)}"
        if where:
            query += " WHERE " + " AND ".join(where)
        for status, count in self.conn.execute(query + " GROUP BY status", parameters):
            counts[status] = count
            counts["total"] += count
        return counts

    def iter_pending_batches(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        *,
        retry_no_data: bool = False,
        recheck_no_data_after: float | timedelta | None = None,
        validate_files: bool = True,
        batch_size: int = 512,
    ) -> Iterator[list[TickChunk]]:
        """Yield each scanned batch, including empty batches when all cached.

        Start and end are normalized to UTC and floored, matching the download
        API. A validated matching file fingerprint avoids repeated decompression.
        Changed/missing/corrupt files are checked and repaired or retried; valid
        files left by an interrupted metadata write are recovered without HTTP.
        Confirmed no-data is retried only when explicitly requested or expired.
        """
        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        symbol = normalize_symbol(symbol)
        start, end = normalize_datetime(start), normalize_datetime(end)
        if end < start:
            raise ValueError("end must not be before start")
        current, stop = hour_floor(start), hour_floor(end)
        self._ensure_table_exists(symbol)
        seconds = (
            recheck_no_data_after.total_seconds()
            if isinstance(recheck_no_data_after, timedelta)
            else recheck_no_data_after
        )
        if seconds is not None and seconds < 0:
            raise ValueError("recheck_no_data_after must not be negative")
        cutoff = (
            datetime.now(UTC).timestamp() - seconds if seconds is not None else None
        )
        while current < stop:
            batch_end = min(stop, current + timedelta(hours=batch_size))
            rows = {
                row[0]: (row[1], row[2], row[3], row[4])
                for row in self.conn.execute(
                    f"SELECT timestamp,status,checked_at,file_size,file_mtime_ns FROM {self._table(symbol)} "
                    "WHERE timestamp>=? AND timestamp<? ORDER BY timestamp",
                    (int(current.timestamp()), int(batch_end.timestamp())),
                )
            }
            pending_chunks = []
            while current < batch_end:
                chunk = TickChunk(symbol=symbol, time=current)
                status, checked_at, file_size, file_mtime_ns = rows.get(
                    int(current.timestamp()), (None, 0, None, None)
                )
                pending = status not in _COMPLETE
                if status == "no_data":
                    pending = retry_no_data or (
                        cutoff is not None and checked_at <= cutoff
                    )
                if validate_files:
                    try:
                        local_stat = chunk.path().stat()
                    except FileNotFoundError as exc:
                        if status == "downloaded":
                            self.record_result(chunk, "failed", error=str(exc))
                            pending = True
                    except OSError as exc:
                        self.record_result(chunk, "failed", error=str(exc))
                        pending = True
                    else:
                        try:
                            fingerprint_matches = status in _COMPLETE and (
                                local_stat.st_size,
                                local_stat.st_mtime_ns,
                            ) == (file_size, file_mtime_ns)
                            local_status = cast(DownloadStatus, status)
                            if not fingerprint_matches:
                                local_status, file_size, file_mtime_ns = (
                                    self._validate_local(chunk)
                                )
                                # Revalidating a compressed-empty local cache is
                                # not a fresh provider observation. Keep the
                                # original expiry and any requested retry.
                                observation_time = (
                                    checked_at
                                    if status == local_status == "no_data"
                                    else int(datetime.now(UTC).timestamp())
                                )
                                with self.conn:
                                    self._upsert(
                                        chunk,
                                        local_status,
                                        None,
                                        file_size,
                                        file_mtime_ns,
                                        checked_at=observation_time,
                                    )
                                checked_at = observation_time
                            # A valid published nonempty file wins over every
                            # older metadata result, including no_data. An empty
                            # file cannot satisfy an explicit or expired retry.
                            pending = local_status == "no_data" and (
                                retry_no_data
                                or (cutoff is not None and checked_at <= cutoff)
                            )
                        except (OSError, ValueError) as exc:
                            self.record_result(chunk, "failed", error=str(exc))
                            pending = True
                if pending:
                    pending_chunks.append(chunk)
                current += timedelta(hours=1)
            yield pending_chunks

    def iter_pending_chunks(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        *,
        retry_no_data: bool = False,
        recheck_no_data_after: float | timedelta | None = None,
        validate_files: bool = True,
        batch_size: int = 512,
    ) -> Iterator[TickChunk]:
        """Flatten iter_pending_batches; bounded by batch_size scanned hours.

        Async producers should use iter_pending_batches and yield to their event
        loop after every scanned batch, including batches with no pending work.
        """
        for batch in self.iter_pending_batches(
            symbol,
            start,
            end,
            retry_no_data=retry_no_data,
            recheck_no_data_after=recheck_no_data_after,
            validate_files=validate_files,
            batch_size=batch_size,
        ):
            yield from batch

    def find_not_attempted_chunks(
        self, symbol: str, start: datetime, end: datetime, **kwargs: Any
    ) -> list[TickChunk]:
        """Compatibility list wrapper; use iter_pending_chunks for large ranges."""
        return list(self.iter_pending_chunks(symbol, start, end, **kwargs))

    def _boundary(self, symbol: str, aggregate: str) -> TickChunk | None:
        symbol = normalize_symbol(symbol)
        self._ensure_table_exists(symbol)
        timestamp = self.conn.execute(
            f"SELECT {aggregate}(timestamp) FROM {self._table(symbol)} "
            "WHERE status IN ('downloaded','no_data')"
        ).fetchone()[0]
        if timestamp is None:
            return None
        return TickChunk(symbol=symbol, time=datetime.fromtimestamp(timestamp, UTC))

    def first_chunk(self, symbol: str) -> TickChunk | None:
        """Earliest successfully resolved hour (including confirmed no-data)."""
        return self._boundary(symbol, "MIN")

    def last_chunk(self, symbol: str) -> TickChunk | None:
        """Latest successfully resolved hour (including confirmed no-data)."""
        return self._boundary(symbol, "MAX")

    def check_for_gaps(self, symbol: str, start: datetime, end: datetime) -> None:
        """Require every overlapping hour to have downloaded or no-data status."""
        start, end = normalize_datetime(start), normalize_datetime(end)
        if end < start:
            raise ValueError("end must not be before start")
        if end == start:
            return
        symbol = normalize_symbol(symbol)
        self._ensure_table_exists(symbol)
        first, stop = hour_floor(start), hour_ceil(end)
        expected = int((stop - first).total_seconds() // 3600)
        cursor = self.conn.execute(
            f"SELECT timestamp FROM {self._table(symbol)} "
            "WHERE timestamp>=? AND timestamp<? AND status IN ('downloaded','no_data') "
            "ORDER BY timestamp",
            (int(first.timestamp()), int(stop.timestamp())),
        )
        current = int(first.timestamp())
        missing_count = 0
        examples: list[str] = []
        for (timestamp,) in cursor:
            while current < timestamp:
                missing_count += 1
                if len(examples) < 10:
                    examples.append(datetime.fromtimestamp(current, UTC).isoformat())
                current += 3600
            current = timestamp + 3600
        while current < int(stop.timestamp()):
            missing_count += 1
            if len(examples) < 10:
                examples.append(datetime.fromtimestamp(current, UTC).isoformat())
            current += 3600
        if missing_count:
            raise RuntimeError(
                f"Data gaps for {symbol}: {missing_count} unresolved hours of {expected}. "
                f"Missing or failed timestamps: {examples}"
                + (" ..." if missing_count > 10 else "")
            )

    def iter_available_chunks(
        self, symbol: str, start: datetime, end: datetime
    ) -> Iterator[TickChunk]:
        """Yield downloaded chunks overlapping the exact UTC [start,end) range."""
        start, end = normalize_datetime(start), normalize_datetime(end)
        if end < start:
            raise ValueError("end must not be before start")
        if end == start:
            return
        symbol = normalize_symbol(symbol)
        self._ensure_table_exists(symbol)
        cursor = self.conn.execute(
            f"SELECT timestamp FROM {self._table(symbol)} "
            "WHERE timestamp>=? AND timestamp<? AND status='downloaded' ORDER BY timestamp",
            (int(hour_floor(start).timestamp()), int(hour_ceil(end).timestamp())),
        )
        for (timestamp,) in cursor:
            yield TickChunk(symbol=symbol, time=datetime.fromtimestamp(timestamp, UTC))

    def get_available_chunks(
        self, symbol: str, start: datetime, end: datetime
    ) -> list[TickChunk]:
        return list(self.iter_available_chunks(symbol, start, end))

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "MetadataDB":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()

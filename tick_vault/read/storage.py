"""Streaming SQLite and optional HDF5 exports and indexed range queries."""

import os
import shutil
import sqlite3
import tempfile
from collections.abc import Iterator
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import numpy as np
import pandas as pd

from ..core.filelock import destination_lock, publish_no_clobber
from ..core.paths import validate_export_destination
from ..utils import normalize_datetime, normalize_symbol
from .reader import empty_tick_frame, iter_tick_data

WriteMode = Literal["replace", "append", "fail"]
_COLUMNS = ["time", "ask", "bid", "ask_volume", "bid_volume"]
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _check_mode(mode: WriteMode) -> None:
    if mode not in ("replace", "append", "fail"):
        raise ValueError("if_exists must be 'replace', 'append' or 'fail'")


def _export_path(path: str | Path) -> Path:
    path = validate_export_destination(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _temporary_sibling(path: Path) -> Path:
    descriptor, name = tempfile.mkstemp(
        prefix=path.name + ".", suffix=".tmp", dir=path.parent
    )
    os.close(descriptor)
    return Path(name)


def _milliseconds_bound(value: datetime) -> int:
    """Ceiling to tick precision preserves exact microsecond query bounds."""
    delta = normalize_datetime(value) - _EPOCH
    micros = (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds
    return -(-micros // 1000)


def _bounds(
    start: datetime | None, end: datetime | None
) -> tuple[int | None, int | None]:
    start = normalize_datetime(start) if start is not None else None
    end = normalize_datetime(end) if end is not None else None
    if start is not None and end is not None and end < start:
        raise ValueError("end must not be before start")
    return (
        _milliseconds_bound(start) if start is not None else None,
        _milliseconds_bound(end) if end is not None else None,
    )


def _on_disk_frame(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    frame["time"] = frame["time"].to_numpy(dtype="datetime64[ms]").astype(np.int64)
    return frame


def _tick_frame(rows: list[tuple[Any, ...]]) -> pd.DataFrame:
    frame = pd.DataFrame.from_records(rows, columns=_COLUMNS)
    frame["time"] = pd.to_datetime(frame["time"], unit="ms")
    for side in ("ask", "bid"):
        frame[side] = frame[side].astype(np.float64)
    for side in ("ask_volume", "bid_volume"):
        frame[side] = frame[side].astype(np.int64)
    return frame


def export_sqlite(
    path: str | Path,
    symbol: str,
    start: datetime | None = None,
    end: datetime | None = None,
    *,
    if_exists: WriteMode = "replace",
    lock_timeout: float = 30.0,
    **reader_kwargs: Any,
) -> int:
    """Stream decoded ticks to an indexed SQLite database; return rows written.

    The ``ticks`` table stores symbol, integer UTC milliseconds in ``time``,
    numeric price/volume columns, and an insertion ``id`` preserving duplicate
    tick timestamps. Replace affects only this symbol. Append adds rows, including
    duplicates. A temporary SQLite snapshot is atomically published only when
    the complete export succeeds, preserving other symbols and original files.
    """
    _check_mode(if_exists)
    symbol = normalize_symbol(symbol)
    path = _export_path(path)
    with destination_lock(path, timeout=lock_timeout):
        temporary = _temporary_sibling(path)
        count = 0
        try:
            destination_exists = path.exists()
            with (
                closing(sqlite3.connect(temporary)) as conn,
                closing(iter_tick_data(symbol, start, end, **reader_kwargs)) as frames,
            ):
                if destination_exists:
                    with closing(
                        sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
                    ) as source:
                        source.backup(conn)
                with conn:
                    conn.execute(
                        "CREATE TABLE IF NOT EXISTS ticks (id INTEGER PRIMARY KEY, "
                        "symbol TEXT NOT NULL, time INTEGER NOT NULL, "
                        "ask REAL NOT NULL,bid REAL NOT NULL,ask_volume INTEGER NOT NULL,"
                        "bid_volume INTEGER NOT NULL)"
                    )
                    conn.execute(
                        "CREATE INDEX IF NOT EXISTS ticks_symbol_time ON ticks(symbol,time)"
                    )
                    exists = conn.execute(
                        "SELECT 1 FROM ticks WHERE symbol=? LIMIT 1", (symbol,)
                    ).fetchone()
                    if if_exists == "fail" and exists:
                        raise ValueError(f"Ticks for {symbol} already exist in {path}")
                    if if_exists == "replace":
                        conn.execute("DELETE FROM ticks WHERE symbol=?", (symbol,))
                    for frame in frames:
                        disk = _on_disk_frame(frame)
                        conn.executemany(
                            "INSERT INTO ticks(symbol,time,ask,bid,ask_volume,bid_volume) "
                            "VALUES (?,?,?,?,?,?)",
                            (
                                (symbol, *row)
                                for row in disk.itertuples(index=False, name=None)
                            ),
                        )
                        count += len(frame)
            with temporary.open("r+b") as file:
                os.fsync(file.fileno())
            if if_exists == "fail" and not destination_exists:
                publish_no_clobber(temporary, path)
            else:
                os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
    return count


def iter_sqlite(
    path: str | Path,
    symbol: str,
    start: datetime | None = None,
    end: datetime | None = None,
    *,
    batch_size: int = 100_000,
    lock_timeout: float = 30.0,
) -> Iterator[pd.DataFrame]:
    """Read indexed SQLite ranges with bounded batches, preserving tick order."""
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    symbol = normalize_symbol(symbol)
    lower, upper = _bounds(start, end)
    path = Path(path).resolve()
    with destination_lock(path, shared=True, timeout=lock_timeout):
        # Read-only URI prevents silently creating an empty database for a typo.
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as conn:
            where = ["symbol=?"]
            args: list[str | int] = [symbol]
            if lower is not None:
                where.append("time>=?")
                args.append(lower)
            if upper is not None:
                where.append("time<?")
                args.append(upper)
            cursor = conn.execute(
                "SELECT time,ask,bid,ask_volume,bid_volume FROM ticks WHERE "
                + " AND ".join(where)
                + " ORDER BY time,id",
                args,
            )
            while rows := cursor.fetchmany(batch_size):
                yield _tick_frame(rows)


def read_sqlite(
    path: str | Path,
    symbol: str,
    start: datetime | None = None,
    end: datetime | None = None,
    *,
    batch_size: int = 100_000,
    lock_timeout: float = 30.0,
) -> pd.DataFrame:
    frames = list(
        iter_sqlite(
            path, symbol, start, end, batch_size=batch_size, lock_timeout=lock_timeout
        )
    )
    return pd.concat(frames, ignore_index=True) if frames else empty_tick_frame()


def _require_tables() -> None:
    try:
        import tables  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            "HDF5 support requires PyTables; install 'tick-vault[hdf5]'."
        ) from exc


def _hdf_key(symbol: str) -> str:
    # Hex is collision-free for underscores/dashes and a valid HDF identifier.
    return "/ticks_" + normalize_symbol(symbol).encode("ascii").hex()


def export_hdf5(
    path: str | Path,
    symbol: str,
    start: datetime | None = None,
    end: datetime | None = None,
    *,
    if_exists: WriteMode = "replace",
    lock_timeout: float = 30.0,
    **reader_kwargs: Any,
) -> int:
    """Stream to a compressed indexed HDF5 table; atomically publish on success.

    Existing symbols are preserved. Append requires ticks at or after the last
    stored tick and keeps duplicate timestamps. This maintains sorted streaming
    queries. A temporary sibling copy protects the original on failure.
    """
    _require_tables()
    _check_mode(if_exists)
    symbol = normalize_symbol(symbol)
    path = _export_path(path)
    with destination_lock(path, timeout=lock_timeout):
        temporary = _temporary_sibling(path)
        count = 0
        try:
            destination_exists = path.exists()
            if destination_exists:
                shutil.copyfile(path, temporary)
            else:
                temporary.unlink()
            key = _hdf_key(symbol)
            with (
                closing(iter_tick_data(symbol, start, end, **reader_kwargs)) as frames,
                pd.HDFStore(temporary, mode="a", complevel=5, complib="zlib") as store,
            ):
                exists = key in store
                if exists and if_exists == "fail":
                    raise ValueError(f"Ticks for {symbol} already exist in {path}")
                if exists and if_exists == "replace":
                    store.remove(key)
                last_time = None
                if key in store:
                    nrows = store.get_storer(key).nrows
                    if nrows:
                        last_time = int(
                            store.select(key, start=nrows - 1, stop=nrows)["time"].iloc[
                                0
                            ]
                        )
                for frame in frames:
                    disk = _on_disk_frame(frame)
                    if last_time is not None and int(disk["time"].iloc[0]) < last_time:
                        raise ValueError(
                            "HDF5 append requires chronological ticks at or after the last stored tick"
                        )
                    store.append(
                        key, disk, format="table", data_columns=["time"], index=False
                    )
                    last_time = int(disk["time"].iloc[-1])
                    count += len(frame)
                if key in store:
                    store.create_table_index(
                        key, columns=["time"], optlevel=6, kind="medium"
                    )
                    attrs = store.get_storer(key).attrs
                    attrs.tick_vault_symbol = symbol
                    attrs.tick_vault_time_unit = "UTC milliseconds"
                store.flush(fsync=True)
            if if_exists == "fail" and not destination_exists:
                publish_no_clobber(temporary, path)
            else:
                os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
    return count


def iter_hdf5(
    path: str | Path,
    symbol: str,
    start: datetime | None = None,
    end: datetime | None = None,
    *,
    batch_size: int = 100_000,
    lock_timeout: float = 30.0,
) -> Iterator[pd.DataFrame]:
    """Read optional HDF5 indexed ranges in bounded frames."""
    _require_tables()
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    lower, upper = _bounds(start, end)
    key = _hdf_key(symbol)
    with destination_lock(path, shared=True, timeout=lock_timeout):
        with pd.HDFStore(path, mode="r") as store:
            if key not in store:
                return
            where = []
            if lower is not None:
                where.append(f"time >= {lower}")
            if upper is not None:
                where.append(f"time < {upper}")
            for frame in store.select(
                key, where=where, iterator=True, chunksize=batch_size
            ):
                frame = frame.reset_index(drop=True)
                frame["time"] = pd.to_datetime(frame["time"], unit="ms")
                yield frame


def read_hdf5(
    path: str | Path,
    symbol: str,
    start: datetime | None = None,
    end: datetime | None = None,
    *,
    batch_size: int = 100_000,
    lock_timeout: float = 30.0,
) -> pd.DataFrame:
    frames = list(
        iter_hdf5(
            path, symbol, start, end, batch_size=batch_size, lock_timeout=lock_timeout
        )
    )
    return pd.concat(frames, ignore_index=True) if frames else empty_tick_frame()

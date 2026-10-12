"""Local transactional state shared by provider coordinators and managed jobs."""

import os
import sqlite3
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path


def state_path(value: str | Path | None = None) -> Path:
    """Resolve a stable user state location, never a per-vault default."""
    if value is None:
        local = os.environ.get("LOCALAPPDATA")
        root = (
            Path(local) / "TickVault" / "provider"
            if local
            else Path.home() / ".local" / "state" / "tick-vault" / "provider"
        )
        return root / "provider.sqlite3"
    path = Path(value).expanduser().absolute()
    return (
        path
        if path.suffix.lower() in {".db", ".sqlite", ".sqlite3"}
        else path / "provider.sqlite3"
    )


SCHEMA = """
CREATE TABLE IF NOT EXISTS providers (
    provider_key TEXT PRIMARY KEY,
    last_seen REAL NOT NULL,
    next_request REAL NOT NULL DEFAULT 0,
    cooldown_until REAL NOT NULL DEFAULT 0,
    circuit TEXT NOT NULL DEFAULT 'closed',
    probe_token TEXT,
    throttle_count INTEGER NOT NULL DEFAULT 0,
    request_interval REAL NOT NULL,
    max_concurrency INTEGER NOT NULL,
    minute_budget INTEGER NOT NULL,
    hour_budget INTEGER NOT NULL,
    no_header_cooldown REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS requests (
    token TEXT PRIMARY KEY,
    provider_key TEXT NOT NULL,
    started_at REAL NOT NULL,
    lease_until REAL NOT NULL,
    is_probe INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS requests_provider ON requests(provider_key, lease_until);
CREATE TABLE IF NOT EXISTS request_starts (
    token TEXT PRIMARY KEY,
    provider_key TEXT NOT NULL,
    started_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS starts_provider ON request_starts(provider_key, started_at);
CREATE TABLE IF NOT EXISTS provider_access (
    provider_key TEXT NOT NULL,
    source_key TEXT NOT NULL,
    symbol TEXT NOT NULL,
    hour TEXT NOT NULL,
    tick_count INTEGER NOT NULL,
    verified_at REAL NOT NULL,
    PRIMARY KEY(provider_key, source_key)
);
CREATE TABLE IF NOT EXISTS hours (
    provider_key TEXT NOT NULL,
    symbol TEXT NOT NULL,
    hour TEXT NOT NULL,
    state TEXT NOT NULL,
    token TEXT,
    lease_until REAL,
    result TEXT,
    artifact_path TEXT,
    digest TEXT,
    byte_count INTEGER,
    completed_at REAL,
    PRIMARY KEY(provider_key, symbol, hour)
);
CREATE TABLE IF NOT EXISTS managed_jobs (
    job_id TEXT PRIMARY KEY,
    request_json TEXT NOT NULL,
    config_json TEXT NOT NULL,
    state TEXT NOT NULL,
    total INTEGER NOT NULL DEFAULT 0,
    completed INTEGER NOT NULL DEFAULT 0,
    pending INTEGER NOT NULL DEFAULT 0,
    failed INTEGER NOT NULL DEFAULT 0,
    next_eligible_at REAL,
    retry_reason TEXT,
    error TEXT,
    report_json TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    run_token TEXT,
    run_lease_until REAL
);
"""


def _configure(connection: sqlite3.Connection) -> None:
    """WAL's first-use lock upgrade can ignore SQLite's ordinary busy timeout."""
    connection.execute("PRAGMA busy_timeout=30000")
    deadline = time.monotonic() + 30
    while True:
        try:
            mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
            if mode.lower() != "wal":
                connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=FULL")
            connection.executescript(SCHEMA)
            return
        except sqlite3.OperationalError as exc:
            if (
                "locked" not in str(exc).lower() and "busy" not in str(exc).lower()
            ) or time.monotonic() >= deadline:
                raise
            # This function runs only inside asyncio.to_thread. No event loop
            # sleep or active transaction is held during a first-use retry.
            time.sleep(0.02)


@contextmanager
def transaction(path: Path) -> Iterator[sqlite3.Connection]:
    """All callers invoke this on a worker thread; SQLite serializes processes."""
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30, isolation_level=None)
    connection.row_factory = sqlite3.Row
    try:
        _configure(connection)
        connection.execute("BEGIN IMMEDIATE")
        yield connection
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()

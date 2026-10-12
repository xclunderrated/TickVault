"""Run an offline streaming/export tutorial using explicitly synthetic ticks.

Usage: python examples/offline_analysis.py
       python examples/offline_analysis.py --base-directory ./tutorial_data

Install TickVault first with ``pip install -e .``. No HTTP requests are made.
"""

import argparse
import lzma
import struct
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

from tick_vault import config
from tick_vault.chunk import TickChunk
from tick_vault.metadata import MetadataDB
from tick_vault.read.storage import export_sqlite, iter_sqlite, read_sqlite
from tick_vault.reader import iter_tick_data


def create_demo_archive(directory: str | Path) -> tuple[datetime, datetime]:
    """Create two BI5 files with synthetic EURUSD ticks for learning/testing."""
    directory = Path(directory)
    if (directory / "metadata.db").exists():
        raise FileExistsError("Synthetic tutorial requires a fresh data directory")
    start = datetime(2024, 1, 2, 12, tzinfo=UTC)
    chunks = [
        TickChunk(symbol="EURUSD", time=start + timedelta(hours=hour))
        for hour in range(2)
    ]
    if any(chunk.path(directory / "downloads").exists() for chunk in chunks):
        raise FileExistsError(
            "Synthetic tutorial will not overwrite existing tick files"
        )
    config.reload_config(base_directory=directory, console_log_level="ERROR")
    for hour in range(2):
        chunk = chunks[hour]
        # The feed format is 20-byte, big-endian milliseconds/ask/bid/volumes.
        records = b"".join(
            struct.pack(
                ">IIIff",
                minute * 60_000,
                110020 + hour * 10 + minute,
                110000 + hour * 10 + minute,
                0.5 + minute / 100,
                0.25 + minute / 100,
            )
            for minute in (0, 15, 30, 45)
        )
        chunk.save(lzma.compress(records, format=lzma.FORMAT_ALONE))
    with MetadataDB() as db:
        db.insert_rows(chunks)
    return start, start + timedelta(hours=2)


def demonstrate(directory: Path) -> None:
    start, end = create_demo_archive(directory)
    print(
        "Synthetic tutorial data; these ticks are not historical market observations."
    )
    rows = 0
    for frame in iter_tick_data(
        "EURUSD",
        start,
        end,
        batch_size=3,
        backend="thread",
        max_workers=2,
        show_progress=False,
    ):
        rows += len(frame)
        print(frame.assign(spread=frame["ask"] - frame["bid"]))
    assert rows == 8
    # The SQL backend receives bounded batches without materializing all ticks.
    database = directory / "analysis.sqlite"
    exported = export_sqlite(
        database, "EURUSD", start, end, batch_size=3, show_progress=False
    )
    assert exported == rows
    subset = read_sqlite(
        database,
        "EURUSD",
        start + timedelta(minutes=15),
        start + timedelta(minutes=45),
    )
    assert len(subset) == 2  # Exact [12:15, 12:45); the tick at 12:45 is excluded.
    print("Exact half-hour SQLite query:")
    print(subset)
    assert (
        sum(len(frame) for frame in iter_sqlite(database, "EURUSD", batch_size=3))
        == rows
    )
    print(f"Exported and queried {exported} synthetic ticks: {database}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-directory", type=Path)
    args = parser.parse_args()
    if args.base_directory:
        demonstrate(args.base_directory)
    else:
        original = config.CONFIG.model_dump()
        with tempfile.TemporaryDirectory(prefix="tickvault-tutorial-") as directory:
            try:
                demonstrate(Path(directory))
            finally:
                # Close the demo log before Windows removes the temporary folder.
                config.reload_config(**original)


if __name__ == "__main__":
    main()

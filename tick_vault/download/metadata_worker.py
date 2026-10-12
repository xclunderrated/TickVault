"""Single metadata writer with durable batch flush on stop and cancellation."""

import asyncio
from pathlib import Path

from ..chunk import TickChunk
from ..config import get_config
from ..core.asyncutils import run_blocking
from ..metadata import DownloadStatus, MetadataDB
from .models import DownloadResult


async def metadata_worker(
    result_queue: asyncio.Queue[DownloadResult | TickChunk | None],
    db_path: str | Path | None = None,
) -> None:
    """Persist explicit outcomes in batches; idle workers never imply a crash.

    Plain TickChunk messages remain accepted for older internal callers. Absence
    on disk is then uncertain and recorded as failed by MetadataDB.insert_rows.
    Queue acknowledgements are made only after the corresponding commit.
    """
    settings = get_config()
    location = db_path if db_path is not None else settings.metadata_db_path
    batch: list[DownloadResult | TickChunk] = []

    def commit() -> None:
        grouped: dict[tuple[DownloadStatus | None, str | None], list[TickChunk]] = {}
        for item in batch:
            key: tuple[DownloadStatus | None, str | None]
            if isinstance(item, DownloadResult):
                key = (item.status, item.error)
                chunk = item.chunk
            else:
                key = (None, None)
                chunk = item
            grouped.setdefault(key, []).append(chunk)
        with MetadataDB(location) as db:
            for (status, error), chunks in grouped.items():
                db.insert_rows(chunks, status=status, error=error)

    def acknowledge() -> None:
        for _ in batch:
            result_queue.task_done()
        batch.clear()

    async def flush() -> None:
        if not batch:
            return
        try:
            await run_blocking(commit)
        except asyncio.CancelledError:
            # run_blocking finishes the commit before propagating cancellation.
            acknowledge()
            raise
        acknowledge()

    try:
        while True:
            try:
                async with asyncio.timeout(settings.metadata_update_batch_timeout):
                    item = await result_queue.get()
            except TimeoutError:
                await flush()
                continue
            if item is None:
                try:
                    await flush()
                finally:
                    result_queue.task_done()
                return
            batch.append(item)
            if len(batch) >= settings.metadata_update_batch_size:
                await flush()
    except asyncio.CancelledError:
        # Explicit shutdown preserves receipts already delivered to this queue.
        while not result_queue.empty():
            item = result_queue.get_nowait()
            if item is None:
                result_queue.task_done()
            else:
                batch.append(item)
        await flush()
        raise

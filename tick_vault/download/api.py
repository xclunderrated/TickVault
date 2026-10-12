"""Supervised, bounded asynchronous download pipeline."""

import asyncio
import inspect
from collections.abc import Awaitable, Callable
from contextlib import AsyncExitStack
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import monotonic, time
from typing import Any

from httpx import AsyncClient, Limits
from tqdm import tqdm

from ..chunk import TickChunk
from ..config import Config, snapshot_config, use_config
from ..core.asyncutils import run_blocking
from ..logger import logger, setup_logger
from ..metadata import MetadataDB, SourceIdentityError
from ..provider import ProviderDeferred
from ..utils import normalize_datetime
from .admission import provider_coordinator, receipt_coordinator, source_key
from .concurrency import AdaptiveController
from .metadata_worker import metadata_worker
from .models import DownloadProgress, DownloadReport, DownloadResult
from .worker import download_worker


class _DownloadStopped(Exception):
    pass


def _utc(value: datetime, name: str) -> datetime:
    try:
        return normalize_datetime(value)
    except TypeError:
        raise TypeError(f"{name} must be a datetime") from None


def download_bounds(start: datetime, end: datetime | None) -> tuple[datetime, datetime]:
    """Cover explicit historical intervals and exclude incomplete/future hours."""
    start = _utc(start, "start")
    now = datetime.now(UTC)
    requested_end = now if end is None else _utc(end, "end")
    if requested_end <= start:
        raise ValueError("end must be after start")
    start_hour = start.replace(minute=0, second=0, microsecond=0)
    end_hour = requested_end.replace(minute=0, second=0, microsecond=0)
    if end is not None and end_hour < requested_end:
        end_hour += timedelta(hours=1)
    return start_hour, max(
        start_hour, min(end_hour, now.replace(minute=0, second=0, microsecond=0))
    )


async def _send_to_metadata(
    queue: asyncio.Queue[DownloadResult | TickChunk | None],
    writer: asyncio.Task[None],
    item: DownloadResult | None,
) -> None:
    """A failed writer wakes blocked queue producers immediately."""
    put = asyncio.create_task(queue.put(item))
    try:
        done, _ = await asyncio.wait((put, writer), return_when=asyncio.FIRST_COMPLETED)
        if writer in done:
            writer.result()
            if item is None and put.done():
                return
            raise RuntimeError("Metadata worker stopped before shutdown")
        await put
    finally:
        if not put.done():
            put.cancel()
            await asyncio.gather(put, return_exceptions=True)


async def _download_range(
    symbol: str,
    start: datetime,
    end: datetime | None = None,
    proxies: list[str] | None = None,
    *,
    settings: Config,
    state_path: str | Path | None = None,
    stop_event: asyncio.Event | None = None,
    progress_callback: (
        Callable[[DownloadProgress], Awaitable[None] | None] | None
    ) = None,
    show_progress: bool = True,
    retry_no_data: bool = False,
) -> DownloadReport:
    """Download complete hours covering ``[start, end)`` and return final counts.

    Naive datetimes mean UTC. Explicit historical end times are rounded up to
    cover the last requested partial hour; the current incomplete hour is always
    excluded. One common bounded work queue balances fast and slow workers.
    Existing files and confirmed no-data outcomes are reused unless stale.

    A set ``stop_event`` cancels in-flight requests, flushes completed receipts,
    and returns a cancelled report. Task cancellation instead propagates
    CancelledError after the same durable flush. Child exceptions stop siblings
    and are raised directly, without waiting on a result queue indefinitely.
    """
    setup_logger()
    start_hour, end_hour = download_bounds(start, end)
    # Validate/normalize symbols even when the range contains no complete hours.
    symbol = TickChunk(symbol=symbol, time=start_hour).symbol
    total = max(0, int((end_hour - start_hour).total_seconds() // 3600))
    started = monotonic()
    operation_started_at = time()
    attempted = downloaded = no_data = failed = bytes_downloaded = scheduled = 0
    cached = 0
    shared_cached = 0
    discovery_complete = False
    cancelled = False

    def report() -> DownloadReport:
        return DownloadReport(
            symbol,
            start_hour,
            end_hour,
            total,
            attempted,
            downloaded,
            no_data,
            cached,
            failed,
            cancelled,
            bytes_downloaded,
            monotonic() - started,
            state="paused" if cancelled else "complete",
            complete=not cancelled and failed == 0 and attempted + cached >= total,
            pending=max(0, total - downloaded - no_data - cached),
            completed=downloaded + no_data + cached,
        )

    worker_proxies: list[str | None]
    if proxies:
        worker_proxies = list(dict.fromkeys(proxies))
    else:
        worker_proxies = [None]
    worker_count = min(total, len(worker_proxies) * settings.worker_per_proxy)
    controller = AdaptiveController(
        max(1, worker_count),
        enabled=settings.adaptive_workers,
        minimum=settings.adaptive_min_workers,
        interval=settings.adaptive_adjustment_interval,
    )

    async def notify_progress(
        watch_tasks: set[asyncio.Task[Any]] | None = None,
    ) -> None:
        if progress_callback is not None:
            snapshot = DownloadProgress(
                symbol,
                total,
                attempted,
                downloaded,
                no_data,
                cached,
                failed,
                controller.in_flight,
                bytes_downloaded,
                monotonic() - started,
                discovery_complete,
            )
            result = progress_callback(snapshot)
            if inspect.isawaitable(result):
                callback_task = asyncio.ensure_future(result)
                callback_stop = (
                    asyncio.create_task(stop_event.wait())
                    if stop_event is not None
                    else None
                )
                try:
                    if callback_stop is None and not watch_tasks:
                        await callback_task
                    else:
                        watched = set(watch_tasks or ())
                        while True:
                            waiting = watched | {callback_task}
                            if callback_stop is not None:
                                waiting.add(callback_stop)
                            done, _ = await asyncio.wait(
                                waiting, return_when=asyncio.FIRST_COMPLETED
                            )
                            if stop_event is not None and stop_event.is_set():
                                raise _DownloadStopped
                            for child in done & watched:
                                child.result()
                                if child is writer:
                                    raise RuntimeError(
                                        "Metadata worker stopped unexpectedly"
                                    )
                                watched.remove(child)
                            if callback_task in done:
                                await callback_task
                                break
                finally:
                    for callback_child in (callback_task, callback_stop):
                        if callback_child is not None and not callback_child.done():
                            callback_child.cancel()
                    await asyncio.gather(
                        *(
                            child
                            for child in (callback_task, callback_stop)
                            if child is not None
                        ),
                        return_exceptions=True,
                    )
            if stop_event is not None and stop_event.is_set():
                raise _DownloadStopped

    try:
        await notify_progress()
    except _DownloadStopped:
        cancelled = True
        return report()
    if total == 0 or (stop_event is not None and stop_event.is_set()):
        cancelled = stop_event is not None and stop_event.is_set()
        discovery_complete = total == 0
        return report()

    input_queue: asyncio.Queue[TickChunk | None] = asyncio.Queue(settings.queue_size)
    output_queue: asyncio.Queue[DownloadResult] = asyncio.Queue(settings.queue_size)
    metadata_queue: asyncio.Queue[DownloadResult | TickChunk | None] = asyncio.Queue(
        settings.queue_size
    )
    pending_results: dict[tuple[str, datetime], DownloadResult] = {}
    provider = provider_coordinator(settings, state_path)
    receipts = receipt_coordinator(settings, provider)

    def count_result(result: DownloadResult) -> None:
        nonlocal attempted, downloaded, no_data, failed, bytes_downloaded, cached, shared_cached
        if result.reused:
            shared_cached += 1
            cached += 1
        else:
            attempted += 1
            downloaded += int(result.status == "downloaded")
            no_data += int(result.status == "no_data")
        failed += int(result.status == "failed")
        bytes_downloaded += result.bytes_downloaded
        pbar.update(1)

    async def produce() -> None:
        nonlocal scheduled, cached, discovery_complete

        def bind_source() -> None:
            with MetadataDB(settings.metadata_db_path) as source_db:
                source_db.bind_source(
                    source_key(settings), raw_directory=settings.save_directory
                )

        await run_blocking(bind_source)

        def discover(start: datetime, end: datetime) -> list[TickChunk]:
            # SQLite connections belong to the worker thread. Bound discovery
            # to eight hours so disk validation cannot starve lease heartbeats.
            with MetadataDB(settings.metadata_db_path) as db:
                return next(
                    db.iter_pending_batches(
                        symbol,
                        start,
                        end,
                        retry_no_data=retry_no_data,
                        recheck_no_data_after=settings.no_data_retry_after,
                        batch_size=8,
                    )
                )

        cursor = start_hour
        while cursor < end_hour:
            batch_end = min(end_hour, cursor + timedelta(hours=8))
            batch = await run_blocking(discover, cursor, batch_end)
            for chunk in batch:
                await input_queue.put(chunk)
                scheduled += 1
                await asyncio.sleep(0)
            cursor = batch_end
        cached = total - scheduled + shared_cached
        discovery_complete = True
        # Shared receipts advance the bar when collected; count only existing
        # local coverage here so deduplicated results are not counted twice.
        pbar.update(total - scheduled)
        for _ in range(worker_count):
            await input_queue.put(None)

    async with AsyncExitStack() as stack:
        clients: dict[str | None, AsyncClient] = {}
        for proxy in worker_proxies:
            clients[proxy] = await stack.enter_async_context(
                AsyncClient(
                    proxy=proxy,
                    timeout=settings.fetch_timeout,
                    limits=Limits(
                        max_connections=settings.worker_per_proxy,
                        max_keepalive_connections=settings.worker_per_proxy,
                    ),
                )
            )
        pbar = tqdm(
            total=total,
            desc=f"Downloading {symbol}",
            unit="hour",
            disable=not show_progress,
        )
        writer = asyncio.create_task(
            metadata_worker(metadata_queue), name="tickvault-metadata"
        )
        producer = asyncio.create_task(produce(), name="tickvault-producer")
        workers = [
            asyncio.create_task(
                download_worker(
                    worker_proxies[index % len(worker_proxies)],
                    input_queue,
                    output_queue,
                    client=clients[worker_proxies[index % len(worker_proxies)]],
                    controller=controller,
                    stop_event=stop_event,
                    pending_results=pending_results,
                    provider=provider,
                    receipts=receipts,
                    settings=settings,
                    require_access=settings.provider_require_verified_access
                    and total > 1,
                    retry_no_data=retry_no_data,
                    operation_started_at=operation_started_at,
                ),
                name=f"tickvault-download-{index}",
            )
            for index in range(worker_count)
        ]
        stop_task = (
            asyncio.create_task(stop_event.wait(), name="tickvault-stop")
            if stop_event is not None
            else None
        )
        next_result: asyncio.Task[DownloadResult] | None = None
        supervised: set[asyncio.Task[Any]] = {producer, writer, *workers}
        failure: BaseException | None = None
        try:
            while True:
                if stop_event is not None and stop_event.is_set():
                    raise _DownloadStopped
                if all(worker.done() for worker in workers) and output_queue.empty():
                    # Resultless workers are only valid after producer discovery.
                    for task in (producer, *workers):
                        if task.done():
                            task.result()
                    break
                if next_result is None:
                    next_result = asyncio.create_task(output_queue.get())
                waiting: set[asyncio.Task[Any]] = supervised | {next_result}
                if stop_task is not None:
                    waiting.add(stop_task)
                done, _ = await asyncio.wait(
                    waiting, return_when=asyncio.FIRST_COMPLETED
                )
                if stop_event is not None and stop_event.is_set():
                    raise _DownloadStopped
                for task in done & supervised:
                    task.result()  # Wake and propagate failures without another queue result.
                    if task is writer:
                        raise RuntimeError("Metadata worker stopped unexpectedly")
                    supervised.remove(task)
                if next_result in done:
                    result = next_result.result()
                    next_result = None
                    await _send_to_metadata(metadata_queue, writer, result)
                    pending_results.pop((result.chunk.symbol, result.chunk.time), None)
                    output_queue.task_done()
                    count_result(result)
                    await notify_progress(supervised)
        except _DownloadStopped:
            cancelled = True
        except BaseException as exc:
            failure = exc
        finally:
            # Stop only network/discovery tasks. The metadata owner stays alive
            # until every completed or failed receipt has been persisted.
            for cleanup_task in (producer, *workers, next_result, stop_task):
                if cleanup_task is not None and not cleanup_task.done():
                    cleanup_task.cancel()
            await asyncio.gather(
                *(
                    task
                    for task in (producer, *workers, next_result, stop_task)
                    if task is not None
                ),
                return_exceptions=True,
            )

            async def flush_receipts() -> None:
                if writer.done():
                    writer.result()
                for result in list(pending_results.values()):
                    await _send_to_metadata(metadata_queue, writer, result)
                    count_result(result)
                pending_results.clear()
                await _send_to_metadata(metadata_queue, writer, None)
                await writer

            try:
                await flush_receipts()
            except BaseException as exc:
                if failure is None:
                    failure = exc
                if not writer.done():
                    writer.cancel()
                await asyncio.gather(writer, return_exceptions=True)
            pbar.close()
            await receipts.close()
            await provider.close()
        if failure is not None:
            current = report()
            state = "needs_action"
            deadline = getattr(failure, "next_eligible_at", None)
            reason = None
            description = type(failure).__name__
            if isinstance(failure, ProviderDeferred):
                state = (
                    "needs_action"
                    if failure.reason == "access_unverified"
                    else "waiting_for_provider"
                )
                reason = failure.reason
                if failure.reason == "access_unverified":
                    deadline = None
                    description = "Provider access is unverified; run a one-hour canary before bulk downloads"
            elif isinstance(failure, SourceIdentityError):
                description = (
                    "This vault belongs to another feed; use a separate base directory"
                )
            else:
                from .fetcher import FetchExhaustedError, RateLimitError, RetryableError

                last_error = getattr(failure, "last_error", failure)
                if isinstance(last_error, RateLimitError):
                    reason = "rate_limited"
                    state = "waiting_for_provider"
                    deadline = getattr(last_error, "next_eligible_at", deadline)
                elif isinstance(failure, FetchExhaustedError) and isinstance(
                    last_error, RetryableError
                ):
                    state = "waiting_for_network"
                    reason = "temporary_network_or_server_error"
            failed_report = replace(
                current,
                state=state,
                complete=False,
                next_eligible_at=(
                    datetime.fromtimestamp(deadline, UTC)
                    if isinstance(deadline, int | float)
                    else None
                ),
                error_type=type(failure).__name__,
                error=description,
                retry_reason=reason,
            )
            failure.failure_report = failed_report  # type: ignore[attr-defined]
            raise failure
    try:
        await notify_progress()
    except _DownloadStopped:
        cancelled = True
    logger.info(
        "Download finished for %s: %d downloaded, %d no-data, %d cached%s",
        symbol,
        downloaded,
        no_data,
        cached,
        " (stopped)" if cancelled else "",
    )
    return report()


async def download_range(
    symbol: str,
    start: datetime,
    end: datetime | None = None,
    proxies: list[str] | None = None,
    *,
    stop_event: asyncio.Event | None = None,
    progress_callback: (
        Callable[[DownloadProgress], Awaitable[None] | None] | None
    ) = None,
    show_progress: bool = True,
    retry_no_data: bool = False,
    settings: Config | None = None,
    state_path: str | Path | None = None,
) -> DownloadReport:
    """Strict download API with shared durable quota and an immutable snapshot.

    Failures retain a failure_report attribute. Use managed jobs for resumable
    waiting states; cache-only reads/download discovery need no provider access.
    """
    captured = (settings or snapshot_config()).model_copy(deep=True)
    with use_config(captured):
        return await _download_range(
            symbol,
            start,
            end,
            proxies,
            settings=captured,
            state_path=state_path,
            stop_event=stop_event,
            progress_callback=progress_callback,
            show_progress=show_progress,
            retry_no_data=retry_no_data,
        )

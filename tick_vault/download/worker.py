"""Workers own one shared hourly claim and reacquire quota for every attempt."""

import asyncio
import time
from datetime import datetime

from httpx import AsyncClient

from ..chunk import TickChunk
from ..config import Config, get_config
from ..core.asyncutils import run_blocking
from ..core.binary import validate_payload
from ..provider import (
    ProviderCoordinator,
    ProviderDeferred,
    ProviderStatus,
    RequestLease,
)
from .admission import source_key
from .concurrency import AdaptiveController
from .fetcher import RateLimitError, fetch_with_retry
from .models import DownloadResult, FetchObservation


def _existing_data_is_valid(chunk: TickChunk) -> bool:
    try:
        return bool(validate_payload(chunk.load()))
    except (OSError, ValueError):
        return False


async def download_worker(
    proxy: str | None,
    input_queue: asyncio.Queue[TickChunk | None],
    output_queue: asyncio.Queue[DownloadResult],
    *,
    client: AsyncClient | None = None,
    controller: AdaptiveController | None = None,
    stop_event: asyncio.Event | None = None,
    pending_results: dict[tuple[str, datetime], DownloadResult] | None = None,
    provider: ProviderCoordinator | None = None,
    receipts: ProviderCoordinator | None = None,
    settings: Config | None = None,
    require_access: bool = False,
    retry_no_data: bool = False,
    operation_started_at: float | None = None,
) -> None:
    settings = settings or get_config()
    operation_started_at = (
        time.time() if operation_started_at is None else operation_started_at
    )

    async def save_receipt(
        chunk: TickChunk, content: bytes, *, reused: bool = False
    ) -> DownloadResult:
        save_task = asyncio.create_task(
            asyncio.to_thread(chunk.save, content, settings.save_directory)
        )
        cancelled = False
        try:
            await asyncio.shield(save_task)
        except asyncio.CancelledError:
            await save_task
            cancelled = True
        result = DownloadResult(
            chunk, "downloaded", 0 if reused else len(content), reused=reused
        )
        if pending_results is not None:
            pending_results[(chunk.symbol, chunk.time)] = result
        if cancelled:
            raise asyncio.CancelledError
        return result

    if client is None:
        async with AsyncClient(proxy=proxy, timeout=settings.fetch_timeout) as owned:
            await download_worker(
                proxy,
                input_queue,
                output_queue,
                client=owned,
                controller=controller,
                stop_event=stop_event,
                pending_results=pending_results,
                provider=provider,
                receipts=receipts,
                settings=settings,
                require_access=require_access,
                retry_no_data=retry_no_data,
                operation_started_at=operation_started_at,
            )
        return

    request_lease: RequestLease | None = None
    local_acquired = False

    async def before_attempt() -> None:
        nonlocal request_lease, local_acquired
        if controller is not None:
            await controller.acquire()
            local_acquired = True
        try:
            if provider is not None:
                status = await provider.status()
                if (
                    status.circuit == "open"
                    and status.next_eligible_at - time.time()
                    > settings.fetch_max_retry_delay
                ):
                    raise ProviderDeferred("cooldown", status.next_eligible_at)
                request_lease = await provider.acquire(
                    max_wait=settings.provider_admission_wait_budget,
                    stop_event=stop_event,
                    defer_on_cooldown=False,
                )
        except BaseException:
            if local_acquired and controller is not None:
                await controller.release()
                local_acquired = False
            raise

    async def after_attempt() -> None:
        nonlocal request_lease, local_acquired
        try:
            if provider is not None and request_lease is not None:
                await provider.release(request_lease)
        finally:
            request_lease = None
            if local_acquired and controller is not None:
                await controller.release()
                local_acquired = False

    async def observe(sample: FetchObservation) -> None:
        if controller is not None:
            await controller.observe(sample)
        if sample.error is None and provider is not None and request_lease is not None:
            await provider.observe_success(request_lease)

    async def throttled(error: RateLimitError) -> ProviderStatus | None:
        if provider is None:
            return None
        return await provider.observe_rate_limit(error.retry_after, lease=request_lease)

    while stop_event is None or not stop_event.is_set():
        chunk = await input_queue.get()
        claim = None
        try:
            if chunk is None:
                return
            try:
                if receipts is not None:
                    while True:
                        if stop_event is not None and stop_event.is_set():
                            raise asyncio.CancelledError
                        claim = await receipts.claim_hour(
                            chunk.symbol,
                            chunk.time,
                            max_no_data_age=settings.no_data_retry_after,
                            force_refresh=retry_no_data,
                            no_data_refreshed_since=operation_started_at,
                        )
                        if claim.state != "pending":
                            break
                        await asyncio.sleep(0.05)
                    if claim.state == "complete":
                        if claim.result == "downloaded":
                            if claim.artifact_path is None:
                                raise RuntimeError(
                                    "Shared downloaded receipt has no artifact"
                                )
                            shared_content = await asyncio.to_thread(
                                claim.artifact_path.read_bytes
                            )
                            await save_receipt(chunk, shared_content, reused=True)
                        result = DownloadResult(
                            chunk, claim.result or "no_data", reused=True
                        )
                        if pending_results is not None:
                            pending_results[(chunk.symbol, chunk.time)] = result
                        await output_queue.put(result)
                        continue

                if (
                    require_access
                    and provider is not None
                    and not await provider.has_validated_access(source_key(settings))
                ):
                    raise ProviderDeferred("access_unverified", time.time())

                content = await fetch_with_retry(
                    client,
                    chunk.url,
                    settings=settings,
                    stop_event=stop_event,
                    observer=observe,
                    before_attempt=before_attempt,
                    after_attempt=after_attempt,
                    on_rate_limit=throttled,
                )
                raw = (
                    await run_blocking(validate_payload, content)
                    if content is not None
                    else b""
                )
                if content is not None and raw:
                    result = await save_receipt(chunk, content)
                    if provider is not None:
                        await provider.record_validated_payload(
                            chunk.symbol,
                            chunk.time,
                            len(raw) // 20,
                            source_key=source_key(settings),
                        )
                elif await asyncio.to_thread(_existing_data_is_valid, chunk):
                    result = DownloadResult(chunk, "downloaded", reused=True)
                else:
                    result = DownloadResult(chunk, "no_data")
                if pending_results is not None:
                    pending_results[(chunk.symbol, chunk.time)] = result
                if receipts is not None and claim is not None and claim.owned:
                    await receipts.publish_hour(
                        claim,
                        chunk.path() if result.status == "downloaded" else None,
                        result=(
                            "downloaded" if result.status == "downloaded" else "no_data"
                        ),
                    )
            except Exception as exc:
                result = DownloadResult(chunk, "failed", error=type(exc).__name__)
                if pending_results is not None:
                    pending_results[(chunk.symbol, chunk.time)] = result
                raise
            if pending_results is not None:
                pending_results[(chunk.symbol, chunk.time)] = result
            await output_queue.put(result)
        finally:
            if receipts is not None and claim is not None and claim.owned:
                await receipts.release_hour(claim)
            if provider is not None and request_lease is not None:
                await provider.release(request_lease)
            if local_acquired and controller is not None:
                await controller.release()
            input_queue.task_done()

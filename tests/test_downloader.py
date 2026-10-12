import asyncio
import lzma
import struct
from datetime import UTC, datetime, timedelta, timezone
from time import monotonic

import httpx
import pytest

from tick_vault.chunk import TickChunk
from tick_vault.config import CONFIG, reload_config
from tick_vault.download import api
from tick_vault.download.admission import source_key
from tick_vault.download.api import download_bounds, download_range
from tick_vault.download.concurrency import AdaptiveController
from tick_vault.download.fetcher import ForbiddenError
from tick_vault.download.metadata_worker import metadata_worker
from tick_vault.download.models import DownloadResult, FetchObservation
from tick_vault.metadata import MetadataDB


@pytest.fixture(autouse=True)
def archive(tmp_path):
    original = CONFIG.model_dump()
    reload_config(
        base_directory=tmp_path,
        worker_per_proxy=2,
        adaptive_workers=False,
        fetch_max_retry_attempts=0,
        console_log_level="ERROR",
        queue_size=1,
        datafeed_base_url="https://mock.invalid/datafeed/",
        env_file=None,
    )
    # Orphan files in this fixture have known synthetic mock-feed provenance.
    with MetadataDB() as db:
        db.bind_source(source_key(CONFIG))
    yield tmp_path
    reload_config(**original)


def payload():
    return lzma.compress(
        struct.pack(">IIIff", 100, 110020, 110000, 1.25, 2.5), format=lzma.FORMAT_ALONE
    )


def install_transport(monkeypatch, handler):
    created = []
    real_client = httpx.AsyncClient

    def factory(*args, **kwargs):
        created.append(kwargs.get("proxy"))
        # Explicit fake proxy identities are only used to test pooling.
        kwargs.pop("proxy", None)
        return real_client(*args, transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(api, "AsyncClient", factory)
    return created


@pytest.mark.asyncio
async def test_child_failure_wakes_waiting_collector_and_records_failure(monkeypatch):
    async def handle(request):
        if "00h_" in request.url.path:
            await asyncio.Event().wait()
        return httpx.Response(403, request=request)

    install_transport(monkeypatch, handle)
    with pytest.raises(ForbiddenError):
        await asyncio.wait_for(
            download_range(
                "EURUSD",
                datetime(2024, 1, 2),
                datetime(2024, 1, 2, 3),
                show_progress=False,
            ),
            1,
        )
    with MetadataDB() as db:
        assert (
            db.get_status(TickChunk(symbol="EURUSD", time=datetime(2024, 1, 2, 1)))
            == "failed"
        )
        assert (
            db.get_status(TickChunk(symbol="EURUSD", time=datetime(2024, 1, 2, 0)))
            is None
        )
    assert not [
        task for task in asyncio.all_tasks() if task.get_name().startswith("tickvault-")
    ]


@pytest.mark.asyncio
async def test_child_error_interrupts_blocked_progress_callback(monkeypatch):
    callback_entered = asyncio.Event()

    async def handle(request):
        if "00h_" in request.url.path:
            return httpx.Response(200, content=payload(), request=request)
        await callback_entered.wait()
        return httpx.Response(403, request=request)

    async def progress(snapshot):
        if snapshot.downloaded:
            callback_entered.set()
            await asyncio.Event().wait()

    install_transport(monkeypatch, handle)
    with pytest.raises(ForbiddenError):
        await asyncio.wait_for(
            download_range(
                "EURUSD",
                datetime(2024, 1, 2),
                datetime(2024, 1, 2, 2),
                progress_callback=progress,
                show_progress=False,
            ),
            1,
        )
    with MetadataDB() as db:
        assert db.status("EURUSD") == {
            "downloaded": 1,
            "no_data": 0,
            "failed": 1,
            "total": 2,
        }


@pytest.mark.asyncio
async def test_cooperative_stop_flushes_completed_results_and_resumes(monkeypatch):
    stop = asyncio.Event()
    hung = asyncio.Event()
    calls = []

    async def handle(request):
        calls.append(request.url.path)
        if "00h_" in request.url.path:
            return httpx.Response(200, content=payload(), request=request)
        hung.set()
        await asyncio.Event().wait()

    async def progress(snapshot):
        if snapshot.downloaded:
            await hung.wait()
            stop.set()

    install_transport(monkeypatch, handle)
    report = await asyncio.wait_for(
        download_range(
            "EURUSD",
            datetime(2024, 1, 2),
            datetime(2024, 1, 2, 4),
            stop_event=stop,
            progress_callback=progress,
            show_progress=False,
        ),
        1,
    )
    assert report.cancelled
    assert report.downloaded == report.attempted == 1
    with MetadataDB() as db:
        assert (
            db.get_status(TickChunk(symbol="EURUSD", time=datetime(2024, 1, 2)))
            == "downloaded"
        )

    install_transport(
        monkeypatch,
        lambda request: httpx.Response(200, content=payload(), request=request),
    )
    resumed = await download_range(
        "EURUSD", datetime(2024, 1, 2), datetime(2024, 1, 2, 4), show_progress=False
    )
    assert resumed.cached == 1
    assert resumed.downloaded == 3


@pytest.mark.asyncio
async def test_external_cancellation_flushes_success_without_swallowing(monkeypatch):
    success = asyncio.Event()
    hold_callback = asyncio.Event()

    async def handle(request):
        if "00h_" in request.url.path:
            return httpx.Response(200, content=payload(), request=request)
        await asyncio.Event().wait()

    async def progress(snapshot):
        if snapshot.downloaded:
            success.set()
            await hold_callback.wait()

    install_transport(monkeypatch, handle)
    task = asyncio.create_task(
        download_range(
            "EURUSD",
            datetime(2024, 1, 2),
            datetime(2024, 1, 2, 3),
            progress_callback=progress,
            show_progress=False,
        )
    )
    await asyncio.wait_for(success.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 1)
    with MetadataDB() as db:
        assert (
            db.get_status(TickChunk(symbol="EURUSD", time=datetime(2024, 1, 2)))
            == "downloaded"
        )


@pytest.mark.asyncio
async def test_stop_interrupts_initial_async_callback_without_starting_http(
    monkeypatch,
):
    stop = asyncio.Event()
    entered = asyncio.Event()
    callbacks_before = set(asyncio.all_tasks())
    calls = []
    install_transport(monkeypatch, lambda request: calls.append(request))

    async def progress(snapshot):
        entered.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(
        download_range(
            "EURUSD",
            datetime(2024, 1, 2),
            datetime(2024, 1, 2, 1),
            progress_callback=progress,
            stop_event=stop,
            show_progress=False,
        )
    )
    await entered.wait()
    stop.set()
    report = await asyncio.wait_for(task, 1)
    assert report.cancelled
    assert report.attempted == 0
    assert not calls
    assert not (set(asyncio.all_tasks()) - callbacks_before)


@pytest.mark.asyncio
async def test_warm_all_cached_discovery_yields_for_stop(monkeypatch):
    start = datetime(2024, 1, 2)
    chunks = [
        TickChunk(symbol="EURUSD", time=start + timedelta(hours=i)) for i in range(100)
    ]
    for chunk in chunks:
        chunk.save(payload())
    with MetadataDB() as db:
        db.insert_rows(chunks, status="downloaded")
    calls = []
    install_transport(monkeypatch, lambda request: calls.append(request))
    stop = asyncio.Event()
    task = asyncio.create_task(
        download_range(
            "EURUSD",
            start,
            start + timedelta(hours=100),
            stop_event=stop,
            show_progress=False,
        )
    )
    # A cached batch must yield before all 100 validation checks finish.
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    stop.set()
    report = await asyncio.wait_for(task, 1)
    assert report.cancelled
    assert not calls


@pytest.mark.asyncio
async def test_completed_receipts_survive_backpressured_output_cancel(monkeypatch):
    stop = asyncio.Event()
    callback_entered = asyncio.Event()
    allow_callback = asyncio.Event()

    async def handle(request):
        await asyncio.sleep(0)
        return httpx.Response(200, content=payload(), request=request)

    async def progress(snapshot):
        if snapshot.attempted == 1:
            callback_entered.set()
            await allow_callback.wait()

    install_transport(monkeypatch, handle)
    task = asyncio.create_task(
        download_range(
            "EURUSD",
            datetime(2024, 1, 2),
            datetime(2024, 1, 2, 8),
            stop_event=stop,
            progress_callback=progress,
            show_progress=False,
        )
    )
    await callback_entered.wait()
    # Workers complete files while the callback holds a queue of size one.
    for _ in range(10):
        await asyncio.sleep(0)
    stop.set()
    allow_callback.set()
    report = await asyncio.wait_for(task, 1)
    assert report.cancelled
    with MetadataDB() as db:
        counts = db.status("EURUSD")
        files = list(CONFIG.save_directory.rglob("*.bi5"))
        assert counts["downloaded"] == report.downloaded == len(files)
        assert counts["failed"] == 0


@pytest.mark.asyncio
async def test_metadata_failure_cannot_deadlock_bounded_pipeline(monkeypatch):
    install_transport(
        monkeypatch,
        lambda request: httpx.Response(200, content=payload(), request=request),
    )

    async def broken_writer(queue):
        raise OSError("metadata unavailable")

    monkeypatch.setattr(api, "metadata_worker", broken_writer)
    with pytest.raises(OSError, match="metadata unavailable"):
        await asyncio.wait_for(
            download_range(
                "EURUSD",
                datetime(2024, 1, 2),
                datetime(2024, 1, 4),
                show_progress=False,
            ),
            1,
        )


@pytest.mark.asyncio
async def test_no_data_stays_distinct_from_invalid_payload(monkeypatch):
    install_transport(
        monkeypatch,
        lambda request: httpx.Response(
            200, content=lzma.compress(b""), request=request
        ),
    )
    report = await download_range(
        "EURUSD", datetime(2024, 1, 2), datetime(2024, 1, 2, 1), show_progress=False
    )
    assert report.no_data == 1
    assert report.downloaded == 0
    assert not list(CONFIG.save_directory.rglob("*.bi5"))
    install_transport(
        monkeypatch,
        lambda request: httpx.Response(
            200, content=b"<html>blocked</html>", request=request
        ),
    )
    with pytest.raises(ValueError, match="LZMA"):
        await asyncio.wait_for(
            download_range(
                "EURUSD",
                datetime(2024, 1, 2),
                datetime(2024, 1, 2, 1),
                retry_no_data=True,
                show_progress=False,
            ),
            1,
        )
    with MetadataDB() as db:
        assert (
            db.get_status(TickChunk(symbol="EURUSD", time=datetime(2024, 1, 2)))
            == "failed"
        )


@pytest.mark.asyncio
async def test_clients_shared_per_unique_proxy_and_fast_workers_take_more_jobs(
    monkeypatch,
):
    # Four worker tasks use two unique clients; duplicate proxy entries do not
    # inflate client or worker counts.
    reload_config(
        worker_per_proxy=2,
        datafeed_base_url=CONFIG.datafeed_base_url,
        env_file=None,
        base_directory=CONFIG.base_directory,
        adaptive_workers=False,
    )
    active = maximum = 0

    async def handle(request):
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        await asyncio.sleep(0.002)
        active -= 1
        return httpx.Response(200, content=payload(), request=request)

    created = install_transport(monkeypatch, handle)
    report = await download_range(
        "EURUSD",
        datetime(2024, 1, 2),
        datetime(2024, 1, 2, 12),
        proxies=["http://a.invalid", "http://b.invalid", "http://a.invalid"],
        show_progress=False,
    )
    assert len(created) == 2
    assert report.downloaded == 12
    assert maximum <= 4


@pytest.mark.asyncio
async def test_waiting_proxy_does_not_block_shared_queue_consumers(monkeypatch):
    original_client = httpx.AsyncClient
    calls = {"http://slow.invalid": 0, "http://fast.invalid": 0}
    fast_finished = asyncio.Event()

    def factory(*args, **kwargs):
        proxy = kwargs.pop("proxy")

        async def handle(request):
            calls[proxy] += 1
            if "slow" in proxy:
                await asyncio.wait_for(fast_finished.wait(), timeout=5)
            elif calls[proxy] == 8:
                fast_finished.set()
            return httpx.Response(200, content=payload(), request=request)

        return original_client(*args, transport=httpx.MockTransport(handle), **kwargs)

    monkeypatch.setattr(api, "AsyncClient", factory)
    report = await download_range(
        "EURUSD",
        datetime(2024, 1, 2),
        datetime(2024, 1, 2, 12),
        proxies=list(calls),
        show_progress=False,
    )
    assert report.downloaded == 12
    assert calls["http://fast.invalid"] >= 8
    assert calls["http://slow.invalid"] <= 4


@pytest.mark.asyncio
async def test_rate_cooldown_blocks_new_requests_across_all_proxy_clients(monkeypatch):
    reload_config(
        base_directory=CONFIG.base_directory,
        worker_per_proxy=1,
        datafeed_base_url=CONFIG.datafeed_base_url,
        adaptive_workers=False,
        fetch_max_retry_attempts=1,
        console_log_level="ERROR",
        env_file=None,
    )
    original_client = httpx.AsyncClient
    calls = []
    provider_deadline = None

    def factory(*args, **kwargs):
        proxy = kwargs.pop("proxy")

        async def handle(request):
            nonlocal provider_deadline
            calls.append((proxy, monotonic()))
            if len(calls) == 1:
                provider_deadline = monotonic() + 0.06
                return httpx.Response(
                    429, headers={"Retry-After": "0.06"}, request=request
                )
            return httpx.Response(200, content=payload(), request=request)

        return original_client(*args, transport=httpx.MockTransport(handle), **kwargs)

    monkeypatch.setattr(api, "AsyncClient", factory)
    report = await download_range(
        "EURUSD",
        datetime(2024, 1, 2),
        datetime(2024, 1, 2, 6),
        proxies=["http://a.invalid", "http://b.invalid"],
        show_progress=False,
    )
    assert report.downloaded == 6
    assert len(calls) == 7  # Exactly one bounded retry, no proxy rotation.
    assert provider_deadline is not None
    # The first two requests can already be in flight when the 429 is observed.
    assert all(when >= provider_deadline for _, when in calls[2:])


@pytest.mark.asyncio
async def test_unrecorded_atomic_file_is_recovered_without_network(monkeypatch):
    chunk = TickChunk(symbol="EURUSD", time=datetime(2024, 1, 2))
    chunk.save(payload())
    calls = []

    def handle(request):
        calls.append(request)
        raise AssertionError("A validated local file should not be fetched again")

    install_transport(monkeypatch, handle)
    report = await download_range(
        "EURUSD", datetime(2024, 1, 2), datetime(2024, 1, 2, 1), show_progress=False
    )
    assert report.cached == 1
    assert report.attempted == 0
    assert not calls
    with MetadataDB() as db:
        assert db.get_status(chunk) == "downloaded"


@pytest.mark.asyncio
async def test_idle_metadata_writer_waits_until_explicit_shutdown():
    reload_config(
        base_directory=CONFIG.base_directory,
        worker_queue_timeout=0.05,
        metadata_update_batch_timeout=0.01,
        env_file=None,
    )
    queue = asyncio.Queue()
    task = asyncio.create_task(metadata_worker(queue))
    await asyncio.sleep(0.07)
    assert not task.done()
    chunk = TickChunk(symbol="EURUSD", time=datetime(2024, 1, 2))
    await queue.put(DownloadResult(chunk, "no_data"))
    await queue.put(None)
    await asyncio.wait_for(task, 1)
    with MetadataDB() as db:
        assert db.get_status(chunk) == "no_data"


@pytest.mark.asyncio
async def test_adaptive_capacity_uses_latency_errors_and_shared_rate_cooldown():
    controller = AdaptiveController(8, minimum=1, interval=2)
    assert controller.limit == 2
    for _ in range(2):
        await controller.observe(FetchObservation(0.01, 100))
    assert controller.limit == 3
    for _ in range(2):
        await controller.observe(FetchObservation(0.02, 100))
    assert controller.limit == 2  # Latency regression reduces concurrency.
    await controller.observe(FetchObservation(0.01, 0, "RetryableError"))
    await controller.observe(FetchObservation(0.01, 100))
    assert controller.limit == 1
    controller.limit = 6
    await controller.observe(FetchObservation(0.01, 0, "RateLimitError", 0.04))
    assert controller.limit == 3
    task = asyncio.create_task(controller.acquire())
    await asyncio.sleep(0.01)
    assert not task.done()
    await asyncio.wait_for(task, 1)
    await controller.release()


def test_timezone_normalized_before_comparing_and_covering_historical_hour():
    start = datetime(2024, 1, 2, 21, 30)
    end = datetime(2024, 1, 3, 0, 40, tzinfo=timezone(timedelta(hours=3)))
    assert download_bounds(start, end) == (
        datetime(2024, 1, 2, 21, tzinfo=UTC),
        datetime(2024, 1, 2, 22, tzinfo=UTC),
    )


def test_default_end_uses_current_clock_and_excludes_partial_hour(monkeypatch):
    class Clock(datetime):
        value = datetime(2024, 1, 2, 12, 30, tzinfo=UTC)

        @classmethod
        def now(cls, tz=None):
            return cls.value

    monkeypatch.setattr(api, "datetime", Clock)
    # isinstance checks normally require datetime; construct the input with the
    # patched subclass so this test only verifies current-clock behavior.
    start = Clock(2024, 1, 2, 0)
    assert download_bounds(start, None)[1].hour == 12
    Clock.value = Clock(2024, 1, 2, 14, 45, tzinfo=UTC)
    assert download_bounds(start, None)[1].hour == 14

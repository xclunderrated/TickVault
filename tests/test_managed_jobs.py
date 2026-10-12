import asyncio
import lzma
import struct
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from tick_vault.config import CONFIG, reload_config
from tick_vault.download import api as download_api
from tick_vault.download import jobs
from tick_vault.download.fetcher import (
    FetchExhaustedError,
    ForbiddenError,
    RateLimitError,
    RetryableError,
)
from tick_vault.download.models import DownloadProgress, DownloadReport
from tick_vault.provider import ProviderDeferred
from tick_vault.provider.jobs import JobLeaseLost, JobStore

START = datetime(2024, 1, 1, tzinfo=UTC)
END = START + timedelta(hours=2)


@pytest.fixture(autouse=True)
def settings(tmp_path):
    original = CONFIG.model_dump()
    reload_config(
        base_directory=tmp_path / "vault",
        provider_state_directory=tmp_path / "shared",
        env_file=None,
    )
    yield
    reload_config(**original)


def report(*, downloaded=2, failed=0, cancelled=False):
    return DownloadReport(
        "EURUSD",
        START,
        END,
        2,
        downloaded + failed,
        downloaded,
        0,
        0,
        failed,
        cancelled,
        downloaded * 20,
        1.0,
    )


@pytest.mark.asyncio
async def test_queue_is_durable_and_never_starts_download(monkeypatch):
    async def forbidden(*args, **kwargs):
        raise AssertionError("Queued work must not perform HTTP requests")

    monkeypatch.setattr(jobs, "download_range", forbidden)
    queued = await jobs.submit_download_job("eurusd", START, END, run=False)
    restored = await jobs.get_download_job(queued.job_id)
    assert queued.state == restored.state == "queued"
    assert queued.pending == 2
    assert restored.completed == restored.failed == 0
    assert restored.complete is False
    assert restored.to_dict()["complete"] is False


@pytest.mark.asyncio
async def test_resume_uses_immutable_configuration_and_fixed_bounds(
    monkeypatch, tmp_path
):
    original_vault = CONFIG.base_directory
    queued = await jobs.submit_download_job("EURUSD", START, END, run=False)
    location = CONFIG.provider_state_directory
    seen = []

    async def download(symbol, start, end, proxies, **kwargs):
        seen.append((symbol, start, end, kwargs["settings"].base_directory))
        return report()

    reload_config(base_directory=tmp_path / "different", env_file=None)
    monkeypatch.setattr(jobs, "download_range", download)
    result = await jobs.resume_download_job(queued.job_id, state_path=location)
    assert seen == [("EURUSD", START, END, original_vault)]
    assert result.complete is True
    assert result.state == "complete"
    assert result.completed == 2
    assert result.pending == result.failed == 0
    assert await jobs.resume_download_job(queued.job_id, state_path=location) == result
    assert len(seen) == 1


@pytest.mark.asyncio
async def test_rate_limit_waiting_preserves_partial_counts_and_restart_deadline(
    monkeypatch,
):
    attempts = []
    error = FetchExhaustedError("Provider paused", 1, RateLimitError("HTTP 429", 3600))
    error.failure_report = report(downloaded=1, failed=1)

    async def download(*args, **kwargs):
        attempts.append(1)
        raise error

    monkeypatch.setattr(jobs, "download_range", download)
    waiting = await jobs.submit_download_job("EURUSD", START, END)
    restored = await jobs.get_download_job(waiting.job_id)
    assert restored.state == "waiting_for_provider"
    assert restored.complete is False
    assert restored.completed == restored.pending == restored.failed == 1
    assert restored.next_eligible_at >= error.next_eligible_at
    assert restored.report["downloaded"] == 1
    assert await jobs.resume_download_job(waiting.job_id) == restored
    assert len(attempts) == 1


@pytest.mark.asyncio
async def test_admission_deferred_is_durable_waiting_result(monkeypatch):
    deadline = jobs.time() + 300

    async def download(*args, **kwargs):
        raise ProviderDeferred("cooldown", deadline)

    monkeypatch.setattr(jobs, "download_range", download)
    result = await jobs.submit_download_job("EURUSD", START, END)
    assert result.state == "waiting_for_provider"
    assert result.next_eligible_at == deadline
    assert result.retry_reason == "cooldown"
    assert result.pending == 2


@pytest.mark.asyncio
async def test_unverified_access_requires_action_without_retry_deadline(monkeypatch):
    async def download(*args, **kwargs):
        error = ProviderDeferred("access_unverified", jobs.time())
        error.failure_report = replace(
            report(downloaded=0, failed=1), state="needs_action", complete=False
        )
        raise error

    monkeypatch.setattr(jobs, "download_range", download)
    result = await jobs.submit_download_job("EURUSD", START, END)
    assert result.state == "needs_action"
    assert result.retry_reason == "access_unverified"
    assert result.next_eligible_at is None
    assert result.complete is False
    assert result.pending == 2
    assert result.failed == 1
    assert "explicit feed probe" in result.error


@pytest.mark.asyncio
async def test_temporary_transport_failure_waits_for_network(monkeypatch):
    async def download(*args, **kwargs):
        raise FetchExhaustedError("Retries exhausted", 4, RetryableError("timeout"))

    monkeypatch.setattr(jobs, "download_range", download)
    result = await jobs.submit_download_job("EURUSD", START, END)
    assert result.state == "waiting_for_network"
    assert result.next_eligible_at > jobs.time()
    assert result.complete is False


@pytest.mark.parametrize(
    "error",
    [ForbiddenError("Access denied"), ValueError("Corrupt BI5"), OSError("disk full")],
)
@pytest.mark.asyncio
async def test_permanent_failure_needs_action(monkeypatch, error):
    async def download(*args, **kwargs):
        raise error

    monkeypatch.setattr(jobs, "download_range", download)
    result = await jobs.submit_download_job("EURUSD", START, END)
    assert result.state == "needs_action"
    assert result.complete is False
    assert result.pending == 2
    assert result.error


@pytest.mark.asyncio
async def test_cancellation_keeps_saved_progress_and_propagates(monkeypatch):
    progressed = asyncio.Event()

    async def download(*args, **kwargs):
        await kwargs["progress_callback"](
            DownloadProgress("EURUSD", 2, 1, 1, 0, 0, 0, 1, 20, 1.0, True)
        )
        progressed.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(jobs, "download_range", download)
    queued = await jobs.submit_download_job("EURUSD", START, END, run=False)
    task = asyncio.create_task(jobs.resume_download_job(queued.job_id))
    await progressed.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 1)
    paused = await jobs.get_download_job(queued.job_id)
    assert paused.state == "paused"
    assert paused.completed == paused.pending == 1
    assert paused.complete is False


@pytest.mark.asyncio
async def test_cooperative_stop_returns_paused_report(monkeypatch):
    async def download(*args, **kwargs):
        return report(downloaded=1, cancelled=True)

    monkeypatch.setattr(jobs, "download_range", download)
    paused = await jobs.submit_download_job("EURUSD", START, END)
    assert paused.state == "paused"
    assert paused.completed == paused.pending == 1
    assert paused.retry_reason == "stopped"


@pytest.mark.asyncio
async def test_competing_resumes_have_one_owner(monkeypatch):
    started = asyncio.Event()
    finish = asyncio.Event()
    calls = []

    async def download(*args, **kwargs):
        calls.append(1)
        started.set()
        await finish.wait()
        return report()

    monkeypatch.setattr(jobs, "download_range", download)
    queued = await jobs.submit_download_job("EURUSD", START, END, run=False)
    owner = asyncio.create_task(jobs.resume_download_job(queued.job_id))
    await started.wait()
    subscriber = await jobs.resume_download_job(queued.job_id)
    assert subscriber.state == "running"
    assert calls == [1]
    finish.set()
    assert (await owner).state == "complete"


@pytest.mark.asyncio
async def test_proxy_credentials_are_not_persisted_and_resume_requires_them(
    monkeypatch,
):
    proxy = "http://username:private-password@proxy.invalid:8080"
    queued = await jobs.submit_download_job("EURUSD", START, END, [proxy], run=False)
    serialized = str(queued.to_dict())
    assert "private-password" not in serialized
    assert "username" not in serialized
    assert queued.request["proxies"] == ["http://proxy.invalid:8080"]
    result = await jobs.resume_download_job(queued.job_id)
    assert result.state == "needs_action"
    assert result.retry_reason == "proxy_credentials_required"
    seen = []

    async def download(*args, **kwargs):
        seen.extend(args[3])
        return report()

    monkeypatch.setattr(jobs, "download_range", download)
    assert (await jobs.resume_download_job(queued.job_id, proxies=[proxy])).complete
    assert seen == [proxy]


@pytest.mark.asyncio
async def test_raw_transport_error_message_cannot_persist_credentials(monkeypatch):
    async def download(*args, **kwargs):
        error = RuntimeError(
            "https://username:private-password@proxy.invalid/?token=private"
        )
        error.failure_report = replace(
            report(downloaded=1, failed=1),
            error_type="RuntimeError",
            error=str(error),
        )
        raise error

    monkeypatch.setattr(jobs, "download_range", download)
    result = await jobs.submit_download_job("EURUSD", START, END)
    assert result.error == "RuntimeError"
    assert result.report["error"] == "RuntimeError"
    assert "private-password" not in str(result.to_dict())


@pytest.mark.asyncio
async def test_unknown_job_raises_without_requests():
    assert await jobs.get_download_job("missing") is None
    with pytest.raises(KeyError, match="Unknown download job"):
        await jobs.resume_download_job("missing")


@pytest.mark.asyncio
async def test_empty_complete_hour_range_finishes_without_downloader(monkeypatch):
    async def forbidden(*args, **kwargs):
        raise AssertionError("An empty range must not start a downloader")

    monkeypatch.setattr(jobs, "download_range", forbidden)
    future = datetime.now(UTC) + timedelta(hours=2)
    result = await jobs.submit_download_job(
        "EURUSD", future, future + timedelta(hours=1)
    )
    assert result.state == "complete"
    assert result.complete is True
    assert result.total == result.pending == result.completed == 0


@pytest.mark.asyncio
async def test_expired_run_cannot_replace_current_job_with_completion(monkeypatch):
    class RecoveredStore(JobStore):
        async def update(self, job_id, *, lease=None, **fields):
            if fields.get("state") == "complete":
                raise JobLeaseLost("The recovered owner holds this job")
            return await super().update(job_id, lease=lease, **fields)

    async def download(*args, **kwargs):
        return report()

    monkeypatch.setattr(jobs, "JobStore", RecoveredStore)
    monkeypatch.setattr(jobs, "download_range", download)
    result = await jobs.submit_download_job("EURUSD", START, END)
    assert result.state == "running"
    assert result.complete is False
    assert result.completed == 0
    assert result.pending == 2


@pytest.mark.asyncio
async def test_real_pipeline_retains_first_hour_and_cache_works_during_cooldown(
    monkeypatch,
):
    payload = lzma.compress(
        struct.pack(">IIIff", 0, 110020, 110000, 1.0, 1.0),
        format=lzma.FORMAT_ALONE,
    )
    calls = []

    def handle(request):
        assert request.url.host == "mock.invalid"
        calls.append(request.url.path)
        if len(calls) == 1:
            return httpx.Response(200, content=payload, request=request)
        return httpx.Response(429, headers={"Retry-After": "3600"}, request=request)

    transport = httpx.MockTransport(handle)
    client_type = httpx.AsyncClient

    def client(*args, **kwargs):
        return client_type(*args, transport=transport, **kwargs)

    monkeypatch.setattr(download_api, "AsyncClient", client)
    snapshot = CONFIG.model_copy(
        update={
            "datafeed_base_url": "https://mock.invalid/datafeed/",
            "worker_per_proxy": 1,
            "adaptive_workers": False,
            "fetch_max_retry_attempts": 0,
            "provider_request_interval": 0.0,
            "provider_admission_wait_budget": 0.0,
        }
    )
    waiting = await jobs.submit_download_job("EURUSD", START, END, settings=snapshot)
    assert waiting.state == "waiting_for_provider"
    assert waiting.complete is False
    assert waiting.completed == waiting.failed == waiting.pending == 1
    assert len(calls) == 2
    assert (
        await jobs.resume_download_job(waiting.job_id)
    ).state == "waiting_for_provider"
    assert len(calls) == 2
    cached = await jobs.submit_download_job(
        "EURUSD", START, START + timedelta(hours=1), settings=snapshot
    )
    assert cached.complete is True
    assert cached.report["cached"] == 1
    assert len(calls) == 2

"""Exercise shared control through the public APIs, never the live provider."""

import asyncio
import json
import lzma
import struct
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from tick_vault import cli
from tick_vault.config import Config, get_config, snapshot_config
from tick_vault.download import api, probe
from tick_vault.download.admission import provider_coordinator, source_key
from tick_vault.download.jobs import get_download_job, submit_download_job
from tick_vault.provider import ProviderDeferred

HOUR = datetime(2024, 1, 2, 12, tzinfo=UTC)


def payload():
    return lzma.compress(struct.pack(">IIIff", 100, 110020, 110000, 1.25, 2.5))


def settings(tmp_path, vault="first", **overrides):
    return Config(
        **(
            snapshot_config().model_dump()
            | {
                "base_directory": tmp_path / vault,
                "provider_state_directory": tmp_path / "shared",
                "provider_max_concurrency": 1,
                "provider_request_interval": 0,
                "provider_no_header_cooldown": 900,
                "provider_admission_wait_budget": 0.3,
                "worker_per_proxy": 1,
                "adaptive_workers": False,
                "fetch_max_retry_attempts": 0,
                "console_log_level": "ERROR",
            }
            | overrides
        ),
        _env_file=None,
    )


def mock_feed(monkeypatch, handler):
    original = httpx.AsyncClient

    def factory(*args, **kwargs):
        kwargs.pop("proxy", None)
        return original(*args, transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(api, "AsyncClient", factory)
    monkeypatch.setattr(probe, "AsyncClient", factory)


@pytest.mark.asyncio
async def test_overlapping_explicit_no_data_rechecks_share_the_new_result(
    tmp_path, monkeypatch
):
    calls = []

    async def handle(request):
        calls.append(request)
        await asyncio.sleep(0.04)
        return httpx.Response(404)

    mock_feed(monkeypatch, handle)
    first, second = settings(tmp_path, no_data_retry_after=0.0), settings(
        tmp_path, "second", no_data_retry_after=0.0
    )
    await api.download_range(
        "EURUSD", HOUR, HOUR + timedelta(hours=1), settings=first, show_progress=False
    )
    assert len(calls) == 1
    results = await asyncio.gather(
        *(
            api.download_range(
                "EURUSD",
                HOUR,
                HOUR + timedelta(hours=1),
                settings=captured,
                retry_no_data=True,
                show_progress=False,
            )
            for captured in (first, second)
        )
    )
    assert len(calls) == 2 and all(report.complete for report in results)
    assert sum(report.no_data for report in results) == 1
    assert sum(report.cached for report in results) == 1
    later = await api.download_range(
        "EURUSD",
        HOUR,
        HOUR + timedelta(hours=1),
        settings=first,
        retry_no_data=True,
        show_progress=False,
    )
    assert later.no_data == 1 and len(calls) == 3


@pytest.mark.asyncio
async def test_overlapping_vaults_share_one_hour_and_independent_configuration(
    tmp_path, monkeypatch
):
    calls = []

    async def handle(request):
        calls.append(request)
        await asyncio.sleep(0.03)
        return httpx.Response(200, content=payload())

    mock_feed(monkeypatch, handle)
    first, second = settings(tmp_path), settings(tmp_path, "second")
    reports = await asyncio.gather(
        *(
            api.download_range(
                "EURUSD",
                HOUR,
                HOUR + timedelta(hours=1),
                settings=captured,
                show_progress=False,
            )
            for captured in (first, second)
        )
    )
    assert len(calls) == 1
    assert all(report.complete and report.pending == 0 for report in reports)
    assert sum(report.downloaded for report in reports) == 1
    assert sum(report.cached for report in reports) == 1
    relative = "EURUSD/2024/00/02/12h_ticks.bi5"
    assert (first.save_directory / relative).read_bytes() == (
        second.save_directory / relative
    ).read_bytes()
    # A third consumer reuses the durable shared receipt without any request.
    third = await api.download_range(
        "EURUSD",
        HOUR,
        HOUR + timedelta(hours=1),
        settings=settings(tmp_path, "third"),
        show_progress=False,
    )
    assert third.cached == 1 and third.complete and len(calls) == 1


@pytest.mark.asyncio
async def test_rate_limit_is_shared_across_symbols_routes_vaults_and_later_calls(
    tmp_path, monkeypatch
):
    calls = []

    def handle(request):
        calls.append(request)
        return httpx.Response(429, headers={"Retry-After": "3600"})

    mock_feed(monkeypatch, handle)
    failures = await asyncio.gather(
        api.download_range(
            "EURUSD",
            HOUR,
            HOUR + timedelta(hours=1),
            proxies=["http://first.invalid"],
            settings=settings(tmp_path),
            show_progress=False,
        ),
        api.download_range(
            "GBPUSD",
            HOUR,
            HOUR + timedelta(hours=1),
            proxies=["http://second.invalid"],
            settings=settings(tmp_path, "second"),
            show_progress=False,
        ),
        return_exceptions=True,
    )
    assert len(calls) == 1
    assert all(isinstance(error, Exception) for error in failures)
    assert all(
        error.failure_report.state == "waiting_for_provider" for error in failures
    )
    assert all(not error.failure_report.complete for error in failures)
    with pytest.raises(ProviderDeferred) as deferred:
        await api.download_range(
            "USDJPY",
            HOUR,
            HOUR + timedelta(hours=1),
            settings=settings(tmp_path, "third"),
            show_progress=False,
        )
    assert deferred.value.next_eligible_at > datetime.now(UTC).timestamp() + 3500
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_bulk_access_gate_requires_a_nonempty_real_transport_payload(
    tmp_path, monkeypatch
):
    calls = []

    def handle(request):
        calls.append(request)
        return httpx.Response(200, content=payload())

    mock_feed(monkeypatch, handle)
    captured = settings(tmp_path, provider_require_verified_access=True)
    with pytest.raises(ProviderDeferred) as unverified:
        await api.download_range(
            "EURUSD",
            HOUR,
            HOUR + timedelta(hours=2),
            settings=captured,
            show_progress=False,
        )
    assert unverified.value.reason == "access_unverified" and calls == []
    assert unverified.value.failure_report.state == "needs_action"
    checked = await probe.probe_feed("EURUSD", HOUR, settings=captured)
    assert checked.complete and checked.requests == 1 and checked.tick_count == 1
    coordinator = provider_coordinator(captured)
    try:
        assert await coordinator.has_validated_access(source_key(captured))
        other = captured.model_copy(
            update={"datafeed_base_url": "https://other.invalid/"}
        )
        assert not await coordinator.has_validated_access(source_key(other))
    finally:
        await coordinator.close()
    result = await api.download_range(
        "EURUSD",
        HOUR,
        HOUR + timedelta(hours=2),
        settings=captured,
        show_progress=False,
    )
    assert (
        result.complete
        and result.cached == 1
        and result.downloaded == 1
        and len(calls) == 2
    )


@pytest.mark.asyncio
async def test_canary_records_no_header_cooldown_then_makes_zero_requests(
    tmp_path, monkeypatch
):
    calls = []

    def handle(request):
        calls.append(request)
        return httpx.Response(
            429,
            headers={
                "Server": "example",
                "Via": "http://user:secret@proxy.invalid/?token=hidden",
            },
        )

    mock_feed(monkeypatch, handle)
    captured = settings(tmp_path)
    first = await probe.probe_feed("EURUSD", HOUR, settings=captured)
    second = await probe.probe_feed("EURUSD", HOUR, settings=captured)
    assert first.state == second.state == "waiting_for_provider"
    assert not first.complete and not second.complete
    assert first.requests == 1 and second.requests == 0 and len(calls) == 1
    assert first.response.status_code == 429
    assert first.next_eligible_at > datetime.now(UTC) + timedelta(seconds=890)
    assert "secret" not in json.dumps(first.to_dict(), default=str)
    assert "hidden" not in json.dumps(first.to_dict(), default=str)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,content,error_type",
    [
        (418, b"", "HTTPStatusError"),
        (403, b"", "ForbiddenError"),
        (200, b"invalid compressed data", "ValueError"),
        (200, b"a" * 101, "ResponseTooLargeError"),
    ],
)
async def test_failed_canary_retains_status_and_never_establishes_access(
    tmp_path, monkeypatch, status, content, error_type
):
    mock_feed(monkeypatch, lambda request: httpx.Response(status, content=content))
    captured = settings(tmp_path, fetch_max_response_bytes=100)
    result = await probe.probe_feed("EURUSD", HOUR, settings=captured)
    assert result.state == "needs_action" and not result.complete
    assert result.requests == 1 and result.response.status_code == status
    assert result.error_type == error_type
    coordinator = provider_coordinator(captured)
    try:
        assert not await coordinator.has_validated_access(source_key(captured))
    finally:
        await coordinator.close()


def test_cli_failed_download_json_retains_state_and_progress_without_credentials(
    monkeypatch, capsys
):
    async def fail(*args, **kwargs):
        raise RuntimeError("https://user:private-password@proxy.invalid/?token=secret")

    monkeypatch.setattr(cli, "download_range", fail)
    assert (
        cli.main(
            [
                "download",
                "EURUSD",
                HOUR.isoformat(),
                "--end",
                (HOUR + timedelta(hours=1)).isoformat(),
                "--json",
            ]
        )
        == 1
    )
    captured = capsys.readouterr()
    result = json.loads(captured.out)
    assert result["state"] == "needs_action" and result["complete"] is False
    assert "private-password" not in captured.out + captured.err
    assert "secret" not in captured.out + captured.err


def test_cli_queued_job_status_provider_status_and_cached_resume(
    tmp_path, monkeypatch, capsys
):
    captured = settings(tmp_path, provider_require_verified_access=True)
    job = asyncio.run(
        submit_download_job(
            "EURUSD", HOUR, HOUR + timedelta(hours=2), settings=captured, run=False
        )
    )
    assert (
        cli.main(
            [
                "job-status",
                job.job_id,
                "--state-directory",
                str(captured.provider_state_directory),
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["state"] == "queued"
    mock_feed(
        monkeypatch,
        lambda request: (_ for _ in ()).throw(
            AssertionError("unverified resume must be local")
        ),
    )
    assert (
        cli.main(
            [
                "resume",
                job.job_id,
                "--state-directory",
                str(captured.provider_state_directory),
            ]
        )
        == 1
    )
    result = json.loads(capsys.readouterr().out)
    assert (
        result["state"] == "needs_action"
        and result["retry_reason"] == "access_unverified"
    )
    assert result["pending"] == 2 and not result["complete"]
    assert (
        asyncio.run(
            get_download_job(job.job_id, state_path=captured.provider_state_directory)
        ).state
        == "needs_action"
    )
    assert (
        cli.main(
            [
                "provider-status",
                "--state-directory",
                str(captured.provider_state_directory),
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["hour_used"] == 0


@pytest.mark.asyncio
async def test_convenience_reader_uses_the_original_vault_after_global_reload(
    tmp_path, monkeypatch
):
    import pandas as pd

    from tick_vault import api as convenience
    from tick_vault.config import CONFIG

    original = settings(tmp_path)
    observed = []

    async def download(*args, **kwargs):
        monkeypatch.setattr(CONFIG, "base_directory", tmp_path / "changed")
        from types import SimpleNamespace

        return SimpleNamespace(cancelled=False)

    def read(*args, **kwargs):
        observed.append(get_config().base_directory)
        return pd.DataFrame()

    monkeypatch.setattr(convenience, "download_range", download)
    monkeypatch.setattr(convenience, "read_tick_data", read)
    await convenience.download_and_read(
        "EURUSD",
        HOUR,
        HOUR + timedelta(hours=1),
        settings=original,
        show_progress=False,
    )
    assert observed == [original.base_directory]

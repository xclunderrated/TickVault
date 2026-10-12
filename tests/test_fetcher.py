import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace

import httpx
import pytest

from tick_vault.config import CONFIG, reload_config
from tick_vault.download import fetcher
from tick_vault.download.fetcher import (
    FetchExhaustedError,
    ForbiddenError,
    HTTPStatusError,
    RateLimitError,
    ResponseTooLargeError,
    RetryableError,
    _fetch,
    fetch_with_retry,
    parse_retry_after,
)


@pytest.fixture(autouse=True)
def restore_config(tmp_path):
    original = CONFIG.model_dump()
    reload_config(base_directory=tmp_path, env_file=None)
    yield
    reload_config(**original)


def test_retry_after_zero_http_date_invalid_and_expired():
    now = datetime(2024, 1, 1, tzinfo=UTC)
    assert parse_retry_after("0", now=now) == 0
    assert parse_retry_after("120", now=now) == 120
    assert parse_retry_after("Mon, 01 Jan 2024 00:02:00 GMT", now=now) == 120
    assert parse_retry_after("Sun, 31 Dec 2023 23:59:00 GMT", now=now) == 0
    assert parse_retry_after("invalid", now=now) is None
    assert parse_retry_after("-1", now=now) is None
    assert parse_retry_after("inf", now=now) is None


@pytest.mark.parametrize(
    "status,error",
    [
        (401, ForbiddenError),
        (403, ForbiddenError),
        (451, ForbiddenError),
        (400, HTTPStatusError),
        (302, HTTPStatusError),
        (500, RetryableError),
        (408, RetryableError),
    ],
)
@pytest.mark.asyncio
async def test_http_errors_remain_typed(status, error):
    transport = httpx.MockTransport(lambda req: httpx.Response(status, request=req))
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(error):
            await _fetch(client, "https://feed.invalid/hour.bi5")


@pytest.mark.parametrize("status", [429, 503])
@pytest.mark.asyncio
async def test_rate_limit_header_does_not_get_wrapped(status):
    transport = httpx.MockTransport(
        lambda req: httpx.Response(status, headers={"Retry-After": "0"}, request=req)
    )
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(RateLimitError) as caught:
            await _fetch(client, "https://feed.invalid/hour.bi5")
    assert caught.value.retry_after == 0


@pytest.mark.asyncio
async def test_retry_count_and_zero_server_delay(monkeypatch):
    calls = []
    delays = []

    def handle(request):
        calls.append(request)
        if len(calls) < 3:
            return httpx.Response(429, headers={"Retry-After": "0"}, request=request)
        return httpx.Response(200, content=b"ticks", request=request)

    async def sleep(delay, event):
        delays.append(delay)

    reload_config(fetch_max_retry_attempts=2, fetch_base_retry_delay=10, env_file=None)
    monkeypatch.setattr(fetcher, "_sleep_or_stop", sleep)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        assert (
            await fetch_with_retry(client, "https://feed.invalid/hour.bi5") == b"ticks"
        )
    assert len(calls) == 3
    assert delays == [0, 0]


@pytest.mark.asyncio
async def test_retries_exhaust_with_original_category(monkeypatch):
    calls = []

    def handle(request):
        calls.append(request)
        return httpx.Response(503, request=request)

    async def sleep(delay, event):
        return None

    reload_config(fetch_max_retry_attempts=2, env_file=None)
    monkeypatch.setattr(fetcher, "_sleep_or_stop", sleep)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(FetchExhaustedError) as caught:
            await fetch_with_retry(client, "https://feed.invalid/hour.bi5")
    assert len(calls) == caught.value.attempts == 3
    assert isinstance(caught.value.last_error, RetryableError)


@pytest.mark.asyncio
async def test_long_retry_after_surfaces_without_early_retry():
    calls = []

    def handle(request):
        calls.append(request)
        return httpx.Response(429, headers={"Retry-After": "3600"}, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(FetchExhaustedError) as caught:
            await asyncio.wait_for(
                fetch_with_retry(client, "https://feed.invalid/hour.bi5"), 1
            )
    assert len(calls) == 1
    assert caught.value.retry_after == 3600
    assert isinstance(caught.value.last_error, RateLimitError)


@pytest.mark.asyncio
async def test_forbidden_never_retried_and_url_credentials_redacted():
    calls = []

    def handle(request):
        calls.append(request)
        return httpx.Response(403, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(ForbiddenError) as caught:
            await fetch_with_retry(
                client, "https://user:secret@feed.invalid/hour.bi5?token=private"
            )
    assert len(calls) == 1
    assert "secret" not in str(caught.value)
    assert "private" not in str(caught.value)


@pytest.mark.asyncio
async def test_whole_request_deadline_bounds_a_hanging_transport():
    async def handle(request):
        await asyncio.Event().wait()

    reload_config(fetch_timeout=0.02, env_file=None)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(RetryableError, match="deadline"):
            await asyncio.wait_for(_fetch(client, "https://feed.invalid/hour.bi5"), 1)


@pytest.mark.asyncio
async def test_stop_interrupts_retry_wait():
    stop = asyncio.Event()
    requested = asyncio.Event()

    def handle(request):
        requested.set()
        return httpx.Response(429, headers={"Retry-After": "30"}, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        task = asyncio.create_task(
            fetch_with_retry(client, "https://feed.invalid/hour.bi5", stop_event=stop)
        )
        await requested.wait()
        stop.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 1)


class ControlledStream(httpx.AsyncByteStream):
    def __init__(self, chunks=(), *, stall=False, close_error=None):
        self.chunks = chunks
        self.stall = stall
        self.close_error = close_error
        self.reads = 0
        self.closed = False
        self.started = asyncio.Event()

    async def __aiter__(self):
        self.started.set()
        self.reads += 1
        if self.stall:
            await asyncio.Event().wait()
        for chunk in self.chunks:
            yield chunk

    async def aclose(self):
        self.closed = True
        if self.close_error:
            raise self.close_error


def test_http_date_retry_after_uses_server_clock():
    local = datetime(2024, 1, 1, 12, tzinfo=UTC)
    assert (
        parse_retry_after(
            "Mon, 01 Jan 2024 10:02:00 GMT",
            now=local,
            server_date="Mon, 01 Jan 2024 10:00:00 GMT",
        )
        == 120
    )
    assert parse_retry_after("17", now=local, server_date="invalid") == 17
    assert (
        parse_retry_after(
            "Mon, 01 Jan 2024 12:01:00 GMT", now=local, server_date="invalid"
        )
        == 60
    )


@pytest.mark.parametrize("status", [429, 503])
@pytest.mark.parametrize("broken_close", [False, True])
@pytest.mark.asyncio
async def test_rate_limit_is_persisted_without_reading_stalled_body(
    status, broken_close
):
    stream = ControlledStream(
        stall=True,
        close_error=(
            httpx.ReadError("secret transport detail") if broken_close else None
        ),
    )
    persisted = []

    async def record(error):
        assert stream.reads == 0
        persisted.append(error.retry_after)

    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            status,
            headers={"Retry-After": "3600"},
            stream=stream,
            request=request,
        )
    )
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(RateLimitError) as caught:
            await asyncio.wait_for(
                _fetch(client, "https://feed.invalid/hour.bi5", on_rate_limit=record),
                1,
            )
    assert persisted == [3600]
    assert caught.value.retry_after == 3600
    assert stream.reads == 0
    assert stream.closed


@pytest.mark.parametrize("broken_close", [False, True])
@pytest.mark.asyncio
async def test_rate_limit_persistence_failure_remains_actionable(broken_close):
    stream = ControlledStream(
        stall=True,
        close_error=httpx.ReadError("close failure") if broken_close else None,
    )
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            429, headers={"Retry-After": "3600"}, stream=stream, request=request
        )
    )

    async def record(error):
        raise OSError("Durable cooldown could not be saved")

    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(OSError, match="Durable cooldown"):
            await _fetch(client, "https://feed.invalid/hour.bi5", on_rate_limit=record)
    assert stream.reads == 0
    assert stream.closed


@pytest.mark.asyncio
async def test_final_attempt_records_cooldown_and_releases_admission():
    reload_config(fetch_max_retry_attempts=0, env_file=None)
    events = []

    async def acquire():
        events.append("acquire")

    async def release():
        events.append("release")

    async def record(error):
        events.append(("cooldown", error.retry_after))

    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            429, headers={"Retry-After": "3600"}, request=request
        )
    )
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(FetchExhaustedError) as caught:
            await fetch_with_retry(
                client,
                "https://feed.invalid/hour.bi5",
                before_attempt=acquire,
                after_attempt=release,
                on_rate_limit=record,
            )
    assert events == ["acquire", ("cooldown", 3600), "release"]
    assert caught.value.next_eligible_at is not None


@pytest.mark.asyncio
async def test_durable_cooldown_can_extend_but_not_shorten_header(monkeypatch):
    sleeps = []

    async def sleep(delay, event):
        sleeps.append(delay)

    async def record(error):
        return 2.0

    responses = [429, 200]
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            responses.pop(0),
            headers={"Retry-After": "10"},
            content=b"ticks",
            request=request,
        )
    )
    monkeypatch.setattr(fetcher, "_sleep_or_stop", sleep)
    async with httpx.AsyncClient(transport=transport) as client:
        assert (
            await fetch_with_retry(
                client, "https://feed.invalid/hour.bi5", on_rate_limit=record
            )
            == b"ticks"
        )
    assert sleeps == [10]


@pytest.mark.asyncio
async def test_missing_header_uses_durable_cooldown_without_early_retry():
    calls = []
    deadline = fetcher.time() + 900

    async def record(error):
        return SimpleNamespace(next_eligible_at=deadline)

    def handle(request):
        calls.append(request)
        return httpx.Response(429, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(FetchExhaustedError) as caught:
            await fetch_with_retry(
                client, "https://feed.invalid/hour.bi5", on_rate_limit=record
            )
    assert len(calls) == 1
    assert caught.value.next_eligible_at == deadline
    assert caught.value.retry_after > 899


@pytest.mark.asyncio
async def test_streamed_success_hard_size_limit_and_close():
    stream = ControlledStream([b"a" * 65536, b"b" * 65536])
    settings = CONFIG.model_copy(update={"fetch_max_response_bytes": 65536})
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, stream=stream, request=request)
    )
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(ResponseTooLargeError):
            await _fetch(client, "https://feed.invalid/hour.bi5", settings=settings)
    assert stream.closed


@pytest.mark.asyncio
async def test_declared_oversized_success_body_is_not_read():
    stream = ControlledStream(stall=True)
    settings = CONFIG.model_copy(update={"fetch_max_response_bytes": 10})
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200, headers={"Content-Length": "11"}, stream=stream, request=request
        )
    )
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(ResponseTooLargeError):
            await _fetch(client, "https://feed.invalid/hour.bi5", settings=settings)
    assert stream.reads == 0
    assert stream.closed


@pytest.mark.asyncio
async def test_success_body_deadline_and_cooperative_stop_close_stream():
    for cooperative in (False, True):
        stream = ControlledStream(stall=True)
        stop = asyncio.Event() if cooperative else None
        settings = CONFIG.model_copy(update={"fetch_timeout": 0.03})
        transport = httpx.MockTransport(
            lambda request, current_stream=stream: httpx.Response(
                200, stream=current_stream, request=request
            )
        )
        async with httpx.AsyncClient(transport=transport) as client:
            task = asyncio.create_task(
                _fetch(
                    client,
                    "https://feed.invalid/hour.bi5",
                    settings=settings,
                    stop_event=stop,
                )
            )
            await stream.started.wait()
            if stop is not None:
                stop.set()
            with pytest.raises(
                asyncio.CancelledError if cooperative else RetryableError
            ):
                await asyncio.wait_for(task, 1)
        assert stream.closed


@pytest.mark.asyncio
async def test_job_settings_do_not_change_during_retry(monkeypatch):
    reload_config(fetch_max_retry_attempts=1, env_file=None)
    calls = []

    def handle(request):
        calls.append(request)
        return httpx.Response(503, request=request)

    async def sleep(delay, event):
        reload_config(fetch_max_retry_attempts=0, fetch_timeout=0.001, env_file=None)

    monkeypatch.setattr(fetcher, "_sleep_or_stop", sleep)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(FetchExhaustedError) as caught:
            await fetch_with_retry(client, "https://feed.invalid/hour.bi5")
    assert caught.value.attempts == len(calls) == 2


@pytest.mark.asyncio
async def test_structured_details_are_bounded_and_redacted():
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            429,
            headers={
                "Retry-After": "1",
                "Server": "https://user:secret@proxy.invalid/path?token=private",
                "Via": "proxy " + "x" * 1000,
            },
            request=request,
        )
    )
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(RateLimitError) as caught:
            await _fetch(
                client, "https://user:secret@feed.invalid/hour.bi5?token=private"
            )
    details = caught.value.details.to_dict()
    assert details["status_code"] == 429
    assert "secret" not in str(details)
    assert "private" not in str(details)
    assert len(details["via"]) <= 256

"""Finite HTTP retries that preserve provider errors and respect rate limits."""

import asyncio
import inspect
import math
import re
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from time import monotonic, time
from urllib.parse import urlsplit, urlunsplit

from httpx import AsyncClient, RequestError

from ..config import CONFIG, Config
from ..logger import logger
from .models import FetchObservation


@dataclass(frozen=True)
class ResponseDetails:
    """Bounded response diagnostics without request credentials or bodies."""

    status_code: int
    url: str
    date: str | None = None
    retry_after: str | None = None
    content_type: str | None = None
    server: str | None = None
    via: str | None = None

    def to_dict(self) -> dict[str, str | int | None]:
        return asdict(self)


class FetchError(Exception):
    """Base exception for HTTP and transport failures."""

    def __init__(self, message: str, *, details: ResponseDetails | None = None):
        super().__init__(message)
        self.details = details


class RateLimitError(FetchError):
    """The provider has requested a pause (429 or 503 with Retry-After)."""

    def __init__(
        self,
        message: str,
        retry_after: float | None = None,
        *,
        details: ResponseDetails | None = None,
    ):
        super().__init__(message, details=details)
        self.retry_after = retry_after
        self.next_eligible_at = (
            time() + retry_after if retry_after is not None else None
        )


class ForbiddenError(FetchError):
    """Access was denied. Changing proxies or endpoints is not a remedy."""


class RetryableError(FetchError):
    """A transient transport or server failure."""


class ResponseTooLargeError(FetchError):
    """A successful response exceeded the configured compressed payload limit."""


class HTTPStatusError(FetchError):
    """An unexpected, non-retryable HTTP response."""

    def __init__(
        self,
        message: str,
        status_code: int,
        *,
        details: ResponseDetails | None = None,
    ):
        super().__init__(message, details=details)
        self.status_code = status_code


class FetchExhaustedError(FetchError):
    """Retry count or permitted wait time was exhausted, retaining its cause."""

    def __init__(self, message: str, attempts: int, last_error: FetchError):
        super().__init__(message, details=last_error.details)
        self.attempts = attempts
        self.last_error = last_error
        self.retry_after = getattr(last_error, "retry_after", None)
        self.next_eligible_at = getattr(last_error, "next_eligible_at", None)


def safe_url(url: str) -> str:
    """Remove credentials and queries before a URL enters a log or exception."""
    try:
        parts = urlsplit(url)
        host = parts.hostname or "unknown-host"
        if ":" in host:
            host = f"[{host}]"
        if parts.port is not None:
            host += f":{parts.port}"
        return urlunsplit((parts.scheme, host, parts.path, "", ""))
    except ValueError:
        return "[invalid URL]"


def parse_retry_after(
    value: str | None,
    *,
    now: datetime | None = None,
    server_date: str | None = None,
) -> float | None:
    """Parse seconds or dates, using server Date to account for clock skew."""
    if value is None:
        return None
    value = value.strip()
    try:
        seconds = float(value)
        if math.isfinite(seconds) and seconds >= 0:
            return seconds
        return None
    except ValueError:
        pass
    try:
        deadline = parsedate_to_datetime(value)
        if deadline.tzinfo is None:
            deadline = deadline.replace(tzinfo=UTC)
        current = now or datetime.now(UTC)
        if server_date:
            try:
                current = parsedate_to_datetime(server_date)
            except (TypeError, ValueError, OverflowError):
                pass
        if current.tzinfo is None:
            current = current.replace(tzinfo=UTC)
        return max(0.0, (deadline - current).total_seconds())
    except (TypeError, ValueError, OverflowError):
        return None


RateLimitCallback = Callable[[RateLimitError], Awaitable[object] | object]


def _safe_header(value: str | None) -> str | None:
    if value is None:
        return None
    value = " ".join(value.split())[:256]
    value = re.sub(r"https?://[^\s,;]+", lambda match: safe_url(match[0]), value)
    value = re.sub(r"[^\s/:,;]+:[^\s/@,;]+@", "[redacted]@", value)
    return re.sub(r"([?&][^=\s]+)=([^\s&,;]+)", r"\1=[redacted]", value)


async def _record_rate_limit(
    error: RateLimitError, callback: RateLimitCallback | None
) -> None:
    if callback is None:
        return
    result = callback(error)
    if inspect.isawaitable(result):
        result = await result
    deadline = getattr(result, "next_eligible_at", None)
    if isinstance(deadline, datetime):
        deadline = deadline.timestamp()
    if isinstance(deadline, int | float) and math.isfinite(deadline):
        error.next_eligible_at = max(error.next_eligible_at or deadline, deadline)
    delay = result if isinstance(result, int | float) else None
    if delay is not None and math.isfinite(delay) and delay >= 0:
        callback_deadline = time() + delay
        error.next_eligible_at = max(
            error.next_eligible_at or callback_deadline, callback_deadline
        )
    if error.next_eligible_at is not None:
        delay = max(float(delay or 0), error.next_eligible_at - time())
    if delay is not None and math.isfinite(delay) and delay >= 0:
        error.retry_after = max(error.retry_after or 0.0, delay)


async def _fetch(
    client: AsyncClient,
    url: str,
    *,
    settings: Config | None = None,
    stop_event: asyncio.Event | None = None,
    on_rate_limit: RateLimitCallback | None = None,
) -> bytes | None:
    """Inspect headers first, then read a bounded BI5 response under one deadline.

    Rate-limit persistence happens before the response body or stream cleanup.
    Error bodies are never consumed. Request HTTP identity encoding so the size
    limit applies directly to the compressed BI5 bytes retained in the vault.
    """
    settings = settings or CONFIG
    if stop_event is not None:
        if stop_event.is_set():
            raise asyncio.CancelledError
        return await _await_or_stop(
            _fetch(client, url, settings=settings, on_rate_limit=on_rate_limit),
            stop_event,
        )
    public_url = safe_url(url)
    classified: FetchError | None = None
    persistence_failure: Exception | None = None
    details: ResponseDetails | None = None
    try:
        async with asyncio.timeout(settings.fetch_timeout):
            async with client.stream(
                "GET",
                url,
                timeout=settings.fetch_timeout,
                headers={"Accept-Encoding": "identity"},
            ) as response:
                status = response.status_code
                details = ResponseDetails(
                    status,
                    safe_url(str(response.url)),
                    _safe_header(response.headers.get("Date")),
                    _safe_header(response.headers.get("Retry-After")),
                    _safe_header(response.headers.get("Content-Type")),
                    _safe_header(response.headers.get("Server")),
                    _safe_header(response.headers.get("Via")),
                )
                if status == 429 or (
                    status == 503 and "Retry-After" in response.headers
                ):
                    classified = RateLimitError(
                        f"Provider requested a pause (HTTP {status}) for {public_url}",
                        parse_retry_after(
                            response.headers.get("Retry-After"),
                            server_date=response.headers.get("Date"),
                        ),
                        details=details,
                    )
                    try:
                        await _record_rate_limit(classified, on_rate_limit)
                    except Exception as error:
                        # A failed durable write must remain actionable. Stream
                        # cleanup may fail too, so preserve the persistence cause
                        # separately from the observed provider response.
                        persistence_failure = error
                        classified = None
                        raise
                    raise classified
                if status in (204, 404):
                    return None
                if status in (401, 403, 451):
                    classified = ForbiddenError(
                        f"Access denied (HTTP {status}) for {public_url}",
                        details=details,
                    )
                elif status == 408 or 500 <= status < 600:
                    classified = RetryableError(
                        f"Server error (HTTP {status}) for {public_url}",
                        details=details,
                    )
                elif status != 200:
                    classified = HTTPStatusError(
                        f"Unexpected HTTP {status} for {public_url}",
                        status,
                        details=details,
                    )
                if classified is not None:
                    raise classified
                if response.headers.get("Content-Encoding", "identity").lower() not in (
                    "identity",
                    "",
                ):
                    classified = HTTPStatusError(
                        f"Unsupported HTTP content encoding for {public_url}",
                        status,
                        details=details,
                    )
                    raise classified
                limit = settings.fetch_max_response_bytes
                size_header = response.headers.get("Content-Length")
                if size_header and size_header.isdecimal() and int(size_header) > limit:
                    classified = ResponseTooLargeError(
                        f"Response exceeds {limit} compressed bytes for {public_url}",
                        details=details,
                    )
                    raise classified
                content = bytearray()
                # Mock transports may supply an already-buffered response; real
                # streaming responses use bounded raw chunks without HTTP decoding.
                if response.is_stream_consumed:
                    if len(response.content) > limit:
                        classified = ResponseTooLargeError(
                            f"Response exceeds {limit} compressed bytes for {public_url}",
                            details=details,
                        )
                        raise classified
                    return response.content or None
                async for chunk in response.aiter_raw(chunk_size=65536):
                    if len(content) + len(chunk) > limit:
                        classified = ResponseTooLargeError(
                            f"Response exceeds {limit} compressed bytes for {public_url}",
                            details=details,
                        )
                        raise classified
                    content.extend(chunk)
                return bytes(content) or None
    except TimeoutError:
        if persistence_failure is not None:
            raise persistence_failure from None
        if classified is not None:
            raise classified from None
        raise RetryableError(
            f"Request deadline exceeded for {public_url}", details=details
        ) from None
    except RequestError as exc:
        if persistence_failure is not None:
            raise persistence_failure from None
        if classified is not None:
            raise classified from None
        # httpx exception text and request URLs can contain proxy credentials.
        raise RetryableError(
            f"{type(exc).__name__} while fetching {public_url}", details=details
        ) from None
    except Exception:
        if persistence_failure is not None:
            raise persistence_failure from None
        if classified is not None:
            raise classified from None
        raise


async def _await_or_stop(
    operation: Awaitable[bytes | None], stop_event: asyncio.Event
) -> bytes | None:
    request = asyncio.ensure_future(operation)
    stop = asyncio.create_task(stop_event.wait())
    try:
        await asyncio.wait((request, stop), return_when=asyncio.FIRST_COMPLETED)
        if stop_event.is_set():
            raise asyncio.CancelledError
        return await request
    finally:
        for task in (request, stop):
            if not task.done():
                task.cancel()
        await asyncio.gather(request, stop, return_exceptions=True)


async def _sleep_or_stop(delay: float, stop_event: asyncio.Event | None) -> None:
    if stop_event is None:
        await asyncio.sleep(delay)
        return
    if stop_event.is_set():
        raise asyncio.CancelledError
    try:
        async with asyncio.timeout(delay):
            await stop_event.wait()
    except TimeoutError:
        return
    raise asyncio.CancelledError


async def fetch_with_retry(
    client: AsyncClient,
    url: str,
    *,
    settings: Config | None = None,
    stop_event: asyncio.Event | None = None,
    observer: Callable[[FetchObservation], Awaitable[None] | None] | None = None,
    before_attempt: Callable[[], Awaitable[None]] | None = None,
    after_attempt: Callable[[], Awaitable[None]] | None = None,
    on_rate_limit: RateLimitCallback | None = None,
) -> bytes | None:
    """Fetch with finite attempts, finite requests, and provider-wide cooldown hooks.

    ``fetch_max_retry_attempts`` excludes the initial attempt. A server delay
    above ``fetch_max_retry_delay`` is surfaced immediately; it is never shortened
    to send an early retry. External cancellation is propagated unchanged.
    """
    settings = (settings or CONFIG).model_copy(deep=True)
    retries = settings.fetch_max_retry_attempts
    max_delay = settings.fetch_max_retry_delay
    public_url = safe_url(url)
    for attempt in range(retries + 1):
        if stop_event is not None and stop_event.is_set():
            raise asyncio.CancelledError
        if before_attempt is not None:
            await before_attempt()
        started = monotonic()
        error: FetchError | None = None
        content: bytes | None = None
        delay = 0.0
        try:
            try:
                content = await _fetch(
                    client,
                    url,
                    settings=settings,
                    stop_event=stop_event,
                    on_rate_limit=on_rate_limit,
                )
            except FetchError as exc:
                error = exc
                if isinstance(exc, RateLimitError | RetryableError):
                    backoff = min(
                        max_delay, settings.fetch_base_retry_delay * 2**attempt
                    )
                    delay = (
                        exc.retry_after
                        if isinstance(exc, RateLimitError)
                        and exc.retry_after is not None
                        else backoff
                    )

            if observer is not None:
                observation = FetchObservation(
                    elapsed_seconds=monotonic() - started,
                    bytes_downloaded=len(content) if content is not None else 0,
                    error=type(error).__name__ if error is not None else None,
                    retry_delay=delay,
                )
                observed = observer(observation)
                if inspect.isawaitable(observed):
                    await observed
        finally:
            if after_attempt is not None:
                await after_attempt()

        if error is None:
            return content
        if not isinstance(error, RateLimitError | RetryableError):
            raise error
        if attempt == retries or delay > max_delay:
            reason = (
                "retry count exhausted"
                if attempt == retries
                else "provider delay exceeds wait budget"
            )
            raise FetchExhaustedError(
                f"Fetching {public_url} stopped after {attempt + 1} attempts: {reason}",
                attempt + 1,
                error,
            ) from error
        logger.info("Retrying after %.1fs (%d/%d)", delay, attempt + 1, retries)
        await _sleep_or_stop(delay, stop_event)
    raise AssertionError("Retry loop cannot finish without a result")

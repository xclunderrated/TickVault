"""One explicit, quota-governed real-payload access canary."""

import asyncio
import sqlite3
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from httpx import AsyncClient, Response

from ..chunk import TickChunk
from ..config import Config, snapshot_config, use_config
from ..core.asyncutils import run_blocking
from ..core.binary import validate_payload
from ..metadata import MetadataDB, SourceIdentityError
from ..provider import ProviderDeferred, ProviderStatus
from ..utils import hour_floor, normalize_datetime
from .admission import provider_coordinator, source_key
from .fetcher import (
    FetchError,
    ForbiddenError,
    RateLimitError,
    ResponseDetails,
    RetryableError,
    _fetch,
    _safe_header,
    safe_url,
)


@dataclass(frozen=True)
class FeedProbe:
    symbol: str
    hour: datetime
    state: str
    complete: bool
    requests: int = 0
    tick_count: int = 0
    compressed_bytes: int = 0
    response: ResponseDetails | None = None
    next_eligible_at: datetime | None = None
    error_type: str | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


async def probe_feed(
    symbol: str,
    hour: datetime,
    *,
    proxy: str | None = None,
    settings: Config | None = None,
    state_path: str | Path | None = None,
) -> FeedProbe:
    """Make at most one GET of one aligned complete historical hour.

    A received nonempty validated payload is saved and records source-scoped
    access evidence. A stored restriction prevents the GET. No automatic retry,
    proxy rotation or endpoint substitution is performed.
    """
    captured = (settings or snapshot_config()).model_copy(deep=True)
    hour = normalize_datetime(hour)
    if hour != hour_floor(hour) or hour >= hour_floor(datetime.now(UTC)):
        raise ValueError("canary needs an aligned complete historical UTC hour")
    with use_config(captured):
        chunk = TickChunk(symbol=symbol, time=hour)
        provider = provider_coordinator(captured, state_path)
        details: ResponseDetails | None = None
        requests = 0

        async def received(response: Response) -> None:
            nonlocal details
            details = ResponseDetails(
                response.status_code,
                safe_url(str(response.url)),
                _safe_header(response.headers.get("Date")),
                _safe_header(response.headers.get("Retry-After")),
                _safe_header(response.headers.get("Content-Type")),
                _safe_header(response.headers.get("Server")),
                _safe_header(response.headers.get("Via")),
            )

        async def throttled(error: RateLimitError) -> ProviderStatus:
            return await provider.observe_rate_limit(error.retry_after)

        try:

            def bind_source() -> None:
                with MetadataDB(captured.metadata_db_path) as source_db:
                    source_db.bind_source(
                        source_key(captured), raw_directory=captured.save_directory
                    )

            await asyncio.to_thread(bind_source)
            lease = await provider.acquire(wait=False)
            async with lease:
                async with AsyncClient(
                    proxy=proxy,
                    timeout=captured.fetch_timeout,
                    event_hooks={"response": [received]},
                ) as client:
                    requests = 1
                    content = await _fetch(
                        client, chunk.url, settings=captured, on_rate_limit=throttled
                    )
                raw = (
                    await run_blocking(validate_payload, content)
                    if content is not None
                    else b""
                )
                await provider.observe_success(lease)
                if not raw or content is None:
                    return FeedProbe(
                        chunk.symbol,
                        hour,
                        "needs_action",
                        False,
                        requests=requests,
                        response=details,
                        error="No nonempty tick payload; access is not verified",
                    )
                await run_blocking(chunk.save, content, captured.save_directory)

                def record() -> None:
                    with MetadataDB(captured.metadata_db_path) as db:
                        db.record_result(chunk, "downloaded")

                await run_blocking(record)
                await provider.record_validated_payload(
                    chunk.symbol, hour, len(raw) // 20, source_key=source_key(captured)
                )
                return FeedProbe(
                    chunk.symbol,
                    hour,
                    "complete",
                    True,
                    requests,
                    len(raw) // 20,
                    len(content),
                    details,
                )
        except ProviderDeferred as error:
            return FeedProbe(
                chunk.symbol,
                hour,
                "waiting_for_provider",
                False,
                requests=requests,
                response=details,
                next_eligible_at=datetime.fromtimestamp(error.next_eligible_at, UTC),
                error_type=type(error).__name__,
                error=error.reason,
            )
        except RateLimitError as error:
            deadline = error.next_eligible_at
            return FeedProbe(
                chunk.symbol,
                hour,
                "waiting_for_provider",
                False,
                requests=requests,
                response=details,
                next_eligible_at=(
                    datetime.fromtimestamp(deadline, UTC) if deadline else None
                ),
                error_type=type(error).__name__,
                error=str(error),
            )
        except RetryableError as error:
            return FeedProbe(
                chunk.symbol,
                hour,
                "waiting_for_network",
                False,
                requests=requests,
                response=details,
                error_type=type(error).__name__,
                error=str(error),
            )
        except (ForbiddenError, OSError, ValueError, sqlite3.Error) as error:
            return FeedProbe(
                chunk.symbol,
                hour,
                "needs_action",
                False,
                requests=requests,
                response=details,
                error_type=type(error).__name__,
                error=(
                    "This vault belongs to another feed; use a separate base directory"
                    if isinstance(error, SourceIdentityError)
                    else type(error).__name__
                ),
            )
        except FetchError as error:
            return FeedProbe(
                chunk.symbol,
                hour,
                "needs_action",
                False,
                requests=requests,
                response=details,
                error_type=type(error).__name__,
                error=type(error).__name__,
            )
        finally:
            await provider.close()

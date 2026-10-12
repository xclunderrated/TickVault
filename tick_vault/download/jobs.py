"""Optional durable download jobs layered over the strict download API."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable
from dataclasses import asdict, is_dataclass
from datetime import datetime
from pathlib import Path
from time import time
from typing import Any
from urllib.parse import urlsplit

from ..config import Config
from ..core.config import get_config
from ..provider import ProviderDeferred
from ..provider.jobs import JobLeaseLost, JobStore, ManagedJob
from ..utils import normalize_symbol
from .api import download_bounds, download_range
from .fetcher import (
    FetchError,
    FetchExhaustedError,
    RateLimitError,
    RetryableError,
    safe_url,
)
from .models import DownloadProgress, DownloadReport

ProgressCallback = Callable[[DownloadProgress], Awaitable[None] | None]


def _json_value(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        value = asdict(value)
    if isinstance(value, datetime | Path):
        return value.isoformat() if isinstance(value, datetime) else str(value)
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json_value(item) for item in value]
    return value


def _deadline(value: Any) -> float | None:
    if value is None:
        return None
    return value.timestamp() if isinstance(value, datetime) else float(value)


def _report_fields(
    report: DownloadReport | DownloadProgress | None, current: ManagedJob
) -> dict[str, Any]:
    if report is None:
        return {}
    # Failed receipts are pending work, rather than successful completion.
    completed = max(
        current.completed, report.downloaded + report.no_data + report.cached
    )
    completed = min(current.total, completed)
    pending = current.total - completed
    serialized = _json_value(report)
    if serialized.get("error"):
        # A strict report may retain an arbitrary callback/transport message.
        # The managed job's sanitized HTTP error carries the actionable detail.
        serialized["error"] = serialized.get("error_type") or "DownloadError"
    return {
        "completed": completed,
        "pending": pending,
        "failed": min(report.failed, pending),
        "report": serialized,
    }


async def get_download_job(
    job_id: str, *, state_path: Path | str | None = None
) -> ManagedJob | None:
    """Read durable status without starting requests or modifying the job."""
    location = (
        state_path if state_path is not None else get_config().provider_state_directory
    )
    return await JobStore(location).get(job_id)


async def submit_download_job(
    symbol: str,
    start: datetime,
    end: datetime | None = None,
    proxies: list[str] | None = None,
    *,
    settings: Config | None = None,
    state_path: Path | str | None = None,
    job_id: str | None = None,
    run: bool = True,
    stop_event: asyncio.Event | None = None,
    progress_callback: ProgressCallback | None = None,
    show_progress: bool = False,
    retry_no_data: bool = False,
) -> ManagedJob:
    """Save an immutable request and optionally make one bounded run now.

    Deferred provider/network work returns an explicitly incomplete durable job.
    ``run=False`` queues work without HTTP requests. Proxy credentials remain in
    memory only; pass them explicitly when resuming a job that requires them.
    """
    snapshot = (settings or get_config()).model_copy(deep=True)
    # Durable jobs may resume from another shell or scheduler working directory.
    snapshot.base_directory = snapshot.base_directory.resolve()
    location = (
        state_path if state_path is not None else snapshot.provider_state_directory
    )
    start_hour, end_hour = download_bounds(start, end)
    symbol = normalize_symbol(symbol)
    routes = list(dict.fromkeys(proxies or ()))
    safe_routes = [safe_url(route) for route in routes]
    needs_credentials = any(
        urlsplit(route).username is not None
        or urlsplit(route).password is not None
        or urlsplit(route).query
        for route in routes
    )
    request = {
        "symbol": symbol,
        "start": start_hour.isoformat(),
        "end": end_hour.isoformat(),
        "total": max(0, int((end_hour - start_hour).total_seconds() // 3600)),
        "proxies": safe_routes,
        "requires_proxy_credentials": needs_credentials,
        "retry_no_data": retry_no_data,
    }
    store = JobStore(location)
    snapshot.provider_state_directory = store.state_path.resolve()
    job = await store.create(request, snapshot.model_dump(mode="json"), job_id=job_id)
    if not run:
        return job
    return await resume_download_job(
        job.job_id,
        state_path=location,
        proxies=routes or None,
        stop_event=stop_event,
        progress_callback=progress_callback,
        show_progress=show_progress,
    )


async def resume_download_job(
    job_id: str,
    *,
    state_path: Path | str | None = None,
    proxies: list[str] | None = None,
    stop_event: asyncio.Event | None = None,
    progress_callback: ProgressCallback | None = None,
    show_progress: bool = False,
) -> ManagedJob:
    """Resume missing hours, retaining all progress and observed cooldowns.

    A stored future deadline returns the current waiting state immediately. No
    background task is created; callers or a scheduler may resume it later.
    External task cancellation is propagated after saving a paused state.
    """
    location = (
        state_path if state_path is not None else get_config().provider_state_directory
    )
    store = JobStore(location)
    loaded = await store.get(job_id)
    if loaded is None:
        raise KeyError(f"Unknown download job: {job_id}")
    job = loaded
    if job.state == "complete":
        return job
    deadline = _deadline(job.next_eligible_at)
    if deadline is not None and deadline > time():
        return job
    lease = await store.claim_run(job_id)
    if lease is None:
        current = await store.get(job_id)
        if current is None:
            raise KeyError(f"Unknown download job: {job_id}")
        return current
    try:
        # Re-read after ownership acquisition to include another completed run.
        refreshed = await store.get(job_id)
        assert refreshed is not None
        job = refreshed
        if job.state == "complete":
            return job
        deadline = _deadline(job.next_eligible_at)
        if deadline is not None and deadline > time():
            return job
        if job.total == 0:
            return await store.update(
                job_id, lease=lease, state="complete", completed=0, pending=0, failed=0
            )
        request = job.request
        if proxies is None and request.get("requires_proxy_credentials", False):
            return await store.update(
                job_id,
                lease=lease,
                state="needs_action",
                error="Authenticated proxy credentials must be supplied when resuming",
                retry_reason="proxy_credentials_required",
            )
        routes = proxies if proxies is not None else request.get("proxies") or None
        snapshot = Config.model_validate(job.config)
        job = await store.update(
            job_id,
            lease=lease,
            state="running",
            next_eligible_at=None,
            retry_reason=None,
            error=None,
        )

        async def progress(value: DownloadProgress) -> None:
            nonlocal job
            job = await store.update(job_id, lease=lease, **_report_fields(value, job))
            if progress_callback is not None:
                result = progress_callback(value)
                if inspect.isawaitable(result):
                    await result

        try:
            report = await download_range(
                request["symbol"],
                datetime.fromisoformat(request["start"]),
                datetime.fromisoformat(request["end"]),
                routes,
                settings=snapshot,
                state_path=location,
                stop_event=stop_event,
                progress_callback=progress,
                show_progress=show_progress,
                retry_no_data=request.get("retry_no_data", False),
            )
        except BaseException as error:
            if isinstance(error, JobLeaseLost):
                raise
            partial = getattr(error, "failure_report", None)
            fields = _report_fields(partial, job)
            deadline = _deadline(getattr(error, "next_eligible_at", None))
            cause = (
                error.last_error if isinstance(error, FetchExhaustedError) else error
            )
            if deadline is None:
                deadline = _deadline(getattr(cause, "next_eligible_at", None))
            if isinstance(error, asyncio.CancelledError):
                await store.update(
                    job_id,
                    lease=lease,
                    state="paused",
                    retry_reason="cancelled",
                    **fields,
                )
                raise
            if not isinstance(error, Exception):
                await store.update(
                    job_id,
                    lease=lease,
                    state="needs_action",
                    error=type(error).__name__,
                    **fields,
                )
                raise
            access_unverified = (
                isinstance(error, ProviderDeferred)
                and error.reason == "access_unverified"
            )
            if access_unverified:
                state = "needs_action"
                reason = "access_unverified"
                deadline = None
            elif isinstance(error, ProviderDeferred) or isinstance(
                cause, RateLimitError
            ):
                state = "waiting_for_provider"
                reason = getattr(error, "reason", "rate_limited")
                if deadline is None:
                    deadline = time() + snapshot.provider_no_header_cooldown
            elif isinstance(cause, RetryableError):
                state = "waiting_for_network"
                reason = "temporary_network_or_server_error"
                retry_after = getattr(error, "retry_after", None)
                deadline = time() + (
                    retry_after
                    if retry_after is not None
                    else snapshot.fetch_base_retry_delay
                )
            else:
                state = "needs_action"
                reason = type(cause).__name__
            # HTTP errors are already sanitized. Other exception messages may
            # carry credential-bearing transport URLs, so persist the type only.
            description = (
                str(error) if isinstance(error, FetchError) else type(error).__name__
            )
            if access_unverified:
                description = (
                    "Provider access is unverified; run an explicit feed probe"
                )
            return await store.update(
                job_id,
                lease=lease,
                state=state,
                next_eligible_at=deadline,
                retry_reason=reason,
                error=description,
                **fields,
            )
        fields = _report_fields(report, job)
        state = (
            "paused"
            if report.cancelled
            else (
                "complete"
                if fields.get("pending", job.pending) == 0
                and fields.get("failed", job.failed) == 0
                else "needs_action"
            )
        )
        return await store.update(
            job_id,
            lease=lease,
            state=state,
            retry_reason="stopped" if report.cancelled else None,
            error=(
                None
                if state in ("complete", "paused")
                else "Incomplete download report"
            ),
            **fields,
        )
    except JobLeaseLost:
        # A recovered owner controls the job now. Never overwrite its state or
        # counts with a stale run's cancellation/failure/completion snapshot.
        current = await store.get(job_id)
        if current is None:
            raise
        return current
    finally:
        await store.release_run(lease)


run_download_job = resume_download_job

__all__ = [
    "ManagedJob",
    "get_download_job",
    "submit_download_job",
    "resume_download_job",
    "run_download_job",
]

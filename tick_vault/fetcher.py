"""Compatibility exports; transport implementation lives in tick_vault.download."""

from .download.fetcher import (
    FetchError,
    FetchExhaustedError,
    ForbiddenError,
    HTTPStatusError,
    RateLimitError,
    RetryableError,
    _fetch,
    fetch_with_retry,
    parse_retry_after,
    safe_url,
)

__all__ = [
    "FetchError",
    "FetchExhaustedError",
    "ForbiddenError",
    "HTTPStatusError",
    "RateLimitError",
    "RetryableError",
    "fetch_with_retry",
    "parse_retry_after",
    "_fetch",
    "safe_url",
]

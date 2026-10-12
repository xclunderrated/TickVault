"""Build shared provider controls from an immutable operation configuration."""

import hashlib
from pathlib import Path

from ..config import Config
from ..provider import ProviderCoordinator, ProviderPolicy


def source_key(settings: Config) -> str:
    return hashlib.sha256(settings.datafeed_base_url.encode("utf-8")).hexdigest()


def provider_coordinator(
    settings: Config, state_path: str | Path | None = None
) -> ProviderCoordinator:
    return ProviderCoordinator(
        state_path if state_path is not None else settings.provider_state_directory,
        provider_key=settings.provider_key,
        policy=ProviderPolicy(
            request_interval=settings.provider_request_interval,
            max_concurrency=settings.provider_max_concurrency,
            minute_budget=settings.provider_minute_budget,
            hour_budget=settings.provider_hour_budget,
            no_header_cooldown=settings.provider_no_header_cooldown,
            cooldown_jitter=settings.provider_cooldown_jitter,
            request_lease_seconds=settings.provider_request_lease_seconds,
            hour_lease_seconds=settings.provider_hour_lease_seconds,
        ),
    )


def receipt_coordinator(
    settings: Config, provider: ProviderCoordinator
) -> ProviderCoordinator:
    # Raw data/protocol identity is distinct from the shared quota identity.
    # This coordinator only owns cache receipts; it never admits HTTP requests.
    return ProviderCoordinator(
        provider.state_path,
        provider_key=settings.provider_key + "::raw:" + source_key(settings),
        policy=provider.policy,
    )

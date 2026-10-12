"""Isolate offline tests from the user's durable provider state and budgets."""

import httpx
import pytest

from tick_vault.config import CONFIG


@pytest.fixture(autouse=True)
def forbid_live_http(monkeypatch):
    """MockTransport remains usable; accidental real HTTP must fail offline."""

    def forbidden(*args, **kwargs):
        raise AssertionError("Live HTTP is disabled in the offline test suite")

    async def forbidden_async(*args, **kwargs):
        forbidden()

    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", forbidden)
    monkeypatch.setattr(
        httpx.AsyncHTTPTransport, "handle_async_request", forbidden_async
    )


@pytest.fixture(autouse=True)
def isolated_provider_state(tmp_path, monkeypatch):
    values = {
        "provider_state_directory": tmp_path / "provider",
        "provider_request_interval": 0.0,
        "provider_max_concurrency": 20,
        "provider_minute_budget": 100000,
        "provider_hour_budget": 1000000,
        "provider_no_header_cooldown": 0.01,
        "provider_cooldown_jitter": 0.0,
        "provider_require_verified_access": False,
    }
    for name, value in values.items():
        monkeypatch.setenv("TICK_VAULT_" + name.upper(), str(value))
        monkeypatch.setattr(CONFIG, name, value)

"""Exact symbol, date, proxy setting and sub-hour bounds from upstream #3."""

from datetime import datetime

import httpx
import pytest

from tick_vault import config, download_and_read
from tick_vault.download import api
from tick_vault.metadata import MetadataDB


@pytest.mark.asyncio
async def test_issue3_xau_subhour_proxy_request_can_finish_as_confirmed_empty(
    tmp_path, monkeypatch
):
    original = config.CONFIG.model_dump()
    config.reload_config(
        base_directory=tmp_path,
        console_log_level="ERROR",
        env_file=None,
        fetch_max_retry_attempts=0,
    )
    requested, routes = [], []
    real_client = httpx.AsyncClient

    def respond(request):
        requested.append(request.url.path)
        return httpx.Response(404, request=request)

    def factory(**kwargs):
        routes.append(kwargs.pop("proxy"))
        return real_client(transport=httpx.MockTransport(respond), **kwargs)

    monkeypatch.setattr(api, "AsyncClient", factory)
    try:
        frame = await download_and_read(
            "XAUUSD",
            datetime(2026, 8, 7, 21, 30),
            datetime(2026, 8, 7, 21, 40),
            proxies=["http://127.0.0.1:10808"],
            show_progress=False,
        )
        assert frame.empty
        assert requested == ["/datafeed/XAUUSD/2026/07/07/21h_ticks.bi5"]
        assert routes == ["http://127.0.0.1:10808"]
        with MetadataDB() as db:
            assert db.status("XAUUSD") == {
                "downloaded": 0,
                "no_data": 1,
                "failed": 0,
                "total": 1,
            }
    finally:
        config.reload_config(**original)

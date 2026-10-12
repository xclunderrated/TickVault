from datetime import datetime

import pytest

from tick_vault.chunk import TickChunk
from tick_vault.config import CONFIG, reload_config, snapshot_config, use_config
from tick_vault.core.logger import logger


def test_failed_logger_preparation_preserves_settings_and_handlers(tmp_path):
    previous = CONFIG.model_dump()
    reload_config(base_directory=tmp_path / "valid", env_file=None)
    settings, handlers = CONFIG.model_dump(), list(logger.handlers)
    bad = tmp_path / "file"
    bad.write_text("preserve")
    try:
        with pytest.raises(OSError):
            reload_config(base_directory=bad, env_file=None)
        assert CONFIG.model_dump() == settings
        assert logger.handlers == handlers
        assert all(not handler._closed for handler in handlers)
    finally:
        reload_config(**previous, env_file=None)


def test_operation_snapshot_survives_global_reload(tmp_path):
    previous = CONFIG.model_dump()
    reload_config(base_directory=tmp_path / "a", env_file=None)
    captured = snapshot_config()
    try:
        with use_config(captured):
            reload_config(base_directory=tmp_path / "b", env_file=None)
            chunk = TickChunk(symbol="EURUSD", time=datetime(2024, 1, 2))
            assert chunk.path().is_relative_to(tmp_path / "a")
        assert chunk.path().is_relative_to(tmp_path / "b")
    finally:
        reload_config(**previous, env_file=None)

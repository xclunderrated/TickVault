"""Logging configured on use, without filesystem effects during import."""

import logging
import sys

from .config import Config, get_config

logger = logging.getLogger("tick_vault")
logger.addHandler(logging.NullHandler())


class _ConsoleHandler(logging.StreamHandler):
    """Resolve stderr when emitting so embedded callers can redirect it safely."""

    _tick_vault = True

    def emit(self, record: logging.LogRecord) -> None:
        self.stream = sys.stderr
        super().emit(record)


class _FileHandler(logging.FileHandler):
    _tick_vault = True


def setup_logger(
    *, force: bool = False, settings: Config | None = None
) -> logging.Logger:
    settings = settings or get_config()
    managed = [h for h in logger.handlers if getattr(h, "_tick_vault", False)]
    if managed and not force:
        return logger
    console = _ConsoleHandler()
    console.setLevel(settings.console_log_level)
    try:
        settings.log_file_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = _FileHandler(settings.log_file_path, encoding="utf-8")
    except BaseException:
        console.close()
        raise
    file_handler.setLevel(settings.base_log_level)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    for prepared_handler in (console, file_handler):
        prepared_handler.setFormatter(formatter)
    logger.setLevel(settings.base_log_level)
    logger.propagate = False
    for old_handler in managed:
        logger.removeHandler(old_handler)
        old_handler.close()
    for prepared_handler in (console, file_handler):
        logger.addHandler(prepared_handler)
    return logger

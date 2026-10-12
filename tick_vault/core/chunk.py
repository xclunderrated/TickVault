"""An immutable hourly chunk with validated paths and atomic cache writes."""

import os
import tempfile
from datetime import datetime
from pathlib import Path

from pydantic import BaseModel, field_validator

from .binary import validate_payload
from .config import CONFIG, get_config  # noqa: F401 - old import compatibility
from .utils import format_relative_tick_path, normalize_datetime, normalize_symbol


class TickChunk(BaseModel, frozen=True):
    symbol: str
    time: datetime

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, value: str) -> str:
        return normalize_symbol(value)

    @field_validator("time")
    @classmethod
    def validate_and_normalize_time(cls, value: datetime) -> datetime:
        value = normalize_datetime(value)
        if value.minute or value.second or value.microsecond:
            raise ValueError("chunk time must be rounded to an hour in UTC")
        return value

    @property
    def url(self) -> str:
        return get_config().datafeed_base_url + format_relative_tick_path(
            self.symbol, self.time
        )

    def path(self, base: str | Path | None = None) -> Path:
        base_dir = Path(base) if base is not None else get_config().save_directory
        if base_dir.is_file():
            raise ValueError(f"Expected a directory for the base path: {base_dir}")
        return base_dir / format_relative_tick_path(self.symbol, self.time)

    def save(self, content: bytes, base: str | Path | None = None) -> None:
        validate_payload(content)
        target = self.path(base)
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                dir=target.parent, prefix=".tick-", suffix=".tmp", delete=False
            ) as handle:
                temporary = Path(handle.name)
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, target)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def load(self, base: str | Path | None = None) -> bytes:
        path = self.path(base)
        try:
            return path.read_bytes()
        except FileNotFoundError as exc:
            raise FileNotFoundError(
                f"Chunk file missing for {self.symbol} at {self.time.isoformat()}: {path}"
            ) from exc

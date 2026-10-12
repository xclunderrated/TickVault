"""Public snapshots and private events for the asynchronous download pipeline."""

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from ..chunk import TickChunk


@dataclass(frozen=True)
class DownloadProgress:
    """A consistent progress snapshot; total includes already cached hours."""

    symbol: str
    total: int
    attempted: int
    downloaded: int
    no_data: int
    cached: int
    failed: int
    active_workers: int
    bytes_downloaded: int
    elapsed_seconds: float
    discovery_complete: bool

    @property
    def completed(self) -> int:
        return self.attempted + self.cached

    @property
    def throughput_bytes_per_second(self) -> float:
        return (
            self.bytes_downloaded / self.elapsed_seconds
            if self.elapsed_seconds
            else 0.0
        )


@dataclass(frozen=True)
class DownloadReport:
    """Final download counts. Cooperative stop returns cancelled=True."""

    symbol: str
    start: datetime
    end: datetime
    total: int
    attempted: int
    downloaded: int
    no_data: int
    cached: int
    failed: int
    cancelled: bool
    bytes_downloaded: int
    elapsed_seconds: float
    state: str = "complete"
    complete: bool = True
    pending: int = 0
    next_eligible_at: datetime | None = None
    error_type: str | None = None
    error: str | None = None
    retry_reason: str | None = None
    completed: int = 0


@dataclass(frozen=True)
class DownloadResult:
    chunk: TickChunk
    status: Literal["downloaded", "no_data", "failed"]
    bytes_downloaded: int = 0
    error: str | None = None
    reused: bool = False


@dataclass(frozen=True)
class FetchObservation:
    elapsed_seconds: float
    bytes_downloaded: int
    error: str | None = None
    retry_delay: float = 0.0

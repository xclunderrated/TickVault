"""UTC date handling and safe Dukascopy instrument paths."""

import re
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta


def normalize_symbol(symbol: str) -> str:
    if not isinstance(symbol, str):
        raise TypeError("symbol must be a string")
    symbol = symbol.strip().upper()
    if not re.fullmatch(r"[A-Z0-9][A-Z0-9_-]{0,63}", symbol):
        raise ValueError("symbol must contain only ASCII letters, digits, '_' or '-'")
    return symbol


def normalize_datetime(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError("expected a datetime")
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def hour_floor(value: datetime) -> datetime:
    return normalize_datetime(value).replace(minute=0, second=0, microsecond=0)


def hour_ceil(value: datetime) -> datetime:
    value = normalize_datetime(value)
    floor = hour_floor(value)
    return floor if floor == value else floor + timedelta(hours=1)


def iter_hourly_datetimes(start: datetime, end: datetime) -> Iterator[datetime]:
    """Yield [floor(start), floor(end)) hours; naive dates mean UTC."""
    start, end = normalize_datetime(start), normalize_datetime(end)
    if end <= start:
        raise ValueError("end must be after start")
    current, stop = hour_floor(start), hour_floor(end)
    while current < stop:
        yield current
        current += timedelta(hours=1)


def generate_hourly_datetimes(start: datetime, end: datetime) -> list[datetime]:
    """Compatibility list wrapper around the bounded hourly iterator."""
    return list(iter_hourly_datetimes(start, end))


def format_relative_tick_path(symbol: str, time: datetime) -> str:
    time = normalize_datetime(time)
    return (
        f"{normalize_symbol(symbol)}/{time.year}/{time.month - 1:02d}/"
        f"{time.day:02d}/{time.hour:02d}h_ticks.bi5"
    )

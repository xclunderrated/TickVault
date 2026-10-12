"""Shared capacity and cooldown decisions for all download workers."""

import asyncio
from time import monotonic

from .models import FetchObservation


class AdaptiveController:
    """Adjust capacity from measured latency/errors while sharing rate cooldowns.

    Existing requests can complete after a capacity reduction. New requests wait
    for free capacity and the common provider deadline. Proxies never evade that
    deadline. A stable interval grows capacity by one, an error/latency regression
    shrinks it, and any rate limit halves it immediately.
    """

    def __init__(
        self, maximum: int, *, enabled: bool = True, minimum: int = 1, interval: int = 8
    ) -> None:
        self.maximum = maximum
        self.minimum = min(maximum, minimum)
        self.enabled = enabled
        self.interval = interval
        self.limit = min(maximum, max(self.minimum, 2)) if enabled else maximum
        self.in_flight = 0
        self.pause_until = 0.0
        self._condition = asyncio.Condition()
        self._samples: list[float] = []
        self._errors = 0
        self._previous_latency: float | None = None

    async def wait_until_ready(self) -> None:
        while True:
            delay = self.pause_until - monotonic()
            if delay <= 0:
                return
            await asyncio.sleep(delay)

    async def acquire(self) -> None:
        while True:
            await self.wait_until_ready()
            async with self._condition:
                if self.pause_until > monotonic():
                    continue
                if self.in_flight < self.limit:
                    self.in_flight += 1
                    return
                await self._condition.wait()

    async def release(self) -> None:
        async with self._condition:
            self.in_flight -= 1
            self._condition.notify_all()

    async def observe(self, sample: FetchObservation) -> None:
        async with self._condition:
            self._samples.append(sample.elapsed_seconds)
            self._errors += int(sample.error is not None)
            if sample.error == "RateLimitError":
                self.pause_until = max(
                    self.pause_until, monotonic() + sample.retry_delay
                )
                if self.enabled:
                    self.limit = max(self.minimum, self.limit // 2)
                self._samples.clear()
                self._errors = 0
                self._previous_latency = None
            elif len(self._samples) >= self.interval:
                latency = sum(self._samples) / len(self._samples)
                if self.enabled:
                    regressed = (
                        self._previous_latency is not None
                        and latency > self._previous_latency * 1.5
                    )
                    if self._errors or regressed:
                        self.limit = max(self.minimum, self.limit - 1)
                    else:
                        self.limit = min(self.maximum, self.limit + 1)
                self._previous_latency = latency
                self._samples.clear()
                self._errors = 0
            self._condition.notify_all()

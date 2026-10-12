"""Durable admission, recovery probes and single-flight hourly raw receipts.

The provider key represents the declared provider/account/network group. Proxy
routes and vault directories deliberately do not participate in admission keys.
Persisted policy limits may become stricter, but a later caller cannot raise them.
"""

import asyncio
import hashlib
import math
import os
import random
import sqlite3
import tempfile
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import Any, Literal, TypeVar

from ..core.binary import MAX_RAW_BYTES, validate_payload
from ..core.utils import normalize_datetime, normalize_symbol
from ._state import state_path as resolve_state_path
from ._state import transaction

T = TypeVar("T")


@dataclass(frozen=True)
class ProviderPolicy:
    """Immutable job policy; these defaults are client limits, not a vendor quota."""

    request_interval: float = 2.0
    max_concurrency: int = 1
    minute_budget: int = 30
    hour_budget: int = 1800
    no_header_cooldown: float = 900.0
    cooldown_jitter: float = 1.0
    request_lease_seconds: float = 90.0
    hour_lease_seconds: float = 180.0

    def __post_init__(self) -> None:
        for name in (
            "request_interval",
            "no_header_cooldown",
            "cooldown_jitter",
            "request_lease_seconds",
            "hour_lease_seconds",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not math.isfinite(value)
                or value < 0
                or (name not in {"request_interval", "cooldown_jitter"} and value == 0)
            ):
                raise ValueError(
                    f"{name} must be finite and {'nonnegative' if name in {'request_interval', 'cooldown_jitter'} else 'positive'}"
                )
        for name in ("max_concurrency", "minute_budget", "hour_budget"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")


class ProviderDeferred(Exception):
    """Admission is deferred; a durable deadline remains after the caller exits."""

    def __init__(self, reason: str, next_eligible_at: float):
        super().__init__(f"Provider admission deferred: {reason}")
        self.reason = reason
        self.next_eligible_at = next_eligible_at


class HourClaimLost(RuntimeError):
    """A stale owner cannot publish or replace another owner's receipt."""


@dataclass(frozen=True)
class ProviderStatus:
    provider_key: str
    circuit: str
    cooldown_until: float
    next_eligible_at: float
    in_flight: int
    minute_used: int
    hour_used: int
    throttle_count: int
    request_interval: float
    max_concurrency: int
    minute_budget: int
    hour_budget: int
    reason: str | None = None
    access_verified_at: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RequestLease:
    token: str
    provider_key: str
    started_at: float
    lease_until: float
    is_probe: bool
    _coordinator: "ProviderCoordinator" = field(repr=False, compare=False)

    async def __aenter__(self) -> "RequestLease":
        self._coordinator._bind_owner(self.token)
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self._coordinator.release(self)


@dataclass(frozen=True)
class HourClaim:
    provider_key: str
    symbol: str
    hour: str
    state: Literal["owned", "pending", "complete"]
    token: str | None = None
    lease_until: float | None = None
    result: Literal["downloaded", "no_data"] | None = None
    artifact_path: Path | None = None
    digest: str | None = None
    completed_at: float | None = None
    _coordinator: "ProviderCoordinator | None" = field(
        default=None, repr=False, compare=False
    )

    @property
    def owned(self) -> bool:
        return self.state == "owned"

    async def __aenter__(self) -> "HourClaim":
        if self.token is not None and self._coordinator is not None:
            self._coordinator._bind_owner(self.token)
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self.owned and self._coordinator is not None:
            await self._coordinator.release_hour(self)


async def _durable_call(function: Callable[..., T], *args: Any) -> T:
    """Cancellation waits for a mutation to commit, without blocking the loop."""
    task = asyncio.create_task(asyncio.to_thread(function, *args))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        await task
        raise


class ProviderCoordinator:
    def __init__(
        self,
        state_path: str | Path | None = None,
        provider_key: str = "dukascopy",
        policy: ProviderPolicy | None = None,
        *,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ):
        if (
            not provider_key
            or len(provider_key) > 200
            or any(ord(char) < 32 for char in provider_key)
        ):
            raise ValueError("provider_key must be a nonempty stable group identity")
        self.state_path = resolve_state_path(state_path)
        self.cache_directory = self.state_path.parent / (self.state_path.stem + "-raw")
        self.provider_key = provider_key
        self.policy = policy or ProviderPolicy()
        self._clock = clock
        self._sleep = sleep
        self._request_heartbeats: dict[str, asyncio.Task[None]] = {}
        self._hour_heartbeats: dict[str, asyncio.Task[None]] = {}
        self._heartbeat_owners: dict[str, asyncio.Task[Any]] = {}

    def _provider(self, db: sqlite3.Connection) -> tuple[sqlite3.Row, float]:
        now = self._clock()
        if not math.isfinite(now):
            raise ValueError("clock must return a finite UTC timestamp")
        policy = self.policy
        db.execute(
            "INSERT OR IGNORE INTO providers(provider_key,last_seen,request_interval,max_concurrency,minute_budget,hour_budget,no_header_cooldown) VALUES(?,?,?,?,?,?,?)",
            (
                self.provider_key,
                now,
                policy.request_interval,
                policy.max_concurrency,
                policy.minute_budget,
                policy.hour_budget,
                policy.no_header_cooldown,
            ),
        )
        db.execute(
            "UPDATE providers SET last_seen=MAX(last_seen,?),request_interval=MAX(request_interval,?),max_concurrency=MIN(max_concurrency,?),minute_budget=MIN(minute_budget,?),hour_budget=MIN(hour_budget,?),no_header_cooldown=MAX(no_header_cooldown,?) WHERE provider_key=?",
            (
                now,
                policy.request_interval,
                policy.max_concurrency,
                policy.minute_budget,
                policy.hour_budget,
                policy.no_header_cooldown,
                self.provider_key,
            ),
        )
        row = db.execute(
            "SELECT * FROM providers WHERE provider_key=?", (self.provider_key,)
        ).fetchone()
        now = float(row["last_seen"])
        last_start = db.execute(
            "SELECT MAX(started_at) FROM request_starts WHERE provider_key=?",
            (self.provider_key,),
        ).fetchone()[0]
        if (
            last_start is not None
            and last_start + row["request_interval"] > row["next_request"]
        ):
            db.execute(
                "UPDATE providers SET next_request=? WHERE provider_key=?",
                (last_start + row["request_interval"], self.provider_key),
            )
            row = db.execute(
                "SELECT * FROM providers WHERE provider_key=?", (self.provider_key,)
            ).fetchone()
        # A process dying during the one recovery probe cannot unblock a wave.
        if row["circuit"] == "half_open":
            probe = db.execute(
                "SELECT lease_until FROM requests WHERE token=? AND provider_key=?",
                (row["probe_token"], self.provider_key),
            ).fetchone()
            if probe is None or probe["lease_until"] <= now:
                db.execute(
                    "UPDATE providers SET circuit='open',probe_token=NULL,cooldown_until=MAX(cooldown_until,?) WHERE provider_key=?",
                    (now + row["no_header_cooldown"], self.provider_key),
                )
                row = db.execute(
                    "SELECT * FROM providers WHERE provider_key=?", (self.provider_key,)
                ).fetchone()
        db.execute(
            "DELETE FROM requests WHERE provider_key=? AND lease_until<=?",
            (self.provider_key, now),
        )
        db.execute(
            "DELETE FROM request_starts WHERE provider_key=? AND started_at<=?",
            (self.provider_key, now - 3600),
        )
        return row, now

    def _availability(
        self, db: sqlite3.Connection, row: sqlite3.Row, now: float
    ) -> tuple[str | None, float, list[sqlite3.Row], list[float]]:
        leases = db.execute(
            "SELECT * FROM requests WHERE provider_key=? ORDER BY lease_until",
            (self.provider_key,),
        ).fetchall()
        starts = [
            float(item[0])
            for item in db.execute(
                "SELECT started_at FROM request_starts WHERE provider_key=? ORDER BY started_at",
                (self.provider_key,),
            )
        ]
        reasons: list[tuple[str, float]] = []
        if row["cooldown_until"] > now:
            reasons.append(("cooldown", float(row["cooldown_until"])))
        if row["circuit"] == "half_open":
            probe = next(
                (lease for lease in leases if lease["token"] == row["probe_token"]),
                None,
            )
            reasons.append(
                (
                    "recovery_probe",
                    (
                        float(probe["lease_until"])
                        if probe
                        else now + row["no_header_cooldown"]
                    ),
                )
            )
        if row["next_request"] > now:
            reasons.append(("pacing", float(row["next_request"])))
        minute = [start for start in starts if start > now - 60]
        if len(minute) >= row["minute_budget"]:
            reasons.append(
                ("minute_budget", minute[len(minute) - row["minute_budget"]] + 60)
            )
        if len(starts) >= row["hour_budget"]:
            reasons.append(
                ("hour_budget", starts[len(starts) - row["hour_budget"]] + 3600)
            )
        if len(leases) >= row["max_concurrency"]:
            reasons.append(
                (
                    "in_flight",
                    float(leases[len(leases) - row["max_concurrency"]]["lease_until"]),
                )
            )
        # Cooldown classification remains visible even if a longer budget binds.
        deadline = max((deadline for _, deadline in reasons), default=now)
        reason = (
            "cooldown"
            if any(reason == "cooldown" for reason, _ in reasons)
            else (max(reasons, key=lambda item: item[1])[0] if reasons else None)
        )
        return reason, deadline, leases, starts

    def _try_acquire(self) -> RequestLease | ProviderDeferred:
        with transaction(self.state_path) as db:
            row, now = self._provider(db)
            reason, deadline, _, _ = self._availability(db, row, now)
            if reason is not None:
                return ProviderDeferred(reason, deadline)
            token = uuid.uuid4().hex
            is_probe = row["circuit"] == "open"
            lease_until = now + self.policy.request_lease_seconds
            db.execute(
                "INSERT INTO requests VALUES(?,?,?,?,?)",
                (token, self.provider_key, now, lease_until, int(is_probe)),
            )
            db.execute(
                "INSERT INTO request_starts VALUES(?,?,?)",
                (token, self.provider_key, now),
            )
            db.execute(
                "UPDATE providers SET next_request=?,circuit=?,probe_token=? WHERE provider_key=?",
                (
                    now + row["request_interval"],
                    "half_open" if is_probe else "closed",
                    token if is_probe else None,
                    self.provider_key,
                ),
            )
            return RequestLease(
                token, self.provider_key, now, lease_until, is_probe, self
            )

    async def acquire(
        self,
        *,
        wait: bool = True,
        max_wait: float | None = None,
        stop_event: asyncio.Event | None = None,
        defer_on_cooldown: bool = True,
    ) -> RequestLease:
        """Consume one attempt; released, cancelled and 404 attempts are not refunded."""
        if max_wait is not None and (not math.isfinite(max_wait) or max_wait < 0):
            raise ValueError("max_wait must be finite and nonnegative")
        began = asyncio.get_running_loop().time()
        while True:
            if stop_event is not None and stop_event.is_set():
                raise asyncio.CancelledError
            task = asyncio.create_task(asyncio.to_thread(self._try_acquire))
            try:
                result = await asyncio.shield(task)
            except asyncio.CancelledError:
                result = await task
                if isinstance(result, RequestLease):
                    await _durable_call(self._release, result)
                raise
            if isinstance(result, RequestLease):
                if stop_event is not None and stop_event.is_set():
                    await _durable_call(self._release, result)
                    raise asyncio.CancelledError
                self._start_heartbeat(
                    result.token,
                    self._request_heartbeats,
                    self.policy.request_lease_seconds,
                    partial(self.heartbeat, result),
                )
                return result
            remaining = (
                None
                if max_wait is None
                else max_wait - (asyncio.get_running_loop().time() - began)
            )
            if (
                not wait
                or (defer_on_cooldown and result.reason == "cooldown")
                or (remaining is not None and remaining <= 0)
            ):
                raise result
            delay = max(0.001, min(0.5, result.next_eligible_at - self._clock()))
            if remaining is not None:
                delay = min(delay, remaining)
            if stop_event is None:
                await self._sleep(delay)
            else:
                sleeper = asyncio.ensure_future(self._sleep(delay))
                stopper = asyncio.create_task(stop_event.wait())
                try:
                    done, _ = await asyncio.wait(
                        (sleeper, stopper), return_when=asyncio.FIRST_COMPLETED
                    )
                    if stopper in done:
                        raise asyncio.CancelledError
                    await sleeper
                finally:
                    sleeper.cancel()
                    stopper.cancel()
                    await asyncio.gather(sleeper, stopper, return_exceptions=True)

    def _start_heartbeat(
        self,
        token: str,
        tasks: dict[str, asyncio.Task[None]],
        lease_seconds: float,
        beat: Callable[[], Awaitable[bool]],
    ) -> None:
        self._bind_owner(token)

        def stop_owner() -> None:
            owner = self._heartbeat_owners.get(token)
            if owner is not None and not owner.done():
                owner.cancel()

        async def renew() -> None:
            try:
                while True:
                    await asyncio.sleep(max(0.01, lease_seconds / 3))
                    if not await beat():
                        stop_owner()
                        return
            except (OSError, sqlite3.Error):
                # Work must stop before an expired slot can be recovered.
                stop_owner()
                return

        tasks[token] = asyncio.create_task(
            renew(), name=f"tickvault-provider-heartbeat-{token}"
        )

    def _bind_owner(self, token: str) -> None:
        owner = asyncio.current_task()
        if owner is not None:
            self._heartbeat_owners[token] = owner

    async def _stop_heartbeat(
        self, token: str, tasks: dict[str, asyncio.Task[None]]
    ) -> None:
        self._heartbeat_owners.pop(token, None)
        task = tasks.pop(token, None)
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    def _heartbeat(self, lease: RequestLease) -> bool:
        with transaction(self.state_path) as db:
            _, now = self._provider(db)
            result = db.execute(
                "UPDATE requests SET lease_until=? WHERE token=? AND provider_key=? AND lease_until>?",
                (
                    now + self.policy.request_lease_seconds,
                    lease.token,
                    self.provider_key,
                    now,
                ),
            )
            return result.rowcount == 1

    async def heartbeat(self, lease: RequestLease) -> bool:
        self._check_provider(lease.provider_key)
        return await _durable_call(self._heartbeat, lease)

    def _release(self, lease: RequestLease) -> None:
        with transaction(self.state_path) as db:
            row, now = self._provider(db)
            if row["circuit"] == "half_open" and row["probe_token"] == lease.token:
                db.execute(
                    "UPDATE providers SET circuit='open',probe_token=NULL,cooldown_until=MAX(cooldown_until,?) WHERE provider_key=?",
                    (now + row["no_header_cooldown"], self.provider_key),
                )
            db.execute(
                "DELETE FROM requests WHERE token=? AND provider_key=?",
                (lease.token, self.provider_key),
            )

    async def release(self, lease: RequestLease) -> None:
        self._check_provider(lease.provider_key)
        await self._stop_heartbeat(lease.token, self._request_heartbeats)
        await _durable_call(self._release, lease)

    def _check_provider(self, key: str) -> None:
        if key != self.provider_key:
            raise ValueError("lease belongs to a different provider group")

    @asynccontextmanager
    async def request(self, **options: Any) -> AsyncIterator[RequestLease]:
        lease = await self.acquire(**options)
        try:
            yield lease
        finally:
            await self.release(lease)

    def _observe_rate_limit(self, retry_after: float | None) -> ProviderStatus:
        with transaction(self.state_path) as db:
            row, now = self._provider(db)
            delay = row["no_header_cooldown"] if retry_after is None else retry_after
            delay += random.uniform(0.0, self.policy.cooldown_jitter)
            # Trouble can only reduce rate. A configured zero interval remains
            # usable for deterministic tests, while positive policies slow down.
            interval = min(300.0, row["request_interval"] * 2)
            interval = max(interval, row["request_interval"])
            db.execute(
                "UPDATE providers SET cooldown_until=MAX(cooldown_until,?),circuit='open',probe_token=NULL,throttle_count=throttle_count+1,request_interval=? WHERE provider_key=?",
                (now + delay, interval, self.provider_key),
            )
        return self._status()

    async def observe_rate_limit(
        self, retry_after: float | None, *, lease: RequestLease | None = None
    ) -> ProviderStatus:
        if lease is not None:
            self._check_provider(lease.provider_key)
        if retry_after is not None and (
            not math.isfinite(retry_after) or retry_after < 0
        ):
            retry_after = None
        return await _durable_call(self._observe_rate_limit, retry_after)

    def _observe_success(self, lease: RequestLease) -> None:
        with transaction(self.state_path) as db:
            row, _ = self._provider(db)
            if row["circuit"] == "half_open" and row["probe_token"] == lease.token:
                db.execute(
                    "UPDATE providers SET circuit='closed',probe_token=NULL WHERE provider_key=?",
                    (self.provider_key,),
                )

    async def observe_success(self, lease: RequestLease) -> None:
        self._check_provider(lease.provider_key)
        await _durable_call(self._observe_success, lease)

    def _status(self) -> ProviderStatus:
        with transaction(self.state_path) as db:
            row, now = self._provider(db)
            reason, deadline, leases, starts = self._availability(db, row, now)
            access = db.execute(
                "SELECT MAX(verified_at) FROM provider_access WHERE provider_key=?",
                (self.provider_key,),
            ).fetchone()[0]
            return ProviderStatus(
                self.provider_key,
                row["circuit"],
                row["cooldown_until"],
                deadline,
                len(leases),
                sum(start > now - 60 for start in starts),
                len(starts),
                row["throttle_count"],
                row["request_interval"],
                row["max_concurrency"],
                row["minute_budget"],
                row["hour_budget"],
                reason,
                access,
            )

    async def status(self) -> ProviderStatus:
        return await _durable_call(self._status)

    def _record_validated_payload(
        self, symbol: str, hour: str, tick_count: int, source_key: str
    ) -> None:
        with transaction(self.state_path) as db:
            _, now = self._provider(db)
            db.execute(
                "INSERT INTO provider_access VALUES(?,?,?,?,?,?) ON CONFLICT(provider_key,source_key) DO UPDATE SET symbol=excluded.symbol,hour=excluded.hour,tick_count=excluded.tick_count,verified_at=excluded.verified_at",
                (self.provider_key, source_key, symbol, hour, tick_count, now),
            )

    async def record_validated_payload(
        self,
        symbol: str,
        hour: datetime | str,
        tick_count: int,
        *,
        source_key: str = "legacy",
    ) -> None:
        """Record a caller-proven nonempty network payload for this protocol/source."""
        if (
            isinstance(tick_count, bool)
            or not isinstance(tick_count, int)
            or tick_count <= 0
        ):
            raise ValueError("access proof requires a positive validated tick count")
        if not source_key or len(source_key) > 200:
            raise ValueError("source_key must be a nonempty sanitized source identity")
        symbol, hour = self._hour_key(symbol, hour)
        await _durable_call(
            self._record_validated_payload, symbol, hour, tick_count, source_key
        )

    def _has_validated_access(self, source_key: str) -> bool:
        with transaction(self.state_path) as db:
            return (
                db.execute(
                    "SELECT 1 FROM provider_access WHERE provider_key=? AND source_key=?",
                    (self.provider_key, source_key),
                ).fetchone()
                is not None
            )

    async def has_validated_access(self, source_key: str = "legacy") -> bool:
        return await _durable_call(self._has_validated_access, source_key)

    @staticmethod
    def _hour_key(symbol: str, hour: datetime | str) -> tuple[str, str]:
        symbol = normalize_symbol(symbol)
        value = datetime.fromisoformat(hour) if isinstance(hour, str) else hour
        value = normalize_datetime(value)
        if value.minute or value.second or value.microsecond:
            raise ValueError("hour must be aligned to a complete UTC hour")
        return symbol, value.isoformat()

    def _receipt_valid(self, row: sqlite3.Row) -> bool:
        if row["result"] == "no_data":
            return True
        digest = row["digest"]
        if (
            not digest
            or len(digest) != 64
            or any(char not in "0123456789abcdef" for char in digest)
        ):
            return False
        expected = self.cache_directory / digest[:2] / (digest + ".bi5")
        if row["artifact_path"] != str(expected):
            return False
        try:
            content = self._read_artifact(expected)
            return bool(
                len(content) == row["byte_count"]
                and hashlib.sha256(content).hexdigest() == digest
            )
        except (OSError, ValueError):
            return False

    @staticmethod
    def _read_artifact(path: Path) -> bytes:
        with path.open("rb") as handle:
            content = handle.read(MAX_RAW_BYTES + 1)
        if len(content) > MAX_RAW_BYTES:
            raise ValueError("compressed hourly artifact exceeds cache size bound")
        validate_payload(content)
        return content

    def _claim_hour(
        self,
        symbol: str,
        hour: str,
        max_no_data_age: float,
        force_refresh: bool,
        no_data_refreshed_since: float | None,
    ) -> HourClaim:
        with transaction(self.state_path) as db:
            _, now = self._provider(db)
            row = db.execute(
                "SELECT * FROM hours WHERE provider_key=? AND symbol=? AND hour=?",
                (self.provider_key, symbol, hour),
            ).fetchone()
            if row is not None and row["state"] == "complete":
                refreshed = (
                    no_data_refreshed_since is not None
                    and row["completed_at"] >= no_data_refreshed_since
                )
                fresh = (
                    row["result"] == "downloaded"
                    or refreshed
                    or (
                        not force_refresh
                        and now - row["completed_at"] < max_no_data_age
                    )
                )
                if fresh and self._receipt_valid(row):
                    return HourClaim(
                        self.provider_key,
                        symbol,
                        hour,
                        "complete",
                        result=row["result"],
                        artifact_path=(
                            Path(row["artifact_path"]) if row["artifact_path"] else None
                        ),
                        digest=row["digest"],
                        completed_at=row["completed_at"],
                        _coordinator=self,
                    )
            if (
                row is not None
                and row["state"] == "pending"
                and row["lease_until"] > now
            ):
                return HourClaim(
                    self.provider_key,
                    symbol,
                    hour,
                    "pending",
                    lease_until=row["lease_until"],
                    _coordinator=self,
                )
            token = uuid.uuid4().hex
            deadline = now + self.policy.hour_lease_seconds
            db.execute(
                "INSERT INTO hours(provider_key,symbol,hour,state,token,lease_until) VALUES(?,?,?,'pending',?,?) ON CONFLICT(provider_key,symbol,hour) DO UPDATE SET state='pending',token=excluded.token,lease_until=excluded.lease_until,result=NULL,artifact_path=NULL,digest=NULL,byte_count=NULL,completed_at=NULL",
                (self.provider_key, symbol, hour, token, deadline),
            )
            return HourClaim(
                self.provider_key,
                symbol,
                hour,
                "owned",
                token=token,
                lease_until=deadline,
                _coordinator=self,
            )

    async def claim_hour(
        self,
        symbol: str,
        hour: datetime | str,
        *,
        max_no_data_age: float = 86400.0,
        force_refresh: bool = False,
        no_data_refreshed_since: float | None = None,
    ) -> HourClaim:
        """Reuse a negative result published since this operation began.

        This optional threshold lets overlapping explicit rechecks share a fresh
        outcome, including when max_no_data_age is zero. Older outcomes still
        obey force_refresh and the configured age limit.
        """
        if not math.isfinite(max_no_data_age) or max_no_data_age < 0:
            raise ValueError("max_no_data_age must be finite and nonnegative")
        if no_data_refreshed_since is not None and not math.isfinite(
            no_data_refreshed_since
        ):
            raise ValueError("no_data_refreshed_since must be finite")
        symbol, hour = self._hour_key(symbol, hour)
        task = asyncio.create_task(
            asyncio.to_thread(
                self._claim_hour,
                symbol,
                hour,
                max_no_data_age,
                force_refresh,
                no_data_refreshed_since,
            )
        )
        try:
            claim = await asyncio.shield(task)
        except asyncio.CancelledError:
            claim = await task
            if claim.owned:
                await _durable_call(self._release_hour, claim)
            raise
        if claim.owned and claim.token is not None:
            self._start_heartbeat(
                claim.token,
                self._hour_heartbeats,
                self.policy.hour_lease_seconds,
                lambda: self.heartbeat_hour(claim),
            )
        return claim

    def _heartbeat_hour(self, claim: HourClaim) -> bool:
        with transaction(self.state_path) as db:
            _, now = self._provider(db)
            result = db.execute(
                "UPDATE hours SET lease_until=? WHERE provider_key=? AND symbol=? AND hour=? AND state='pending' AND token=? AND lease_until>?",
                (
                    now + self.policy.hour_lease_seconds,
                    self.provider_key,
                    claim.symbol,
                    claim.hour,
                    claim.token,
                    now,
                ),
            )
            return result.rowcount == 1

    async def heartbeat_hour(self, claim: HourClaim) -> bool:
        self._check_provider(claim.provider_key)
        return await _durable_call(self._heartbeat_hour, claim)

    def _release_hour(self, claim: HourClaim) -> None:
        with transaction(self.state_path) as db:
            db.execute(
                "DELETE FROM hours WHERE provider_key=? AND symbol=? AND hour=? AND state='pending' AND token=?",
                (self.provider_key, claim.symbol, claim.hour, claim.token),
            )

    async def release_hour(self, claim: HourClaim) -> None:
        self._check_provider(claim.provider_key)
        if claim.token is not None:
            await self._stop_heartbeat(claim.token, self._hour_heartbeats)
            await _durable_call(self._release_hour, claim)

    def _publish_hour(
        self,
        claim: HourClaim,
        artifact: Path | None,
        result: Literal["downloaded", "no_data"],
    ) -> HourClaim:
        # Validation and publication both happen on the worker thread. The
        # content-addressed copy outlives the originating vault and its files.
        digest = None
        target = None
        content = None
        if result == "downloaded":
            if artifact is None:
                raise ValueError("downloaded receipt requires a validated artifact")
            content = self._read_artifact(artifact)
            digest = hashlib.sha256(content).hexdigest()
            target = self.cache_directory / digest[:2] / (digest + ".bi5")
            target.parent.mkdir(parents=True, exist_ok=True)
            temporary: Path | None = None
            existing_valid = False
            try:
                existing_valid = self._read_artifact(target) == content
            except (OSError, ValueError):
                pass
            try:
                if not existing_valid:
                    with tempfile.NamedTemporaryFile(
                        dir=target.parent, prefix=".raw-", suffix=".tmp", delete=False
                    ) as handle:
                        temporary = Path(handle.name)
                        handle.write(content)
                        handle.flush()
                        os.fsync(handle.fileno())
                    # Reusing valid objects avoids replacing a reader's open
                    # Windows file; only absent/corrupt objects need publication.
                    os.replace(temporary, target)
            finally:
                if temporary is not None:
                    temporary.unlink(missing_ok=True)
        with transaction(self.state_path) as db:
            _, now = self._provider(db)
            updated = db.execute(
                "UPDATE hours SET state='complete',token=NULL,lease_until=NULL,result=?,artifact_path=?,digest=?,byte_count=?,completed_at=? WHERE provider_key=? AND symbol=? AND hour=? AND state='pending' AND token=? AND lease_until>?",
                (
                    result,
                    str(target) if target else None,
                    digest,
                    len(content) if content is not None else 0,
                    now,
                    self.provider_key,
                    claim.symbol,
                    claim.hour,
                    claim.token,
                    now,
                ),
            )
            if updated.rowcount != 1:
                raise HourClaimLost("Hourly ownership expired before publication")
            return HourClaim(
                self.provider_key,
                claim.symbol,
                claim.hour,
                "complete",
                result=result,
                artifact_path=target,
                digest=digest,
                completed_at=now,
                _coordinator=self,
            )

    async def publish_hour(
        self,
        claim: HourClaim,
        validated_artifact_path: str | Path | None = None,
        *,
        result: Literal["downloaded", "no_data"] = "downloaded",
    ) -> HourClaim:
        self._check_provider(claim.provider_key)
        if not claim.owned or claim.token is None:
            raise HourClaimLost("Only the current hourly owner may publish a receipt")
        if result not in {"downloaded", "no_data"}:
            raise ValueError("hour result must be downloaded or no_data")
        if result == "no_data" and validated_artifact_path is not None:
            raise ValueError("no_data receipts cannot contain an artifact")
        artifact = (
            Path(validated_artifact_path)
            if validated_artifact_path is not None
            else None
        )
        receipt = await _durable_call(self._publish_hour, claim, artifact, result)
        await self._stop_heartbeat(claim.token, self._hour_heartbeats)
        return receipt

    async def close(self) -> None:
        """Stop local renewal tasks; persisted leases expire after a process exit."""
        for tasks in (self._request_heartbeats, self._hour_heartbeats):
            for token in tuple(tasks):
                await self._stop_heartbeat(token, tasks)

    async def __aenter__(self) -> "ProviderCoordinator":
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

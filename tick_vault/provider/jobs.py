"""Durable managed job snapshots and expiring ownership of each job run."""

import asyncio
import json
import math
import sqlite3
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, cast

from ._state import state_path as resolve_state_path
from ._state import transaction
from .coordinator import _durable_call

JobState = Literal[
    "queued",
    "running",
    "waiting_for_provider",
    "waiting_for_network",
    "paused",
    "complete",
    "needs_action",
]
JOB_STATES = frozenset(
    {
        "queued",
        "running",
        "waiting_for_provider",
        "waiting_for_network",
        "paused",
        "complete",
        "needs_action",
    }
)


class JobLeaseLost(RuntimeError):
    """An expired run owner cannot mutate a recovered job's progress."""


@dataclass(frozen=True)
class ManagedJob:
    job_id: str
    request: dict[str, Any]
    config: dict[str, Any]
    state: JobState
    total: int
    completed: int
    pending: int
    failed: int
    next_eligible_at: float | None
    retry_reason: str | None
    error: str | None
    report: dict[str, Any] | None
    created_at: float
    updated_at: float

    @property
    def complete(self) -> bool:
        return self.state == "complete" and self.pending == 0 and self.failed == 0

    def to_dict(self) -> dict[str, Any]:
        # Fresh dictionaries protect the persisted request/config from callers.
        return cast(
            dict[str, Any],
            json.loads(
                json.dumps(
                    {
                        "job_id": self.job_id,
                        "request": self.request,
                        "config": self.config,
                        "state": self.state,
                        "complete": self.complete,
                        "total": self.total,
                        "completed": self.completed,
                        "pending": self.pending,
                        "failed": self.failed,
                        "next_eligible_at": self.next_eligible_at,
                        "retry_reason": self.retry_reason,
                        "error": self.error,
                        "report": self.report,
                        "created_at": self.created_at,
                        "updated_at": self.updated_at,
                    },
                    allow_nan=False,
                )
            ),
        )


@dataclass(frozen=True)
class JobLease:
    job_id: str
    token: str
    lease_until: float
    lease_seconds: float
    _store: "JobStore" = field(repr=False, compare=False)

    async def __aenter__(self) -> "JobLease":
        self._store._bind_owner(self.token)
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self._store.release_run(self)


def _job(row: sqlite3.Row) -> ManagedJob:
    return ManagedJob(
        row["job_id"],
        json.loads(row["request_json"]),
        json.loads(row["config_json"]),
        row["state"],
        row["total"],
        row["completed"],
        row["pending"],
        row["failed"],
        row["next_eligible_at"],
        row["retry_reason"],
        row["error"],
        json.loads(row["report_json"]) if row["report_json"] is not None else None,
        row["created_at"],
        row["updated_at"],
    )


class JobStore:
    def __init__(
        self,
        state_path: str | Path | None = None,
        *,
        clock: Callable[[], float] = time.time,
    ):
        self.state_path = resolve_state_path(state_path)
        self._clock = clock
        self._heartbeats: dict[str, asyncio.Task[None]] = {}
        self._heartbeat_owners: dict[str, asyncio.Task[Any]] = {}

    def _now(self) -> float:
        now = self._clock()
        if not math.isfinite(now):
            raise ValueError("clock must return a finite UTC timestamp")
        return now

    def _create(
        self, request_json: str, config_json: str, job_id: str, total: int
    ) -> ManagedJob:
        with transaction(self.state_path) as db:
            now = self._now()
            row = db.execute(
                "SELECT * FROM managed_jobs WHERE job_id=?", (job_id,)
            ).fetchone()
            if row is not None:
                if (
                    row["request_json"] != request_json
                    or row["config_json"] != config_json
                ):
                    raise ValueError(
                        "job_id already belongs to a different immutable request/config"
                    )
                return _job(row)
            db.execute(
                "INSERT INTO managed_jobs(job_id,request_json,config_json,state,total,pending,created_at,updated_at) VALUES(?,?,?,'queued',?,?,?,?)",
                (job_id, request_json, config_json, total, total, now, now),
            )
            return _job(
                db.execute(
                    "SELECT * FROM managed_jobs WHERE job_id=?", (job_id,)
                ).fetchone()
            )

    async def create(
        self, request: dict[str, Any], config: dict[str, Any], job_id: str | None = None
    ) -> ManagedJob:
        job_id = job_id or uuid.uuid4().hex
        if len(job_id) > 200 or any(ord(char) < 32 for char in job_id):
            raise ValueError("job_id must be a nonempty sanitized identity")
        total = request.get("total", 0)
        if isinstance(total, bool) or not isinstance(total, int) or total < 0:
            raise ValueError("job total must be a nonnegative integer")
        request_json = json.dumps(
            request, sort_keys=True, separators=(",", ":"), allow_nan=False
        )
        config_json = json.dumps(
            config, sort_keys=True, separators=(",", ":"), allow_nan=False
        )
        return await _durable_call(
            self._create, request_json, config_json, job_id, total
        )

    def _get(self, job_id: str) -> ManagedJob | None:
        with transaction(self.state_path) as db:
            row = db.execute(
                "SELECT * FROM managed_jobs WHERE job_id=?", (job_id,)
            ).fetchone()
            return _job(row) if row is not None else None

    async def get(self, job_id: str) -> ManagedJob | None:
        return await _durable_call(self._get, job_id)

    def _update(
        self, job_id: str, fields: dict[str, Any], lease: JobLease | None
    ) -> ManagedJob:
        with transaction(self.state_path) as db:
            row = db.execute(
                "SELECT * FROM managed_jobs WHERE job_id=?", (job_id,)
            ).fetchone()
            if row is None:
                raise KeyError(job_id)
            current = _job(row)
            now = max(self._now(), current.updated_at)
            if lease is not None:
                if (
                    lease.job_id != job_id
                    or row["run_token"] != lease.token
                    or row["run_lease_until"] <= now
                ):
                    raise JobLeaseLost(
                        "Job run ownership expired before its progress update"
                    )
            elif row["run_token"] is not None and row["run_lease_until"] > now:
                raise JobLeaseLost("An active job run requires its ownership token")
            values = current.to_dict()
            values.update(fields)
            if values["state"] not in JOB_STATES:
                raise ValueError("invalid managed job state")
            if current.state == "complete" and values["state"] != "complete":
                raise ValueError("a completed job cannot return to an incomplete state")
            for name in ("total", "completed", "pending", "failed"):
                value = values[name]
                if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                    raise ValueError(f"{name} must be a nonnegative integer")
            if values["total"] != current.total:
                raise ValueError("job total is part of its immutable request")
            if values["completed"] < current.completed:
                raise ValueError("completed durable progress cannot decrease")
            if (
                values["completed"] + values["pending"] != values["total"]
                or values["failed"] > values["pending"]
            ):
                raise ValueError(
                    "completed + pending must equal total; failed is a subset of pending"
                )
            if values["state"] == "complete" and (
                values["pending"] or values["failed"]
            ):
                raise ValueError("complete jobs cannot have pending or failed hours")
            deadline = values["next_eligible_at"]
            if deadline is not None and (not math.isfinite(deadline) or deadline < 0):
                raise ValueError("next_eligible_at must be a finite UTC timestamp")
            for name in ("retry_reason", "error"):
                if values[name] is not None and not isinstance(values[name], str):
                    raise ValueError(f"{name} must be a string or None")
            report = (
                json.dumps(values["report"], sort_keys=True, allow_nan=False)
                if values["report"] is not None
                else None
            )
            db.execute(
                "UPDATE managed_jobs SET state=?,completed=?,pending=?,failed=?,next_eligible_at=?,retry_reason=?,error=?,report_json=?,updated_at=? WHERE job_id=?",
                (
                    values["state"],
                    values["completed"],
                    values["pending"],
                    values["failed"],
                    deadline,
                    values["retry_reason"],
                    values["error"],
                    report,
                    now,
                    job_id,
                ),
            )
            return _job(
                db.execute(
                    "SELECT * FROM managed_jobs WHERE job_id=?", (job_id,)
                ).fetchone()
            )

    async def update(
        self, job_id: str, *, lease: JobLease | None = None, **fields: Any
    ) -> ManagedJob:
        allowed = {
            "state",
            "total",
            "completed",
            "pending",
            "failed",
            "next_eligible_at",
            "retry_reason",
            "error",
            "report",
        }
        if fields.keys() - allowed:
            raise ValueError("request, config and job identity are immutable")
        # JSON round-trip prevents a caller mutating a report during thread work.
        fields = json.loads(json.dumps(fields, allow_nan=False))
        return await _durable_call(self._update, job_id, fields, lease)

    def _claim_run(self, job_id: str, lease_seconds: float) -> JobLease | None:
        with transaction(self.state_path) as db:
            row = db.execute(
                "SELECT * FROM managed_jobs WHERE job_id=?", (job_id,)
            ).fetchone()
            if row is None:
                raise KeyError(job_id)
            now = max(self._now(), row["updated_at"])
            if row["state"] == "complete" or (
                row["run_token"] is not None and row["run_lease_until"] > now
            ):
                return None
            if (
                row["state"] in {"waiting_for_provider", "waiting_for_network"}
                and row["next_eligible_at"] is not None
                and row["next_eligible_at"] > now
            ):
                return None
            token = uuid.uuid4().hex
            deadline = now + lease_seconds
            db.execute(
                "UPDATE managed_jobs SET run_token=?,run_lease_until=? WHERE job_id=?",
                (token, deadline, job_id),
            )
            return JobLease(job_id, token, deadline, lease_seconds, self)

    async def claim_run(
        self, job_id: str, lease_seconds: float = 180.0
    ) -> JobLease | None:
        if not math.isfinite(lease_seconds) or lease_seconds <= 0:
            raise ValueError("lease_seconds must be finite and positive")
        task = asyncio.create_task(
            asyncio.to_thread(self._claim_run, job_id, lease_seconds)
        )
        try:
            lease = await asyncio.shield(task)
        except asyncio.CancelledError:
            lease = await task
            if lease is not None:
                await _durable_call(self._release_run, lease)
            raise
        if lease is not None:
            self._bind_owner(lease.token)

            def stop_owner() -> None:
                owner = self._heartbeat_owners.get(lease.token)
                if owner is not None and not owner.done():
                    owner.cancel()

            async def renew() -> None:
                try:
                    while True:
                        await asyncio.sleep(max(0.01, lease_seconds / 3))
                        if not await self.heartbeat_run(lease):
                            stop_owner()
                            return
                except (OSError, sqlite3.Error):
                    stop_owner()
                    return

            self._heartbeats[lease.token] = asyncio.create_task(
                renew(), name=f"tickvault-job-heartbeat-{lease.token}"
            )
        return lease

    def _bind_owner(self, token: str) -> None:
        owner = asyncio.current_task()
        if owner is not None:
            self._heartbeat_owners[token] = owner

    def _heartbeat_run(self, lease: JobLease) -> bool:
        with transaction(self.state_path) as db:
            now = self._now()
            updated = db.execute(
                "UPDATE managed_jobs SET run_lease_until=MAX(run_lease_until,?) WHERE job_id=? AND run_token=? AND run_lease_until>?",
                (now + lease.lease_seconds, lease.job_id, lease.token, now),
            )
            return updated.rowcount == 1

    async def heartbeat_run(self, lease: JobLease) -> bool:
        return await _durable_call(self._heartbeat_run, lease)

    def _release_run(self, lease: JobLease) -> None:
        with transaction(self.state_path) as db:
            db.execute(
                "UPDATE managed_jobs SET run_token=NULL,run_lease_until=NULL WHERE job_id=? AND run_token=?",
                (lease.job_id, lease.token),
            )

    async def release_run(self, lease: JobLease) -> None:
        self._heartbeat_owners.pop(lease.token, None)
        task = self._heartbeats.pop(lease.token, None)
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await _durable_call(self._release_run, lease)

    async def close(self) -> None:
        for token in tuple(self._heartbeats):
            self._heartbeat_owners.pop(token, None)
            task = self._heartbeats.pop(token)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

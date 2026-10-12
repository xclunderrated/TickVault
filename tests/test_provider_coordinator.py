"""Offline provider gates: fake time, independent SQLite owners and crash recovery."""

import asyncio
import lzma
import multiprocessing
import sqlite3
import struct
import threading
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime

import pytest

from tick_vault.provider import (
    HourClaimLost,
    ProviderCoordinator,
    ProviderDeferred,
    ProviderPolicy,
)

HOUR = datetime(2024, 1, 2, tzinfo=UTC)


class Clock:
    def __init__(self, now=1000.0):
        self.now = now
        self.lock = threading.Lock()

    def __call__(self):
        with self.lock:
            return self.now

    def advance(self, seconds):
        with self.lock:
            self.now += seconds

    async def sleep(self, seconds):
        self.advance(seconds)
        await asyncio.sleep(0)


def payload():
    return lzma.compress(
        struct.pack(">IIIff", 10, 110020, 110000, 1.0, 2.0), format=lzma.FORMAT_ALONE
    )


def policy(**kwargs):
    return ProviderPolicy(
        **({"request_interval": 0.0, "cooldown_jitter": 0.0} | kwargs)
    )


@pytest.mark.asyncio
async def test_cooldown_jitter_only_extends_the_server_deadline(tmp_path, monkeypatch):
    from tick_vault.provider import coordinator as module

    monkeypatch.setattr(module.random, "uniform", lambda low, high: 0.75)
    clock = Clock()
    owner = ProviderCoordinator(tmp_path, policy=policy(cooldown_jitter=1), clock=clock)
    async with owner:
        status = await owner.observe_rate_limit(10)
        assert status.cooldown_until == 1010.75
        clock.advance(10)
        with pytest.raises(ProviderDeferred):
            await owner.acquire(wait=False)
        clock.advance(0.75)
        lease = await owner.acquire(wait=False)
        assert lease.is_probe
        await owner.observe_success(lease)
        await owner.release(lease)


@pytest.mark.asyncio
async def test_pacing_budgets_and_no_refund_across_instances(tmp_path):
    clock = Clock()
    first = ProviderCoordinator(
        tmp_path,
        policy=policy(request_interval=2, minute_budget=2, hour_budget=3),
        clock=clock,
        sleep=clock.sleep,
    )
    other_vault = ProviderCoordinator(
        tmp_path,
        policy=policy(max_concurrency=10, minute_budget=100, hour_budget=100),
        clock=clock,
    )
    async with first, other_vault:
        lease = await first.acquire(wait=False)
        await first.release(lease)
        with pytest.raises(ProviderDeferred) as paused:
            await other_vault.acquire(wait=False)
        assert paused.value.reason == "pacing"
        assert paused.value.next_eligible_at == 1002
        second = await first.acquire(max_wait=1)
        assert second.started_at == 1002
        await first.release(second)
        clock.advance(2)
        with pytest.raises(ProviderDeferred) as minute:
            await other_vault.acquire(wait=False)
        assert minute.value.reason == "minute_budget"
        assert minute.value.next_eligible_at == 1060
        clock.advance(56)
        third = await first.acquire(wait=False)
        await first.release(third)
        clock.advance(62)
        with pytest.raises(ProviderDeferred) as hour:
            await other_vault.acquire(wait=False)
        assert hour.value.reason == "hour_budget"
        assert hour.value.next_eligible_at == 4600
        status = await other_vault.status()
        assert status.max_concurrency == 1
        assert status.minute_budget == 2
        assert status.hour_budget == 3
        assert status.hour_used == 3


@pytest.mark.asyncio
async def test_aggregate_cap_stale_lease_and_explicit_heartbeat(tmp_path):
    clock = Clock()
    owner = ProviderCoordinator(
        tmp_path, policy=policy(request_lease_seconds=9), clock=clock
    )
    peer = ProviderCoordinator(
        tmp_path, policy=policy(max_concurrency=8, request_lease_seconds=9), clock=clock
    )
    async with owner, peer:
        lease = await owner.acquire(wait=False)
        clock.advance(8)
        assert await owner.heartbeat(lease)
        clock.advance(2)
        with pytest.raises(ProviderDeferred) as capacity:
            await peer.acquire(wait=False)
        assert capacity.value.reason == "in_flight"
        # Closing models a crashed process: no heartbeat, lease still persisted.
        await owner.close()
        clock.advance(8)
        replacement = await peer.acquire(wait=False)
        assert replacement.token != lease.token
        await owner.release(lease)
        assert (await peer.status()).in_flight == 1
        await peer.release(replacement)


@pytest.mark.asyncio
async def test_cooldown_survives_restart_clock_rollback_and_one_probe(tmp_path):
    clock = Clock()
    owner = ProviderCoordinator(tmp_path, policy=policy(), clock=clock)
    lease = await owner.acquire(wait=False)
    await owner.observe_rate_limit(3600, lease=lease)
    await owner.release(lease)
    await owner.close()
    restarted = ProviderCoordinator(
        tmp_path, policy=policy(max_concurrency=20), clock=clock
    )
    async with restarted:
        clock.advance(-100)
        with pytest.raises(ProviderDeferred) as paused:
            await restarted.acquire()
        assert paused.value.reason == "cooldown"
        assert paused.value.next_eligible_at == 4600
        clock.advance(3700)
        outcomes = await asyncio.gather(
            *(restarted.acquire(wait=False) for _ in range(8)), return_exceptions=True
        )
        admitted = [
            outcome for outcome in outcomes if not isinstance(outcome, BaseException)
        ]
        assert len(admitted) == 1
        probe = admitted[0]
        assert probe.is_probe
        assert all(
            isinstance(outcome, ProviderDeferred)
            for outcome in outcomes
            if outcome is not probe
        )
        await restarted.observe_success(probe)
        await restarted.release(probe)
        assert (await restarted.status()).circuit == "closed"
        normal = await restarted.acquire(wait=False)
        assert not normal.is_probe
        await restarted.release(normal)


@pytest.mark.asyncio
async def test_probe_failure_or_crash_keeps_durable_fallback_cooldown(tmp_path):
    clock = Clock()
    coordinator = ProviderCoordinator(
        tmp_path,
        policy=policy(request_lease_seconds=9, no_header_cooldown=900),
        clock=clock,
    )
    async with coordinator:
        await coordinator.observe_rate_limit(None)
        assert (await coordinator.status()).cooldown_until == 1900
        clock.advance(900)
        probe = await coordinator.acquire(wait=False)
        await coordinator.release(probe)
        assert (await coordinator.status()).cooldown_until == 2800
        clock.advance(900)
        crashed = await coordinator.acquire(wait=False)
        await coordinator.close()
        clock.advance(9)
        assert (await coordinator.status()).cooldown_until == 3709
        with pytest.raises(ProviderDeferred):
            await coordinator.acquire(wait=False)
        # A stale probe success cannot close a later circuit opening.
        await coordinator.observe_success(crashed)
        assert (await coordinator.status()).circuit == "open"


@pytest.mark.asyncio
async def test_late_success_cannot_clear_throttle_and_new_policy_cannot_relax(tmp_path):
    clock = Clock()
    initial = ProviderCoordinator(
        tmp_path, policy=policy(request_interval=2), clock=clock
    )
    async with initial:
        lease = await initial.acquire(wait=False)
        await initial.observe_rate_limit(0)
        await initial.observe_success(lease)
        await initial.release(lease)
        assert (await initial.status()).request_interval == 4
        assert (await initial.status()).circuit == "open"
        stricter = ProviderCoordinator(
            tmp_path, policy=policy(request_interval=10), clock=clock
        )
        async with stricter:
            clock.advance(4)
            with pytest.raises(ProviderDeferred) as pacing:
                await stricter.acquire(wait=False)
            assert pacing.value.next_eligible_at == 1010
            clock.advance(6)
            lease = await initial.acquire(wait=False)
            assert lease.is_probe
            await initial.observe_success(lease)
            await initial.release(lease)
            assert (await initial.status()).request_interval == 10


@pytest.mark.asyncio
async def test_sqlite_wait_and_artifact_validation_never_block_event_loop(tmp_path):
    coordinator = ProviderCoordinator(tmp_path, policy=policy())
    async with coordinator:
        await coordinator.status()
        connection = sqlite3.connect(coordinator.state_path, isolation_level=None)
        connection.execute("BEGIN IMMEDIATE")
        task = asyncio.create_task(coordinator.acquire())
        beats = 0
        try:
            for _ in range(6):
                await asyncio.sleep(0.01)
                beats += 1
            assert not task.done()
        finally:
            connection.rollback()
            connection.close()
        lease = await task
        await coordinator.release(lease)
        assert beats == 6


@pytest.mark.asyncio
async def test_cancel_during_thread_admission_releases_lease_without_refund(
    tmp_path, monkeypatch
):
    coordinator = ProviderCoordinator(tmp_path, policy=policy())
    entered = threading.Event()
    proceed = threading.Event()
    original = coordinator._try_acquire

    def delayed():
        entered.set()
        assert proceed.wait(2)
        return original()

    monkeypatch.setattr(coordinator, "_try_acquire", delayed)
    task = asyncio.create_task(coordinator.acquire())
    while not entered.is_set():
        await asyncio.sleep(0.001)
    task.cancel()
    proceed.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    status = await coordinator.status()
    assert status.in_flight == 0
    assert status.minute_used == 1
    await coordinator.close()


@pytest.mark.asyncio
async def test_hour_single_flight_shared_immutable_receipt_and_corruption_recovery(
    tmp_path,
):
    clock = Clock()
    owner = ProviderCoordinator(tmp_path / "state", policy=policy(), clock=clock)
    peer = ProviderCoordinator(tmp_path / "state", policy=policy(), clock=clock)
    artifact = tmp_path / "owner-vault.bi5"
    artifact.write_bytes(payload())
    async with owner, peer:
        claims = await asyncio.gather(
            *(
                coordinator.claim_hour("EURUSD", HOUR)
                for coordinator in [owner, peer] * 5
            )
        )
        owned = [claim for claim in claims if claim.owned]
        assert len(owned) == 1
        claim = owned[0]
        receipt = await claim._coordinator.publish_hour(claim, artifact)
        assert receipt.artifact_path != artifact
        artifact.unlink()
        shared = await peer.claim_hour("EURUSD", HOUR)
        assert shared.state == "complete"
        assert shared.artifact_path.read_bytes() == payload()
        shared.artifact_path.write_bytes(b"corrupt")
        replacement = await owner.claim_hour("EURUSD", HOUR)
        assert replacement.owned
        artifact.write_bytes(payload())
        await owner.publish_hour(replacement, artifact)
        assert (await peer.claim_hour("EURUSD", HOUR)).state == "complete"


@pytest.mark.asyncio
async def test_hour_claim_recovery_negative_expiry_and_stale_owner_protection(tmp_path):
    clock = Clock()
    owner = ProviderCoordinator(
        tmp_path, policy=policy(hour_lease_seconds=9), clock=clock
    )
    peer = ProviderCoordinator(
        tmp_path, policy=policy(hour_lease_seconds=9), clock=clock
    )
    async with owner, peer:
        stale = await owner.claim_hour("EURUSD", HOUR)
        clock.advance(8)
        assert await owner.heartbeat_hour(stale)
        clock.advance(2)
        assert (await peer.claim_hour("EURUSD", HOUR)).state == "pending"
        await owner.close()
        clock.advance(8)
        fresh = await peer.claim_hour("EURUSD", HOUR)
        assert fresh.owned
        with pytest.raises(HourClaimLost):
            await owner.publish_hour(stale, result="no_data")
        await owner.release_hour(stale)
        assert (await peer.claim_hour("EURUSD", HOUR)).state == "pending"
        await peer.publish_hour(fresh, result="no_data")
        assert (
            await owner.claim_hour("EURUSD", HOUR, max_no_data_age=10)
        ).state == "complete"
        clock.advance(10)
        expired = await owner.claim_hour("EURUSD", HOUR, max_no_data_age=10)
        assert expired.owned
        await owner.publish_hour(expired, result="no_data")
        forced = await peer.claim_hour("EURUSD", HOUR, force_refresh=True)
        assert forced.owned
        await peer.release_hour(forced)


@pytest.mark.asyncio
async def test_automatic_request_and_hour_heartbeats(tmp_path):
    coordinator = ProviderCoordinator(
        tmp_path, policy=policy(request_lease_seconds=0.15, hour_lease_seconds=0.15)
    )
    peer = ProviderCoordinator(
        tmp_path, policy=policy(request_lease_seconds=0.15, hour_lease_seconds=0.15)
    )
    async with coordinator, peer:
        async with await coordinator.acquire() as lease:
            async with await coordinator.claim_hour("EURUSD", HOUR) as claim:
                await asyncio.sleep(0.35)
                with pytest.raises(ProviderDeferred):
                    await peer.acquire(wait=False)
                assert (await peer.claim_hour("EURUSD", HOUR)).state == "pending"
                assert await coordinator.heartbeat(lease)
                assert await coordinator.heartbeat_hour(claim)
        assert (await peer.status()).in_flight == 0
        fresh = await peer.claim_hour("EURUSD", HOUR)
        assert fresh.owned
        await peer.release_hour(fresh)


@pytest.mark.asyncio
async def test_source_access_proof_is_nonempty_persisted_and_endpoint_scoped(tmp_path):
    coordinator = ProviderCoordinator(tmp_path, policy=policy())
    assert not await coordinator.has_validated_access("endpoint-one")
    with pytest.raises(ValueError):
        await coordinator.record_validated_payload(
            "EURUSD", HOUR, 0, source_key="endpoint-one"
        )
    await coordinator.record_validated_payload(
        "EURUSD", HOUR, 1, source_key="endpoint-one"
    )
    restarted = ProviderCoordinator(tmp_path, policy=policy())
    assert await restarted.has_validated_access("endpoint-one")
    assert not await restarted.has_validated_access("endpoint-two")
    assert (await restarted.status()).access_verified_at is not None
    with pytest.raises(FrozenInstanceError):
        coordinator.policy.max_concurrency = 5


@pytest.mark.asyncio
async def test_lost_request_heartbeat_stops_owner_and_releases_capacity(
    tmp_path, monkeypatch
):
    coordinator = ProviderCoordinator(
        tmp_path, policy=policy(request_lease_seconds=0.06)
    )
    acquired = asyncio.Event()

    async def lost(lease):
        return False

    monkeypatch.setattr(coordinator, "heartbeat", lost)

    async def work():
        async with await coordinator.acquire():
            acquired.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(work())
    await acquired.wait()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 1)
    assert (await coordinator.status()).in_flight == 0
    await coordinator.close()


@pytest.mark.asyncio
async def test_wait_budget_and_stop_event_release_waiters_without_new_attempt(tmp_path):
    coordinator = ProviderCoordinator(tmp_path, policy=policy())
    async with coordinator:
        lease = await coordinator.acquire()
        with pytest.raises(ProviderDeferred) as paused:
            await coordinator.acquire(max_wait=0.03)
        assert paused.value.reason == "in_flight"
        stop = asyncio.Event()
        waiter = asyncio.create_task(coordinator.acquire(stop_event=stop))
        await asyncio.sleep(0.01)
        stop.set()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        assert (await coordinator.status()).minute_used == 1
        await coordinator.release(lease)


@pytest.mark.asyncio
async def test_republishing_identical_valid_cache_does_not_replace_open_object(
    tmp_path, monkeypatch
):
    coordinator = ProviderCoordinator(tmp_path / "state", policy=policy())
    artifact = tmp_path / "raw.bi5"
    artifact.write_bytes(payload())
    async with coordinator:
        first = await coordinator.claim_hour("EURUSD", HOUR)
        receipt = await coordinator.publish_hour(first, artifact)

        def cannot_replace(*args):
            raise AssertionError("valid cached object must be reused")

        monkeypatch.setattr(
            "tick_vault.provider.coordinator.os.replace", cannot_replace
        )
        another = await coordinator.claim_hour("USDJPY", HOUR)
        shared = await coordinator.publish_hour(another, artifact)
        assert shared.artifact_path == receipt.artifact_path
        assert shared.digest == receipt.digest


def _independent_process(state_directory, ready, output):
    async def run():
        coordinator = ProviderCoordinator(
            state_directory, policy=policy(minute_budget=1), clock=lambda: 1000.0
        )
        ready.wait(5)
        try:
            lease = await coordinator.acquire(wait=False)
            output.put(("admitted", lease.started_at))
            await coordinator.release(lease)
        except ProviderDeferred as deferred:
            output.put(("deferred", deferred.reason))
        finally:
            await coordinator.close()

    asyncio.run(run())


def test_independent_processes_share_rolling_budget(tmp_path):
    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    output = context.Queue()
    processes = [
        context.Process(
            target=_independent_process, args=(str(tmp_path), ready, output)
        )
        for _ in range(2)
    ]
    for process in processes:
        process.start()
    ready.set()
    try:
        outcomes = [output.get(timeout=10) for _ in processes]
        assert sum(outcome[0] == "admitted" for outcome in outcomes) == 1
        assert sum(outcome[0] == "deferred" for outcome in outcomes) == 1
    finally:
        for process in processes:
            process.join(timeout=5)
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
            assert process.exitcode == 0
        output.close()


def _process_hour_owner(state_directory, artifact, ready, output):
    async def run():
        coordinator = ProviderCoordinator(
            state_directory, policy=policy(), clock=lambda: 1000.0
        )
        ready.wait(5)
        try:
            claim = await coordinator.claim_hour("EURUSD", HOUR)
            if claim.owned:
                await coordinator.publish_hour(claim, artifact)
            output.put(claim.state)
        finally:
            await coordinator.close()

    asyncio.run(run())


def test_independent_processes_share_one_validated_hour_receipt(tmp_path):
    artifact = tmp_path / "owner-raw.bi5"
    artifact.write_bytes(payload())
    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    output = context.Queue()
    state_directory = tmp_path / "state"
    processes = [
        context.Process(
            target=_process_hour_owner,
            args=(str(state_directory), str(artifact), ready, output),
        )
        for _ in range(2)
    ]
    for process in processes:
        process.start()
    ready.set()
    try:
        outcomes = [output.get(timeout=10) for _ in processes]
        assert outcomes.count("owned") == 1
        assert all(state in {"owned", "pending", "complete"} for state in outcomes)
    finally:
        for process in processes:
            process.join(timeout=5)
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
            assert process.exitcode == 0
        output.close()
    artifact.unlink()

    async def check():
        coordinator = ProviderCoordinator(
            state_directory, policy=policy(), clock=lambda: 1000.0
        )
        receipt = await coordinator.claim_hour("EURUSD", HOUR)
        assert receipt.state == "complete"
        assert receipt.artifact_path.read_bytes() == payload()

    asyncio.run(check())

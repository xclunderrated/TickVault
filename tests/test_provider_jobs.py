import asyncio

import pytest
from test_provider_coordinator import Clock

from tick_vault.provider import JobLeaseLost, JobStore


@pytest.mark.asyncio
async def test_job_persistence_immutable_snapshot_and_complete_truth(tmp_path):
    clock = Clock()
    store = JobStore(tmp_path, clock=clock)
    request = {"symbol": "EURUSD", "total": 3}
    config = {"fetch_max_retry_attempts": 0, "base_directory": "vault-one"}
    job = await store.create(request, config, job_id="durable-job")
    request["symbol"] = "USDJPY"
    config["fetch_max_retry_attempts"] = 10
    assert job.request["symbol"] == "EURUSD"
    assert job.config["fetch_max_retry_attempts"] == 0
    assert not job.complete
    assert job.pending == 3
    waiting = await store.update(
        job.job_id,
        state="waiting_for_provider",
        completed=1,
        pending=2,
        failed=1,
        next_eligible_at=4600,
        retry_reason="cooldown",
    )
    assert not waiting.to_dict()["complete"]
    restarted = JobStore(tmp_path, clock=clock)
    persisted = await restarted.get(job.job_id)
    assert persisted == waiting
    assert persisted.next_eligible_at == 4600
    with pytest.raises(ValueError):
        await store.update(job.job_id, config={})
    with pytest.raises(ValueError):
        await store.update(job.job_id, state="complete")
    with pytest.raises(ValueError):
        await store.update(job.job_id, completed=0, pending=3)
    complete = await restarted.update(
        job.job_id,
        state="complete",
        completed=3,
        pending=0,
        failed=0,
        next_eligible_at=None,
        retry_reason=None,
    )
    assert complete.to_dict()["complete"]
    with pytest.raises(ValueError):
        await store.update(job.job_id, state="running")


@pytest.mark.asyncio
async def test_only_one_run_owner_and_stale_release_cannot_clear_new_owner(tmp_path):
    clock = Clock()
    first = JobStore(tmp_path, clock=clock)
    second = JobStore(tmp_path, clock=clock)
    job = await first.create({"total": 1}, {})
    leases = await asyncio.gather(
        first.claim_run(job.job_id, 9), second.claim_run(job.job_id, 9)
    )
    owners = [lease for lease in leases if lease is not None]
    assert len(owners) == 1
    stale = owners[0]
    clock.advance(8)
    assert await stale._store.heartbeat_run(stale)
    clock.advance(2)
    assert await second.claim_run(job.job_id, 9) is None
    await first.close()
    await second.close()
    clock.advance(8)
    fresh = await second.claim_run(job.job_id, 9)
    assert fresh is not None
    await stale._store.release_run(stale)
    assert await first.claim_run(job.job_id, 9) is None
    await second.release_run(fresh)
    assert await first.claim_run(job.job_id, 9) is not None
    await first.close()
    await second.close()


@pytest.mark.asyncio
async def test_job_run_context_renews_long_work_and_releases(tmp_path):
    owner = JobStore(tmp_path)
    peer = JobStore(tmp_path)
    job = await owner.create({"total": 1}, {})
    async with await owner.claim_run(job.job_id, 0.15):
        await asyncio.sleep(0.35)
        assert await peer.claim_run(job.job_id, 0.15) is None
    lease = await peer.claim_run(job.job_id)
    assert lease is not None
    await peer.release_run(lease)
    await owner.close()
    await peer.close()


@pytest.mark.asyncio
async def test_stale_run_cannot_write_after_recovery_and_wait_deadline_is_durable(
    tmp_path,
):
    clock = Clock()
    owner = JobStore(tmp_path, clock=clock)
    peer = JobStore(tmp_path, clock=clock)
    job = await owner.create({"total": 2}, {})
    stale = await owner.claim_run(job.job_id, 9)
    await owner.update(job.job_id, lease=stale, state="running", completed=1, pending=1)
    with pytest.raises(JobLeaseLost):
        await peer.update(job.job_id, state="paused")
    await owner.close()
    clock.advance(10)
    fresh = await peer.claim_run(job.job_id, 9)
    with pytest.raises(JobLeaseLost):
        await owner.update(
            job.job_id, lease=stale, state="complete", completed=2, pending=0
        )
    waiting = await peer.update(
        job.job_id,
        lease=fresh,
        state="waiting_for_provider",
        next_eligible_at=1900,
        retry_reason="cooldown",
    )
    await peer.release_run(fresh)
    assert await owner.claim_run(job.job_id) is None
    assert (await owner.get(job.job_id)) == waiting
    clock.advance(890)
    eligible = await owner.claim_run(job.job_id)
    assert eligible is not None
    await owner.release_run(eligible)
    await peer.close()


@pytest.mark.asyncio
async def test_lost_job_heartbeat_stops_the_run_owner(tmp_path, monkeypatch):
    store = JobStore(tmp_path)
    job = await store.create({"total": 1}, {})
    entered = asyncio.Event()

    async def lost(lease):
        return False

    monkeypatch.setattr(store, "heartbeat_run", lost)

    async def work():
        async with await store.claim_run(job.job_id, 0.06):
            entered.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(work())
    await entered.wait()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 1)
    lease = await store.claim_run(job.job_id)
    assert lease is not None
    await store.release_run(lease)
    await store.close()

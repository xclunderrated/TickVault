import errno
import multiprocessing
import time

import pytest

from tick_vault.core.filelock import destination_lock, publish_no_clobber


def _hold_destination(path, acquired):
    with destination_lock(path, timeout=5):
        acquired.set()
        # Do not terminate a process while it waits on a multiprocessing Event:
        # it may leave that Event's internal semaphore owned on Windows.
        time.sleep(30)


def _race_publication(temporary, destination, ready, publish, results):
    ready.set()
    publish.wait(10)
    try:
        publish_no_clobber(temporary, destination)
        results.put("published")
    except FileExistsError:
        results.put("exists")


def test_no_clobber_refuses_destination_created_after_initial_check(tmp_path):
    destination = tmp_path / "ticks.csv"
    temporary = tmp_path / ".ticks.tmp"
    temporary.write_bytes(b"completed export")
    assert not destination.exists()
    destination.write_bytes(b"external creator")
    with pytest.raises(FileExistsError):
        publish_no_clobber(temporary, destination)
    assert destination.read_bytes() == b"external creator"
    assert temporary.read_bytes() == b"completed export"


def test_no_clobber_cross_process_race_has_one_winner(tmp_path):
    context = multiprocessing.get_context("spawn")
    destination = tmp_path / "ticks.csv"
    temporaries = [tmp_path / ".first.tmp", tmp_path / ".second.tmp"]
    for number, temporary in enumerate(temporaries):
        temporary.write_bytes(f"export {number}".encode())
    ready = [context.Event(), context.Event()]
    publish = context.Event()
    results = context.Queue()
    processes = [
        context.Process(
            target=_race_publication,
            args=(temporary, destination, ready[index], publish, results),
        )
        for index, temporary in enumerate(temporaries)
    ]
    try:
        for process in processes:
            process.start()
        assert all(event.wait(15) for event in ready)
        publish.set()
        assert sorted(results.get(timeout=15) for _ in processes) == [
            "exists",
            "published",
        ]
        for process in processes:
            process.join(15)
            assert process.exitcode == 0
        assert destination.read_bytes() in (b"export 0", b"export 1")
        assert sum(temporary.exists() for temporary in temporaries) == 1
    finally:
        publish.set()
        for process in processes:
            if process.is_alive():
                process.terminate()
            process.join(15)
        results.close()


@pytest.mark.parametrize("error", [errno.ENOSPC, errno.EACCES])
def test_no_clobber_publication_failure_preserves_complete_temporary(
    tmp_path, monkeypatch, error
):
    destination = tmp_path / "ticks.csv"
    temporary = tmp_path / ".ticks.tmp"
    temporary.write_bytes(b"completed export")

    def failed_link(*args, **kwargs):
        raise OSError(error, "simulated publication failure")

    monkeypatch.setattr("tick_vault.core.filelock.os.link", failed_link)
    with pytest.raises(OSError, match="simulated publication failure"):
        publish_no_clobber(temporary, destination)
    assert not destination.exists()
    assert temporary.read_bytes() == b"completed export"


def test_no_clobber_success_consumes_sibling_temporary(tmp_path):
    temporary = tmp_path / ".ticks.tmp"
    destination = tmp_path / "ticks.csv"
    temporary.write_bytes(b"completed export")
    publish_no_clobber(temporary, destination)
    assert destination.read_bytes() == b"completed export"
    assert not temporary.exists()
    with pytest.raises(ValueError, match="same-directory"):
        publish_no_clobber(tmp_path / "elsewhere" / "temporary", destination)


def test_destination_lock_cross_process_timeout_and_process_cleanup(tmp_path):
    context = multiprocessing.get_context("spawn")
    destination = tmp_path / "ticks.db"
    acquired = context.Event()
    process = context.Process(target=_hold_destination, args=(destination, acquired))
    process.start()
    try:
        assert acquired.wait(15)
        started = time.monotonic()
        with pytest.raises(TimeoutError, match="ticks.db"):
            with destination_lock(destination, timeout=0.2):
                pytest.fail("a second process acquired an owned destination")
        elapsed = time.monotonic() - started
        assert 0.18 <= elapsed < 3
        process.terminate()
        process.join(15)
        with destination_lock(destination, timeout=1):
            pass
    finally:
        if process.is_alive():
            process.terminate()
        process.join(15)


def test_destination_lock_releases_on_exception_and_checks_timeout(tmp_path):
    destination = tmp_path / "ticks.db"
    with pytest.raises(KeyboardInterrupt):
        with destination_lock(destination):
            raise KeyboardInterrupt
    with destination_lock(destination, timeout=0):
        pass
    for timeout in (-1, float("inf"), float("nan")):
        with pytest.raises(ValueError, match="finite non-negative"):
            with destination_lock(destination, timeout=timeout):
                pass

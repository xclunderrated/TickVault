"""Cooperative process locks and atomic publication without overwriting files."""

import errno
import math
import os
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def destination_lock(
    path: str | Path, *, shared: bool = False, timeout: float = 30.0
) -> Iterator[None]:
    """Hold a destination lock across its snapshot, use and publication.

    The sidecar remains on disk: deleting it could let another process lock a
    different inode while a waiter still owns the old one. OS locks are released
    on normal exit, exceptions and process termination. POSIX readers can share
    a lock; Windows uses an exclusive byte lock for readers and writers so open
    reader handles cannot prevent a cooperating writer's atomic replacement.
    """
    if not math.isfinite(timeout) or timeout < 0:
        raise ValueError("lock timeout must be a finite non-negative number")
    destination = Path(path).resolve()
    lock_path = destination.with_name("." + destination.name + ".tick-vault.lock")
    deadline = time.monotonic() + timeout
    with lock_path.open("a+b") as handle:
        # msvcrt locks a byte region; initialize the persistent sidecar once.
        if os.fstat(handle.fileno()).st_size == 0:
            handle.write(b"\0")
            handle.flush()
        if sys.platform == "win32":
            import msvcrt

            def acquire() -> None:
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)

            def release() -> None:
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)

        else:
            import fcntl

            def acquire() -> None:
                operation = fcntl.LOCK_SH if shared else fcntl.LOCK_EX
                fcntl.flock(handle.fileno(), operation | fcntl.LOCK_NB)

            def release() -> None:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

        while True:
            try:
                acquire()
                break
            except OSError as exc:
                if exc.errno not in (errno.EACCES, errno.EAGAIN, errno.EDEADLK):
                    raise
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        f"Timed out after {timeout:g}s waiting for destination lock: {destination}"
                    ) from exc
                time.sleep(min(0.05, remaining))
        try:
            yield
        finally:
            release()


def publish_no_clobber(temporary: str | Path, destination: str | Path) -> None:
    """Atomically publish a sibling temporary file only if destination is absent.

    A hard link creates the destination directory entry atomically and refuses
    any existing entry, including one created after an earlier existence check.
    Both paths must be in the same directory. Filesystems without hard-link
    support fail safely rather than falling back to an overwriting rename.
    Callers remain responsible for flushing file content before publication and
    removing their temporary file after a failure.
    """
    temporary, destination = Path(temporary), Path(destination)
    if temporary.parent.resolve() != destination.parent.resolve():
        raise ValueError("Atomic publication requires a same-directory temporary file")
    if temporary.absolute() == destination.absolute():
        raise ValueError("Temporary file and destination must be different paths")
    os.link(temporary, destination)
    temporary.unlink()

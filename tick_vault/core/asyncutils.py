"""Offload owned disk operations without leaving them running after cancellation."""

import asyncio
from collections.abc import Callable
from typing import ParamSpec, TypeVar

P = ParamSpec("P")
T = TypeVar("T")


async def run_blocking(
    function: Callable[P, T], *args: P.args, **kwargs: P.kwargs
) -> T:
    """Keep the event loop free and finish the owned operation before exiting."""
    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        await task
        raise

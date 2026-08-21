import time
from typing import Awaitable, TypeVar

T = TypeVar("T")


async def timed(coro: Awaitable[T]) -> tuple[T, float]:
    """Wraps a single coroutine and returns (result, duration_ms), measured
    with a monotonic clock - never datetime subtraction, which is unsuitable
    for performance timing (not guaranteed monotonic, affected by clock
    adjustments)."""
    start = time.monotonic()
    result = await coro
    duration_ms = (time.monotonic() - start) * 1000
    return result, duration_ms

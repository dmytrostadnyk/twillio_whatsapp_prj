"""
Token bucket rate limiter for outbound Twilio API calls.

WHY token bucket instead of a fixed-window counter:
A fixed window allows a burst right at the boundary — e.g. 5 calls in the
last second of minute 1, then 5 more in the first second of minute 2 = 10
requests in 2 seconds. A token bucket prevents this by refilling tokens
continuously, so there's no boundary to exploit.

WHY in-memory and not Redis-backed:
This service runs as a single process in dev/demo. A single in-memory bucket
is exactly correct for one process. If you scale horizontally, each instance
gets its own bucket, which multiplies the effective limit. At that point,
replace this with a Redis-backed distributed counter. That change is isolated
to this module — callers see the same interface.

WHY asyncio.Lock instead of threading.Lock:
This runs inside an async event loop. asyncio.Lock never blocks the event loop;
it suspends the coroutine until the lock is free. threading.Lock would block the
thread, freezing all other coroutines until it's released.
"""

from __future__ import annotations

import asyncio
import time


class RateLimitExceededError(Exception):
    """Raised by TokenBucket.consume() when the bucket is empty."""


class TokenBucket:
    """
    Async-safe continuous-refill token bucket.

    capacity:     max tokens stored (= max burst size).
    refill_rate:  tokens added per second. Pass (capacity / 60) for a
                  "capacity requests per minute" limit.

    The bucket starts full. Each consume() takes one token after first
    topping up from elapsed time. When empty, consume() raises RateLimitExceeded.
    """

    def __init__(self, capacity: int, refill_rate: float) -> None:
        self._capacity = capacity
        self._refill_rate = refill_rate
        self._tokens = float(capacity)
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def consume(self) -> None:
        """
        Consume one token, refilling from elapsed wall-clock time first.
        Raises RateLimitExceeded if fewer than one token is available.
        """
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens = min(
                float(self._capacity),
                self._tokens + elapsed * self._refill_rate,
            )
            self._last_refill = now

            if self._tokens < 1.0:
                raise RateLimitExceededError(
                    f"Outbound rate limit exhausted. "
                    f"Capacity: {self._capacity} requests/minute. "
                    "Try again in a few seconds."
                )
            self._tokens -= 1.0

    @property
    def available(self) -> float:
        """Approximate current token count — for observability and tests only."""
        return self._tokens

"""An event loop on a thread of its own, for adapters built on asyncio clients.

The control plane serves its API, runs every agent and keeps its Kafka consumer's heartbeats on one
event loop, so nothing on that loop may wait on the network. The Kubernetes and Kafka clients are
asyncio clients, and each adapter runs them on a `BackgroundLoop` of its own, where their
connections live: a synchronous caller blocks until a call answers (`BackgroundLoop.run`), and a
coroutine awaits it without holding its own loop (`BackgroundLoop.run_async`).

The adapters give every wait a deadline. When it passes, the caller stops waiting and the call is
cancelled on the background loop, so a server that never answers costs the deadline, not forever.
"""

from __future__ import annotations

import asyncio
import atexit
from collections.abc import Coroutine
from concurrent.futures import Future
import threading
from typing import Any, TypeVar

T = TypeVar("T")


class AdapterTimeoutError(TimeoutError):
    """A call to Kubernetes or Kafka that did not finish within its deadline, and was cancelled.

    A `TimeoutError` and never a `ValueError`: the workflow records the step as failed rather than
    treating the work event as a contract violation.
    """

    def __init__(self, call: str, timeout: float) -> None:
        self.call = call
        self.timeout = timeout
        super().__init__(f"{call} timed out after {timeout:g}s")


class BackgroundLoop:
    """Run coroutines on an event loop of their own, on a daemon thread."""

    def __init__(self, *, name: str) -> None:
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run_loop, name=name, daemon=True)
        self._thread.start()
        atexit.register(self.close)

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def submit(self, coroutine: Coroutine[Any, Any, T]) -> Future[T]:
        """Schedule a coroutine on the background loop without waiting for it.

        Callers on another event loop await it with `asyncio.wrap_future`, which keeps that loop
        serving requests meanwhile. Cancelling the returned future cancels the coroutine.
        """
        return asyncio.run_coroutine_threadsafe(coroutine, self._loop)

    def run(
        self,
        coroutine: Coroutine[Any, Any, T],
        *,
        timeout: float | None = None,
        description: str | None = None,
    ) -> T:
        """Block until the coroutine finishes, or for at most `timeout` seconds when one is given.

        At the deadline the coroutine is cancelled and `AdapterTimeoutError` raised, naming the call
        by `description`. Without a timeout this waits as long as the coroutine takes.
        """
        future = self.submit(coroutine)
        if timeout is None:
            return future.result()
        try:
            return future.result(timeout=timeout)
        except TimeoutError:
            if future.cancel():
                raise AdapterTimeoutError(description or _describe(coroutine), timeout) from None
            # It finished as the deadline passed, or raised a TimeoutError of its own.
            return future.result()

    async def run_async(
        self,
        coroutine: Coroutine[Any, Any, T],
        *,
        timeout: float,
        description: str | None = None,
    ) -> T:
        """Await the coroutine from another event loop, for at most `timeout` seconds.

        The caller's loop keeps running meanwhile. At the deadline the coroutine is cancelled and
        `AdapterTimeoutError` raised; cancelling the caller cancels the coroutine too.
        """
        future = self.submit(coroutine)
        try:
            return await asyncio.wait_for(asyncio.wrap_future(future), timeout)
        except TimeoutError:
            # `wait_for` has cancelled the wrapper, and with it `future`, unless it had finished.
            if future.cancel():
                raise AdapterTimeoutError(description or _describe(coroutine), timeout) from None
            return future.result()

    def close(self) -> None:
        """Stop the loop and close it, so that a later call fails at once rather than never runs."""
        if not self._loop.is_running():
            return
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=1)
        if not self._thread.is_alive():
            self._loop.close()


async def call_adapter(client: Any, method: str, /, *args: Any, **kwargs: Any) -> Any:
    """Call an adapter's `method`, awaiting its `<method>_async` variant when it has one.

    The adapters wait on loops of their own, so awaiting the variant leaves the caller's loop free.
    A client without one, such as a stub or a test fake that answers at once, is called as it is
    and returns without giving up the loop.
    """
    variant = getattr(client, f"{method}_async", None)
    if variant is None:
        return getattr(client, method)(*args, **kwargs)
    return await variant(*args, **kwargs)


def _describe(coroutine: Coroutine[Any, Any, Any]) -> str:
    return getattr(coroutine, "__qualname__", None) or repr(coroutine)

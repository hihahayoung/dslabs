"""Interfaces (Protocols) that decouple algorithms from I/O and timers.

Why this matters for learning:
- Your algorithm code depends only on these minimal abstractions, so you can
  swap in the deterministic simulator or a real runtime without changes.
"""

from typing import Protocol, Callable, Dict, Any


class Transport(Protocol):
    """Message transport abstraction used by nodes.

    Implementations should be fire-and-forget (no return value). Delivery may
    be delayed, dropped, duplicated, or reordered depending on the runtime.
    """

    def send(self, to: str, msg: Dict[str, Any]) -> None:
        """Send `msg` to peer `to`.

        `msg` is a JSON-serializable dict. Implementations may add metadata
        (e.g., headers) but should preserve the payload.
        """
        raise NotImplemented

    def register(self, node_id: str, handler: Callable[[Dict[str, Any]], None]) -> None:
        """Register a receive handler for `node_id`.

        The handler is called with the decoded message dict upon delivery.
        """
        raise NotImplemented


class SchedulerCancel(Protocol):
    """Callable returned by `Scheduler.call_later` to cancel a pending event."""

    def __call__(self) -> None:
        raise NotImplemented


class Scheduler(Protocol):
    """Scheduler abstraction; used by nodes to schedule future work."""

    def call_later(self, ms: int, cb: Callable[[], None]) -> SchedulerCancel:
        """Schedule callback `cb` to run in `ms` milliseconds."""
        raise NotImplemented

    def now_ms(self) -> int:
        """Return current time in milliseconds for this scheduler domain."""
        raise NotImplemented

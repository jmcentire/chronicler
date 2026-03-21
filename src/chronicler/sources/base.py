"""Base source protocol and shared utilities."""

from __future__ import annotations

import inspect
import logging
from enum import Enum
from typing import Callable, Protocol, runtime_checkable

logger = logging.getLogger(__name__)


@runtime_checkable
class SourceProtocol(Protocol):
    """Protocol for event sources."""
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    def subscribe(self, callback: Callable) -> None: ...


class SourceState(str, Enum):
    """Lifecycle state of a source instance."""
    CREATED = "CREATED"
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"


async def emit(subscribers: list, event) -> None:
    """Shared utility: call each subscriber with the event, catching errors.

    Supports both sync and async callbacks.
    """
    for i, sub in enumerate(subscribers):
        try:
            result = sub(event)
            if inspect.isawaitable(result):
                await result
        except Exception as e:
            event_id = getattr(event, 'event_id', 'unknown')
            logger.error("Subscriber %d error for event %s: %s", i, event_id, e)

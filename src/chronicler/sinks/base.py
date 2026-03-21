"""Base sink protocol."""

from typing import Protocol, runtime_checkable


@runtime_checkable
class SinkProtocol(Protocol):
    """Protocol for story sinks."""
    async def start(self) -> None: ...
    async def emit(self, story) -> None: ...
    async def close(self) -> None: ...

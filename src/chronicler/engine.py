"""Top-level async orchestrator for Chronicler."""

from __future__ import annotations

import asyncio
import logging

logger = logging.getLogger(__name__)


class ChroniclerEngine:
    """Async orchestrator that manages sources, correlation, stories, and sinks."""

    def __init__(self, config=None):
        self._config = config
        self._phase = "CREATED"

    async def start(self) -> None:
        self._phase = "RUNNING"

    async def run(self) -> None:
        await self.start()
        # Main processing loop would go here
        await self.stop()

    async def stop(self) -> None:
        self._phase = "STOPPED"

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()
        return False

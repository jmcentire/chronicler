"""Stigmergy placeholder sink."""

from __future__ import annotations

import logging

logger = logging.getLogger("chronicler.sinks.stigmergy")


class StigmergySink:
    """Placeholder: logs stories that would be emitted as Signals to Stigmergy."""

    def __init__(self):
        self._started = False

    async def start(self) -> None:
        logger.debug("StigmergySink starting")
        self._started = True

    async def emit(self, story) -> None:
        story_id = getattr(story, 'story_id', 'unknown')
        story_type = getattr(story, 'story_type', 'unknown')
        logger.debug("StigmergySink would emit story %s (type=%s)", story_id, story_type)

    async def close(self) -> None:
        logger.debug("StigmergySink closing")
        self._started = False

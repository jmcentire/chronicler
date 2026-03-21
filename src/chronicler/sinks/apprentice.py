"""Apprentice placeholder sink."""

from __future__ import annotations

import logging

logger = logging.getLogger("chronicler.sinks.apprentice")


class ApprenticeSink:
    """Placeholder: logs stories that would be emitted as training sequences."""

    def __init__(self):
        self._started = False

    async def start(self) -> None:
        logger.debug("ApprenticeSink starting")
        self._started = True

    async def emit(self, story) -> None:
        story_id = getattr(story, 'story_id', 'unknown')
        event_count = len(getattr(story, 'events', []))
        logger.debug("ApprenticeSink would emit story %s (%d events)", story_id, event_count)

    async def close(self) -> None:
        logger.debug("ApprenticeSink closing")
        self._started = False

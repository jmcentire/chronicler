"""Kindex knowledge capture sink."""

from __future__ import annotations

import logging
from datetime import datetime

from pydantic import BaseModel, Field

logger = logging.getLogger("chronicler.sinks.kindex")


class KindexSinkConfig(BaseModel):
    """Configuration for the Kindex knowledge capture sink."""
    model_config = {"frozen": True}
    story_types: set[str] | None = None
    min_event_count: int = Field(default=1, ge=1)
    min_duration_seconds: float = Field(default=0.0, ge=0.0)


def is_noteworthy(story, config: KindexSinkConfig) -> bool:
    """Pure predicate: does this story meet the noteworthiness thresholds?"""
    # Check story_types filter
    if config.story_types is not None:
        if getattr(story, 'story_type', '') not in config.story_types:
            return False

    # Check min_event_count
    events = getattr(story, 'events', [])
    if len(events) < config.min_event_count:
        return False

    # Check min_duration_seconds
    opened_at = getattr(story, 'opened_at', None)
    closed_at = getattr(story, 'closed_at', None)
    if opened_at and closed_at:
        try:
            opened_dt = datetime.fromisoformat(str(opened_at))
            closed_dt = datetime.fromisoformat(str(closed_at))
            duration = (closed_dt - opened_dt).total_seconds()
            if duration < config.min_duration_seconds:
                return False
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid timestamps: {e}")

    return True


class KindexSink:
    """Filtered emission of noteworthy stories as knowledge nodes."""

    def __init__(self, config: KindexSinkConfig):
        self._config = config
        self._started = False

    async def start(self) -> None:
        logger.debug(
            "KindexSink starting with config: story_types=%s, "
            "min_event_count=%d, min_duration_seconds=%.1f",
            self._config.story_types,
            self._config.min_event_count,
            self._config.min_duration_seconds,
        )
        self._started = True

    async def emit(self, story) -> None:
        if is_noteworthy(story, self._config):
            story_id = getattr(story, 'story_id', 'unknown')
            story_type = getattr(story, 'story_type', 'unknown')
            logger.debug(
                "KindexSink would emit knowledge node for story %s (type=%s)",
                story_id, story_type,
            )
        else:
            logger.debug("KindexSink: story not noteworthy, skipping")

    async def close(self) -> None:
        logger.debug("KindexSink closing")
        self._started = False

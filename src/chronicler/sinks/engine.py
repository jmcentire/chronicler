"""Safe emit wrapper for the sink fan-out protocol."""

from __future__ import annotations

import logging

from chronicler.sinks.types import SafeEmitOutcome, SinkEmitResult

logger = logging.getLogger(__name__)


async def safe_emit(sink_name: str, sink_emit, story) -> SafeEmitOutcome:
    """Orchestrator wrapper that calls sink.emit(story) safely.

    Never raises -- all exceptions are captured in the outcome.
    """
    story_id = getattr(story, 'story_id', 'unknown')
    try:
        await sink_emit(story)
        return SafeEmitOutcome(
            sink_name=sink_name,
            story_id=story_id,
            result=SinkEmitResult.emitted,
            error_message="",
        )
    except Exception as e:
        logger.error("Sink '%s' error for story '%s': %s", sink_name, story_id, e)
        return SafeEmitOutcome(
            sink_name=sink_name,
            story_id=story_id,
            result=SinkEmitResult.error,
            error_message=str(e),
        )

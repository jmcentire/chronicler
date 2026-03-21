"""JSONL disk persistence sink."""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path

import aiofiles

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class DiskSinkConfig(BaseModel):
    """Frozen Pydantic configuration model for the disk JSONL sink."""
    model_config = {"frozen": True}
    path: str = Field(min_length=1)
    flush_on_write: bool = True


class DiskSink:
    """Writes closed stories as append-only JSONL."""

    def __init__(self, config: DiskSinkConfig):
        self._config = config
        self._file = None
        self._lock = asyncio.Lock()
        self._started = False
        self._closed = False

    async def start(self) -> None:
        if self._started:
            raise RuntimeError("DiskSink is already started")
        # Ensure parent directory exists (raise if not)
        parent = Path(self._config.path).parent
        if not parent.exists():
            raise OSError(f"Parent directory does not exist: {parent}")
        self._file = await aiofiles.open(self._config.path, mode='a')
        self._started = True
        self._closed = False

    async def emit(self, story) -> None:
        if not self._started or self._closed:
            raise RuntimeError("DiskSink is not started or has been closed")
        async with self._lock:
            data = story.model_dump(mode='json') if hasattr(story, 'model_dump') else _story_to_dict(story)
            line = json.dumps(data, ensure_ascii=True, default=str)
            await self._file.write(line + '\n')
            if self._config.flush_on_write:
                await self._file.flush()

    async def close(self) -> None:
        if not self._started:
            raise RuntimeError("DiskSink was never started")
        if self._file and not self._closed:
            await self._file.flush()
            await self._file.close()
            self._closed = True


def _story_to_dict(story) -> dict:
    """Fallback serialization for Story objects."""
    if hasattr(story, '__dict__'):
        return {k: v for k, v in story.__dict__.items() if not k.startswith('_')}
    return {}


async def read_stories(path: str):
    """Async generator that reads a JSONL file and yields deserialized Story objects."""
    from chronicler.schemas import Story

    if not Path(path).exists():
        raise FileNotFoundError(f"File not found: {path}")

    async with aiofiles.open(path, mode='r') as f:
        async for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                yield Story.model_validate(data)
            except Exception as e:
                logger.warning("Skipping corrupt JSONL line: %s", e)
                continue

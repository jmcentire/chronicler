"""JSONL file tailer source."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid

from pydantic import BaseModel, Field

from chronicler.sources.base import SourceState, emit
from chronicler.sources.webhook import Event, FilePath, FilePosition

logger = logging.getLogger(__name__)


class FileSourceConfig(BaseModel):
    """Configuration for the file tailer source."""
    model_config = {"frozen": True}
    path: str = Field(min_length=1)
    poll_interval_seconds: float = Field(ge=0.1, le=300.0)
    source_name: str = Field(min_length=1, max_length=128)


class FileSource:
    """JSONL file tailer using asyncio polling."""

    def __init__(self, config: FileSourceConfig | None = None):
        self._config = config
        self._subscribers: list = []
        self._state = SourceState.CREATED
        self._poll_task: asyncio.Task | None = None
        self._file_position: int = 0
        self._last_inode: int | None = None
        self._last_mtime: float = 0.0
        self._line_buffer: str = ""

    def subscribe(self, callback) -> None:
        self._subscribers.append(callback)

    async def start(self) -> None:
        if self._state == SourceState.STOPPED:
            raise RuntimeError("Cannot restart a stopped FileSource")
        if self._state == SourceState.RUNNING:
            raise RuntimeError("FileSource is already started")
        self._state = SourceState.RUNNING
        # Start at end of existing file
        if self._config:
            path = self._config.path
            if os.path.exists(path):
                stat = os.stat(path)
                self._file_position = stat.st_size
                self._last_inode = stat.st_ino
                self._last_mtime = stat.st_mtime
            self._poll_task = asyncio.create_task(self._poll_loop())
        logger.info("FileSource started")

    async def stop(self) -> None:
        if self._state == SourceState.STOPPED:
            return  # idempotent
        self._state = SourceState.STOPPED
        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
        logger.info("FileSource stopped")

    async def _poll_loop(self) -> None:
        """Poll the file for new lines."""
        path = self._config.path
        interval = self._config.poll_interval_seconds
        source_name = self._config.source_name

        while self._state == SourceState.RUNNING:
            try:
                if os.path.exists(path):
                    stat = os.stat(path)
                    current_size = stat.st_size
                    current_inode = stat.st_ino

                    current_mtime = stat.st_mtime

                    # Detect truncation or file replacement
                    if current_size < self._file_position:
                        self._file_position = 0
                        self._line_buffer = ""
                    elif (current_mtime != self._last_mtime
                          and current_size <= self._file_position
                          and self._file_position > 0):
                        # File was modified but didn't grow — likely
                        # truncated and rewritten to same or smaller size
                        self._file_position = 0
                        self._line_buffer = ""
                    if self._last_inode is not None and current_inode != self._last_inode:
                        self._file_position = 0
                        self._line_buffer = ""
                    self._last_inode = current_inode
                    self._last_mtime = current_mtime

                    if current_size > self._file_position:
                        with open(path, "r") as f:
                            f.seek(self._file_position)
                            raw = f.read()
                            self._file_position = f.tell()

                        # Handle partial lines
                        data = self._line_buffer + raw
                        lines = data.split("\n")
                        # Last element is either "" (if data ended with \n) or a partial line
                        self._line_buffer = lines[-1]  # carry over partial
                        complete_lines = lines[:-1]

                        for line in complete_lines:
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                event_data = json.loads(line)
                                event = Event.model_validate(event_data)
                                await emit(self._subscribers, event)
                            except Exception as e:
                                logger.debug("Skipping line: %s", e)
            except Exception as e:
                logger.error("FileSource poll error: %s", e)

            await asyncio.sleep(interval)

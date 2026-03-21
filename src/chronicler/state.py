"""JSONL persistence for Chronicler state."""

from __future__ import annotations

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


async def append_record(path: str, record: dict) -> None:
    """Append a single state record to the JSONL state file."""
    try:
        line = json.dumps(record, ensure_ascii=True, default=str)
        with open(path, 'a') as f:
            f.write(line + '\n')
            f.flush()
    except Exception as e:
        logger.error("I/O error writing state record: %s", e)

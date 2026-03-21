"""Shared types for the sinks module."""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


class FilePath(BaseModel):
    """A validated non-empty filesystem path string."""
    model_config = {"frozen": True}
    value: str = Field(min_length=1)

    def __init__(self, value: str = "", **kwargs):
        if 'value' not in kwargs and isinstance(value, str):
            kwargs['value'] = value
        # Reject whitespace-only
        val = kwargs.get('value', value)
        if isinstance(val, str) and val.strip() == '' and len(val) > 0:
            raise ValueError("FilePath must not be whitespace-only")
        if isinstance(val, str) and len(val) > 4096:
            raise ValueError("FilePath must not exceed 4096 characters")
        super().__init__(**kwargs)


class PositiveInt(BaseModel):
    """An integer >= 1."""
    model_config = {"frozen": True}
    value: int = Field(ge=1)

    def __init__(self, value: int = 1, **kwargs):
        if 'value' not in kwargs:
            kwargs['value'] = value
        super().__init__(**kwargs)


class NonNegativeFloat(BaseModel):
    """A float >= 0.0."""
    model_config = {"frozen": True}
    value: float = Field(ge=0.0)

    def __init__(self, value: float = 0.0, **kwargs):
        if 'value' not in kwargs:
            kwargs['value'] = value
        super().__init__(**kwargs)


class SinkName(str, Enum):
    """Canonical names for the four built-in sink implementations."""
    disk = "disk"
    stigmergy = "stigmergy"
    apprentice = "apprentice"
    kindex = "kindex"


class SinkEmitResult(str, Enum):
    """Outcome of a single sink emit attempt."""
    emitted = "emitted"
    filtered = "filtered"
    error = "error"


class SafeEmitOutcome(BaseModel):
    """Result record from the safe_emit wrapper."""
    model_config = {"frozen": True}
    sink_name: str
    story_id: str
    result: SinkEmitResult
    error_message: str = ""

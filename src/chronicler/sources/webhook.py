"""HTTP POST webhook event source."""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone

from pydantic import BaseModel, Field, field_validator

from chronicler.sources.base import SourceState, emit

logger = logging.getLogger(__name__)


# ── Config types ───────────────────────────────────────────────────────────

class BindAddress(BaseModel):
    """Bind address for an HTTP listener."""
    model_config = {"frozen": True}
    host: str
    port: int = Field(ge=0, le=65535)

    @field_validator("host")
    @classmethod
    def _validate_host(cls, v: str) -> str:
        import re
        # Allow IP addresses (v4, v6) and localhost
        ip4 = re.compile(r"^\d{1,3}(\.\d{1,3}){3}$")
        if v in ("::", "::1", "localhost", "127.0.0.1", "0.0.0.0") or ip4.match(v):
            return v
        raise ValueError(f"BindAddress host must be an IP address, got: {v!r}")


class WebhookSourceConfig(BaseModel):
    """Configuration for the webhook HTTP source."""
    model_config = {"frozen": True}
    bind: BindAddress
    client_max_size_bytes: int = Field(ge=1024, le=10485760)
    source_name: str = Field(min_length=1, max_length=128)


class HttpResponse(BaseModel):
    """Simple HTTP response descriptor."""
    model_config = {"frozen": True}
    status: int = Field(ge=100, le=599)
    body: str = ""


class EventKind(str):
    """Dot-separated event kind identifier for sources."""
    import re as _re
    _PATTERN = _re.compile(r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$")

    def __new__(cls, value: str = "", **kwargs) -> EventKind:
        if 'value' in kwargs:
            value = kwargs['value']
        if not cls._PATTERN.match(value):
            raise ValueError(
                f"EventKind must be dot-separated (2+ segments) lowercase: {value!r}"
            )
        instance = str.__new__(cls, value)
        instance.value = value
        return instance


class Event(BaseModel):
    """Minimal event model for source ingestion."""
    model_config = {"frozen": True}
    event_id: str
    event_kind: str
    timestamp: str
    source: str
    payload: dict = Field(default_factory=dict)
    correlation_keys: list | dict = Field(default_factory=list)

    @field_validator('event_id')
    @classmethod
    def _validate_event_id(cls, v: str) -> str:
        import re
        uuid4_pattern = re.compile(
            r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
        )
        if not uuid4_pattern.match(v):
            raise ValueError(f"event_id must be a valid UUID4: {v!r}")
        return v

    @field_validator('timestamp')
    @classmethod
    def _validate_timestamp(cls, v: str) -> str:
        import re
        # Must be ISO 8601 with timezone (Z or +/-offset)
        ts_pattern = re.compile(
            r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$'
        )
        if not ts_pattern.match(v):
            raise ValueError(f"timestamp must be ISO 8601 with timezone: {v!r}")
        return v


class FilePath(str):
    """A validated non-empty filesystem path string (max 4096 chars)."""
    def __new__(cls, value: str = "", **kwargs) -> FilePath:
        if 'value' in kwargs:
            value = kwargs['value']
        if not value:
            raise ValueError("FilePath must not be empty")
        if '\0' in value:
            raise ValueError("FilePath must not contain null bytes")
        if len(value) > 4096:
            raise ValueError("FilePath must not exceed 4096 characters")
        instance = str.__new__(cls, value)
        instance.value = value
        return instance


class FilePosition(BaseModel):
    """Tracks file reading position."""
    model_config = {"frozen": True}
    offset: int = Field(ge=0)
    inode: int = 0
    line_buffer: str = ""


# ── WebhookSource ──────────────────────────────────────────────────────────

class WebhookSource:
    """HTTP POST receiver accepting JSON events."""

    def __init__(self, config: WebhookSourceConfig | None = None):
        self._config = config
        self._subscribers: list = []
        self._state = SourceState.CREATED
        self._runner = None
        self._site = None

    def subscribe(self, callback) -> None:
        self._subscribers.append(callback)

    async def start(self) -> None:
        if self._state == SourceState.STOPPED:
            raise RuntimeError("Cannot restart a stopped WebhookSource")
        if self._state == SourceState.RUNNING:
            raise RuntimeError("WebhookSource is already started")
        self._state = SourceState.RUNNING
        logger.info("WebhookSource started")

    async def stop(self) -> None:
        if self._state == SourceState.STOPPED:
            return  # idempotent
        self._state = SourceState.STOPPED
        logger.info("WebhookSource stopped")

    async def _handle_post(self, body: bytes, content_type: str) -> HttpResponse:
        """Handle an incoming HTTP POST request."""
        # Content-Type validation
        ct_lower = content_type.lower() if content_type else ""
        if not ct_lower.startswith("application/json"):
            return HttpResponse(status=415, body='{"error": "Unsupported Media Type"}')

        # Empty body check
        if not body:
            return HttpResponse(status=400, body='{"error": "Empty body"}')

        # Parse JSON
        try:
            data = json.loads(body)
        except (json.JSONDecodeError, ValueError) as e:
            return HttpResponse(status=400, body=json.dumps({"error": f"Invalid JSON: {e}"}))

        # Must be a dict, not array or scalar
        if not isinstance(data, dict):
            return HttpResponse(status=400, body='{"error": "Body must be a JSON object"}')

        # Validate as Event
        try:
            event = Event.model_validate(data)
        except Exception as e:
            return HttpResponse(status=400, body=json.dumps({"error": f"Invalid event: {e}"}))

        # Emit to subscribers
        await emit(self._subscribers, event)

        return HttpResponse(
            status=200,
            body=json.dumps({"event_id": event.event_id, "status": "accepted"}),
        )

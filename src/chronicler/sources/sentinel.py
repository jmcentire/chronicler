"""Sentinel incident receiver source."""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone

from pydantic import BaseModel, Field, field_validator

from chronicler.sources.base import SourceState, emit
from chronicler.sources.webhook import (
    BindAddress,
    Event,
    HttpResponse,
)

logger = logging.getLogger(__name__)


class SentinelSourceConfig(BaseModel):
    """Configuration for the Sentinel incident webhook source."""
    model_config = {"frozen": True}
    bind: BindAddress
    client_max_size_bytes: int = Field(ge=1024, le=10485760)
    event_kind_prefix: str = Field(min_length=1, max_length=64)
    source_name: str = Field(min_length=1, max_length=128)

    @field_validator("event_kind_prefix")
    @classmethod
    def _validate_prefix(cls, v: str) -> str:
        import re
        if not re.match(r"^[a-z][a-z0-9_]*$", v):
            raise ValueError(
                f"event_kind_prefix must be lowercase letters/digits/underscores, "
                f"starting with a letter: {v!r}"
            )
        return v


class SentinelSource:
    """Sentinel incident lifecycle webhook."""

    def __init__(self, config: SentinelSourceConfig | None = None):
        self._config = config
        self._subscribers: list = []
        self._state = SourceState.CREATED

    def subscribe(self, callback) -> None:
        self._subscribers.append(callback)

    async def start(self) -> None:
        if self._state == SourceState.STOPPED:
            raise RuntimeError("Cannot restart a stopped SentinelSource")
        if self._state == SourceState.RUNNING:
            raise RuntimeError("SentinelSource is already started")
        self._state = SourceState.RUNNING
        logger.info("SentinelSource started")

    async def stop(self) -> None:
        if self._state == SourceState.STOPPED:
            return  # idempotent
        self._state = SourceState.STOPPED
        logger.info("SentinelSource stopped")

    async def _handle_post(self, body: bytes, content_type: str) -> HttpResponse:
        """Handle an incoming Sentinel incident webhook."""
        ct_lower = content_type.lower() if content_type else ""
        if not ct_lower.startswith("application/json"):
            return HttpResponse(status=415, body='{"error": "Unsupported Media Type"}')

        if not body:
            return HttpResponse(status=400, body='{"error": "Empty body"}')

        try:
            data = json.loads(body)
        except (json.JSONDecodeError, ValueError) as e:
            return HttpResponse(status=400, body=json.dumps({"error": str(e)}))

        if not isinstance(data, dict):
            return HttpResponse(status=400, body='{"error": "Body must be a JSON object"}')

        # Validate Sentinel payload structure
        if "properties" not in data and "type" not in data:
            return HttpResponse(status=400, body='{"error": "Unrecognized payload structure"}')

        # Extract action from payload
        action = data.get("action", "")
        if not action:
            props = data.get("properties", {})
            action = props.get("status", "unknown")

        prefix = self._config.event_kind_prefix if self._config else "sentinel"
        # Normalize action for event_kind
        action_normalized = action.lower().replace("-", "_").replace(" ", "_")
        event_kind = f"{prefix}.incident.{action_normalized}"

        props = data.get("properties", {})
        incident_id = props.get("incidentId") or data.get("name") or str(uuid.uuid4())
        source_name = self._config.source_name if self._config else "sentinel"

        event = Event(
            event_id=str(uuid.uuid4()),
            event_kind=event_kind,
            timestamp=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            source=source_name,
            payload=data,
            correlation_keys=[f"incident_id={incident_id}"],
        )

        await emit(self._subscribers, event)

        return HttpResponse(
            status=200,
            body=json.dumps({"incident_id": incident_id, "status": "accepted"}),
        )

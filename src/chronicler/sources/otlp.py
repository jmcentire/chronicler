"""OTLP/HTTP JSON trace receiver source."""

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


class OtlpSourceConfig(BaseModel):
    """Configuration for the OTLP HTTP source."""
    model_config = {"frozen": True}
    bind: BindAddress
    client_max_size_bytes: int = Field(ge=1024, le=52428800)
    source_name: str = Field(min_length=1, max_length=128)


class OtlpSpan(BaseModel):
    """Flattened span from an OTLP ExportTraceServiceRequest."""
    model_config = {"frozen": True}
    trace_id: str
    span_id: str
    name: str
    start_time_unix_nano: int
    end_time_unix_nano: int
    attributes: dict | list = Field(default_factory=dict)
    resource_attributes: dict | list = Field(default_factory=dict)
    status_code: int = 0


class OtlpSource:
    """OTLP/HTTP JSON receiver that converts trace spans to Chronicler events."""

    def __init__(self, config: OtlpSourceConfig | None = None):
        self._config = config
        self._subscribers: list = []
        self._state = SourceState.CREATED

    def subscribe(self, callback) -> None:
        self._subscribers.append(callback)

    async def start(self) -> None:
        if self._state == SourceState.STOPPED:
            raise RuntimeError("Cannot restart a stopped OtlpSource")
        if self._state == SourceState.RUNNING:
            raise RuntimeError("OtlpSource is already started")
        self._state = SourceState.RUNNING
        logger.info("OtlpSource started")

    async def stop(self) -> None:
        if self._state == SourceState.STOPPED:
            return  # idempotent
        self._state = SourceState.STOPPED
        logger.info("OtlpSource stopped")

    async def _handle_traces(self, body: bytes, content_type: str) -> HttpResponse:
        """Handle an incoming OTLP ExportTraceServiceRequest."""
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

        if "resourceSpans" not in data:
            return HttpResponse(status=400, body='{"error": "Missing resourceSpans"}')

        # Flatten spans
        spans = self._flatten_spans(data)

        source_name = self._config.source_name if self._config else "otlp"
        for span in spans:
            event = self._span_to_event(span, source_name)
            await emit(self._subscribers, event)

        return HttpResponse(status=200, body="{}")

    def _flatten_spans(self, otlp_request: dict) -> list[OtlpSpan]:
        """Flatten an OTLP ExportTraceServiceRequest into a list of OtlpSpan."""
        if "resourceSpans" not in otlp_request:
            raise ValueError("Missing resourceSpans in OTLP request")
        spans = []
        for rs in otlp_request["resourceSpans"]:
            resource_attrs = rs.get("resource", {}).get("attributes", {})
            for ss in rs.get("scopeSpans", []):
                for raw_span in ss.get("spans", []):
                    spans.append(OtlpSpan(
                        trace_id=raw_span.get("traceId", ""),
                        span_id=raw_span.get("spanId", ""),
                        name=raw_span.get("name", ""),
                        start_time_unix_nano=raw_span.get("startTimeUnixNano", 0),
                        end_time_unix_nano=raw_span.get("endTimeUnixNano", 0),
                        attributes=raw_span.get("attributes", {}),
                        resource_attributes=resource_attrs,
                        status_code=raw_span.get("status", {}).get("code", 0),
                    ))
        return spans

    def _span_to_event(self, span: OtlpSpan, source_name: str) -> Event:
        """Convert an OtlpSpan into a Chronicler Event."""
        # Convert nanoseconds to ISO 8601 UTC
        ts_seconds = span.start_time_unix_nano / 1_000_000_000
        dt = datetime.fromtimestamp(ts_seconds, tz=timezone.utc)
        timestamp = dt.strftime("%Y-%m-%dT%H:%M:%S") + "Z"

        # Normalize span name to event_kind: replace hyphens/spaces with underscores
        normalized_name = span.name.replace("-", "_").replace(" ", "_")
        event_kind = f"otlp.trace.{normalized_name}"

        return Event(
            event_id=str(uuid.uuid4()),
            event_kind=event_kind,
            timestamp=timestamp,
            source=source_name,
            payload={
                "trace_id": span.trace_id,
                "span_id": span.span_id,
                "name": span.name,
                "attributes": span.attributes,
                "status_code": span.status_code,
            },
            correlation_keys=[
                f"trace_id={span.trace_id}",
                f"span_id={span.span_id}",
            ],
        )

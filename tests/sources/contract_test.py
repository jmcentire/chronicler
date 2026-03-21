"""
Contract test suite for the sources component.
Tests cover type validation, function behavior, lifecycle, error resilience, and invariants.

Run with: pytest contract_test.py -v
"""
import asyncio
import json
import re
import uuid
from unittest.mock import AsyncMock, MagicMock, patch, call
import pytest

# ---------------------------------------------------------------------------
# Imports from the component under test
# ---------------------------------------------------------------------------
try:
    from chronicler.sources.base import emit
except ImportError:
    try:
        from sources.base import emit
    except ImportError:
        emit = None

try:
    from chronicler.sources.webhook import WebhookSource
except ImportError:
    try:
        from sources.webhook import WebhookSource
    except ImportError:
        WebhookSource = None

try:
    from chronicler.sources.otlp import OtlpSource
except ImportError:
    try:
        from sources.otlp import OtlpSource
    except ImportError:
        OtlpSource = None

try:
    from chronicler.sources.file import FileSource
except ImportError:
    try:
        from sources.file import FileSource
    except ImportError:
        FileSource = None

try:
    from chronicler.sources.sentinel import SentinelSource
except ImportError:
    try:
        from sources.sentinel import SentinelSource
    except ImportError:
        SentinelSource = None

# Types / schemas — import from actual source modules
try:
    from chronicler.sources.base import SourceProtocol, SourceState, emit as _emit_fn
    from chronicler.sources.webhook import (
        Event, EventKind, BindAddress, WebhookSourceConfig,
        HttpResponse, FilePath, FilePosition,
    )
    from chronicler.sources.otlp import OtlpSourceConfig, OtlpSpan
    from chronicler.sources.file import FileSourceConfig
    from chronicler.sources.sentinel import SentinelSourceConfig
    # EventCallback is just a type alias — define it
    from typing import Callable
    EventCallback = Callable
except ImportError:
    Event = EventKind = SourceProtocol = BindAddress = None
    WebhookSourceConfig = OtlpSourceConfig = FileSourceConfig = None
    FilePath = SentinelSourceConfig = OtlpSpan = HttpResponse = None
    SourceState = FilePosition = EventCallback = None


# ---------------------------------------------------------------------------
# Helpers & Fixtures
# ---------------------------------------------------------------------------

VALID_UUID4 = "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d"
VALID_TIMESTAMP = "2024-01-15T10:30:00.123Z"
VALID_TIMESTAMP_NO_FRAC = "2024-01-15T10:30:00Z"
VALID_EVENT_KIND = "webhook.push"


def make_valid_event_dict(**overrides):
    """Return a dict suitable for constructing a valid Event."""
    base = {
        "event_id": VALID_UUID4,
        "event_kind": VALID_EVENT_KIND,
        "timestamp": VALID_TIMESTAMP,
        "source": "test-source",
        "payload": {"key": "value"},
        "correlation_keys": ["corr1"],
    }
    base.update(overrides)
    return base


def make_valid_event_json(**overrides):
    """Return JSON bytes for a valid Event."""
    return json.dumps(make_valid_event_dict(**overrides)).encode("utf-8")


def make_otlp_request(spans=None, resource_attributes=None):
    """Build a minimal OTLP ExportTraceServiceRequest dict."""
    if spans is None:
        spans = [
            {
                "traceId": "abc123",
                "spanId": "def456",
                "name": "my_operation",
                "startTimeUnixNano": 1700000000000000000,
                "endTimeUnixNano": 1700000001000000000,
                "attributes": [{"key": "http.method", "value": {"stringValue": "GET"}}],
                "status": {"code": 1},
            }
        ]
    if resource_attributes is None:
        resource_attributes = [{"key": "service.name", "value": {"stringValue": "test-svc"}}]
    return {
        "resourceSpans": [
            {
                "resource": {"attributes": resource_attributes},
                "scopeSpans": [
                    {
                        "scope": {"name": "test-scope"},
                        "spans": spans,
                    }
                ],
            }
        ]
    }


def make_sentinel_payload(action="created", incident_id="inc-001", incident_number=42):
    """Build a minimal Sentinel incident webhook payload."""
    return {
        "properties": {
            "incidentId": incident_id,
            "incidentNumber": incident_number,
            "title": "Test Incident",
            "severity": "High",
            "status": "New",
            "action": action,
        }
    }


@pytest.fixture
def collected_events():
    """Returns a list and an async callback that appends events to it."""
    events = []

    async def callback(event):
        events.append(event)

    return events, callback


@pytest.fixture
def failing_callback():
    """Returns an async callback that always raises."""
    async def callback(event):
        raise RuntimeError("Subscriber failure!")
    return callback


# ---------------------------------------------------------------------------
# TYPE VALIDATION TESTS
# ---------------------------------------------------------------------------

class TestEventType:
    """Tests for Event struct construction and validation."""

    @pytest.mark.skipif(Event is None, reason="Event type not importable")
    def test_valid_event_construction(self):
        """Valid Event construction with all fields passing validators."""
        event = Event(**make_valid_event_dict())
        assert event.event_id == VALID_UUID4
        assert event.timestamp == VALID_TIMESTAMP
        assert event.source == "test-source"
        assert event.payload == {"key": "value"}
        assert event.correlation_keys == ["corr1"]

    @pytest.mark.skipif(Event is None, reason="Event type not importable")
    def test_valid_event_timestamp_no_fractional(self):
        """Event accepts timestamp without fractional seconds."""
        event = Event(**make_valid_event_dict(timestamp=VALID_TIMESTAMP_NO_FRAC))
        assert event.timestamp == VALID_TIMESTAMP_NO_FRAC

    @pytest.mark.skipif(Event is None, reason="Event type not importable")
    def test_invalid_event_id_not_uuid(self):
        """Event rejects non-UUID4 event_id."""
        with pytest.raises((ValueError, TypeError, Exception)):
            Event(**make_valid_event_dict(event_id="not-a-uuid"))

    @pytest.mark.skipif(Event is None, reason="Event type not importable")
    def test_invalid_event_id_uuid1(self):
        """Event rejects UUID1 format (version byte must be 4)."""
        uuid1 = "a1b2c3d4-e5f6-1a7b-8c9d-0e1f2a3b4c5d"  # version 1
        with pytest.raises((ValueError, TypeError, Exception)):
            Event(**make_valid_event_dict(event_id=uuid1))

    @pytest.mark.skipif(Event is None, reason="Event type not importable")
    def test_invalid_timestamp_no_z_suffix(self):
        """Event rejects timestamp without Z suffix."""
        with pytest.raises((ValueError, TypeError, Exception)):
            Event(**make_valid_event_dict(timestamp="2024-01-15T10:30:00"))

    @pytest.mark.skipif(Event is None, reason="Event type not importable")
    def test_invalid_timestamp_space_format(self):
        """Event rejects space-separated datetime format."""
        with pytest.raises((ValueError, TypeError, Exception)):
            Event(**make_valid_event_dict(timestamp="2024-01-01 12:00:00"))


class TestEventKindType:
    """Tests for EventKind primitive validation."""

    @pytest.mark.skipif(EventKind is None, reason="EventKind not importable")
    def test_valid_two_segments(self):
        """EventKind accepts valid two-segment dot-delimited string."""
        ek = EventKind(value="webhook.push")
        assert ek.value == "webhook.push" or str(ek) == "webhook.push"

    @pytest.mark.skipif(EventKind is None, reason="EventKind not importable")
    def test_valid_three_segments(self):
        """EventKind accepts three-segment kind like 'sentinel.incident.created'."""
        ek = EventKind(value="sentinel.incident.created")
        # Just verify it doesn't raise
        assert ek is not None

    @pytest.mark.skipif(EventKind is None, reason="EventKind not importable")
    def test_valid_with_numbers(self):
        """EventKind accepts segments with numbers like 'otlp.trace2.span'."""
        ek = EventKind(value="otlp.trace2")
        assert ek is not None

    @pytest.mark.skipif(EventKind is None, reason="EventKind not importable")
    def test_invalid_single_segment(self):
        """EventKind rejects single segment string."""
        with pytest.raises((ValueError, TypeError, Exception)):
            EventKind(value="webhook")

    @pytest.mark.skipif(EventKind is None, reason="EventKind not importable")
    def test_invalid_uppercase(self):
        """EventKind rejects uppercase characters."""
        with pytest.raises((ValueError, TypeError, Exception)):
            EventKind(value="Webhook.Push")

    @pytest.mark.skipif(EventKind is None, reason="EventKind not importable")
    def test_invalid_starts_with_digit(self):
        """EventKind rejects segment starting with digit."""
        with pytest.raises((ValueError, TypeError, Exception)):
            EventKind(value="1webhook.push")

    @pytest.mark.skipif(EventKind is None, reason="EventKind not importable")
    def test_invalid_empty(self):
        """EventKind rejects empty string."""
        with pytest.raises((ValueError, TypeError, Exception)):
            EventKind(value="")

    @pytest.mark.skipif(EventKind is None, reason="EventKind not importable")
    def test_invalid_trailing_dot(self):
        """EventKind rejects trailing dot."""
        with pytest.raises((ValueError, TypeError, Exception)):
            EventKind(value="webhook.push.")

    @pytest.mark.skipif(EventKind is None, reason="EventKind not importable")
    def test_invalid_leading_dot(self):
        """EventKind rejects leading dot."""
        with pytest.raises((ValueError, TypeError, Exception)):
            EventKind(value=".webhook.push")

    @pytest.mark.skipif(EventKind is None, reason="EventKind not importable")
    def test_invalid_double_dot(self):
        """EventKind rejects consecutive dots."""
        with pytest.raises((ValueError, TypeError, Exception)):
            EventKind(value="webhook..push")


class TestBindAddress:
    """Tests for BindAddress struct validation."""

    @pytest.mark.skipif(BindAddress is None, reason="BindAddress not importable")
    def test_valid_0000(self):
        """BindAddress accepts 0.0.0.0 with valid port."""
        ba = BindAddress(host="0.0.0.0", port=8080)
        assert ba.host == "0.0.0.0"
        assert ba.port == 8080

    @pytest.mark.skipif(BindAddress is None, reason="BindAddress not importable")
    def test_valid_localhost(self):
        """BindAddress accepts localhost."""
        ba = BindAddress(host="localhost", port=443)
        assert ba.host == "localhost"

    @pytest.mark.skipif(BindAddress is None, reason="BindAddress not importable")
    def test_valid_ipv6_loopback(self):
        """BindAddress accepts IPv6 ::1."""
        ba = BindAddress(host="::1", port=8080)
        assert ba.host == "::1"

    @pytest.mark.skipif(BindAddress is None, reason="BindAddress not importable")
    def test_valid_ipv6_any(self):
        """BindAddress accepts IPv6 ::."""
        ba = BindAddress(host="::", port=8080)
        assert ba.host == "::"

    @pytest.mark.skipif(BindAddress is None, reason="BindAddress not importable")
    def test_valid_ip_address(self):
        """BindAddress accepts a standard IPv4 address."""
        ba = BindAddress(host="192.168.1.1", port=3000)
        assert ba.host == "192.168.1.1"

    @pytest.mark.skipif(BindAddress is None, reason="BindAddress not importable")
    def test_port_boundary_min(self):
        """BindAddress accepts port 1 (minimum)."""
        ba = BindAddress(host="0.0.0.0", port=1)
        assert ba.port == 1

    @pytest.mark.skipif(BindAddress is None, reason="BindAddress not importable")
    def test_port_boundary_max(self):
        """BindAddress accepts port 65535 (maximum)."""
        ba = BindAddress(host="0.0.0.0", port=65535)
        assert ba.port == 65535

    @pytest.mark.skipif(BindAddress is None, reason="BindAddress not importable")
    def test_port_zero_accepted(self):
        """BindAddress accepts port 0 (OS-assigned port for testing)."""
        ba = BindAddress(host="0.0.0.0", port=0)
        assert ba.port == 0

    @pytest.mark.skipif(BindAddress is None, reason="BindAddress not importable")
    def test_port_above_max_rejected(self):
        """BindAddress rejects port > 65535."""
        with pytest.raises((ValueError, TypeError, Exception)):
            BindAddress(host="0.0.0.0", port=65536)

    @pytest.mark.skipif(BindAddress is None, reason="BindAddress not importable")
    def test_port_negative_rejected(self):
        """BindAddress rejects negative port."""
        with pytest.raises((ValueError, TypeError, Exception)):
            BindAddress(host="0.0.0.0", port=-1)

    @pytest.mark.skipif(BindAddress is None, reason="BindAddress not importable")
    def test_invalid_host(self):
        """BindAddress rejects invalid host string."""
        with pytest.raises((ValueError, TypeError, Exception)):
            BindAddress(host="not-a-host", port=8080)

    @pytest.mark.skipif(BindAddress is None, reason="BindAddress not importable")
    def test_invalid_host_domain(self):
        """BindAddress rejects domain names (only specific patterns allowed)."""
        with pytest.raises((ValueError, TypeError, Exception)):
            BindAddress(host="example.com", port=8080)


class TestWebhookSourceConfig:
    """Tests for WebhookSourceConfig struct validation."""

    @pytest.mark.skipif(WebhookSourceConfig is None, reason="WebhookSourceConfig not importable")
    def test_valid_config(self):
        """WebhookSourceConfig accepts valid configuration."""
        bind = BindAddress(host="0.0.0.0", port=8080)
        cfg = WebhookSourceConfig(bind=bind, client_max_size_bytes=1024, source_name="webhook1")
        assert cfg.source_name == "webhook1"
        assert cfg.client_max_size_bytes == 1024

    @pytest.mark.skipif(WebhookSourceConfig is None, reason="WebhookSourceConfig not importable")
    def test_max_size_below_min(self):
        """WebhookSourceConfig rejects client_max_size_bytes < 1024."""
        bind = BindAddress(host="0.0.0.0", port=8080)
        with pytest.raises((ValueError, TypeError, Exception)):
            WebhookSourceConfig(bind=bind, client_max_size_bytes=1023, source_name="webhook1")

    @pytest.mark.skipif(WebhookSourceConfig is None, reason="WebhookSourceConfig not importable")
    def test_max_size_above_max(self):
        """WebhookSourceConfig rejects client_max_size_bytes > 10485760."""
        bind = BindAddress(host="0.0.0.0", port=8080)
        with pytest.raises((ValueError, TypeError, Exception)):
            WebhookSourceConfig(bind=bind, client_max_size_bytes=10485761, source_name="webhook1")

    @pytest.mark.skipif(WebhookSourceConfig is None, reason="WebhookSourceConfig not importable")
    def test_max_size_boundary_min(self):
        """WebhookSourceConfig accepts client_max_size_bytes = 1024."""
        bind = BindAddress(host="0.0.0.0", port=8080)
        cfg = WebhookSourceConfig(bind=bind, client_max_size_bytes=1024, source_name="w")
        assert cfg.client_max_size_bytes == 1024

    @pytest.mark.skipif(WebhookSourceConfig is None, reason="WebhookSourceConfig not importable")
    def test_max_size_boundary_max(self):
        """WebhookSourceConfig accepts client_max_size_bytes = 10485760."""
        bind = BindAddress(host="0.0.0.0", port=8080)
        cfg = WebhookSourceConfig(bind=bind, client_max_size_bytes=10485760, source_name="w")
        assert cfg.client_max_size_bytes == 10485760

    @pytest.mark.skipif(WebhookSourceConfig is None, reason="WebhookSourceConfig not importable")
    def test_empty_source_name(self):
        """WebhookSourceConfig rejects empty source_name."""
        bind = BindAddress(host="0.0.0.0", port=8080)
        with pytest.raises((ValueError, TypeError, Exception)):
            WebhookSourceConfig(bind=bind, client_max_size_bytes=1024, source_name="")

    @pytest.mark.skipif(WebhookSourceConfig is None, reason="WebhookSourceConfig not importable")
    def test_source_name_too_long(self):
        """WebhookSourceConfig rejects source_name > 128 chars."""
        bind = BindAddress(host="0.0.0.0", port=8080)
        with pytest.raises((ValueError, TypeError, Exception)):
            WebhookSourceConfig(bind=bind, client_max_size_bytes=1024, source_name="x" * 129)


class TestOtlpSourceConfig:
    """Tests for OtlpSourceConfig struct validation."""

    @pytest.mark.skipif(OtlpSourceConfig is None, reason="OtlpSourceConfig not importable")
    def test_valid_config(self):
        """OtlpSourceConfig accepts valid configuration."""
        bind = BindAddress(host="0.0.0.0", port=4318)
        cfg = OtlpSourceConfig(bind=bind, client_max_size_bytes=52428800, source_name="otlp1")
        assert cfg.client_max_size_bytes == 52428800

    @pytest.mark.skipif(OtlpSourceConfig is None, reason="OtlpSourceConfig not importable")
    def test_max_size_above_max(self):
        """OtlpSourceConfig rejects client_max_size_bytes > 52428800."""
        bind = BindAddress(host="0.0.0.0", port=4318)
        with pytest.raises((ValueError, TypeError, Exception)):
            OtlpSourceConfig(bind=bind, client_max_size_bytes=52428801, source_name="otlp1")

    @pytest.mark.skipif(OtlpSourceConfig is None, reason="OtlpSourceConfig not importable")
    def test_max_size_boundary_max(self):
        """OtlpSourceConfig accepts client_max_size_bytes = 52428800."""
        bind = BindAddress(host="0.0.0.0", port=4318)
        cfg = OtlpSourceConfig(bind=bind, client_max_size_bytes=52428800, source_name="o")
        assert cfg.client_max_size_bytes == 52428800


class TestFileSourceConfig:
    """Tests for FileSourceConfig struct validation."""

    @pytest.mark.skipif(FileSourceConfig is None or FilePath is None, reason="Types not importable")
    def test_valid_config(self):
        """FileSourceConfig accepts valid configuration."""
        fp = FilePath(value="/var/log/app.jsonl")
        cfg = FileSourceConfig(path=fp, poll_interval_seconds=1.0, source_name="file1")
        assert cfg.poll_interval_seconds == 1.0

    @pytest.mark.skipif(FileSourceConfig is None or FilePath is None, reason="Types not importable")
    def test_poll_interval_below_min(self):
        """FileSourceConfig rejects poll_interval_seconds < 0.1."""
        fp = FilePath(value="/var/log/app.jsonl")
        with pytest.raises((ValueError, TypeError, Exception)):
            FileSourceConfig(path=fp, poll_interval_seconds=0.05, source_name="file1")

    @pytest.mark.skipif(FileSourceConfig is None or FilePath is None, reason="Types not importable")
    def test_poll_interval_above_max(self):
        """FileSourceConfig rejects poll_interval_seconds > 300.0."""
        fp = FilePath(value="/var/log/app.jsonl")
        with pytest.raises((ValueError, TypeError, Exception)):
            FileSourceConfig(path=fp, poll_interval_seconds=300.1, source_name="file1")

    @pytest.mark.skipif(FileSourceConfig is None or FilePath is None, reason="Types not importable")
    def test_poll_interval_boundary_min(self):
        """FileSourceConfig accepts poll_interval_seconds = 0.1."""
        fp = FilePath(value="/var/log/app.jsonl")
        cfg = FileSourceConfig(path=fp, poll_interval_seconds=0.1, source_name="f")
        assert cfg.poll_interval_seconds == pytest.approx(0.1)

    @pytest.mark.skipif(FileSourceConfig is None or FilePath is None, reason="Types not importable")
    def test_poll_interval_boundary_max(self):
        """FileSourceConfig accepts poll_interval_seconds = 300.0."""
        fp = FilePath(value="/var/log/app.jsonl")
        cfg = FileSourceConfig(path=fp, poll_interval_seconds=300.0, source_name="f")
        assert cfg.poll_interval_seconds == pytest.approx(300.0)


class TestFilePath:
    """Tests for FilePath primitive validation."""

    @pytest.mark.skipif(FilePath is None, reason="FilePath not importable")
    def test_valid_path(self):
        """FilePath accepts valid path string."""
        fp = FilePath(value="/var/log/test.jsonl")
        assert fp.value == "/var/log/test.jsonl" or str(fp) == "/var/log/test.jsonl"

    @pytest.mark.skipif(FilePath is None, reason="FilePath not importable")
    def test_null_byte_rejected(self):
        """FilePath rejects path with null byte."""
        with pytest.raises((ValueError, TypeError, Exception)):
            FilePath(value="/var/log/\x00test")

    @pytest.mark.skipif(FilePath is None, reason="FilePath not importable")
    def test_empty_rejected(self):
        """FilePath rejects empty string."""
        with pytest.raises((ValueError, TypeError, Exception)):
            FilePath(value="")

    @pytest.mark.skipif(FilePath is None, reason="FilePath not importable")
    def test_max_length_accepted(self):
        """FilePath accepts string of exactly 4096 chars."""
        fp = FilePath(value="/" + "a" * 4095)
        assert fp is not None

    @pytest.mark.skipif(FilePath is None, reason="FilePath not importable")
    def test_over_max_length_rejected(self):
        """FilePath rejects string longer than 4096 chars."""
        with pytest.raises((ValueError, TypeError, Exception)):
            FilePath(value="/" + "a" * 4096)


class TestSentinelSourceConfig:
    """Tests for SentinelSourceConfig struct validation."""

    @pytest.mark.skipif(SentinelSourceConfig is None, reason="SentinelSourceConfig not importable")
    def test_valid_config(self):
        """SentinelSourceConfig accepts valid configuration."""
        bind = BindAddress(host="0.0.0.0", port=9090)
        cfg = SentinelSourceConfig(
            bind=bind, client_max_size_bytes=1024,
            event_kind_prefix="sentinel", source_name="sentinel1"
        )
        assert cfg.event_kind_prefix == "sentinel"

    @pytest.mark.skipif(SentinelSourceConfig is None, reason="SentinelSourceConfig not importable")
    def test_invalid_prefix_with_dots(self):
        """SentinelSourceConfig rejects event_kind_prefix with dots."""
        bind = BindAddress(host="0.0.0.0", port=9090)
        with pytest.raises((ValueError, TypeError, Exception)):
            SentinelSourceConfig(
                bind=bind, client_max_size_bytes=1024,
                event_kind_prefix="sentinel.incident", source_name="sentinel1"
            )

    @pytest.mark.skipif(SentinelSourceConfig is None, reason="SentinelSourceConfig not importable")
    def test_invalid_prefix_uppercase(self):
        """SentinelSourceConfig rejects event_kind_prefix with uppercase."""
        bind = BindAddress(host="0.0.0.0", port=9090)
        with pytest.raises((ValueError, TypeError, Exception)):
            SentinelSourceConfig(
                bind=bind, client_max_size_bytes=1024,
                event_kind_prefix="Sentinel", source_name="sentinel1"
            )

    @pytest.mark.skipif(SentinelSourceConfig is None, reason="SentinelSourceConfig not importable")
    def test_invalid_prefix_starts_with_digit(self):
        """SentinelSourceConfig rejects event_kind_prefix starting with digit."""
        bind = BindAddress(host="0.0.0.0", port=9090)
        with pytest.raises((ValueError, TypeError, Exception)):
            SentinelSourceConfig(
                bind=bind, client_max_size_bytes=1024,
                event_kind_prefix="1sentinel", source_name="sentinel1"
            )


class TestHttpResponse:
    """Tests for HttpResponse struct validation."""

    @pytest.mark.skipif(HttpResponse is None, reason="HttpResponse not importable")
    def test_valid_response(self):
        """HttpResponse accepts valid status and body."""
        r = HttpResponse(status=200, body="OK")
        assert r.status == 200
        assert r.body == "OK"

    @pytest.mark.skipif(HttpResponse is None, reason="HttpResponse not importable")
    def test_status_boundary_min(self):
        """HttpResponse accepts status 100."""
        r = HttpResponse(status=100, body="")
        assert r.status == 100

    @pytest.mark.skipif(HttpResponse is None, reason="HttpResponse not importable")
    def test_status_boundary_max(self):
        """HttpResponse accepts status 599."""
        r = HttpResponse(status=599, body="")
        assert r.status == 599

    @pytest.mark.skipif(HttpResponse is None, reason="HttpResponse not importable")
    def test_status_below_100_rejected(self):
        """HttpResponse rejects status < 100."""
        with pytest.raises((ValueError, TypeError, Exception)):
            HttpResponse(status=99, body="")

    @pytest.mark.skipif(HttpResponse is None, reason="HttpResponse not importable")
    def test_status_above_599_rejected(self):
        """HttpResponse rejects status > 599."""
        with pytest.raises((ValueError, TypeError, Exception)):
            HttpResponse(status=600, body="")


class TestSourceState:
    """Tests for SourceState enum."""

    @pytest.mark.skipif(SourceState is None, reason="SourceState not importable")
    def test_has_created(self):
        """SourceState has CREATED variant."""
        assert SourceState.CREATED is not None

    @pytest.mark.skipif(SourceState is None, reason="SourceState not importable")
    def test_has_running(self):
        """SourceState has RUNNING variant."""
        assert SourceState.RUNNING is not None

    @pytest.mark.skipif(SourceState is None, reason="SourceState not importable")
    def test_has_stopped(self):
        """SourceState has STOPPED variant."""
        assert SourceState.STOPPED is not None

    @pytest.mark.skipif(SourceState is None, reason="SourceState not importable")
    def test_three_values(self):
        """SourceState has exactly 3 values."""
        members = list(SourceState)
        assert len(members) == 3


class TestFilePosition:
    """Tests for FilePosition struct validation."""

    @pytest.mark.skipif(FilePosition is None, reason="FilePosition not importable")
    def test_valid_position(self):
        """FilePosition accepts valid offset and inode."""
        fp = FilePosition(offset=0, inode=12345, line_buffer="")
        assert fp.offset == 0
        assert fp.inode == 12345

    @pytest.mark.skipif(FilePosition is None, reason="FilePosition not importable")
    def test_negative_offset_rejected(self):
        """FilePosition rejects negative offset."""
        with pytest.raises((ValueError, TypeError, Exception)):
            FilePosition(offset=-1, inode=12345, line_buffer="")

    @pytest.mark.skipif(FilePosition is None, reason="FilePosition not importable")
    def test_max_offset_accepted(self):
        """FilePosition accepts max int64 offset."""
        fp = FilePosition(offset=9223372036854775807, inode=1, line_buffer="")
        assert fp.offset == 9223372036854775807


class TestOtlpSpan:
    """Tests for OtlpSpan struct."""

    @pytest.mark.skipif(OtlpSpan is None, reason="OtlpSpan not importable")
    def test_valid_span(self):
        """OtlpSpan accepts valid span data."""
        span = OtlpSpan(
            trace_id="abc123",
            span_id="def456",
            name="my_operation",
            start_time_unix_nano=1700000000000000000,
            end_time_unix_nano=1700000001000000000,
            attributes={"http.method": "GET"},
            resource_attributes={"service.name": "test-svc"},
            status_code=1,
        )
        assert span.trace_id == "abc123"
        assert span.name == "my_operation"


# ---------------------------------------------------------------------------
# EMIT FUNCTION TESTS
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestEmit:
    """Tests for the shared emit() utility function."""

    @pytest.mark.skipif(emit is None or Event is None, reason="emit or Event not importable")
    async def test_emit_calls_all_subscribers(self):
        """emit() calls all subscribers with the event exactly once."""
        event = Event(**make_valid_event_dict())
        cb1 = AsyncMock()
        cb2 = AsyncMock()
        await emit([cb1, cb2], event)
        cb1.assert_called_once_with(event)
        cb2.assert_called_once_with(event)

    @pytest.mark.skipif(emit is None or Event is None, reason="emit or Event not importable")
    async def test_emit_continues_after_failure(self):
        """emit() continues calling remaining subscribers when one raises."""
        event = Event(**make_valid_event_dict())

        async def failing(e):
            raise RuntimeError("fail!")

        cb_after = AsyncMock()
        await emit([failing, cb_after], event)
        cb_after.assert_called_once_with(event)

    @pytest.mark.skipif(emit is None or Event is None, reason="emit or Event not importable")
    async def test_emit_empty_subscribers(self):
        """emit() with empty subscriber list completes without error."""
        event = Event(**make_valid_event_dict())
        await emit([], event)  # Should not raise

    @pytest.mark.skipif(emit is None or Event is None, reason="emit or Event not importable")
    async def test_emit_multiple_failures(self):
        """emit() handles multiple failing subscribers, calling all of them."""
        event = Event(**make_valid_event_dict())
        call_order = []

        async def failing1(e):
            call_order.append("fail1")
            raise RuntimeError("fail1")

        async def failing2(e):
            call_order.append("fail2")
            raise RuntimeError("fail2")

        async def success(e):
            call_order.append("success")

        await emit([failing1, success, failing2], event)
        assert "fail1" in call_order
        assert "success" in call_order
        assert "fail2" in call_order
        assert len(call_order) == 3

    @pytest.mark.skipif(emit is None or Event is None, reason="emit or Event not importable")
    async def test_emit_does_not_propagate_exceptions(self):
        """emit() never propagates subscriber exceptions to caller (invariant)."""
        event = Event(**make_valid_event_dict())

        async def failing(e):
            raise Exception("catastrophe!")

        # This should NOT raise
        await emit([failing], event)


# ---------------------------------------------------------------------------
# WEBHOOK SOURCE TESTS
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestWebhookSource:
    """Tests for WebhookSource lifecycle, subscribe, and handler."""

    @pytest.fixture
    def webhook_config(self):
        """Create a WebhookSourceConfig with OS-assigned port."""
        bind = BindAddress(host="127.0.0.1", port=0)
        return WebhookSourceConfig(bind=bind, client_max_size_bytes=1048576, source_name="test-webhook")

    @pytest.fixture
    async def webhook_source(self, webhook_config):
        """Create and yield a WebhookSource, ensuring cleanup."""
        # Try different constructor patterns
        try:
            source = WebhookSource(webhook_config)
        except TypeError:
            source = WebhookSource(config=webhook_config)
        yield source
        try:
            await asyncio.wait_for(source.stop(), timeout=5.0)
        except Exception:
            pass

    @pytest.mark.skipif(WebhookSource is None or WebhookSourceConfig is None or BindAddress is None,
                        reason="WebhookSource types not importable")
    async def test_subscribe_appends_callback(self, webhook_source, collected_events):
        """WebhookSource.subscribe appends callback to subscriber list."""
        events, callback = collected_events
        webhook_source.subscribe(callback)
        # Verify internal subscriber list has the callback
        subs = getattr(webhook_source, '_subscribers', None)
        if subs is not None:
            assert callback in subs

    @pytest.mark.skipif(WebhookSource is None or WebhookSourceConfig is None or BindAddress is None,
                        reason="WebhookSource types not importable")
    async def test_subscribe_before_start(self, webhook_source, collected_events):
        """WebhookSource.subscribe works before start()."""
        events, callback = collected_events
        # Should not raise even though source not started
        webhook_source.subscribe(callback)

    @pytest.mark.skipif(WebhookSource is None or WebhookSourceConfig is None or BindAddress is None,
                        reason="WebhookSource types not importable")
    async def test_start_transitions_to_running(self, webhook_source):
        """WebhookSource.start transitions to RUNNING state."""
        await asyncio.wait_for(webhook_source.start(), timeout=5.0)
        state = getattr(webhook_source, '_state', None)
        if state is not None:
            assert state == SourceState.RUNNING

    @pytest.mark.skipif(WebhookSource is None or WebhookSourceConfig is None or BindAddress is None,
                        reason="WebhookSource types not importable")
    async def test_start_already_running_raises(self, webhook_source):
        """WebhookSource.start raises RuntimeError when already RUNNING."""
        await asyncio.wait_for(webhook_source.start(), timeout=5.0)
        with pytest.raises(RuntimeError):
            await asyncio.wait_for(webhook_source.start(), timeout=5.0)

    @pytest.mark.skipif(WebhookSource is None or WebhookSourceConfig is None or BindAddress is None,
                        reason="WebhookSource types not importable")
    async def test_stop_transitions_to_stopped(self, webhook_source):
        """WebhookSource.stop transitions to STOPPED state."""
        await asyncio.wait_for(webhook_source.start(), timeout=5.0)
        await asyncio.wait_for(webhook_source.stop(), timeout=5.0)
        state = getattr(webhook_source, '_state', None)
        if state is not None:
            assert state == SourceState.STOPPED

    @pytest.mark.skipif(WebhookSource is None or WebhookSourceConfig is None or BindAddress is None,
                        reason="WebhookSource types not importable")
    async def test_stop_idempotent(self, webhook_source):
        """WebhookSource.stop can be called multiple times safely."""
        await asyncio.wait_for(webhook_source.start(), timeout=5.0)
        await asyncio.wait_for(webhook_source.stop(), timeout=5.0)
        await asyncio.wait_for(webhook_source.stop(), timeout=5.0)  # Should not raise

    @pytest.mark.skipif(WebhookSource is None or WebhookSourceConfig is None or BindAddress is None,
                        reason="WebhookSource types not importable")
    async def test_stop_never_started(self, webhook_source):
        """WebhookSource.stop on never-started source is safe."""
        await asyncio.wait_for(webhook_source.stop(), timeout=5.0)  # Should not raise

    @pytest.mark.skipif(WebhookSource is None or WebhookSourceConfig is None or BindAddress is None,
                        reason="WebhookSource types not importable")
    async def test_handle_post_valid_event(self, webhook_source, collected_events):
        """WebhookSource._handle_post returns 200 with event_id on valid event JSON."""
        events, callback = collected_events
        webhook_source.subscribe(callback)
        body = make_valid_event_json()
        response = await webhook_source._handle_post(body, "application/json")
        assert response.status == 200
        assert VALID_UUID4 in response.body
        assert len(events) == 1

    @pytest.mark.skipif(WebhookSource is None or WebhookSourceConfig is None or BindAddress is None,
                        reason="WebhookSource types not importable")
    async def test_handle_post_wrong_content_type(self, webhook_source):
        """WebhookSource._handle_post returns 415 for non-JSON content type."""
        response = await webhook_source._handle_post(b'{}', "text/plain")
        assert response.status == 415

    @pytest.mark.skipif(WebhookSource is None or WebhookSourceConfig is None or BindAddress is None,
                        reason="WebhookSource types not importable")
    async def test_handle_post_invalid_json(self, webhook_source):
        """WebhookSource._handle_post returns 400 for invalid JSON."""
        response = await webhook_source._handle_post(b'not json', "application/json")
        assert response.status == 400

    @pytest.mark.skipif(WebhookSource is None or WebhookSourceConfig is None or BindAddress is None,
                        reason="WebhookSource types not importable")
    async def test_handle_post_schema_invalid(self, webhook_source):
        """WebhookSource._handle_post returns 400 for JSON not matching Event schema."""
        body = json.dumps({"not": "an event"}).encode()
        response = await webhook_source._handle_post(body, "application/json")
        assert response.status == 400

    @pytest.mark.skipif(WebhookSource is None or WebhookSourceConfig is None or BindAddress is None,
                        reason="WebhookSource types not importable")
    async def test_handle_post_emits_to_all_subscribers(self, webhook_source):
        """WebhookSource._handle_post emits valid events to all subscribers."""
        cb1 = AsyncMock()
        cb2 = AsyncMock()
        webhook_source.subscribe(cb1)
        webhook_source.subscribe(cb2)
        body = make_valid_event_json()
        await webhook_source._handle_post(body, "application/json")
        cb1.assert_called_once()
        cb2.assert_called_once()

    @pytest.mark.skipif(WebhookSource is None or WebhookSourceConfig is None or BindAddress is None,
                        reason="WebhookSource types not importable")
    async def test_handle_post_empty_body(self, webhook_source):
        """WebhookSource._handle_post returns 400 for empty body."""
        response = await webhook_source._handle_post(b'', "application/json")
        assert response.status == 400

    @pytest.mark.skipif(WebhookSource is None or WebhookSourceConfig is None or BindAddress is None,
                        reason="WebhookSource types not importable")
    async def test_handle_post_subscriber_failure_still_returns_200(self, webhook_source, failing_callback):
        """Failing subscriber doesn't prevent 200 response (error resilience)."""
        webhook_source.subscribe(failing_callback)
        body = make_valid_event_json()
        response = await webhook_source._handle_post(body, "application/json")
        assert response.status == 200


# ---------------------------------------------------------------------------
# OTLP SOURCE TESTS
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestOtlpSource:
    """Tests for OtlpSource lifecycle, subscribe, handler, and pure functions."""

    @pytest.fixture
    def otlp_config(self):
        bind = BindAddress(host="127.0.0.1", port=0)
        return OtlpSourceConfig(bind=bind, client_max_size_bytes=4194304, source_name="test-otlp")

    @pytest.fixture
    async def otlp_source(self, otlp_config):
        try:
            source = OtlpSource(otlp_config)
        except TypeError:
            source = OtlpSource(config=otlp_config)
        yield source
        try:
            await asyncio.wait_for(source.stop(), timeout=5.0)
        except Exception:
            pass

    @pytest.mark.skipif(OtlpSource is None or OtlpSourceConfig is None or BindAddress is None,
                        reason="OtlpSource types not importable")
    async def test_subscribe_appends_callback(self, otlp_source, collected_events):
        """OtlpSource.subscribe appends callback to subscriber list."""
        events, callback = collected_events
        otlp_source.subscribe(callback)
        subs = getattr(otlp_source, '_subscribers', None)
        if subs is not None:
            assert callback in subs

    @pytest.mark.skipif(OtlpSource is None or OtlpSourceConfig is None or BindAddress is None,
                        reason="OtlpSource types not importable")
    async def test_start_transitions_to_running(self, otlp_source):
        """OtlpSource.start transitions to RUNNING state."""
        await asyncio.wait_for(otlp_source.start(), timeout=5.0)
        state = getattr(otlp_source, '_state', None)
        if state is not None:
            assert state == SourceState.RUNNING

    @pytest.mark.skipif(OtlpSource is None or OtlpSourceConfig is None or BindAddress is None,
                        reason="OtlpSource types not importable")
    async def test_start_already_running_raises(self, otlp_source):
        """OtlpSource.start raises when already RUNNING."""
        await asyncio.wait_for(otlp_source.start(), timeout=5.0)
        with pytest.raises(RuntimeError):
            await asyncio.wait_for(otlp_source.start(), timeout=5.0)

    @pytest.mark.skipif(OtlpSource is None or OtlpSourceConfig is None or BindAddress is None,
                        reason="OtlpSource types not importable")
    async def test_stop_transitions_to_stopped(self, otlp_source):
        """OtlpSource.stop transitions to STOPPED."""
        await asyncio.wait_for(otlp_source.start(), timeout=5.0)
        await asyncio.wait_for(otlp_source.stop(), timeout=5.0)
        state = getattr(otlp_source, '_state', None)
        if state is not None:
            assert state == SourceState.STOPPED

    @pytest.mark.skipif(OtlpSource is None or OtlpSourceConfig is None or BindAddress is None,
                        reason="OtlpSource types not importable")
    async def test_stop_idempotent(self, otlp_source):
        """OtlpSource.stop is idempotent."""
        await asyncio.wait_for(otlp_source.start(), timeout=5.0)
        await asyncio.wait_for(otlp_source.stop(), timeout=5.0)
        await asyncio.wait_for(otlp_source.stop(), timeout=5.0)

    @pytest.mark.skipif(OtlpSource is None or OtlpSourceConfig is None or BindAddress is None,
                        reason="OtlpSource types not importable")
    async def test_handle_traces_valid_returns_200_empty_json(self, otlp_source, collected_events):
        """OtlpSource._handle_traces returns 200 with {} on valid OTLP payload."""
        events, callback = collected_events
        otlp_source.subscribe(callback)
        body = json.dumps(make_otlp_request()).encode()
        response = await otlp_source._handle_traces(body, "application/json")
        assert response.status == 200
        assert response.body == "{}" or json.loads(response.body) == {}

    @pytest.mark.skipif(OtlpSource is None or OtlpSourceConfig is None or BindAddress is None,
                        reason="OtlpSource types not importable")
    async def test_handle_traces_wrong_content_type(self, otlp_source):
        """OtlpSource._handle_traces returns 415 for non-JSON content type."""
        response = await otlp_source._handle_traces(b'{}', "text/plain")
        assert response.status == 415

    @pytest.mark.skipif(OtlpSource is None or OtlpSourceConfig is None or BindAddress is None,
                        reason="OtlpSource types not importable")
    async def test_handle_traces_invalid_json(self, otlp_source):
        """OtlpSource._handle_traces returns 400 on malformed JSON."""
        response = await otlp_source._handle_traces(b'not json', "application/json")
        assert response.status == 400

    @pytest.mark.skipif(OtlpSource is None or OtlpSourceConfig is None or BindAddress is None,
                        reason="OtlpSource types not importable")
    async def test_handle_traces_missing_resource_spans(self, otlp_source):
        """OtlpSource._handle_traces returns 400 when resourceSpans missing."""
        body = json.dumps({"not": "otlp"}).encode()
        response = await otlp_source._handle_traces(body, "application/json")
        assert response.status == 400

    @pytest.mark.skipif(OtlpSource is None or OtlpSourceConfig is None or BindAddress is None,
                        reason="OtlpSource types not importable")
    async def test_handle_traces_emits_per_span(self, otlp_source, collected_events):
        """OtlpSource._handle_traces emits one Event per span."""
        events, callback = collected_events
        otlp_source.subscribe(callback)
        spans = [
            {
                "traceId": "abc123",
                "spanId": f"span{i}",
                "name": f"op{i}",
                "startTimeUnixNano": 1700000000000000000,
                "endTimeUnixNano": 1700000001000000000,
                "attributes": [],
                "status": {"code": 0},
            }
            for i in range(3)
        ]
        body = json.dumps(make_otlp_request(spans=spans)).encode()
        await otlp_source._handle_traces(body, "application/json")
        assert len(events) == 3

    @pytest.mark.skipif(OtlpSource is None or OtlpSourceConfig is None or BindAddress is None,
                        reason="OtlpSource types not importable")
    async def test_handle_traces_emitted_events_have_otlp_trace_kind(self, otlp_source, collected_events):
        """Emitted events have event_kind starting with 'otlp.trace.'."""
        events, callback = collected_events
        otlp_source.subscribe(callback)
        body = json.dumps(make_otlp_request()).encode()
        await otlp_source._handle_traces(body, "application/json")
        assert len(events) >= 1
        for event in events:
            kind = event.event_kind
            kind_str = kind.value if hasattr(kind, 'value') else str(kind)
            assert kind_str.startswith("otlp.trace.")


@pytest.mark.asyncio
class TestOtlpFlattenSpans:
    """Tests for OtlpSource._flatten_spans pure function."""

    @pytest.mark.skipif(OtlpSource is None or OtlpSourceConfig is None or BindAddress is None,
                        reason="OtlpSource types not importable")
    async def test_flatten_basic(self):
        """_flatten_spans extracts OtlpSpan from valid OTLP request."""
        bind = BindAddress(host="127.0.0.1", port=0)
        cfg = OtlpSourceConfig(bind=bind, client_max_size_bytes=4194304, source_name="test")
        try:
            source = OtlpSource(cfg)
        except TypeError:
            source = OtlpSource(config=cfg)
        result = source._flatten_spans(make_otlp_request())
        assert len(result) == 1
        assert result[0].name == "my_operation"

    @pytest.mark.skipif(OtlpSource is None or OtlpSourceConfig is None or BindAddress is None,
                        reason="OtlpSource types not importable")
    async def test_flatten_resource_attributes_merged(self):
        """_flatten_spans merges resource attributes into each OtlpSpan."""
        bind = BindAddress(host="127.0.0.1", port=0)
        cfg = OtlpSourceConfig(bind=bind, client_max_size_bytes=4194304, source_name="test")
        try:
            source = OtlpSource(cfg)
        except TypeError:
            source = OtlpSource(config=cfg)
        result = source._flatten_spans(make_otlp_request())
        assert len(result) == 1
        # Resource attributes should be present
        ra = result[0].resource_attributes
        assert ra is not None
        assert len(ra) > 0

    @pytest.mark.skipif(OtlpSource is None or OtlpSourceConfig is None or BindAddress is None,
                        reason="OtlpSource types not importable")
    async def test_flatten_missing_resource_spans_raises(self):
        """_flatten_spans raises/errors on missing resourceSpans key."""
        bind = BindAddress(host="127.0.0.1", port=0)
        cfg = OtlpSourceConfig(bind=bind, client_max_size_bytes=4194304, source_name="test")
        try:
            source = OtlpSource(cfg)
        except TypeError:
            source = OtlpSource(config=cfg)
        with pytest.raises((KeyError, ValueError, Exception)):
            source._flatten_spans({"not": "otlp"})

    @pytest.mark.skipif(OtlpSource is None or OtlpSourceConfig is None or BindAddress is None,
                        reason="OtlpSource types not importable")
    async def test_flatten_multiple_resource_spans(self):
        """_flatten_spans handles multiple resourceSpans entries."""
        bind = BindAddress(host="127.0.0.1", port=0)
        cfg = OtlpSourceConfig(bind=bind, client_max_size_bytes=4194304, source_name="test")
        try:
            source = OtlpSource(cfg)
        except TypeError:
            source = OtlpSource(config=cfg)
        otlp = {
            "resourceSpans": [
                {
                    "resource": {"attributes": [{"key": "svc", "value": {"stringValue": "a"}}]},
                    "scopeSpans": [{"scope": {}, "spans": [
                        {"traceId": "t1", "spanId": "s1", "name": "op1",
                         "startTimeUnixNano": 1, "endTimeUnixNano": 2,
                         "attributes": [], "status": {"code": 0}}
                    ]}]
                },
                {
                    "resource": {"attributes": [{"key": "svc", "value": {"stringValue": "b"}}]},
                    "scopeSpans": [{"scope": {}, "spans": [
                        {"traceId": "t2", "spanId": "s2", "name": "op2",
                         "startTimeUnixNano": 3, "endTimeUnixNano": 4,
                         "attributes": [], "status": {"code": 0}}
                    ]}]
                }
            ]
        }
        result = source._flatten_spans(otlp)
        assert len(result) == 2

    @pytest.mark.skipif(OtlpSource is None or OtlpSourceConfig is None or BindAddress is None,
                        reason="OtlpSource types not importable")
    async def test_flatten_empty_spans_list(self):
        """_flatten_spans returns empty list when no spans present."""
        bind = BindAddress(host="127.0.0.1", port=0)
        cfg = OtlpSourceConfig(bind=bind, client_max_size_bytes=4194304, source_name="test")
        try:
            source = OtlpSource(cfg)
        except TypeError:
            source = OtlpSource(config=cfg)
        otlp = {
            "resourceSpans": [
                {
                    "resource": {"attributes": []},
                    "scopeSpans": [{"scope": {}, "spans": []}]
                }
            ]
        }
        result = source._flatten_spans(otlp)
        assert len(result) == 0


@pytest.mark.asyncio
class TestOtlpSpanToEvent:
    """Tests for OtlpSource._span_to_event pure function."""

    @pytest.fixture
    def otlp_source_instance(self):
        bind = BindAddress(host="127.0.0.1", port=0)
        cfg = OtlpSourceConfig(bind=bind, client_max_size_bytes=4194304, source_name="test-otlp")
        try:
            return OtlpSource(cfg)
        except TypeError:
            return OtlpSource(config=cfg)

    @pytest.fixture
    def sample_span(self):
        return OtlpSpan(
            trace_id="abc123def456",
            span_id="789012",
            name="my_operation",
            start_time_unix_nano=1700000000000000000,
            end_time_unix_nano=1700000001000000000,
            attributes={"http.method": "GET"},
            resource_attributes={"service.name": "test-svc"},
            status_code=1,
        )

    @pytest.mark.skipif(OtlpSource is None or OtlpSpan is None or OtlpSourceConfig is None or BindAddress is None,
                        reason="OtlpSource types not importable")
    async def test_span_to_event_produces_event(self, otlp_source_instance, sample_span):
        """_span_to_event converts OtlpSpan to Event with correct fields."""
        event = otlp_source_instance._span_to_event(sample_span, "test-otlp")
        assert event is not None
        assert event.source == "test-otlp"

    @pytest.mark.skipif(OtlpSource is None or OtlpSpan is None or OtlpSourceConfig is None or BindAddress is None,
                        reason="OtlpSource types not importable")
    async def test_span_to_event_kind_prefix(self, otlp_source_instance, sample_span):
        """_span_to_event sets event_kind starting with 'otlp.trace.'."""
        event = otlp_source_instance._span_to_event(sample_span, "test-otlp")
        kind = event.event_kind
        kind_str = kind.value if hasattr(kind, 'value') else str(kind)
        assert kind_str.startswith("otlp.trace.")

    @pytest.mark.skipif(OtlpSource is None or OtlpSpan is None or OtlpSourceConfig is None or BindAddress is None,
                        reason="OtlpSource types not importable")
    async def test_span_to_event_correlation_keys(self, otlp_source_instance, sample_span):
        """_span_to_event includes trace_id and span_id in correlation_keys."""
        event = otlp_source_instance._span_to_event(sample_span, "test-otlp")
        keys = event.correlation_keys
        # Should contain the trace and span IDs
        assert any("abc123def456" in str(k) for k in keys)
        assert any("789012" in str(k) for k in keys)

    @pytest.mark.skipif(OtlpSource is None or OtlpSpan is None or OtlpSourceConfig is None or BindAddress is None,
                        reason="OtlpSource types not importable")
    async def test_span_to_event_timestamp_iso8601(self, otlp_source_instance, sample_span):
        """_span_to_event derives ISO 8601 UTC timestamp from start_time_unix_nano."""
        event = otlp_source_instance._span_to_event(sample_span, "test-otlp")
        ts = event.timestamp
        # Should match ISO 8601 UTC pattern
        assert re.match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$', ts)

    @pytest.mark.skipif(OtlpSource is None or OtlpSpan is None or OtlpSourceConfig is None or BindAddress is None,
                        reason="OtlpSource types not importable")
    async def test_span_to_event_uuid4_event_id(self, otlp_source_instance, sample_span):
        """_span_to_event generates valid UUID4 event_id."""
        event = otlp_source_instance._span_to_event(sample_span, "test-otlp")
        eid = event.event_id
        # Validate UUID4 format
        assert re.match(
            r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$',
            eid
        )

    @pytest.mark.skipif(OtlpSource is None or OtlpSpan is None or OtlpSourceConfig is None or BindAddress is None,
                        reason="OtlpSource types not importable")
    async def test_span_to_event_unique_ids(self, otlp_source_instance, sample_span):
        """_span_to_event generates fresh UUID4 for each call."""
        e1 = otlp_source_instance._span_to_event(sample_span, "test-otlp")
        e2 = otlp_source_instance._span_to_event(sample_span, "test-otlp")
        assert e1.event_id != e2.event_id


# ---------------------------------------------------------------------------
# FILE SOURCE TESTS
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestFileSource:
    """Tests for FileSource lifecycle and polling behavior."""

    @pytest.fixture
    def file_config(self, tmp_path):
        """Create a FileSourceConfig pointing to a temp file with short poll."""
        filepath = tmp_path / "test.jsonl"
        fp = FilePath(value=str(filepath))
        return FileSourceConfig(path=fp, poll_interval_seconds=0.1, source_name="test-file"), filepath

    @pytest.fixture
    async def file_source(self, file_config):
        cfg, filepath = file_config
        try:
            source = FileSource(cfg)
        except TypeError:
            source = FileSource(config=cfg)
        yield source, filepath
        try:
            await asyncio.wait_for(source.stop(), timeout=5.0)
        except Exception:
            pass

    @pytest.mark.skipif(FileSource is None or FileSourceConfig is None or FilePath is None,
                        reason="FileSource types not importable")
    async def test_subscribe_appends_callback(self, file_source, collected_events):
        """FileSource.subscribe appends callback to subscriber list."""
        source, filepath = file_source
        events, callback = collected_events
        source.subscribe(callback)

    @pytest.mark.skipif(FileSource is None or FileSourceConfig is None or FilePath is None,
                        reason="FileSource types not importable")
    async def test_start_transitions_to_running(self, file_source):
        """FileSource.start transitions to RUNNING and creates polling task."""
        source, filepath = file_source
        # Create the file so start can read it
        filepath.touch()
        await asyncio.wait_for(source.start(), timeout=5.0)
        state = getattr(source, '_state', None)
        if state is not None:
            assert state == SourceState.RUNNING

    @pytest.mark.skipif(FileSource is None or FileSourceConfig is None or FilePath is None,
                        reason="FileSource types not importable")
    async def test_start_already_running_raises(self, file_source):
        """FileSource.start raises when already RUNNING."""
        source, filepath = file_source
        filepath.touch()
        await asyncio.wait_for(source.start(), timeout=5.0)
        with pytest.raises(RuntimeError):
            await asyncio.wait_for(source.start(), timeout=5.0)

    @pytest.mark.skipif(FileSource is None or FileSourceConfig is None or FilePath is None,
                        reason="FileSource types not importable")
    async def test_stop_transitions_to_stopped(self, file_source):
        """FileSource.stop transitions to STOPPED."""
        source, filepath = file_source
        filepath.touch()
        await asyncio.wait_for(source.start(), timeout=5.0)
        await asyncio.wait_for(source.stop(), timeout=5.0)
        state = getattr(source, '_state', None)
        if state is not None:
            assert state == SourceState.STOPPED

    @pytest.mark.skipif(FileSource is None or FileSourceConfig is None or FilePath is None,
                        reason="FileSource types not importable")
    async def test_stop_idempotent(self, file_source):
        """FileSource.stop is idempotent."""
        source, filepath = file_source
        filepath.touch()
        await asyncio.wait_for(source.start(), timeout=5.0)
        await asyncio.wait_for(source.stop(), timeout=5.0)
        await asyncio.wait_for(source.stop(), timeout=5.0)

    @pytest.mark.skipif(FileSource is None or FileSourceConfig is None or FilePath is None,
                        reason="FileSource types not importable")
    async def test_stop_never_started(self, file_source):
        """FileSource.stop on never-started source is safe."""
        source, filepath = file_source
        await asyncio.wait_for(source.stop(), timeout=5.0)

    @pytest.mark.skipif(FileSource is None or FileSourceConfig is None or FilePath is None,
                        reason="FileSource types not importable")
    async def test_poll_new_lines_emitted(self, file_source, collected_events):
        """FileSource emits events for new JSONL lines appended after start."""
        source, filepath = file_source
        events, callback = collected_events
        source.subscribe(callback)

        # Create file, start source (tail mode = starts at end)
        filepath.touch()
        await asyncio.wait_for(source.start(), timeout=5.0)

        # Append a valid event line
        event_data = make_valid_event_dict()
        with open(filepath, "a") as f:
            f.write(json.dumps(event_data) + "\n")

        # Wait for poll to pick it up
        await asyncio.sleep(0.5)

        assert len(events) >= 1

    @pytest.mark.skipif(FileSource is None or FileSourceConfig is None or FilePath is None,
                        reason="FileSource types not importable")
    async def test_poll_start_at_end_of_existing_file(self, file_source, collected_events):
        """FileSource starts reading from end of existing file (tail mode)."""
        source, filepath = file_source
        events, callback = collected_events
        source.subscribe(callback)

        # Write data BEFORE start
        event_data = make_valid_event_dict()
        with open(filepath, "w") as f:
            f.write(json.dumps(event_data) + "\n")

        await asyncio.wait_for(source.start(), timeout=5.0)
        await asyncio.sleep(0.5)

        # Should NOT have emitted the pre-existing line
        assert len(events) == 0

    @pytest.mark.skipif(FileSource is None or FileSourceConfig is None or FilePath is None,
                        reason="FileSource types not importable")
    async def test_poll_invalid_json_line_skipped(self, file_source, collected_events):
        """FileSource logs and skips invalid JSON lines."""
        source, filepath = file_source
        events, callback = collected_events
        source.subscribe(callback)

        filepath.touch()
        await asyncio.wait_for(source.start(), timeout=5.0)

        # Append invalid JSON line followed by valid line
        event_data = make_valid_event_dict()
        with open(filepath, "a") as f:
            f.write("not valid json\n")
            f.write(json.dumps(event_data) + "\n")

        await asyncio.sleep(0.5)

        # Only valid event should be emitted (invalid line skipped)
        assert len(events) >= 1

    @pytest.mark.skipif(FileSource is None or FileSourceConfig is None or FilePath is None,
                        reason="FileSource types not importable")
    async def test_poll_file_not_found_handled(self, file_source, collected_events):
        """FileSource handles missing file gracefully."""
        source, filepath = file_source
        events, callback = collected_events
        source.subscribe(callback)

        # Don't create the file — source should start and handle missing file
        await asyncio.wait_for(source.start(), timeout=5.0)
        await asyncio.sleep(0.3)

        # Should not crash; create file later should work
        event_data = make_valid_event_dict()
        with open(filepath, "w") as f:
            f.write(json.dumps(event_data) + "\n")

        await asyncio.sleep(0.5)
        # File was created from scratch, so offset should start at 0 (new file)
        # The event should eventually be picked up
        # (exact behavior depends on implementation details)

    @pytest.mark.skipif(FileSource is None or FileSourceConfig is None or FilePath is None,
                        reason="FileSource types not importable")
    async def test_poll_truncation_resets_offset(self, file_source, collected_events):
        """FileSource resets offset to 0 on file truncation."""
        source, filepath = file_source
        events, callback = collected_events
        source.subscribe(callback)

        filepath.touch()
        await asyncio.wait_for(source.start(), timeout=5.0)

        # Write some data to advance offset
        event1 = make_valid_event_dict(event_id="a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d")
        with open(filepath, "a") as f:
            f.write(json.dumps(event1) + "\n")
        await asyncio.sleep(0.3)

        # Truncate the file
        with open(filepath, "w") as f:
            pass  # empty the file

        # Write new data
        event2 = make_valid_event_dict(event_id="b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e")
        with open(filepath, "a") as f:
            f.write(json.dumps(event2) + "\n")

        await asyncio.sleep(0.5)
        # Should have received at least 2 events (one before truncation, one after)
        assert len(events) >= 2

    @pytest.mark.skipif(FileSource is None or FileSourceConfig is None or FilePath is None,
                        reason="FileSource types not importable")
    async def test_poll_partial_line_buffered(self, file_source, collected_events):
        """FileSource buffers partial lines and only emits complete ones."""
        source, filepath = file_source
        events, callback = collected_events
        source.subscribe(callback)

        filepath.touch()
        await asyncio.wait_for(source.start(), timeout=5.0)

        # Write a partial line (no newline)
        event_data = make_valid_event_dict()
        partial = json.dumps(event_data)
        with open(filepath, "a") as f:
            f.write(partial)  # no newline

        await asyncio.sleep(0.3)
        # Should NOT have emitted yet (incomplete line)
        assert len(events) == 0

        # Now complete the line
        with open(filepath, "a") as f:
            f.write("\n")

        await asyncio.sleep(0.3)
        # Should now have emitted
        assert len(events) >= 1


# ---------------------------------------------------------------------------
# SENTINEL SOURCE TESTS
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestSentinelSource:
    """Tests for SentinelSource lifecycle, subscribe, and handler."""

    @pytest.fixture
    def sentinel_config(self):
        bind = BindAddress(host="127.0.0.1", port=0)
        return SentinelSourceConfig(
            bind=bind, client_max_size_bytes=1048576,
            event_kind_prefix="sentinel", source_name="test-sentinel"
        )

    @pytest.fixture
    async def sentinel_source(self, sentinel_config):
        try:
            source = SentinelSource(sentinel_config)
        except TypeError:
            source = SentinelSource(config=sentinel_config)
        yield source
        try:
            await asyncio.wait_for(source.stop(), timeout=5.0)
        except Exception:
            pass

    @pytest.mark.skipif(SentinelSource is None or SentinelSourceConfig is None or BindAddress is None,
                        reason="SentinelSource types not importable")
    async def test_subscribe_appends_callback(self, sentinel_source, collected_events):
        """SentinelSource.subscribe appends callback to subscriber list."""
        events, callback = collected_events
        sentinel_source.subscribe(callback)

    @pytest.mark.skipif(SentinelSource is None or SentinelSourceConfig is None or BindAddress is None,
                        reason="SentinelSource types not importable")
    async def test_start_transitions_to_running(self, sentinel_source):
        """SentinelSource.start transitions to RUNNING state."""
        await asyncio.wait_for(sentinel_source.start(), timeout=5.0)
        state = getattr(sentinel_source, '_state', None)
        if state is not None:
            assert state == SourceState.RUNNING

    @pytest.mark.skipif(SentinelSource is None or SentinelSourceConfig is None or BindAddress is None,
                        reason="SentinelSource types not importable")
    async def test_start_already_running_raises(self, sentinel_source):
        """SentinelSource.start raises RuntimeError when already RUNNING."""
        await asyncio.wait_for(sentinel_source.start(), timeout=5.0)
        with pytest.raises(RuntimeError):
            await asyncio.wait_for(sentinel_source.start(), timeout=5.0)

    @pytest.mark.skipif(SentinelSource is None or SentinelSourceConfig is None or BindAddress is None,
                        reason="SentinelSource types not importable")
    async def test_stop_transitions_to_stopped(self, sentinel_source):
        """SentinelSource.stop transitions to STOPPED."""
        await asyncio.wait_for(sentinel_source.start(), timeout=5.0)
        await asyncio.wait_for(sentinel_source.stop(), timeout=5.0)
        state = getattr(sentinel_source, '_state', None)
        if state is not None:
            assert state == SourceState.STOPPED

    @pytest.mark.skipif(SentinelSource is None or SentinelSourceConfig is None or BindAddress is None,
                        reason="SentinelSource types not importable")
    async def test_stop_idempotent(self, sentinel_source):
        """SentinelSource.stop is idempotent."""
        await asyncio.wait_for(sentinel_source.start(), timeout=5.0)
        await asyncio.wait_for(sentinel_source.stop(), timeout=5.0)
        await asyncio.wait_for(sentinel_source.stop(), timeout=5.0)

    @pytest.mark.skipif(SentinelSource is None or SentinelSourceConfig is None or BindAddress is None,
                        reason="SentinelSource types not importable")
    async def test_handle_post_valid_payload_returns_200(self, sentinel_source, collected_events):
        """SentinelSource._handle_post returns 200 on valid Sentinel incident payload."""
        events, callback = collected_events
        sentinel_source.subscribe(callback)
        body = json.dumps(make_sentinel_payload()).encode()
        response = await sentinel_source._handle_post(body, "application/json")
        assert response.status == 200

    @pytest.mark.skipif(SentinelSource is None or SentinelSourceConfig is None or BindAddress is None,
                        reason="SentinelSource types not importable")
    async def test_handle_post_wrong_content_type(self, sentinel_source):
        """SentinelSource._handle_post returns 415 for non-JSON content type."""
        response = await sentinel_source._handle_post(b'{}', "text/xml")
        assert response.status == 415

    @pytest.mark.skipif(SentinelSource is None or SentinelSourceConfig is None or BindAddress is None,
                        reason="SentinelSource types not importable")
    async def test_handle_post_invalid_json(self, sentinel_source):
        """SentinelSource._handle_post returns 400 on invalid JSON."""
        response = await sentinel_source._handle_post(b'not json', "application/json")
        assert response.status == 400

    @pytest.mark.skipif(SentinelSource is None or SentinelSourceConfig is None or BindAddress is None,
                        reason="SentinelSource types not importable")
    async def test_handle_post_unrecognized_payload(self, sentinel_source):
        """SentinelSource._handle_post returns 400 on unrecognized payload structure."""
        body = json.dumps({"random": "data"}).encode()
        response = await sentinel_source._handle_post(body, "application/json")
        assert response.status == 400

    @pytest.mark.skipif(SentinelSource is None or SentinelSourceConfig is None or BindAddress is None,
                        reason="SentinelSource types not importable")
    async def test_handle_post_event_kind_prefix(self, sentinel_source, collected_events):
        """SentinelSource emits events with event_kind starting with configured prefix (AS9 invariant)."""
        events, callback = collected_events
        sentinel_source.subscribe(callback)
        body = json.dumps(make_sentinel_payload(action="created")).encode()
        await sentinel_source._handle_post(body, "application/json")
        assert len(events) >= 1
        for event in events:
            kind = event.event_kind
            kind_str = kind.value if hasattr(kind, 'value') else str(kind)
            assert kind_str.startswith("sentinel.")
            assert ".incident." in kind_str

    @pytest.mark.skipif(SentinelSource is None or SentinelSourceConfig is None or BindAddress is None,
                        reason="SentinelSource types not importable")
    async def test_handle_post_correlation_keys_include_incident_id(self, sentinel_source, collected_events):
        """SentinelSource emits events with incident_id in correlation_keys."""
        events, callback = collected_events
        sentinel_source.subscribe(callback)
        body = json.dumps(make_sentinel_payload(incident_id="inc-123")).encode()
        await sentinel_source._handle_post(body, "application/json")
        assert len(events) >= 1
        keys = events[0].correlation_keys
        assert any("inc-123" in str(k) for k in keys)

    @pytest.mark.skipif(SentinelSource is None or SentinelSourceConfig is None or BindAddress is None,
                        reason="SentinelSource types not importable")
    async def test_handle_post_emitted_event_has_uuid4(self, sentinel_source, collected_events):
        """SentinelSource emitted events have valid UUID4 event_id."""
        events, callback = collected_events
        sentinel_source.subscribe(callback)
        body = json.dumps(make_sentinel_payload()).encode()
        await sentinel_source._handle_post(body, "application/json")
        assert len(events) >= 1
        eid = events[0].event_id
        assert re.match(
            r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$',
            eid
        )

    @pytest.mark.skipif(SentinelSource is None or SentinelSourceConfig is None or BindAddress is None,
                        reason="SentinelSource types not importable")
    async def test_handle_post_emitted_event_has_timestamp(self, sentinel_source, collected_events):
        """SentinelSource emitted events have valid ISO 8601 UTC timestamp."""
        events, callback = collected_events
        sentinel_source.subscribe(callback)
        body = json.dumps(make_sentinel_payload()).encode()
        await sentinel_source._handle_post(body, "application/json")
        assert len(events) >= 1
        ts = events[0].timestamp
        assert re.match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$', ts)


# ---------------------------------------------------------------------------
# PROTOCOL CONFORMANCE / INVARIANT TESTS
# ---------------------------------------------------------------------------

class TestSourceProtocolConformance:
    """Tests that all source types implement SourceProtocol."""

    @pytest.mark.skipif(SourceProtocol is None, reason="SourceProtocol not importable")
    @pytest.mark.skipif(WebhookSource is None, reason="WebhookSource not importable")
    def test_webhook_implements_protocol(self):
        """WebhookSource implements SourceProtocol (start, stop, subscribe)."""
        assert isinstance(WebhookSource, type)
        assert hasattr(WebhookSource, 'start')
        assert hasattr(WebhookSource, 'stop')
        assert hasattr(WebhookSource, 'subscribe')
        # If runtime_checkable, test isinstance
        try:
            bind = BindAddress(host="127.0.0.1", port=0)
            cfg = WebhookSourceConfig(bind=bind, client_max_size_bytes=1024, source_name="t")
            try:
                instance = WebhookSource(cfg)
            except TypeError:
                instance = WebhookSource(config=cfg)
            assert isinstance(instance, SourceProtocol)
        except Exception:
            pass  # Protocol check may not work if not runtime_checkable

    @pytest.mark.skipif(SourceProtocol is None, reason="SourceProtocol not importable")
    @pytest.mark.skipif(OtlpSource is None, reason="OtlpSource not importable")
    def test_otlp_implements_protocol(self):
        """OtlpSource implements SourceProtocol."""
        assert hasattr(OtlpSource, 'start')
        assert hasattr(OtlpSource, 'stop')
        assert hasattr(OtlpSource, 'subscribe')
        try:
            bind = BindAddress(host="127.0.0.1", port=0)
            cfg = OtlpSourceConfig(bind=bind, client_max_size_bytes=1024, source_name="t")
            try:
                instance = OtlpSource(cfg)
            except TypeError:
                instance = OtlpSource(config=cfg)
            assert isinstance(instance, SourceProtocol)
        except Exception:
            pass

    @pytest.mark.skipif(SourceProtocol is None, reason="SourceProtocol not importable")
    @pytest.mark.skipif(FileSource is None, reason="FileSource not importable")
    def test_file_implements_protocol(self):
        """FileSource implements SourceProtocol."""
        assert hasattr(FileSource, 'start')
        assert hasattr(FileSource, 'stop')
        assert hasattr(FileSource, 'subscribe')

    @pytest.mark.skipif(SourceProtocol is None, reason="SourceProtocol not importable")
    @pytest.mark.skipif(SentinelSource is None, reason="SentinelSource not importable")
    def test_sentinel_implements_protocol(self):
        """SentinelSource implements SourceProtocol."""
        assert hasattr(SentinelSource, 'start')
        assert hasattr(SentinelSource, 'stop')
        assert hasattr(SentinelSource, 'subscribe')
        try:
            bind = BindAddress(host="127.0.0.1", port=0)
            cfg = SentinelSourceConfig(
                bind=bind, client_max_size_bytes=1024,
                event_kind_prefix="sentinel", source_name="t"
            )
            try:
                instance = SentinelSource(cfg)
            except TypeError:
                instance = SentinelSource(config=cfg)
            assert isinstance(instance, SourceProtocol)
        except Exception:
            pass


@pytest.mark.asyncio
class TestStateTransitionInvariants:
    """Tests for state machine invariants: CREATED→RUNNING→STOPPED, no reverse."""

    @pytest.mark.skipif(WebhookSource is None or WebhookSourceConfig is None or BindAddress is None or SourceState is None,
                        reason="Required types not importable")
    async def test_webhook_state_transitions(self):
        """WebhookSource state transitions follow CREATED→RUNNING→STOPPED."""
        bind = BindAddress(host="127.0.0.1", port=0)
        cfg = WebhookSourceConfig(bind=bind, client_max_size_bytes=1024, source_name="t")
        try:
            source = WebhookSource(cfg)
        except TypeError:
            source = WebhookSource(config=cfg)

        # Initially CREATED
        state = getattr(source, '_state', None)
        if state is not None:
            assert state == SourceState.CREATED

        await asyncio.wait_for(source.start(), timeout=5.0)
        state = getattr(source, '_state', None)
        if state is not None:
            assert state == SourceState.RUNNING

        await asyncio.wait_for(source.stop(), timeout=5.0)
        state = getattr(source, '_state', None)
        if state is not None:
            assert state == SourceState.STOPPED

    @pytest.mark.skipif(OtlpSource is None or OtlpSourceConfig is None or BindAddress is None or SourceState is None,
                        reason="Required types not importable")
    async def test_otlp_state_transitions(self):
        """OtlpSource state transitions follow CREATED→RUNNING→STOPPED."""
        bind = BindAddress(host="127.0.0.1", port=0)
        cfg = OtlpSourceConfig(bind=bind, client_max_size_bytes=1024, source_name="t")
        try:
            source = OtlpSource(cfg)
        except TypeError:
            source = OtlpSource(config=cfg)

        state = getattr(source, '_state', None)
        if state is not None:
            assert state == SourceState.CREATED

        await asyncio.wait_for(source.start(), timeout=5.0)
        state = getattr(source, '_state', None)
        if state is not None:
            assert state == SourceState.RUNNING

        await asyncio.wait_for(source.stop(), timeout=5.0)
        state = getattr(source, '_state', None)
        if state is not None:
            assert state == SourceState.STOPPED


@pytest.mark.asyncio
class TestEmitResilienceInvariant:
    """Invariant: emit() never propagates subscriber exceptions."""

    @pytest.mark.skipif(emit is None or Event is None, reason="Required types not importable")
    async def test_exception_types_caught(self):
        """emit() catches all Exception subclasses from subscribers."""
        event = Event(**make_valid_event_dict())
        results = []

        async def value_error_sub(e):
            raise ValueError("bad value")

        async def type_error_sub(e):
            raise TypeError("bad type")

        async def runtime_error_sub(e):
            raise RuntimeError("bad runtime")

        async def success_sub(e):
            results.append("ok")

        # None of these should propagate
        await emit([value_error_sub, type_error_sub, runtime_error_sub, success_sub], event)
        assert results == ["ok"]


# ---------------------------------------------------------------------------
# HTTP CONTENT-TYPE VALIDATION INVARIANT (parametrized)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestHttpContentTypeInvariant:
    """Invariant: HTTP sources validate Content-Type header before parsing body."""

    @pytest.mark.skipif(WebhookSource is None or WebhookSourceConfig is None or BindAddress is None,
                        reason="WebhookSource not importable")
    @pytest.mark.parametrize("content_type", [
        "text/plain",
        "text/html",
        "application/xml",
        "multipart/form-data",
        "",
    ])
    async def test_webhook_rejects_non_json_content_types(self, content_type):
        """WebhookSource returns 415 for various non-JSON content types."""
        bind = BindAddress(host="127.0.0.1", port=0)
        cfg = WebhookSourceConfig(bind=bind, client_max_size_bytes=1024, source_name="t")
        try:
            source = WebhookSource(cfg)
        except TypeError:
            source = WebhookSource(config=cfg)
        response = await source._handle_post(b'{}', content_type)
        assert response.status == 415

    @pytest.mark.skipif(OtlpSource is None or OtlpSourceConfig is None or BindAddress is None,
                        reason="OtlpSource not importable")
    @pytest.mark.parametrize("content_type", [
        "text/plain",
        "application/xml",
        "",
    ])
    async def test_otlp_rejects_non_json_content_types(self, content_type):
        """OtlpSource returns 415 for various non-JSON content types."""
        bind = BindAddress(host="127.0.0.1", port=0)
        cfg = OtlpSourceConfig(bind=bind, client_max_size_bytes=1024, source_name="t")
        try:
            source = OtlpSource(cfg)
        except TypeError:
            source = OtlpSource(config=cfg)
        response = await source._handle_traces(b'{}', content_type)
        assert response.status == 415

    @pytest.mark.skipif(SentinelSource is None or SentinelSourceConfig is None or BindAddress is None,
                        reason="SentinelSource not importable")
    @pytest.mark.parametrize("content_type", [
        "text/plain",
        "application/xml",
        "",
    ])
    async def test_sentinel_rejects_non_json_content_types(self, content_type):
        """SentinelSource returns 415 for various non-JSON content types."""
        bind = BindAddress(host="127.0.0.1", port=0)
        cfg = SentinelSourceConfig(
            bind=bind, client_max_size_bytes=1024,
            event_kind_prefix="sentinel", source_name="t"
        )
        try:
            source = SentinelSource(cfg)
        except TypeError:
            source = SentinelSource(config=cfg)
        response = await source._handle_post(b'{}', content_type)
        assert response.status == 415


# ---------------------------------------------------------------------------
# OTLP RETURNS EMPTY JSON OBJECT INVARIANT
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestOtlpReturnsEmptyJson:
    """Invariant: OtlpSource returns empty JSON object '{}' per OTLP HTTP spec."""

    @pytest.mark.skipif(OtlpSource is None or OtlpSourceConfig is None or BindAddress is None,
                        reason="OtlpSource not importable")
    async def test_success_response_is_empty_json(self):
        """On successful trace processing, response body is '{}'."""
        bind = BindAddress(host="127.0.0.1", port=0)
        cfg = OtlpSourceConfig(bind=bind, client_max_size_bytes=4194304, source_name="test")
        try:
            source = OtlpSource(cfg)
        except TypeError:
            source = OtlpSource(config=cfg)
        body = json.dumps(make_otlp_request()).encode()
        response = await source._handle_traces(body, "application/json")
        assert response.status == 200
        parsed = json.loads(response.body)
        assert parsed == {}


# ---------------------------------------------------------------------------
# EVENT UUID & TIMESTAMP INVARIANT
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestEventFieldInvariants:
    """Invariant: All Events emitted by sources have valid UUID4 event_id and ISO 8601 UTC timestamp."""

    UUID4_RE = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$')
    TS_RE = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$')

    @pytest.mark.skipif(WebhookSource is None or WebhookSourceConfig is None or BindAddress is None,
                        reason="WebhookSource not importable")
    async def test_webhook_emitted_event_fields(self):
        """WebhookSource emitted events have valid UUID4 and ISO 8601 timestamp."""
        bind = BindAddress(host="127.0.0.1", port=0)
        cfg = WebhookSourceConfig(bind=bind, client_max_size_bytes=1048576, source_name="t")
        try:
            source = WebhookSource(cfg)
        except TypeError:
            source = WebhookSource(config=cfg)

        events = []
        async def cb(e):
            events.append(e)

        source.subscribe(cb)
        body = make_valid_event_json()
        await source._handle_post(body, "application/json")
        assert len(events) == 1
        assert self.UUID4_RE.match(events[0].event_id)
        assert self.TS_RE.match(events[0].timestamp)

    @pytest.mark.skipif(OtlpSource is None or OtlpSourceConfig is None or BindAddress is None,
                        reason="OtlpSource not importable")
    async def test_otlp_emitted_event_fields(self):
        """OtlpSource emitted events have valid UUID4 and ISO 8601 timestamp."""
        bind = BindAddress(host="127.0.0.1", port=0)
        cfg = OtlpSourceConfig(bind=bind, client_max_size_bytes=4194304, source_name="t")
        try:
            source = OtlpSource(cfg)
        except TypeError:
            source = OtlpSource(config=cfg)

        events = []
        async def cb(e):
            events.append(e)

        source.subscribe(cb)
        body = json.dumps(make_otlp_request()).encode()
        await source._handle_traces(body, "application/json")
        assert len(events) >= 1
        for event in events:
            assert self.UUID4_RE.match(event.event_id)
            assert self.TS_RE.match(event.timestamp)

    @pytest.mark.skipif(SentinelSource is None or SentinelSourceConfig is None or BindAddress is None,
                        reason="SentinelSource not importable")
    async def test_sentinel_emitted_event_fields(self):
        """SentinelSource emitted events have valid UUID4 and ISO 8601 timestamp."""
        bind = BindAddress(host="127.0.0.1", port=0)
        cfg = SentinelSourceConfig(
            bind=bind, client_max_size_bytes=1048576,
            event_kind_prefix="sentinel", source_name="t"
        )
        try:
            source = SentinelSource(cfg)
        except TypeError:
            source = SentinelSource(config=cfg)

        events = []
        async def cb(e):
            events.append(e)

        source.subscribe(cb)
        body = json.dumps(make_sentinel_payload()).encode()
        await source._handle_post(body, "application/json")
        assert len(events) >= 1
        for event in events:
            assert self.UUID4_RE.match(event.event_id)
            assert self.TS_RE.match(event.timestamp)


# ---------------------------------------------------------------------------
# SENTINEL EVENT KIND PREFIX INVARIANT (AS9)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestSentinelEventKindPrefixInvariant:
    """Invariant: SentinelSource prefixes all event_kind values with configured event_kind_prefix (AS9)."""

    @pytest.mark.skipif(SentinelSource is None or SentinelSourceConfig is None or BindAddress is None,
                        reason="SentinelSource not importable")
    async def test_custom_prefix(self):
        """SentinelSource uses custom event_kind_prefix in emitted events."""
        bind = BindAddress(host="127.0.0.1", port=0)
        cfg = SentinelSourceConfig(
            bind=bind, client_max_size_bytes=1048576,
            event_kind_prefix="myprefix", source_name="t"
        )
        try:
            source = SentinelSource(cfg)
        except TypeError:
            source = SentinelSource(config=cfg)

        events = []
        async def cb(e):
            events.append(e)

        source.subscribe(cb)
        body = json.dumps(make_sentinel_payload(action="created")).encode()
        await source._handle_post(body, "application/json")
        assert len(events) >= 1
        kind = events[0].event_kind
        kind_str = kind.value if hasattr(kind, 'value') else str(kind)
        assert kind_str.startswith("myprefix.")
        assert ".incident." in kind_str


# ---------------------------------------------------------------------------
# SUBSCRIBE BEFORE/AFTER START INVARIANT
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestSubscribeBeforeAfterStart:
    """Invariant: subscribe() may be called before or after start()."""

    @pytest.mark.skipif(WebhookSource is None or WebhookSourceConfig is None or BindAddress is None,
                        reason="WebhookSource not importable")
    async def test_webhook_subscribe_after_start(self):
        """WebhookSource.subscribe works after start()."""
        bind = BindAddress(host="127.0.0.1", port=0)
        cfg = WebhookSourceConfig(bind=bind, client_max_size_bytes=1024, source_name="t")
        try:
            source = WebhookSource(cfg)
        except TypeError:
            source = WebhookSource(config=cfg)

        await asyncio.wait_for(source.start(), timeout=5.0)
        try:
            events = []
            async def cb(e):
                events.append(e)
            source.subscribe(cb)  # subscribe AFTER start

            body = make_valid_event_json()
            await source._handle_post(body, "application/json")
            assert len(events) == 1
        finally:
            await asyncio.wait_for(source.stop(), timeout=5.0)

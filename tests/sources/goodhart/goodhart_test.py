"""
Adversarial hidden acceptance tests for Event Sources component.
These tests detect implementations that pass visible tests through shortcuts
(hardcoded returns, missing validation, etc.) rather than truly satisfying the contract.
"""

import asyncio
import json
import uuid
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from chronicler.sources.base import SourceState, emit
from chronicler.sources.webhook import (
    WebhookSource, WebhookSourceConfig, BindAddress,
    HttpResponse, Event, EventKind, FilePath, FilePosition,
)
from chronicler.sources.otlp import OtlpSource, OtlpSourceConfig, OtlpSpan
from chronicler.sources.file import FileSource, FileSourceConfig
from chronicler.sources.sentinel import SentinelSource, SentinelSourceConfig


# ─── Helpers ───────────────────────────────────────────────────────────────────

def make_valid_event_json(**overrides):
    """Create a valid Event JSON bytes with optional field overrides."""
    event = {
        "event_id": overrides.get("event_id", str(uuid.uuid4())),
        "event_kind": overrides.get("event_kind", "webhook.push"),
        "timestamp": overrides.get("timestamp", "2024-01-15T10:30:00Z"),
        "source": overrides.get("source", "test-source"),
        "payload": overrides.get("payload", {"key": "value"}),
        "correlation_keys": overrides.get("correlation_keys", ["corr-1"]),
    }
    return json.dumps(event).encode("utf-8")


def make_valid_otlp_payload(spans=None, resource_attrs=None):
    """Create a valid OTLP ExportTraceServiceRequest JSON bytes."""
    if spans is None:
        spans = [{
            "traceId": "abc123",
            "spanId": "def456",
            "name": "http_request",
            "startTimeUnixNano": 1700000000000000000,
            "endTimeUnixNano": 1700000001000000000,
            "attributes": {},
            "status": {"code": 0},
        }]
    if resource_attrs is None:
        resource_attrs = {}
    payload = {
        "resourceSpans": [{
            "resource": {"attributes": resource_attrs},
            "scopeSpans": [{
                "spans": spans
            }]
        }]
    }
    return json.dumps(payload).encode("utf-8")


def make_sentinel_payload(action="created", incident_id="INC-001", **extra):
    """Create a valid Sentinel incident webhook payload."""
    payload = {
        "properties": {
            "incidentNumber": 42,
            "title": "Test Incident",
            "severity": "High",
            "status": "New",
            "owner": {"assignedTo": "user@example.com"},
        },
        "name": incident_id,
        "type": "Microsoft.SecurityInsights/Incidents",
    }
    # Include action in a way the Sentinel parser expects
    # Common Sentinel webhook structures:
    if action:
        payload["properties"]["status"] = action
        # Also try setting it at top level as some implementations look there
        payload["action"] = action
    payload.update(extra)
    return json.dumps(payload).encode("utf-8")


def get_free_port():
    """Get a free TCP port."""
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


# ─── emit() tests ─────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_goodhart_emit_preserves_event_identity():
    """emit() must pass the exact same Event object to each subscriber, not a copy or hardcoded event."""
    event_id = str(uuid.uuid4())
    event_json = make_valid_event_json(event_id=event_id, event_kind="test.identity", source="identity-src")
    event = Event.model_validate_json(event_json)

    received_events = []
    for _ in range(5):
        cb = AsyncMock()
        cb.side_effect = lambda e, store=received_events: store.append(e)
        received_events_cbs = cb

    subscribers = []
    captured = []
    for i in range(5):
        async def cb(e, idx=i):
            captured.append((idx, e))
        subscribers.append(cb)

    await emit(subscribers, event)
    assert len(captured) == 5
    for idx, received in captured:
        assert received is event, f"Subscriber {idx} did not receive the exact event object"
        assert received.event_id == event_id


@pytest.mark.asyncio
async def test_goodhart_emit_order_preserved():
    """emit() must call subscribers in the order they appear in the list."""
    event = Event.model_validate_json(make_valid_event_json())
    call_order = []

    subscribers = []
    for i in range(5):
        async def cb(e, idx=i):
            call_order.append(idx)
        subscribers.append(cb)

    await emit(subscribers, event)
    assert call_order == [0, 1, 2, 3, 4]


@pytest.mark.asyncio
async def test_goodhart_emit_single_failing_in_middle():
    """emit() must call all 5 subscribers even when the 3rd raises."""
    event = Event.model_validate_json(make_valid_event_json())
    called = []

    subscribers = []
    for i in range(5):
        async def cb(e, idx=i):
            called.append(idx)
            if idx == 2:
                raise ValueError("Subscriber 2 fails")
        subscribers.append(cb)

    await emit(subscribers, event)
    assert sorted(called) == [0, 1, 2, 3, 4], f"Not all subscribers called: {called}"


@pytest.mark.asyncio
async def test_goodhart_emit_does_not_propagate_base_exception():
    """emit() catches Exception subclasses from subscribers without propagating."""
    event = Event.model_validate_json(make_valid_event_json())
    called = []

    async def cb_value_error(e):
        called.append("ve")
        raise ValueError("fail")

    async def cb_type_error(e):
        called.append("te")
        raise TypeError("fail")

    async def cb_runtime_error(e):
        called.append("re")
        raise RuntimeError("fail")

    async def cb_ok(e):
        called.append("ok")

    subscribers = [cb_value_error, cb_type_error, cb_runtime_error, cb_ok]
    await emit(subscribers, event)
    assert "ok" in called
    assert len(called) == 4


@pytest.mark.asyncio
async def test_goodhart_emit_async_subscribers():
    """emit() must properly await async subscriber callbacks."""
    event = Event.model_validate_json(make_valid_event_json())
    flag = {"awaited": False}

    async def slow_subscriber(e):
        await asyncio.sleep(0.01)
        flag["awaited"] = True

    await emit([slow_subscriber], event)
    assert flag["awaited"] is True, "Async subscriber was not properly awaited"


# ─── WebhookSource tests ──────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_goodhart_webhook_handle_post_content_type_case_insensitive_check():
    """HTTP Content-Type 'application/json; charset=utf-8' should be accepted."""
    port = get_free_port()
    config = WebhookSourceConfig(
        bind=BindAddress(host="127.0.0.1", port=port),
        client_max_size_bytes=1048576,
        source_name="test-webhook"
    )
    source = WebhookSource(config)
    body = make_valid_event_json()
    result = await source._handle_post(body, "application/json; charset=utf-8")
    assert result.status == 200, f"Expected 200 but got {result.status}"


@pytest.mark.asyncio
async def test_goodhart_webhook_handle_post_text_plain_rejected():
    """HTTP sources must reject Content-Type 'text/plain'."""
    port = get_free_port()
    config = WebhookSourceConfig(
        bind=BindAddress(host="127.0.0.1", port=port),
        client_max_size_bytes=1048576,
        source_name="test-webhook"
    )
    source = WebhookSource(config)
    body = make_valid_event_json()
    result = await source._handle_post(body, "text/plain")
    assert result.status == 415


@pytest.mark.asyncio
async def test_goodhart_webhook_handle_post_empty_body():
    """Webhook handler must return 400 when body is empty bytes."""
    port = get_free_port()
    config = WebhookSourceConfig(
        bind=BindAddress(host="127.0.0.1", port=port),
        client_max_size_bytes=1048576,
        source_name="test-webhook"
    )
    source = WebhookSource(config)
    result = await source._handle_post(b"", "application/json")
    assert result.status == 400


@pytest.mark.asyncio
async def test_goodhart_webhook_handle_post_returns_event_id_in_body():
    """Successful POST must return the actual event_id in the response body, not a hardcoded string."""
    port = get_free_port()
    config = WebhookSourceConfig(
        bind=BindAddress(host="127.0.0.1", port=port),
        client_max_size_bytes=1048576,
        source_name="test-webhook"
    )
    source = WebhookSource(config)

    eid1 = str(uuid.uuid4())
    body1 = make_valid_event_json(event_id=eid1)
    result1 = await source._handle_post(body1, "application/json")
    assert result1.status == 200
    assert eid1 in result1.body, f"event_id {eid1} not found in response body: {result1.body}"

    eid2 = str(uuid.uuid4())
    body2 = make_valid_event_json(event_id=eid2)
    result2 = await source._handle_post(body2, "application/json")
    assert result2.status == 200
    assert eid2 in result2.body, f"event_id {eid2} not found in response body: {result2.body}"
    assert result1.body != result2.body, "Different events should produce different response bodies"


@pytest.mark.asyncio
async def test_goodhart_webhook_subscribe_after_start():
    """Subscribers added after start() must still receive events from subsequent POSTs."""
    port = get_free_port()
    config = WebhookSourceConfig(
        bind=BindAddress(host="127.0.0.1", port=port),
        client_max_size_bytes=1048576,
        source_name="test-webhook"
    )
    source = WebhookSource(config)
    await source.start()

    try:
        received = []
        async def late_cb(e):
            received.append(e)
        source.subscribe(late_cb)

        body = make_valid_event_json()
        result = await source._handle_post(body, "application/json")
        assert result.status == 200
        assert len(received) == 1, "Late subscriber did not receive the event"
    finally:
        await source.stop()


@pytest.mark.asyncio
async def test_goodhart_webhook_subscribe_multiple():
    """Multiple subscribe() calls should accumulate callbacks, each receiving every event."""
    port = get_free_port()
    config = WebhookSourceConfig(
        bind=BindAddress(host="127.0.0.1", port=port),
        client_max_size_bytes=1048576,
        source_name="test-webhook"
    )
    source = WebhookSource(config)

    received_counts = [0, 0, 0]
    for i in range(3):
        async def cb(e, idx=i):
            received_counts[idx] += 1
        source.subscribe(cb)

    body = make_valid_event_json()
    result = await source._handle_post(body, "application/json")
    assert result.status == 200
    assert received_counts == [1, 1, 1], f"Not all subscribers called: {received_counts}"


@pytest.mark.asyncio
async def test_goodhart_webhook_start_from_stopped_raises():
    """Calling start() after stop() must raise RuntimeError (no reverse transitions)."""
    port = get_free_port()
    config = WebhookSourceConfig(
        bind=BindAddress(host="127.0.0.1", port=port),
        client_max_size_bytes=1048576,
        source_name="test-webhook"
    )
    source = WebhookSource(config)
    await source.start()
    await source.stop()

    with pytest.raises(RuntimeError):
        await source.start()


@pytest.mark.asyncio
async def test_goodhart_webhook_handle_post_valid_json_but_array():
    """Valid JSON that is an array should return 400, not 200."""
    port = get_free_port()
    config = WebhookSourceConfig(
        bind=BindAddress(host="127.0.0.1", port=port),
        client_max_size_bytes=1048576,
        source_name="test-webhook"
    )
    source = WebhookSource(config)
    result = await source._handle_post(b'[1, 2, 3]', "application/json")
    assert result.status == 400


@pytest.mark.asyncio
async def test_goodhart_webhook_initial_state_created():
    """A freshly constructed WebhookSource must be in CREATED state."""
    port = get_free_port()
    config = WebhookSourceConfig(
        bind=BindAddress(host="127.0.0.1", port=port),
        client_max_size_bytes=1048576,
        source_name="test-webhook"
    )
    source = WebhookSource(config)
    assert source._state == SourceState.CREATED


# ─── OtlpSource tests ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_goodhart_otlp_handle_traces_body_is_empty_json_object():
    """OTLP spec requires the response body to be exactly '{}' on success."""
    port = get_free_port()
    config = OtlpSourceConfig(
        bind=BindAddress(host="127.0.0.1", port=port),
        client_max_size_bytes=1048576,
        source_name="test-otlp"
    )
    source = OtlpSource(config)
    body = make_valid_otlp_payload()
    result = await source._handle_traces(body, "application/json")
    assert result.status == 200
    assert result.body == "{}", f"Expected '{{}}' but got '{result.body}'"


@pytest.mark.asyncio
async def test_goodhart_otlp_handle_traces_multiple_scopes():
    """Multiple scopeSpans within a single resourceSpan must all be flattened."""
    port = get_free_port()
    config = OtlpSourceConfig(
        bind=BindAddress(host="127.0.0.1", port=port),
        client_max_size_bytes=1048576,
        source_name="test-otlp"
    )
    source = OtlpSource(config)

    received = []
    async def cb(e):
        received.append(e)
    source.subscribe(cb)

    payload = {
        "resourceSpans": [{
            "resource": {"attributes": {}},
            "scopeSpans": [
                {"spans": [
                    {"traceId": "t1", "spanId": "s1", "name": "span_a",
                     "startTimeUnixNano": 1700000000000000000, "endTimeUnixNano": 1700000001000000000,
                     "attributes": {}, "status": {"code": 0}},
                ]},
                {"spans": [
                    {"traceId": "t1", "spanId": "s2", "name": "span_b",
                     "startTimeUnixNano": 1700000000000000000, "endTimeUnixNano": 1700000001000000000,
                     "attributes": {}, "status": {"code": 0}},
                    {"traceId": "t1", "spanId": "s3", "name": "span_c",
                     "startTimeUnixNano": 1700000000000000000, "endTimeUnixNano": 1700000001000000000,
                     "attributes": {}, "status": {"code": 0}},
                ]},
            ]
        }]
    }
    result = await source._handle_traces(json.dumps(payload).encode(), "application/json")
    assert result.status == 200
    assert len(received) == 3, f"Expected 3 events but got {len(received)}"


@pytest.mark.asyncio
async def test_goodhart_otlp_flatten_spans_inherits_correct_resource_attrs():
    """Each span must inherit only its parent resourceSpan's attributes, not a shared set."""
    port = get_free_port()
    config = OtlpSourceConfig(
        bind=BindAddress(host="127.0.0.1", port=port),
        client_max_size_bytes=1048576,
        source_name="test-otlp"
    )
    source = OtlpSource(config)

    otlp_request = {
        "resourceSpans": [
            {
                "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "svc-a"}}]},
                "scopeSpans": [{"spans": [
                    {"traceId": "t1", "spanId": "s1", "name": "span_a",
                     "startTimeUnixNano": 1700000000000000000, "endTimeUnixNano": 1700000001000000000,
                     "attributes": [], "status": {"code": 0}},
                ]}]
            },
            {
                "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "svc-b"}}]},
                "scopeSpans": [{"spans": [
                    {"traceId": "t2", "spanId": "s2", "name": "span_b",
                     "startTimeUnixNano": 1700000000000000000, "endTimeUnixNano": 1700000001000000000,
                     "attributes": [], "status": {"code": 0}},
                ]}]
            },
        ]
    }
    spans = source._flatten_spans(otlp_request)
    assert len(spans) == 2
    # Verify resource attributes are distinct per span
    assert spans[0].resource_attributes != spans[1].resource_attributes or \
           str(spans[0].resource_attributes) != str(spans[1].resource_attributes), \
           "Resource attributes should be different between spans from different resourceSpans"


@pytest.mark.asyncio
async def test_goodhart_otlp_span_to_event_different_span_names():
    """Different span names must produce different event_kind values, not a hardcoded kind."""
    port = get_free_port()
    config = OtlpSourceConfig(
        bind=BindAddress(host="127.0.0.1", port=port),
        client_max_size_bytes=1048576,
        source_name="test-otlp"
    )
    source = OtlpSource(config)

    span1 = OtlpSpan(
        trace_id="t1", span_id="s1", name="http_request",
        start_time_unix_nano=1700000000000000000, end_time_unix_nano=1700000001000000000,
        attributes={}, resource_attributes={}, status_code=0
    )
    span2 = OtlpSpan(
        trace_id="t2", span_id="s2", name="db_query",
        start_time_unix_nano=1700000000000000000, end_time_unix_nano=1700000001000000000,
        attributes={}, resource_attributes={}, status_code=0
    )

    event1 = source._span_to_event(span1, "test-otlp")
    event2 = source._span_to_event(span2, "test-otlp")

    assert event1.event_kind.startswith("otlp.trace.")
    assert event2.event_kind.startswith("otlp.trace.")
    assert event1.event_kind != event2.event_kind, "Different span names must produce different event_kinds"
    assert "http_request" in event1.event_kind or "http.request" in event1.event_kind
    assert "db_query" in event2.event_kind or "db.query" in event2.event_kind


@pytest.mark.asyncio
async def test_goodhart_otlp_span_to_event_unique_ids():
    """Each call to _span_to_event must generate a fresh UUID4."""
    port = get_free_port()
    config = OtlpSourceConfig(
        bind=BindAddress(host="127.0.0.1", port=port),
        client_max_size_bytes=1048576,
        source_name="test-otlp"
    )
    source = OtlpSource(config)

    span = OtlpSpan(
        trace_id="t1", span_id="s1", name="test_span",
        start_time_unix_nano=1700000000000000000, end_time_unix_nano=1700000001000000000,
        attributes={}, resource_attributes={}, status_code=0
    )

    event1 = source._span_to_event(span, "test-otlp")
    event2 = source._span_to_event(span, "test-otlp")
    assert event1.event_id != event2.event_id, "Each call must generate a unique UUID4"
    # Verify UUID4 format
    import re
    uuid4_re = r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
    assert re.match(uuid4_re, event1.event_id)
    assert re.match(uuid4_re, event2.event_id)


@pytest.mark.asyncio
async def test_goodhart_otlp_span_to_event_timestamp_conversion():
    """Timestamp must correctly convert nanoseconds to ISO 8601 UTC."""
    port = get_free_port()
    config = OtlpSourceConfig(
        bind=BindAddress(host="127.0.0.1", port=port),
        client_max_size_bytes=1048576,
        source_name="test-otlp"
    )
    source = OtlpSource(config)

    # 1700000000000000000 ns = 2023-11-14T22:13:20Z
    span = OtlpSpan(
        trace_id="t1", span_id="s1", name="test_span",
        start_time_unix_nano=1700000000000000000, end_time_unix_nano=1700000001000000000,
        attributes={}, resource_attributes={}, status_code=0
    )
    event = source._span_to_event(span, "test-otlp")
    assert event.timestamp.startswith("2023-11-14T22:13:20"), \
        f"Expected timestamp starting with '2023-11-14T22:13:20' but got '{event.timestamp}'"
    assert event.timestamp.endswith("Z"), f"Timestamp must end with 'Z', got '{event.timestamp}'"


@pytest.mark.asyncio
async def test_goodhart_otlp_span_to_event_source_name_passthrough():
    """Event.source must equal the source_name parameter, not a hardcoded value."""
    port = get_free_port()
    config = OtlpSourceConfig(
        bind=BindAddress(host="127.0.0.1", port=port),
        client_max_size_bytes=1048576,
        source_name="test-otlp"
    )
    source = OtlpSource(config)

    span = OtlpSpan(
        trace_id="t1", span_id="s1", name="test_span",
        start_time_unix_nano=1700000000000000000, end_time_unix_nano=1700000001000000000,
        attributes={}, resource_attributes={}, status_code=0
    )

    event1 = source._span_to_event(span, "my_custom_otlp")
    assert event1.source == "my_custom_otlp"

    event2 = source._span_to_event(span, "another_source")
    assert event2.source == "another_source"


@pytest.mark.asyncio
async def test_goodhart_otlp_handle_traces_empty_resource_spans_list():
    """Empty resourceSpans list is valid — return 200 with '{}', emit nothing."""
    port = get_free_port()
    config = OtlpSourceConfig(
        bind=BindAddress(host="127.0.0.1", port=port),
        client_max_size_bytes=1048576,
        source_name="test-otlp"
    )
    source = OtlpSource(config)

    received = []
    async def cb(e):
        received.append(e)
    source.subscribe(cb)

    payload = json.dumps({"resourceSpans": []}).encode()
    result = await source._handle_traces(payload, "application/json")
    assert result.status == 200
    assert result.body == "{}"
    assert len(received) == 0, "No events should be emitted for empty resourceSpans"


@pytest.mark.asyncio
async def test_goodhart_otlp_handle_traces_empty_body():
    """OTLP handler must return 400 when body is empty bytes."""
    port = get_free_port()
    config = OtlpSourceConfig(
        bind=BindAddress(host="127.0.0.1", port=port),
        client_max_size_bytes=1048576,
        source_name="test-otlp"
    )
    source = OtlpSource(config)
    result = await source._handle_traces(b"", "application/json")
    assert result.status == 400


@pytest.mark.asyncio
async def test_goodhart_otlp_start_from_stopped_raises():
    """Calling start() after stop() must raise RuntimeError."""
    port = get_free_port()
    config = OtlpSourceConfig(
        bind=BindAddress(host="127.0.0.1", port=port),
        client_max_size_bytes=1048576,
        source_name="test-otlp"
    )
    source = OtlpSource(config)
    await source.start()
    await source.stop()

    with pytest.raises(RuntimeError):
        await source.start()


@pytest.mark.asyncio
async def test_goodhart_otlp_span_to_event_correlation_keys_values():
    """correlation_keys must contain the actual trace_id and span_id values from the span."""
    port = get_free_port()
    config = OtlpSourceConfig(
        bind=BindAddress(host="127.0.0.1", port=port),
        client_max_size_bytes=1048576,
        source_name="test-otlp"
    )
    source = OtlpSource(config)

    span = OtlpSpan(
        trace_id="unique_trace_xyz", span_id="unique_span_abc",
        name="test_span",
        start_time_unix_nano=1700000000000000000, end_time_unix_nano=1700000001000000000,
        attributes={}, resource_attributes={}, status_code=0
    )
    event = source._span_to_event(span, "test-otlp")

    corr_str = str(event.correlation_keys)
    assert "unique_trace_xyz" in corr_str, \
        f"trace_id 'unique_trace_xyz' not found in correlation_keys: {event.correlation_keys}"
    assert "unique_span_abc" in corr_str, \
        f"span_id 'unique_span_abc' not found in correlation_keys: {event.correlation_keys}"


@pytest.mark.asyncio
async def test_goodhart_otlp_handle_traces_content_type_with_charset():
    """OTLP handler should accept 'application/json; charset=utf-8'."""
    port = get_free_port()
    config = OtlpSourceConfig(
        bind=BindAddress(host="127.0.0.1", port=port),
        client_max_size_bytes=1048576,
        source_name="test-otlp"
    )
    source = OtlpSource(config)
    body = make_valid_otlp_payload()
    result = await source._handle_traces(body, "application/json; charset=utf-8")
    assert result.status == 200


# ─── SentinelSource tests ─────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_goodhart_sentinel_handle_post_custom_prefix():
    """SentinelSource must use configured event_kind_prefix, not hardcode 'sentinel'."""
    port = get_free_port()
    config = SentinelSourceConfig(
        bind=BindAddress(host="127.0.0.1", port=port),
        client_max_size_bytes=1048576,
        event_kind_prefix="mysoc",
        source_name="test-sentinel"
    )
    source = SentinelSource(config)

    received = []
    async def cb(e):
        received.append(e)
    source.subscribe(cb)

    body = make_sentinel_payload(action="created", incident_id="INC-999")
    result = await source._handle_post(body, "application/json")
    assert result.status == 200
    assert len(received) >= 1, "No events emitted"
    event = received[0]
    assert event.event_kind.startswith("mysoc.incident."), \
        f"event_kind should start with 'mysoc.incident.' but got '{event.event_kind}'"
    assert not event.event_kind.startswith("sentinel."), \
        f"event_kind should NOT start with 'sentinel.' when prefix is 'mysoc'"


@pytest.mark.asyncio
async def test_goodhart_sentinel_handle_post_different_actions():
    """SentinelSource must extract action from payload for different incident actions."""
    port = get_free_port()
    config = SentinelSourceConfig(
        bind=BindAddress(host="127.0.0.1", port=port),
        client_max_size_bytes=1048576,
        event_kind_prefix="sentinel",
        source_name="test-sentinel"
    )
    source = SentinelSource(config)

    received = []
    async def cb(e):
        received.append(e)
    source.subscribe(cb)

    body1 = make_sentinel_payload(action="created", incident_id="INC-001")
    result1 = await source._handle_post(body1, "application/json")

    body2 = make_sentinel_payload(action="updated", incident_id="INC-002")
    result2 = await source._handle_post(body2, "application/json")

    if result1.status == 200 and result2.status == 200 and len(received) >= 2:
        assert received[0].event_kind != received[1].event_kind, \
            f"Different actions should produce different event_kinds: {received[0].event_kind} vs {received[1].event_kind}"


@pytest.mark.asyncio
async def test_goodhart_sentinel_handle_post_415_before_body_parse():
    """Content-Type validation must happen before body parsing — wrong Content-Type returns 415."""
    port = get_free_port()
    config = SentinelSourceConfig(
        bind=BindAddress(host="127.0.0.1", port=port),
        client_max_size_bytes=1048576,
        event_kind_prefix="sentinel",
        source_name="test-sentinel"
    )
    source = SentinelSource(config)
    body = make_sentinel_payload()
    result = await source._handle_post(body, "text/html")
    assert result.status == 415


@pytest.mark.asyncio
async def test_goodhart_sentinel_start_from_stopped_raises():
    """Calling start() after stop() must raise RuntimeError."""
    port = get_free_port()
    config = SentinelSourceConfig(
        bind=BindAddress(host="127.0.0.1", port=port),
        client_max_size_bytes=1048576,
        event_kind_prefix="sentinel",
        source_name="test-sentinel"
    )
    source = SentinelSource(config)
    await source.start()
    await source.stop()

    with pytest.raises(RuntimeError):
        await source.start()


@pytest.mark.asyncio
async def test_goodhart_sentinel_handle_post_empty_body():
    """SentinelSource handler must return 400 when body is empty bytes."""
    port = get_free_port()
    config = SentinelSourceConfig(
        bind=BindAddress(host="127.0.0.1", port=port),
        client_max_size_bytes=1048576,
        event_kind_prefix="sentinel",
        source_name="test-sentinel"
    )
    source = SentinelSource(config)
    result = await source._handle_post(b"", "application/json")
    assert result.status == 400


# ─── FileSource tests ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_goodhart_file_start_nonexistent_file():
    """FileSource.start() on a non-existent file should succeed and transition to RUNNING."""
    import tempfile
    import os

    nonexistent = os.path.join(tempfile.gettempdir(), f"nonexistent_{uuid.uuid4().hex}.jsonl")
    config = FileSourceConfig(
        path=nonexistent,
        poll_interval_seconds=1.0,
        source_name="test-file"
    )
    source = FileSource(config)
    await source.start()
    assert source._state == SourceState.RUNNING
    await source.stop()


@pytest.mark.asyncio
async def test_goodhart_file_start_from_stopped_raises():
    """Calling start() after stop() must raise RuntimeError."""
    import tempfile
    import os

    nonexistent = os.path.join(tempfile.gettempdir(), f"nonexistent_{uuid.uuid4().hex}.jsonl")
    config = FileSourceConfig(
        path=nonexistent,
        poll_interval_seconds=1.0,
        source_name="test-file"
    )
    source = FileSource(config)
    await source.start()
    await source.stop()

    with pytest.raises(RuntimeError):
        await source.start()


@pytest.mark.asyncio
async def test_goodhart_file_poll_multiple_lines_emitted_in_order():
    """FileSource must emit events in the order lines appear in the file."""
    import tempfile
    import os

    tmpfile = tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False)
    tmpfile.close()

    config = FileSourceConfig(
        path=tmpfile.name,
        poll_interval_seconds=0.1,
        source_name="test-file"
    )
    source = FileSource(config)

    received = []
    async def cb(e):
        received.append(e)
    source.subscribe(cb)

    await source.start()
    await asyncio.sleep(0.15)  # Let initial poll pass

    # Write multiple lines
    events = []
    with open(tmpfile.name, 'a') as f:
        for i in range(3):
            eid = str(uuid.uuid4())
            events.append(eid)
            event_data = {
                "event_id": eid,
                "event_kind": "test.order",
                "timestamp": "2024-01-15T10:30:00Z",
                "source": "test-file",
                "payload": {"index": i},
                "correlation_keys": []
            }
            f.write(json.dumps(event_data) + "\n")

    await asyncio.sleep(0.3)  # Wait for poll
    await source.stop()

    assert len(received) == 3, f"Expected 3 events but got {len(received)}"
    for i, event in enumerate(received):
        assert event.event_id == events[i], f"Event {i} out of order"

    os.unlink(tmpfile.name)


@pytest.mark.asyncio
async def test_goodhart_file_poll_valid_event_schema_line():
    """FileSource must skip lines that are valid JSON but don't match Event schema."""
    import tempfile
    import os

    tmpfile = tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False)
    tmpfile.close()

    config = FileSourceConfig(
        path=tmpfile.name,
        poll_interval_seconds=0.1,
        source_name="test-file"
    )
    source = FileSource(config)

    received = []
    async def cb(e):
        received.append(e)
    source.subscribe(cb)

    await source.start()
    await asyncio.sleep(0.15)

    valid_eid = str(uuid.uuid4())
    with open(tmpfile.name, 'a') as f:
        # Invalid event (missing required fields)
        f.write(json.dumps({"not": "an_event"}) + "\n")
        # Valid event
        f.write(json.dumps({
            "event_id": valid_eid,
            "event_kind": "test.schema",
            "timestamp": "2024-01-15T10:30:00Z",
            "source": "test-file",
            "payload": {},
            "correlation_keys": []
        }) + "\n")

    await asyncio.sleep(0.3)
    await source.stop()

    assert len(received) == 1, f"Expected 1 valid event but got {len(received)}"
    assert received[0].event_id == valid_eid

    os.unlink(tmpfile.name)


# ─── Type validation boundary tests ──────────────────────────────────────────

def test_goodhart_event_kind_trailing_dot_rejected():
    """EventKind must reject strings ending with a dot."""
    with pytest.raises(Exception):
        EventKind("webhook.push.")


def test_goodhart_event_kind_leading_dot_rejected():
    """EventKind must reject strings starting with a dot."""
    with pytest.raises(Exception):
        EventKind(".webhook.push")


def test_goodhart_event_kind_consecutive_dots_rejected():
    """EventKind must reject strings with consecutive dots."""
    with pytest.raises(Exception):
        EventKind("webhook..push")


def test_goodhart_event_kind_underscore_allowed():
    """EventKind segments may contain underscores."""
    kind = EventKind("webhook.push_event")
    assert str(kind) == "webhook.push_event" or kind.value == "webhook.push_event" or kind == "webhook.push_event"


def test_goodhart_event_kind_hyphen_rejected():
    """EventKind regex only allows lowercase, digits, underscores — hyphens rejected."""
    with pytest.raises(Exception):
        EventKind("webhook.push-event")


def test_goodhart_event_kind_segment_starts_with_underscore_rejected():
    """EventKind segments must start with lowercase letter per regex."""
    with pytest.raises(Exception):
        EventKind("_private.event")


def test_goodhart_bind_address_wildcard_ipv6():
    """BindAddress must accept '::' as valid host."""
    addr = BindAddress(host="::", port=8080)
    assert addr.host == "::"


def test_goodhart_bind_address_0000():
    """BindAddress must accept '0.0.0.0' as valid host."""
    addr = BindAddress(host="0.0.0.0", port=8080)
    assert addr.host == "0.0.0.0"


def test_goodhart_bind_address_hostname_rejected():
    """BindAddress must reject arbitrary hostnames."""
    with pytest.raises(Exception):
        BindAddress(host="myserver.com", port=8080)


def test_goodhart_webhook_config_source_name_boundary_128():
    """WebhookSourceConfig source_name max is 128; 128 accepted, 129 rejected."""
    name_128 = "a" * 128
    config = WebhookSourceConfig(
        bind=BindAddress(host="127.0.0.1", port=8080),
        client_max_size_bytes=1048576,
        source_name=name_128
    )
    assert config.source_name == name_128

    with pytest.raises(Exception):
        WebhookSourceConfig(
            bind=BindAddress(host="127.0.0.1", port=8080),
            client_max_size_bytes=1048576,
            source_name="a" * 129
        )


def test_goodhart_webhook_config_max_size_boundary_min():
    """WebhookSourceConfig client_max_size_bytes=1024 should be accepted."""
    config = WebhookSourceConfig(
        bind=BindAddress(host="127.0.0.1", port=8080),
        client_max_size_bytes=1024,
        source_name="test"
    )
    assert config.client_max_size_bytes == 1024


def test_goodhart_webhook_config_max_size_boundary_max():
    """WebhookSourceConfig client_max_size_bytes=10485760 should be accepted."""
    config = WebhookSourceConfig(
        bind=BindAddress(host="127.0.0.1", port=8080),
        client_max_size_bytes=10485760,
        source_name="test"
    )
    assert config.client_max_size_bytes == 10485760


def test_goodhart_otlp_config_max_size_boundary_min():
    """OtlpSourceConfig client_max_size_bytes=1024 should be accepted."""
    config = OtlpSourceConfig(
        bind=BindAddress(host="127.0.0.1", port=8080),
        client_max_size_bytes=1024,
        source_name="test"
    )
    assert config.client_max_size_bytes == 1024


def test_goodhart_file_config_poll_interval_boundary_min():
    """FileSourceConfig poll_interval_seconds=0.1 should be accepted."""
    config = FileSourceConfig(
        path="/tmp/test.jsonl",
        poll_interval_seconds=0.1,
        source_name="test"
    )
    assert config.poll_interval_seconds == 0.1


def test_goodhart_file_config_poll_interval_boundary_max():
    """FileSourceConfig poll_interval_seconds=300.0 should be accepted."""
    config = FileSourceConfig(
        path="/tmp/test.jsonl",
        poll_interval_seconds=300.0,
        source_name="test"
    )
    assert config.poll_interval_seconds == 300.0


def test_goodhart_sentinel_config_prefix_with_underscores():
    """SentinelSourceConfig event_kind_prefix allows underscores."""
    config = SentinelSourceConfig(
        bind=BindAddress(host="127.0.0.1", port=8080),
        client_max_size_bytes=1048576,
        event_kind_prefix="my_soc",
        source_name="test"
    )
    assert config.event_kind_prefix == "my_soc"


def test_goodhart_sentinel_config_prefix_with_digits():
    """SentinelSourceConfig event_kind_prefix allows digits after first char."""
    config = SentinelSourceConfig(
        bind=BindAddress(host="127.0.0.1", port=8080),
        client_max_size_bytes=1048576,
        event_kind_prefix="soc2",
        source_name="test"
    )
    assert config.event_kind_prefix == "soc2"


def test_goodhart_sentinel_config_prefix_starts_with_digit_rejected():
    """SentinelSourceConfig event_kind_prefix must start with lowercase letter."""
    with pytest.raises(Exception):
        SentinelSourceConfig(
            bind=BindAddress(host="127.0.0.1", port=8080),
            client_max_size_bytes=1048576,
            event_kind_prefix="2soc",
            source_name="test"
        )


def test_goodhart_sentinel_config_prefix_uppercase_rejected():
    """SentinelSourceConfig event_kind_prefix only allows lowercase."""
    with pytest.raises(Exception):
        SentinelSourceConfig(
            bind=BindAddress(host="127.0.0.1", port=8080),
            client_max_size_bytes=1048576,
            event_kind_prefix="Sentinel",
            source_name="test"
        )


def test_goodhart_sentinel_config_empty_prefix_rejected():
    """SentinelSourceConfig event_kind_prefix must not be empty."""
    with pytest.raises(Exception):
        SentinelSourceConfig(
            bind=BindAddress(host="127.0.0.1", port=8080),
            client_max_size_bytes=1048576,
            event_kind_prefix="",
            source_name="test"
        )


def test_goodhart_file_position_offset_zero_valid():
    """FilePosition offset of 0 is valid."""
    pos = FilePosition(offset=0, inode=12345, line_buffer="")
    assert pos.offset == 0


def test_goodhart_http_response_boundary_status_100():
    """HttpResponse status=100 is valid."""
    resp = HttpResponse(status=100, body="")
    assert resp.status == 100


def test_goodhart_http_response_boundary_status_599():
    """HttpResponse status=599 is valid."""
    resp = HttpResponse(status=599, body="")
    assert resp.status == 599


def test_goodhart_filepath_exactly_4096_accepted():
    """FilePath with exactly 4096 characters should be accepted."""
    path = "/" + "a" * 4095
    fp = FilePath(path)
    assert len(str(fp) if not hasattr(fp, 'value') else fp.value) == 4096 or len(path) == 4096

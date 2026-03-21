# === Event Sources (sources) v1 ===
#  Dependencies: schemas
# Four SourceProtocol implementations for ingesting events into Chronicler. (1) webhook.py — aiohttp HTTP POST receiver accepting JSON events, validates against Event schema. (2) otlp.py — OTLP/HTTP JSON receiver at /v1/traces, flattens ExportTraceServiceRequest into individual Events. (3) file.py — JSONL file tailer using asyncio polling with aiofiles for non-blocking reads. (4) sentinel.py — Sentinel incident lifecycle webhook with sentinel-prefixed event_kind. All sources implement start/stop/subscribe, catch exceptions internally, log and continue. Covers AC6 (multiple source types), AC13 (error resilience).

# Module invariants:
#   - All sources implement SourceProtocol (start, stop, subscribe) and are runtime_checkable
#   - Sources never raise exceptions to callers during event processing — all exceptions are caught, logged, and processing continues (AC13)
#   - HTTP sources (webhook, otlp, sentinel) validate Content-Type header before parsing body
#   - HTTP sources enforce client_max_size_bytes on request bodies
#   - The emit() utility calls every subscriber even if earlier subscribers raise exceptions
#   - FileSource handles file truncation by resetting offset to 0
#   - FileSource handles file rotation (inode change) by resetting offset to 0 and clearing line buffer
#   - FileSource buffers partial lines and only emits complete JSONL lines
#   - SentinelSource prefixes all event_kind values with the configured event_kind_prefix (AS9)
#   - OtlpSource flattens resourceSpans→scopeSpans→spans into individual Events
#   - OtlpSource returns empty JSON object '{}' per OTLP HTTP specification
#   - start() raises RuntimeError if source is already in RUNNING state
#   - stop() is idempotent — safe to call multiple times or on never-started sources
#   - subscribe() may be called before or after start()
#   - All Events emitted by sources have a valid UUID4 event_id and ISO 8601 UTC timestamp
#   - Source state transitions follow CREATED→RUNNING→STOPPED (no reverse transitions)

class Event:
    """Immutable event envelope — imported from schemas component. Referenced here for type completeness."""
    event_id: str                            # required, regex(^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$), Unique event identifier (UUID4)
    event_kind: EventKind                    # required, Dot-delimited event kind classifier
    timestamp: str                           # required, regex(^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$), ISO 8601 UTC timestamp
    source: str                              # required, Originating source identifier
    payload: dict = {}                       # optional, Arbitrary event payload
    correlation_keys: list = []              # optional, Keys for story correlation

EventKind = primitive  # Dot-delimited event kind string (e.g. 'webhook.push', 'sentinel.incident.created'). Minimum two segments.

EventCallback = primitive  # Async callable that receives an Event. Type signature: Callable[[Event], Awaitable[None]].

class SourceProtocol:
    """Runtime-checkable typing.Protocol defining the source interface. All sources must implement start(), stop(), and subscribe(). Decorated with @runtime_checkable."""
    start: Callable[[], Awaitable[None]]     # required, Initialize resources and begin accepting/polling events
    stop: Callable[[], Awaitable[None]]      # required, Graceful shutdown, cleanup resources, idempotent
    subscribe: Callable[[EventCallback], None] # required, Register async event handler callback, synchronous registration

class BindAddress:
    """Network bind configuration for HTTP-based sources."""
    host: str = 127.0.0.1                    # optional, regex(^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|localhost|0\.0\.0\.0|::1|::)$), Bind host address
    port: int                                # required, range(1..65535), Bind port number

class WebhookSourceConfig:
    """Configuration for the webhook HTTP POST source."""
    bind: BindAddress                        # required, Network bind address and port
    client_max_size_bytes: int = 1048576     # optional, range(1024..10485760), Maximum request body size in bytes
    source_name: str = webhook               # optional, length(1..128), Source identifier stamped on emitted events

class OtlpSourceConfig:
    """Configuration for the OTLP/HTTP JSON trace receiver source."""
    bind: BindAddress                        # required, Network bind address and port
    client_max_size_bytes: int = 4194304     # optional, range(1024..52428800), Maximum request body size in bytes
    source_name: str = otlp                  # optional, length(1..128), Source identifier stamped on emitted events

class FileSourceConfig:
    """Configuration for the JSONL file tailer source."""
    path: FilePath                           # required, Path to the JSONL file to tail
    poll_interval_seconds: float = 1.0       # optional, range(0.1..300.0), Polling interval in seconds
    source_name: str = file                  # optional, length(1..128), Source identifier stamped on emitted events

FilePath = primitive  # A validated filesystem path string. Must be non-empty and not contain null bytes.

class SentinelSourceConfig:
    """Configuration for the Sentinel incident lifecycle webhook source. Reuses HTTP transport from webhook, adds Sentinel-specific parsing."""
    bind: BindAddress                        # required, Network bind address and port
    client_max_size_bytes: int = 1048576     # optional, range(1024..10485760), Maximum request body size in bytes
    event_kind_prefix: str = sentinel        # optional, regex(^[a-z][a-z0-9_]*$), Prefix for event_kind (per AS9)
    source_name: str = sentinel              # optional, length(1..128), Source identifier stamped on emitted events

class OtlpSpan:
    """Intermediate representation of a single OTLP span extracted from ExportTraceServiceRequest, before conversion to Event."""
    trace_id: str                            # required, Hex-encoded 16-byte trace ID
    span_id: str                             # required, Hex-encoded 8-byte span ID
    name: str                                # required, Span operation name
    start_time_unix_nano: int                # required, Span start time in Unix nanoseconds
    end_time_unix_nano: int                  # required, Span end time in Unix nanoseconds
    attributes: dict = {}                    # optional, Span attributes as key-value pairs
    resource_attributes: dict = {}           # optional, Resource attributes from parent resourceSpan
    status_code: int = 0                     # optional, OTLP status code (0=UNSET, 1=OK, 2=ERROR)

class HttpResponse:
    """Simplified HTTP response representation for webhook/OTLP handler return values."""
    status: int                              # required, range(100..599), HTTP status code
    body: str = None                         # optional, Response body (JSON string)

class SourceState(Enum):
    """Lifecycle state of a source instance."""
    CREATED = "CREATED"
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"

class FilePosition:
    """Tracks current read position in a tailed file for resumption and truncation detection."""
    offset: int                              # required, range(0..9223372036854775807), Byte offset of last read position
    inode: int = 0                           # optional, File inode for rotation detection
    line_buffer: str = None                  # optional, Partial line buffer for incomplete reads

def emit(
    subscribers: list,
    event: Event,
) -> None:
    """
    Shared async utility in base.py. Iterates all registered subscriber callbacks and invokes each with the given Event. Each callback is wrapped in try/except Exception so one failing subscriber does not block others. Errors are logged with structured context (event_id, subscriber index).

    Preconditions:
      - event is a valid Event instance

    Postconditions:
      - Every subscriber in the list has been called exactly once with the event
      - Exceptions from individual subscribers are caught, logged, and do not propagate
      - All subscribers are called regardless of failures in earlier subscribers

    Side effects: none
    Idempotent: no
    """
    ...

def WebhookSource.subscribe(
    callback: EventCallback,
) -> None:
    """
    Register an async event handler callback. Synchronous operation — appends callback to internal subscriber list. May be called before or after start(). No unsubscribe mechanism.

    Preconditions:
      - callback is a non-null async callable

    Postconditions:
      - callback is appended to internal subscriber list
      - callback will receive all subsequent events emitted by this source

    Side effects: none
    Idempotent: no
    """
    ...

def WebhookSource.start() -> None:
    """
    Initialize aiohttp AppRunner and TCPSite, bind to configured host:port, begin accepting HTTP POST requests. Registers a single POST endpoint that validates JSON body against Event schema via model_validate_json(), returns 200 on success, 400 on validation failure, 415 on wrong Content-Type. Sets client_max_size on the aiohttp app.

    Preconditions:
      - Source is in CREATED state (not already started)

    Postconditions:
      - HTTP server is listening on configured bind address and port
      - Source state transitions to RUNNING
      - Incoming valid POST requests will be parsed and emitted to subscribers

    Errors:
      - already_started (RuntimeError): Source state is RUNNING
          message: WebhookSource is already started
      - bind_failed (OSError): Configured host:port is unavailable or in use
          message: Failed to bind to {host}:{port}

    Side effects: none
    Idempotent: no
    """
    ...

def WebhookSource.stop() -> None:
    """
    Gracefully shut down the aiohttp HTTP server. Cleans up AppRunner and TCPSite. Idempotent — safe to call multiple times or on a source that was never started.

    Postconditions:
      - HTTP server is no longer accepting connections
      - All aiohttp resources (AppRunner, TCPSite) are cleaned up
      - Source state transitions to STOPPED

    Side effects: none
    Idempotent: yes
    """
    ...

def WebhookSource._handle_post(
    request_body: bytes,
    content_type: str,
) -> HttpResponse:
    """
    Internal HTTP POST handler. Validates Content-Type is application/json, reads body, parses via Event.model_validate_json(), emits to subscribers via _emit, returns HttpResponse. All exceptions caught — invalid JSON or schema returns 400, other errors return 500.

    Postconditions:
      - Returns status 200 with event_id in body on success
      - Returns status 400 with error detail on validation failure
      - Returns status 415 if Content-Type is not application/json
      - Returns status 500 on unexpected internal error
      - Valid events are emitted to all subscribers before response is returned

    Errors:
      - invalid_content_type (HttpResponse): Content-Type header is not application/json
          status: 415
          body: Unsupported Content-Type
      - invalid_json (HttpResponse): Body is not valid JSON
          status: 400
          body: Invalid JSON
      - validation_error (HttpResponse): JSON does not match Event schema
          status: 400
          body: Event validation failed: {detail}
      - body_too_large (HttpResponse): Request body exceeds client_max_size_bytes
          status: 413
          body: Request body too large
      - internal_error (HttpResponse): Unexpected exception during processing
          status: 500
          body: Internal server error

    Side effects: none
    Idempotent: no
    """
    ...

def OtlpSource.subscribe(
    callback: EventCallback,
) -> None:
    """
    Register an async event handler callback for OTLP-derived events. Synchronous operation.

    Preconditions:
      - callback is a non-null async callable

    Postconditions:
      - callback is appended to internal subscriber list

    Side effects: none
    Idempotent: no
    """
    ...

def OtlpSource.start() -> None:
    """
    Initialize aiohttp AppRunner and TCPSite, bind to configured host:port, begin accepting OTLP/HTTP JSON requests at POST /v1/traces. Sets client_max_size on the aiohttp app.

    Preconditions:
      - Source is in CREATED state (not already started)

    Postconditions:
      - HTTP server is listening on configured bind address and port at /v1/traces
      - Source state transitions to RUNNING

    Errors:
      - already_started (RuntimeError): Source state is RUNNING
          message: OtlpSource is already started
      - bind_failed (OSError): Configured host:port is unavailable or in use
          message: Failed to bind to {host}:{port}

    Side effects: none
    Idempotent: no
    """
    ...

def OtlpSource.stop() -> None:
    """
    Gracefully shut down the OTLP HTTP server. Idempotent.

    Postconditions:
      - HTTP server is no longer accepting connections
      - Source state transitions to STOPPED

    Side effects: none
    Idempotent: yes
    """
    ...

def OtlpSource._handle_traces(
    request_body: bytes,
    content_type: str,
) -> HttpResponse:
    """
    Internal POST /v1/traces handler. Parses OTLP JSON ExportTraceServiceRequest, flattens resourceSpans→scopeSpans→spans into individual OtlpSpan structs, converts each to an Event with event_kind 'otlp.trace.{span.name}', emits to subscribers. Returns empty JSON object '{}' per OTLP HTTP spec. All exceptions caught internally.

    Postconditions:
      - Returns status 200 with body '{}' on success (per OTLP spec)
      - Returns status 400 on malformed OTLP JSON
      - Returns status 415 if Content-Type is not application/json
      - Each span in the request is converted to an individual Event and emitted
      - Partial failures (some spans invalid) still process remaining valid spans

    Errors:
      - invalid_content_type (HttpResponse): Content-Type is not application/json
          status: 415
          body: Unsupported Content-Type
      - invalid_json (HttpResponse): Body is not valid JSON
          status: 400
          body: Invalid JSON
      - malformed_otlp (HttpResponse): JSON does not contain expected resourceSpans structure
          status: 400
          body: Malformed OTLP trace request
      - internal_error (HttpResponse): Unexpected exception
          status: 500
          body: Internal server error

    Side effects: none
    Idempotent: no
    """
    ...

def OtlpSource._flatten_spans(
    otlp_request: dict,
) -> list:
    """
    Pure function that extracts individual OtlpSpan structs from a parsed OTLP ExportTraceServiceRequest dict by iterating resourceSpans→scopeSpans→spans and merging resource attributes down.

    Preconditions:
      - otlp_request is a valid parsed JSON dict

    Postconditions:
      - Returns a list of OtlpSpan, one per span found in the request
      - Each OtlpSpan inherits resource_attributes from its parent resourceSpan
      - Invalid individual spans are skipped with a logged warning

    Errors:
      - missing_resource_spans (ValueError): otlp_request has no 'resourceSpans' key
          message: Missing resourceSpans in OTLP request

    Side effects: none
    Idempotent: yes
    """
    ...

def OtlpSource._span_to_event(
    span: OtlpSpan,
    source_name: str,
) -> Event:
    """
    Pure conversion function: transforms an OtlpSpan into an Event. Generates a UUID4 event_id, converts start_time_unix_nano to ISO 8601 UTC timestamp, sets event_kind to 'otlp.trace.{normalized_span_name}', and packs span attributes into payload. Sets trace_id and span_id as correlation_keys.

    Preconditions:
      - span is a valid OtlpSpan

    Postconditions:
      - Returned Event has a fresh UUID4 event_id
      - Event.event_kind starts with 'otlp.trace.'
      - Event.timestamp is derived from span.start_time_unix_nano in ISO 8601 UTC
      - Event.correlation_keys contains trace_id and span_id
      - Event.source equals source_name parameter

    Side effects: none
    Idempotent: yes
    """
    ...

def FileSource.subscribe(
    callback: EventCallback,
) -> None:
    """
    Register an async event handler callback for file-tailed events. Synchronous operation.

    Preconditions:
      - callback is a non-null async callable

    Postconditions:
      - callback is appended to internal subscriber list

    Side effects: none
    Idempotent: no
    """
    ...

def FileSource.start() -> None:
    """
    Begin the asyncio polling loop. Records initial file offset (end of file if file exists, 0 if not yet created). Uses asyncio.Event for shutdown signaling. The polling loop runs as an asyncio Task.

    Preconditions:
      - Source is in CREATED state (not already started)

    Postconditions:
      - Polling asyncio.Task is created and running
      - Source state transitions to RUNNING
      - If file exists, initial offset is set to end of file (tail mode)
      - If file does not exist, polling waits for file creation

    Errors:
      - already_started (RuntimeError): Source state is RUNNING
          message: FileSource is already started

    Side effects: none
    Idempotent: no
    """
    ...

def FileSource.stop() -> None:
    """
    Signal the polling loop to stop via asyncio.Event, await task completion. Idempotent.

    Postconditions:
      - Polling task has completed
      - Source state transitions to STOPPED
      - asyncio.Event is set (shutdown signal)

    Side effects: none
    Idempotent: yes
    """
    ...

def FileSource._poll_loop() -> None:
    """
    Internal async polling loop. On each iteration: stat file to detect truncation or rotation (inode change), seek to tracked offset, read new data via aiofiles, split into lines buffering partial lines, parse each complete line as JSON then validate against Event schema, emit valid events. Sleeps for poll_interval_seconds between iterations. Loop exits when shutdown asyncio.Event is set.

    Preconditions:
      - Called internally by start(), shutdown_event is not set

    Postconditions:
      - All complete JSONL lines since last poll are parsed and emitted as Events
      - FilePosition.offset is updated to current read position
      - Partial lines are buffered in FilePosition.line_buffer
      - File truncation resets offset to 0
      - File rotation (inode change) resets offset to 0 and clears line_buffer

    Errors:
      - file_not_found (None): File does not exist on poll iteration
          message: File not found, will retry next poll
      - permission_denied (None): File exists but is not readable
          message: Permission denied reading file, will retry
      - invalid_json_line (None): A line is not valid JSON
          message: Skipping invalid JSON line at offset {offset}
      - invalid_event_line (None): JSON is valid but does not match Event schema
          message: Skipping invalid Event at offset {offset}

    Side effects: none
    Idempotent: no
    """
    ...

def SentinelSource.subscribe(
    callback: EventCallback,
) -> None:
    """
    Register an async event handler callback for Sentinel-derived events. Synchronous operation.

    Preconditions:
      - callback is a non-null async callable

    Postconditions:
      - callback is appended to internal subscriber list

    Side effects: none
    Idempotent: no
    """
    ...

def SentinelSource.start() -> None:
    """
    Initialize aiohttp AppRunner and TCPSite (reusing webhook HTTP transport pattern), bind to configured host:port, begin accepting Sentinel incident lifecycle webhook POSTs. Sentinel-specific JSON parsing extracts incident fields and applies event_kind_prefix per AS9.

    Preconditions:
      - Source is in CREATED state (not already started)

    Postconditions:
      - HTTP server is listening on configured bind address and port
      - Source state transitions to RUNNING

    Errors:
      - already_started (RuntimeError): Source state is RUNNING
          message: SentinelSource is already started
      - bind_failed (OSError): Configured host:port is unavailable or in use
          message: Failed to bind to {host}:{port}

    Side effects: none
    Idempotent: no
    """
    ...

def SentinelSource.stop() -> None:
    """
    Gracefully shut down the Sentinel HTTP server. Idempotent.

    Postconditions:
      - HTTP server is no longer accepting connections
      - Source state transitions to STOPPED

    Side effects: none
    Idempotent: yes
    """
    ...

def SentinelSource._handle_post(
    request_body: bytes,
    content_type: str,
) -> HttpResponse:
    """
    Internal HTTP POST handler for Sentinel webhooks. Parses Sentinel-specific JSON structure (incident properties, severity, status, etc.), constructs Event with event_kind = '{event_kind_prefix}.incident.{action}' (e.g. 'sentinel.incident.created'), generates UUID4 event_id, extracts incident_id and incident_number as correlation_keys. All exceptions caught internally.

    Postconditions:
      - Returns status 200 on successful parse and emit
      - Returns status 400 on invalid JSON or unrecognized Sentinel payload
      - Returns status 415 if Content-Type is not application/json
      - Event.event_kind starts with configured event_kind_prefix followed by '.incident.'
      - Event.correlation_keys includes Sentinel incident_id if present

    Errors:
      - invalid_content_type (HttpResponse): Content-Type is not application/json
          status: 415
          body: Unsupported Content-Type
      - invalid_json (HttpResponse): Body is not valid JSON
          status: 400
          body: Invalid JSON
      - unrecognized_payload (HttpResponse): JSON does not contain expected Sentinel incident structure
          status: 400
          body: Unrecognized Sentinel payload
      - internal_error (HttpResponse): Unexpected exception
          status: 500
          body: Internal server error

    Side effects: none
    Idempotent: no
    """
    ...

# ── REQUIRED EXPORTS ──────────────────────────────────
# Your implementation module MUST export ALL of these names
# with EXACTLY these spellings. Tests import them by name.
# __all__ = ['Event', 'SourceProtocol', 'BindAddress', 'WebhookSourceConfig', 'OtlpSourceConfig', 'FileSourceConfig', 'SentinelSourceConfig', 'OtlpSpan', 'HttpResponse', 'SourceState', 'FilePosition', 'emit']

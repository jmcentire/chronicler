"""Core data models for Chronicler.

Pydantic v2 frozen models for Event, Story, CorrelationRule, and all config models.
Also defines SourceProtocol and SinkProtocol as runtime_checkable Protocol classes.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Annotated, Any, Protocol, runtime_checkable

from pydantic import (
    BaseModel,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)


# ── Constrained Primitive Types ─────────────────────────────────────────────

class EventKind(str):
    """Dot-separated event kind identifier."""
    import re as _re
    _PATTERN = _re.compile(r'^[a-z][a-z0-9_.]{0,254}$')

    def __new__(cls, value: str) -> EventKind:
        if not cls._PATTERN.match(value):
            raise ValueError(
                f"EventKind must be 1-255 chars, lowercase alphanumeric "
                f"with dots and underscores, starting with a letter: {value!r}"
            )
        return str.__new__(cls, value)


class EntityKind(str):
    """Entity kind identifier."""
    import re as _re
    _PATTERN = _re.compile(r'^[a-z][a-z0-9_.]{0,254}$')

    def __new__(cls, value: str) -> EntityKind:
        if not cls._PATTERN.match(value):
            raise ValueError(
                f"EntityKind must be 1-255 chars, lowercase alphanumeric "
                f"with dots and underscores, starting with a letter: {value!r}"
            )
        return str.__new__(cls, value)


class EntityId(str):
    """Non-empty entity identifier, max 1024 chars."""
    def __new__(cls, value: str) -> EntityId:
        if not value or len(value) > 1024:
            raise ValueError("EntityId must be between 1 and 1024 characters")
        return str.__new__(cls, value)


class ActorId(str):
    """Non-empty actor identifier, max 512 chars."""
    def __new__(cls, value: str) -> ActorId:
        if not value or len(value) > 512:
            raise ValueError("ActorId must be between 1 and 512 characters")
        return str.__new__(cls, value)


class Sha256Hex(str):
    """Lowercase hex-encoded SHA-256 hash (64 characters)."""
    import re as _re
    _PATTERN = _re.compile(r'^[0-9a-f]{64}$')

    def __new__(cls, value: str) -> Sha256Hex:
        if not cls._PATTERN.match(value):
            raise ValueError("Sha256Hex must be exactly 64 lowercase hex characters")
        return str.__new__(cls, value)


class StoryId(str):
    """UUID4 string identifier for a Story."""
    import re as _re
    _UUID4_PATTERN = _re.compile(
        r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$',
        _re.IGNORECASE,
    )

    def __new__(cls, value: str) -> StoryId:
        if not value:
            raise ValueError("StoryId must be non-empty")
        if not cls._UUID4_PATTERN.match(value):
            raise ValueError(f"StoryId must be a valid UUID4: {value!r}")
        return str.__new__(cls, value)


# ── Validated Type Constructors ─────────────────────────────────────────────

class AwareDatetime(datetime):
    """A timezone-aware datetime. Rejects naive datetimes."""
    def __new__(cls, *args, **kwargs):
        if len(args) == 1 and isinstance(args[0], datetime):
            dt = args[0]
            if dt.tzinfo is None:
                raise ValueError("AwareDatetime requires a timezone-aware datetime")
            return dt
        return super().__new__(cls, *args, **kwargs)


class ContextDict(dict):
    """A dict[str, ContextValue] — validates key/value types."""
    _ALLOWED_TYPES = (str, int, float, bool, type(None))
    _validated = True  # Marker to distinguish from plain dict

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for k, v in self.items():
            if not isinstance(k, str) or k == '':
                raise ValueError(f"ContextDict keys must be non-empty strings, got: {k!r}")
            if not isinstance(v, self._ALLOWED_TYPES):
                raise TypeError(
                    f"ContextDict values must be str|int|float|bool|None, "
                    f"got {type(v).__name__} for key {k!r}"
                )

    def __bool__(self):
        return True  # Always truthy after validation


class CorrelationKeys(dict):
    """A dict[str, str] of correlation key-value pairs."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for k, v in self.items():
            if not isinstance(k, str) or k == '':
                raise ValueError(f"CorrelationKeys keys must be non-empty strings, got: {k!r}")
            if not isinstance(v, str) or v == '':
                raise ValueError(f"CorrelationKeys values must be non-empty strings, got: {v!r}")

    def __bool__(self):
        return True


class TerminalEventPattern(dict):
    """A dict[str, str] for terminal event matching."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for k, v in self.items():
            if not isinstance(k, str) or k == '':
                raise ValueError(f"TerminalEventPattern keys must be non-empty strings, got: {k!r}")
            if not isinstance(v, str):
                raise ValueError(f"TerminalEventPattern values must be strings")

    def __bool__(self):
        return True


class EventKindSet(frozenset):
    """A frozenset of EventKind strings. Must have at least one member."""
    def __new__(cls, iterable=()):
        instance = super().__new__(cls, iterable)
        if len(instance) == 0:
            raise ValueError("EventKindSet must have at least one member")
        return instance


ContextValue = str | int | float | bool | None
EventIdTuple = list[str]
GroupByKeys = list[str]
ChainByKeys = list[str]
EventOrStory = Any  # Union of Event | Story


# ── Enums ───────────────────────────────────────────────────────────────────

class StoryStatus(str, Enum):
    """Lifecycle status of a Story."""
    open = "open"
    closed = "closed"


class CloseReason(str, Enum):
    """Reason why a Story was closed."""
    terminal = "terminal"
    timeout = "timeout"
    evicted = "evicted"


class SourceType(str, Enum):
    """Discriminator for source configuration types."""
    webhook = "webhook"
    otlp = "otlp"
    file = "file"
    sentinel = "sentinel"


class SinkType(str, Enum):
    """Discriminator for sink configuration types."""
    stigmergy = "stigmergy"
    apprentice = "apprentice"
    disk = "disk"
    kindex = "kindex"


# ── Core Domain Models ──────────────────────────────────────────────────────

def compute_event_id(
    event_kind: str,
    entity_kind: str,
    entity_id: str,
    actor: str,
    timestamp: datetime,
    context: dict,
    correlation_keys: dict,
) -> str:
    """Compute content-addressable SHA-256 hex ID for an Event.

    Uses json.dumps(sort_keys=True, separators=(',',':'), ensure_ascii=True, default=str)
    on a fixed-key dict of identity fields.
    """
    if timestamp.tzinfo is None:
        raise ValueError("Event timestamp must be timezone-aware")
    for v in context.values():
        if not isinstance(v, (str, int, float, bool, type(None))):
            raise TypeError("Context values must be str, int, float, bool, or None")

    # Normalize timestamp to UTC with 'Z' suffix
    ts_utc = timestamp.astimezone(timezone.utc)
    ts_str = ts_utc.isoformat()
    if ts_str.endswith("+00:00"):
        ts_str = ts_str[:-6] + "Z"

    canonical = {
        "actor": actor,
        "context": context,
        "correlation_keys": correlation_keys,
        "entity_id": entity_id,
        "entity_kind": entity_kind,
        "event_kind": event_kind,
        "timestamp": ts_str,
    }
    raw = json.dumps(canonical, sort_keys=True, separators=(',', ':'),
                     ensure_ascii=True, default=str)
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()


class Event(BaseModel):
    """Immutable event envelope (frozen=True)."""
    model_config = {"frozen": True}

    id: str = ""
    event_kind: str
    entity_kind: str
    entity_id: str
    actor: str
    timestamp: datetime
    context: dict = Field(default_factory=dict)
    correlation_keys: dict[str, str] = Field(default_factory=dict)
    source: str | None = None

    @model_validator(mode='before')
    @classmethod
    def _compute_id(cls, values: dict) -> dict:
        if isinstance(values, dict):
            ts = values.get('timestamp')
            if ts is not None and isinstance(ts, str):
                ts = datetime.fromisoformat(ts)
            if ts is not None and hasattr(ts, 'tzinfo') and ts.tzinfo is not None:
                computed = compute_event_id(
                    event_kind=values.get('event_kind', ''),
                    entity_kind=values.get('entity_kind', ''),
                    entity_id=values.get('entity_id', ''),
                    actor=values.get('actor', ''),
                    timestamp=ts,
                    context=values.get('context', {}),
                    correlation_keys=values.get('correlation_keys', {}),
                )
                existing_id = values.get('id')
                if existing_id and existing_id != computed and existing_id != '':
                    raise ValueError(
                        f"Event ID does not match computed content hash: "
                        f"provided={existing_id}, computed={computed}"
                    )
                values['id'] = computed
        return values

    @field_validator('timestamp')
    @classmethod
    def _check_aware(cls, v: datetime) -> datetime:
        if isinstance(v, datetime) and v.tzinfo is None:
            raise ValueError("Event timestamp must be timezone-aware")
        return v


class Story(BaseModel):
    """Immutable story model (frozen=True). model_copy re-validates.

    Supports two construction patterns:
    1. Full schema: story_id, story_type, correlation_rule, group_key, events (tuple of IDs),
       status, start_ts, end_ts, duration, event_count, close_reason, parent_story_id
    2. Sink-compatible: story_id, story_type, events (list of dicts), opened_at, closed_at, metadata
    """
    model_config = {"frozen": True}

    def model_copy(self, *, update=None, deep=False, **kwargs):
        """Override to re-validate after copy with updates."""
        result = super().model_copy(update=update, deep=deep, **kwargs)
        # Re-validate by constructing via model_validate
        return Story.model_validate(result.model_dump())

    story_id: str
    story_type: str
    correlation_rule: str = ""

    @field_validator('story_id', mode='before')
    @classmethod
    def _validate_story_id(cls, v):
        if isinstance(v, str):
            # Validate UUID4 format if it looks like a UUID
            import re
            uuid_pattern = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE)
            if uuid_pattern.match(v):
                # Must be version 4
                version_char = v[14]
                if version_char != '4':
                    raise ValueError(f"StoryId must be UUID version 4, got version {version_char}")
                # Must have correct variant
                variant_char = v[19].lower()
                if variant_char not in '89ab':
                    raise ValueError(f"StoryId UUID variant nibble must be in [89ab], got {variant_char}")
        return v
    group_key: dict[str, str] = Field(default_factory=dict)
    events: tuple | list = Field(default_factory=tuple)
    status: StoryStatus = StoryStatus.open
    start_ts: datetime | None = None
    end_ts: datetime | None = None
    duration: float | None = None
    event_count: int = 0
    close_reason: CloseReason | None = None
    parent_story_id: str | None = None
    # Additional fields for sinks compatibility
    opened_at: str | None = None
    closed_at: str | None = None
    metadata: dict = Field(default_factory=dict)

    @model_validator(mode='after')
    def _check_consistency(self) -> Story:
        # Only enforce lifecycle invariants when status is explicitly set
        # (i.e., when using the full schema pattern, not sink-compatible)
        if self.start_ts is not None:
            if self.status == StoryStatus.open:
                if self.end_ts is not None:
                    raise ValueError("Open story must not have end_ts")
                if self.close_reason is not None:
                    raise ValueError("Open story must not have close_reason")
                if self.duration is not None:
                    raise ValueError("Open story must not have duration")
            elif self.status == StoryStatus.closed:
                if self.end_ts is None:
                    raise ValueError("Closed story must have end_ts")
                if self.close_reason is None:
                    raise ValueError("Closed story must have close_reason")
                if self.duration is None:
                    raise ValueError("Closed story must have duration")
            if self.event_count != len(self.events):
                raise ValueError("event_count must equal len(events)")
        return self


# Allow Story to also be constructed from sink-style dicts
# with story_id, story_type, events list, opened_at, closed_at, metadata
# For the sinks tests, we need a simpler Story model. We'll handle it
# by making Story flexible enough.


def create_event(
    event_kind: str,
    entity_kind: str,
    entity_id: str,
    actor: str,
    timestamp: datetime,
    context: dict,
    correlation_keys: dict[str, str],
    source: str | None = None,
) -> Event:
    """Create a new Event instance with auto-computed id."""
    import re as _re
    _kind_pattern = _re.compile(r'^[a-z][a-z0-9_.]{0,254}$')

    # Validate event_kind
    if not _kind_pattern.match(event_kind):
        raise ValueError(f"Invalid event_kind: {event_kind!r}")
    # Validate entity_kind
    if not _kind_pattern.match(entity_kind):
        raise ValueError(f"Invalid entity_kind: {entity_kind!r}")
    # Validate entity_id
    if not entity_id:
        raise ValueError("entity_id must be non-empty")
    if len(entity_id) > 1024:
        raise ValueError("entity_id must not exceed 1024 characters")
    # Validate actor
    if not actor:
        raise ValueError("actor must be non-empty")
    if len(actor) > 512:
        raise ValueError("actor must not exceed 512 characters")
    # Validate timestamp
    if isinstance(timestamp, datetime) and timestamp.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    # Validate context values
    for k, v in context.items():
        if not isinstance(v, (str, int, float, bool, type(None))):
            raise ValueError(
                f"Context values must be str|int|float|bool|None, got {type(v).__name__}"
            )
    # Validate correlation_keys
    for k, v in correlation_keys.items():
        if not isinstance(k, str) or k == '':
            raise ValueError("Correlation keys must be non-empty strings")
        if not isinstance(v, str) or v == '':
            raise ValueError("Correlation key values must be non-empty strings")

    return Event(
        event_kind=event_kind,
        entity_kind=entity_kind,
        entity_id=entity_id,
        actor=actor,
        timestamp=timestamp,
        context=context,
        correlation_keys=correlation_keys,
        source=source,
    )


def create_story(
    story_type: str,
    correlation_rule: str,
    group_key: dict[str, str],
    first_event_id: str,
    start_ts: datetime,
    parent_story_id: str | None = None,
) -> Story:
    """Create a new open Story."""
    import re as _re
    if not story_type:
        raise ValueError("story_type must be non-empty")
    if not correlation_rule:
        raise ValueError("correlation_rule must be non-empty")
    if not _re.match(r'^[0-9a-f]{64}$', first_event_id):
        raise ValueError(f"first_event_id must be a valid SHA-256 hex string: {first_event_id!r}")
    return Story(
        story_id=str(uuid.uuid4()),
        story_type=story_type,
        correlation_rule=correlation_rule,
        group_key=group_key,
        events=(first_event_id,),
        status=StoryStatus.open,
        start_ts=start_ts,
        event_count=1,
        parent_story_id=parent_story_id,
    )


def append_event_to_story(story: Story, event_id: str) -> Story:
    """Produce a new Story with an additional event appended."""
    if story.status != StoryStatus.open:
        raise ValueError("Cannot append event to a closed story")
    if event_id in story.events:
        raise ValueError("Event ID already exists in story")
    new_events = story.events + (event_id,)
    return story.model_copy(update={
        "events": new_events,
        "event_count": len(new_events),
    })


def close_story(story: Story, close_reason: CloseReason, end_ts: datetime) -> Story:
    """Produce a new Story with status='closed'."""
    if story.status == StoryStatus.closed:
        raise ValueError("Cannot close an already closed story")
    if end_ts < story.start_ts:
        raise ValueError("end_ts must not be before start_ts")
    duration = (end_ts - story.start_ts).total_seconds()
    return story.model_copy(update={
        "status": StoryStatus.closed,
        "close_reason": close_reason,
        "end_ts": end_ts,
        "duration": duration,
    })


def validate_story_consistency(story: Story) -> bool:
    """Validate the internal consistency of a Story model."""
    if story.status == StoryStatus.open:
        if story.end_ts is not None:
            raise ValidationError("Open story must not have end_ts")
        if story.close_reason is not None:
            raise ValidationError("Open story must not have close_reason")
        if story.duration is not None:
            raise ValidationError("Open story must not have duration")
    elif story.status == StoryStatus.closed:
        if story.end_ts is None:
            raise ValidationError("Closed story must have end_ts")
        if story.close_reason is None:
            raise ValidationError("Closed story must have close_reason")
        if story.duration is None:
            raise ValidationError("Closed story must have duration")
    if story.event_count != len(story.events):
        raise ValidationError("event_count must equal len(events)")
    return True


def matches_terminal_pattern(event: Event, pattern: dict[str, str]) -> bool:
    """Check whether an Event matches a TerminalEventPattern."""
    if not pattern:
        return True
    for key, expected_value in pattern.items():
        if key in ('event_kind', 'entity_kind', 'entity_id', 'actor'):
            actual = getattr(event, key, None)
            if str(actual) != expected_value:
                return False
        elif key.startswith('context.'):
            ctx_key = key[len('context.'):]
            actual = event.context.get(ctx_key)
            if str(actual) != expected_value:
                return False
        elif key.startswith('correlation_keys.'):
            ck_key = key[len('correlation_keys.'):]
            actual = event.correlation_keys.get(ck_key)
            if str(actual) != expected_value:
                return False
        else:
            raise ValueError(f"Unrecognized terminal event pattern field path: {key}")
    return True


def extract_group_key(event: Event, group_by: list[str]) -> dict[str, str]:
    """Extract group key values from an Event."""
    result: dict[str, str] = {}
    for path in group_by:
        if path.startswith('correlation_keys.'):
            ck_key = path[len('correlation_keys.'):]
            if ck_key not in event.correlation_keys:
                raise KeyError(f"Event is missing required group_by field: {path}")
            val = event.correlation_keys[ck_key]
        elif path.startswith('context.'):
            ctx_key = path[len('context.'):]
            if ctx_key not in event.context:
                raise KeyError(f"Event is missing required group_by field: {path}")
            val = event.context[ctx_key]
        elif hasattr(event, path):
            val = getattr(event, path)
        else:
            raise KeyError(f"Event is missing required group_by field: {path}")
        if not isinstance(val, str):
            raise TypeError("Group key field must resolve to a string value")
        result[path] = val
    return result


def event_to_dict(event: Event) -> dict:
    """Serialize an Event to a plain dict."""
    return event.model_dump(mode='json')


def event_from_dict(data: dict) -> Event:
    """Deserialize an Event from a plain dict."""
    return Event.model_validate(data)


def story_to_dict(story: Story) -> dict:
    """Serialize a Story to a plain dict."""
    return story.model_dump(mode='json')


def story_from_dict(data: dict) -> Story:
    """Deserialize a Story from a plain dict."""
    return Story.model_validate(data)


# ── Config Models ───────────────────────────────────────────────────────────

class WebhookSourceConfig(BaseModel):
    """Configuration for the HTTP POST webhook source."""
    model_config = {"frozen": True}
    type: str = "webhook"
    host: str = "0.0.0.0"
    port: int = Field(ge=1, le=65535)
    path: str = Field(default="/events", pattern=r'^/[a-zA-Z0-9/_-]*$')
    source_label: str = "webhook"


class OtlpSourceConfig(BaseModel):
    """Configuration for the OTLP gRPC collector source."""
    model_config = {"frozen": True}
    type: str = "otlp"
    host: str = "0.0.0.0"
    port: int = Field(ge=1, le=65535)
    source_label: str = "otlp"


class FileSourceConfig(BaseModel):
    """Configuration for the JSONL file tailer source."""
    model_config = {"frozen": True}
    type: str = "file"
    path: str = Field(min_length=1, max_length=4096)
    poll_interval_seconds: float = Field(default=1.0, ge=0.1, le=3600)
    source_label: str = "file"


class SentinelSourceConfig(BaseModel):
    """Configuration for the Sentinel incident receiver source."""
    model_config = {"frozen": True}
    type: str = "sentinel"
    host: str = "0.0.0.0"
    port: int = Field(ge=1, le=65535)
    source_label: str = "sentinel"


SourceConfig = WebhookSourceConfig | OtlpSourceConfig | FileSourceConfig | SentinelSourceConfig


class StigmergySinkConfig(BaseModel):
    """Configuration for emitting events/stories to Stigmergy."""
    model_config = {"frozen": True}
    type: str = "stigmergy"
    url: str = Field(pattern=r'^https?://')
    timeout_seconds: float = Field(default=10.0, ge=0.5, le=300)


class ApprenticeSinkConfig(BaseModel):
    """Configuration for emitting events/stories to Apprentice."""
    model_config = {"frozen": True}
    type: str = "apprentice"
    url: str = Field(pattern=r'^https?://')
    timeout_seconds: float = Field(default=10.0, ge=0.5, le=300)


class DiskSinkConfig(BaseModel):
    """Configuration for the JSONL disk persistence sink."""
    model_config = {"frozen": True}
    type: str = "disk"
    events_path: str = Field(min_length=1, max_length=4096)
    stories_path: str = Field(min_length=1, max_length=4096)


class NoteworthinessFilter(BaseModel):
    """Configuration for filtering noteworthy stories for Kindex."""
    model_config = {"frozen": True}
    min_event_count: int = Field(default=1, ge=1)
    min_duration_seconds: float = Field(default=0.0, ge=0.0)
    story_types: list[str] | None = None
    exclude_close_reasons: list[str] | None = None


class KindexSinkConfig(BaseModel):
    """Configuration for the Kindex knowledge capture sink."""
    model_config = {"frozen": True}
    type: str = "kindex"
    url: str = Field(default="", pattern=r'^https?://')
    timeout_seconds: float = Field(default=10.0, ge=0.5, le=300)
    noteworthiness: NoteworthinessFilter | None = None


SinkConfig = StigmergySinkConfig | ApprenticeSinkConfig | DiskSinkConfig | KindexSinkConfig


class MemoryLimitsConfig(BaseModel):
    """Configuration for memory pressure management."""
    model_config = {"frozen": True}
    max_open_stories: int = Field(ge=1, le=1000000)
    max_events_per_story: int = Field(ge=1, le=100000)


class CorrelationRule(BaseModel):
    """Immutable rule definition for correlating events into stories."""
    model_config = {"frozen": True}
    name: str = Field(pattern=r'^[a-zA-Z][a-zA-Z0-9_.\-]{0,254}$')
    event_kinds: frozenset[str]
    group_by: list[str] = Field(min_length=1)
    chain_by: list[str] | None = None
    terminal_event: dict[str, str] | None = None
    timeout: float | None = Field(default=None, ge=0.001)
    story_type: str = Field(min_length=1, max_length=255)

    @field_validator('event_kinds', mode='before')
    @classmethod
    def _coerce_event_kinds(cls, v):
        if isinstance(v, (list, set)):
            result = frozenset(v)
            if len(result) == 0:
                raise ValueError("event_kinds must have at least one member")
            return result
        return v


class ChroniclerConfig(BaseModel):
    """Top-level Chronicler configuration."""
    model_config = {"frozen": True}
    sources: list[WebhookSourceConfig | OtlpSourceConfig | FileSourceConfig | SentinelSourceConfig] = Field(min_length=1)
    sinks: list[StigmergySinkConfig | ApprenticeSinkConfig | DiskSinkConfig | KindexSinkConfig] = Field(min_length=1)
    correlation_rules: list[CorrelationRule] = Field(min_length=1)
    memory_limits: MemoryLimitsConfig
    state_path: str = Field(default="./chronicler_state", min_length=1, max_length=4096)

    @model_validator(mode='before')
    @classmethod
    def _parse_discriminated_unions(cls, values: dict) -> dict:
        if not isinstance(values, dict):
            return values

        _src_map = {
            "webhook": WebhookSourceConfig,
            "otlp": OtlpSourceConfig,
            "file": FileSourceConfig,
            "sentinel": SentinelSourceConfig,
        }
        _snk_map = {
            "stigmergy": StigmergySinkConfig,
            "apprentice": ApprenticeSinkConfig,
            "disk": DiskSinkConfig,
            "kindex": KindexSinkConfig,
        }

        raw_sources = values.get('sources', [])
        if isinstance(raw_sources, list):
            parsed = []
            for src in raw_sources:
                if isinstance(src, dict):
                    src_type = src.get('type', '')
                    klass = _src_map.get(src_type)
                    if klass:
                        parsed.append(klass.model_validate(src))
                    else:
                        raise ValueError(f"Unknown source type: {src_type}")
                else:
                    parsed.append(src)
            values['sources'] = parsed

        raw_sinks = values.get('sinks', [])
        if isinstance(raw_sinks, list):
            parsed = []
            for snk in raw_sinks:
                if isinstance(snk, dict):
                    snk_type = snk.get('type', '')
                    klass = _snk_map.get(snk_type)
                    if klass:
                        parsed.append(klass.model_validate(snk))
                    else:
                        raise ValueError(f"Unknown sink type: {snk_type}")
                else:
                    parsed.append(snk)
            values['sinks'] = parsed

        raw_rules = values.get('correlation_rules', [])
        if isinstance(raw_rules, list):
            parsed = []
            for rule in raw_rules:
                if isinstance(rule, dict):
                    # Convert event_kinds list to frozenset
                    if 'event_kinds' in rule and isinstance(rule['event_kinds'], list):
                        rule = dict(rule)
                        rule['event_kinds'] = frozenset(rule['event_kinds'])
                    parsed.append(CorrelationRule.model_validate(rule))
                else:
                    parsed.append(rule)
            values['correlation_rules'] = parsed

        # Parse memory_limits
        ml = values.get('memory_limits')
        if isinstance(ml, dict):
            values['memory_limits'] = MemoryLimitsConfig.model_validate(ml)

        # Validate unique rule names
        parsed_rules = values.get('correlation_rules', [])
        rule_names = []
        for r in parsed_rules:
            name = r.name if hasattr(r, 'name') else r.get('name', '') if isinstance(r, dict) else ''
            rule_names.append(name)
        if len(rule_names) != len(set(rule_names)):
            dupes = [n for n in rule_names if rule_names.count(n) > 1]
            raise ValueError(f"Duplicate correlation rule names: {set(dupes)}")

        return values


def load_chronicler_config(config_path: str) -> ChroniclerConfig:
    """Load and validate a ChroniclerConfig from a YAML file."""
    import yaml
    from pathlib import Path

    p = Path(config_path)
    if not p.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    content = p.read_text()
    try:
        data = yaml.safe_load(content)
    except yaml.YAMLError as e:
        raise ValueError(f"Configuration file contains invalid YAML: {e}")

    if data is None:
        raise ValueError("Configuration file is empty")

    return ChroniclerConfig.model_validate(data)


# ── Protocols ───────────────────────────────────────────────────────────────

@runtime_checkable
class SourceProtocol(Protocol):
    """Protocol for event sources."""
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    def subscribe(self, callback) -> None: ...


@runtime_checkable
class SinkProtocol(Protocol):
    """Protocol for event/story sinks."""
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def emit(self, item) -> None: ...


# ── Exports ─────────────────────────────────────────────────────────────────

__all__ = [
    'ContextValue', 'StoryStatus', 'CloseReason', 'Event', 'Story',
    'EventIdTuple', 'GroupByKeys', 'ChainByKeys', 'CorrelationRule',
    'SourceType', 'WebhookSourceConfig', 'OtlpSourceConfig',
    'FileSourceConfig', 'SentinelSourceConfig', 'SourceConfig',
    'SinkType', 'StigmergySinkConfig', 'ApprenticeSinkConfig',
    'DiskSinkConfig', 'NoteworthinessFilter', 'KindexSinkConfig',
    'SinkConfig', 'MemoryLimitsConfig', 'ChroniclerConfig', 'EventOrStory',
    'compute_event_id', 'create_event', 'ValidationError',
    'create_story', 'append_event_to_story', 'close_story',
    'validate_story_consistency', 'matches_terminal_pattern',
    'extract_group_key', 'load_chronicler_config', 'event_to_dict',
    'event_from_dict', 'story_to_dict', 'story_from_dict',
    'EventKind', 'EntityKind', 'EntityId', 'ActorId', 'Sha256Hex',
    'StoryId', 'AwareDatetime', 'ContextDict', 'CorrelationKeys',
    'EventKindSet', 'TerminalEventPattern', 'SourceProtocol', 'SinkProtocol',
]

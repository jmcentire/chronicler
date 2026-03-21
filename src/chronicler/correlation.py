"""Correlation engine for matching events to stories.

Matches incoming events to correlation rules by extracting group_by keys,
O(1) lookup via dict keyed by (rule_name, key_tuple).
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
import uuid
from collections import OrderedDict
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable

from pathlib import Path

from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger(__name__)


# ── Primitive Types ─────────────────────────────────────────────────────────

def _make_positional_init(cls):
    """Add __init__ that accepts a positional argument mapped to 'value'."""
    _orig_init = cls.__init__
    def __init__(self, value=..., **kwargs):
        if value is not ... and 'value' not in kwargs:
            kwargs['value'] = value
        _orig_init(self, **kwargs)
    cls.__init__ = __init__
    return cls


@_make_positional_init
class EventTime(BaseModel):
    """UTC datetime string in ISO 8601 format."""
    model_config = {"frozen": True}
    value: str = Field(pattern=r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$')


@_make_positional_init
class StoryId(BaseModel):
    """Unique story identifier (UUID4)."""
    model_config = {"frozen": True}
    value: str = Field(pattern=r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$')

    def __eq__(self, other):
        if isinstance(other, str):
            return self.value == other
        if isinstance(other, StoryId):
            return self.value == other.value
        return NotImplemented

    def __hash__(self):
        return hash(self.value)

    def __str__(self):
        return self.value


@_make_positional_init
class EventId(BaseModel):
    """Unique event identifier (UUID4)."""
    model_config = {"frozen": True}
    value: str = Field(pattern=r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$')


@_make_positional_init
class RuleName(BaseModel):
    """Name identifier for a correlation rule. Lowercase alphanumeric with underscores/hyphens."""
    model_config = {"frozen": True}
    value: str = Field(min_length=1, max_length=128, pattern=r'^[a-z][a-z0-9_\-]{0,127}$')


@_make_positional_init
class SequenceNumber(BaseModel):
    """Monotonically increasing integer."""
    model_config = {"frozen": True}
    value: int = Field(ge=0)

    def __lt__(self, other):
        if isinstance(other, SequenceNumber):
            return self.value < other.value
        if isinstance(other, int):
            return self.value < other
        return NotImplemented

    def __le__(self, other):
        if isinstance(other, SequenceNumber):
            return self.value <= other.value
        return NotImplemented

    def __gt__(self, other):
        if isinstance(other, SequenceNumber):
            return self.value > other.value
        return NotImplemented

    def __ge__(self, other):
        if isinstance(other, SequenceNumber):
            return self.value >= other.value
        return NotImplemented


@_make_positional_init
class MaxOpenStories(BaseModel):
    """Upper bound on simultaneously open stories."""
    model_config = {"frozen": True}
    value: int = Field(ge=1, le=1000000)


@_make_positional_init
class TimeoutSeconds(BaseModel):
    """Timeout duration in seconds."""
    model_config = {"frozen": True}
    value: float = Field(ge=0.001)


@_make_positional_init
class SweepIntervalSeconds(BaseModel):
    """Interval between timeout sweep passes."""
    model_config = {"frozen": True}
    value: float = Field(ge=0.1, le=3600.0)


@_make_positional_init
class FilePath(BaseModel):
    """Filesystem path for JSONL state file."""
    model_config = {"frozen": True}
    value: str = Field(min_length=1, max_length=4096)


# ── Models ──────────────────────────────────────────────────────────────────

class MatchPredicate(BaseModel):
    """Field-matching predicate within a correlation rule."""
    model_config = {"frozen": True}
    field: str
    value: str
    is_regex: bool = False


class TerminalCondition(BaseModel):
    """Condition that triggers story closure."""
    model_config = {"frozen": True}
    field: str
    value: str
    is_regex: bool = False


class CorrelationRule(BaseModel):
    """Frozen rule defining how events are grouped into stories."""
    model_config = {"frozen": True}
    name: RuleName | str
    match: list[MatchPredicate]
    group_by: list[str] = Field(min_length=1)
    timeout: TimeoutSeconds | float
    terminal_conditions: list[TerminalCondition] = Field(default_factory=list)
    chain_by: list[str] = Field(default_factory=list)
    chain_ttl: TimeoutSeconds | float = TimeoutSeconds(value=300.0)

    @field_validator('name', mode='before')
    @classmethod
    def _coerce_name(cls, v):
        if isinstance(v, str):
            return RuleName(value=v)
        return v

    @field_validator('timeout', 'chain_ttl', mode='before')
    @classmethod
    def _coerce_timeout(cls, v):
        if isinstance(v, (int, float)):
            return TimeoutSeconds(value=float(v))
        return v


class Event(BaseModel):
    """Immutable event envelope."""
    model_config = {"frozen": True}
    event_id: EventId | str
    event_time: EventTime | str
    received_at: EventTime | str
    sequence: SequenceNumber | int
    source_type: str
    payload: dict = Field(default_factory=dict)

    @field_validator('event_id', mode='before')
    @classmethod
    def _coerce_event_id(cls, v):
        if isinstance(v, str):
            return EventId(value=v)
        return v

    @field_validator('event_time', 'received_at', mode='before')
    @classmethod
    def _coerce_event_time(cls, v):
        if isinstance(v, str):
            return EventTime(value=v)
        return v

    @field_validator('sequence', mode='before')
    @classmethod
    def _coerce_sequence(cls, v):
        if isinstance(v, int):
            return SequenceNumber(value=v)
        return v


class StoryStatus(str, Enum):
    """Terminal status of a closed story."""
    completed = "completed"
    timed_out = "timed_out"
    evicted = "evicted"


class Story(BaseModel):
    """Frozen Pydantic model representing a completed story."""
    model_config = {"frozen": True}
    story_id: StoryId | str
    rule_name: RuleName | str
    group_key: dict
    status: StoryStatus
    events: list[Event]
    parent_story_id: str | None = None
    created_at: str
    closed_at: str
    metadata: dict = Field(default_factory=dict)
    event_count: int = 0

    @field_validator('story_id', mode='before')
    @classmethod
    def _coerce_story_id(cls, v):
        if isinstance(v, str):
            try:
                return StoryId(value=v)
            except Exception:
                return v
        return v

    @field_validator('rule_name', mode='before')
    @classmethod
    def _coerce_rule_name(cls, v):
        if isinstance(v, str):
            try:
                return RuleName(value=v)
            except Exception:
                return v
        return v


class CompositeKey(BaseModel):
    """The composite lookup key for the correlation dict."""
    model_config = {"frozen": True}
    rule_name: str
    key_tuple: tuple


class ChainEntry(BaseModel):
    """Entry in the chain-by registry."""
    model_config = {"frozen": True}
    parent_story_id: str
    expires_at: float

    @field_validator('parent_story_id', mode='before')
    @classmethod
    def _unwrap_parent(cls, v):
        return _coerce_str(v)


class RecordType(str, Enum):
    """Discriminator for JSONL state records."""
    story_opened = "story_opened"
    event_appended = "event_appended"
    story_closed = "story_closed"


def _coerce_str(v):
    """Coerce wrapper types with .value to strings."""
    if hasattr(v, 'value'):
        return str(v.value)
    return v


class StoryOpenedRecord(BaseModel):
    """JSONL state record when a new story is created."""
    model_config = {"frozen": True}
    record_type: RecordType = RecordType.story_opened
    story_id: str
    rule_name: str
    group_key: dict
    parent_story_id: str = ""
    created_at: str
    timeout: float

    @field_validator('story_id', 'rule_name', 'parent_story_id', 'created_at', mode='before')
    @classmethod
    def _unwrap(cls, v):
        return _coerce_str(v)


class EventAppendedRecord(BaseModel):
    """JSONL state record when an event is appended."""
    model_config = {"frozen": True}
    record_type: RecordType = RecordType.event_appended
    story_id: str
    event: Event

    @field_validator('story_id', mode='before')
    @classmethod
    def _unwrap(cls, v):
        return _coerce_str(v)


class StoryClosedRecord(BaseModel):
    """JSONL state record when a story is closed."""
    model_config = {"frozen": True}
    record_type: RecordType = RecordType.story_closed
    story_id: str
    status: StoryStatus
    closed_at: str

    @field_validator('story_id', 'closed_at', mode='before')
    @classmethod
    def _unwrap(cls, v):
        return _coerce_str(v)


StateRecord = StoryOpenedRecord | EventAppendedRecord | StoryClosedRecord


class RecoveryResult(BaseModel):
    """Result of replaying JSONL state file."""
    model_config = {"frozen": True}
    open_stories: list[dict] = Field(default_factory=list)
    records_read: int = 0
    records_skipped: int = 0
    chain_entries: list[dict] = Field(default_factory=list)


class CorrelationEngineConfig(BaseModel):
    """Configuration for the CorrelationEngine."""
    model_config = {"frozen": True}
    rules: list[CorrelationRule] = Field(min_length=1)
    max_open_stories: MaxOpenStories | int
    sweep_interval: SweepIntervalSeconds | float
    state_file: FilePath | str
    clock: Any = None
    id_factory: Any = None

    @field_validator('max_open_stories', mode='before')
    @classmethod
    def _coerce_max_open(cls, v):
        if isinstance(v, int):
            return MaxOpenStories(value=v)
        return v

    @field_validator('sweep_interval', mode='before')
    @classmethod
    def _coerce_sweep(cls, v):
        if isinstance(v, (int, float)):
            return SweepIntervalSeconds(value=float(v))
        return v

    @field_validator('state_file', mode='before')
    @classmethod
    def _coerce_state_file(cls, v):
        if isinstance(v, str):
            return FilePath(value=v)
        return v


class EngineStats(BaseModel):
    """Runtime statistics for monitoring."""
    model_config = {"frozen": True}
    open_story_count: int = 0
    total_events_processed: int = 0
    total_stories_created: int = 0
    total_stories_closed: int = 0
    total_stories_evicted: int = 0
    total_chain_links: int = 0
    current_sequence: int = 0
    active_chain_entries: int = 0


# ── Internal Open Story ────────────────────────────────────────────────────

class _OpenStory:
    """Mutable internal representation of an open story."""
    def __init__(
        self,
        story_id: str,
        rule_name: str,
        group_key: dict,
        timeout: float,
        parent_story_id: str = "",
        created_at: str = "",
    ):
        self.story_id = story_id
        self.rule_name = rule_name
        self.group_key = group_key
        self.timeout = timeout
        self.parent_story_id = parent_story_id
        self.created_at = created_at or datetime.now(timezone.utc).isoformat()
        self.events: list[Event] = []
        self.last_activity_time: float = 0.0

    def to_story(self, status: StoryStatus, closed_at: str) -> Story:
        """Convert to frozen Story."""
        sorted_events = sorted(
            self.events,
            key=lambda e: (e.event_time.value, e.received_at.value, e.sequence.value),
        )
        return Story(
            story_id=self.story_id,
            rule_name=self.rule_name,
            group_key=self.group_key,
            status=status,
            events=sorted_events,
            parent_story_id=self.parent_story_id or None,
            created_at=self.created_at,
            closed_at=closed_at,
            event_count=len(sorted_events),
        )


# ── Story Manager ──────────────────────────────────────────────────────────

def _unwrap(val):
    """Unwrap a pydantic model with .value attribute, or return as-is."""
    if hasattr(val, 'value'):
        return val.value
    return val


class StoryManager:
    """Manages open stories in bounded memory with LRU eviction."""

    def __init__(
        self,
        max_open_stories: MaxOpenStories,
        sweep_interval: SweepIntervalSeconds,
        clock: Callable[[], float] | None = None,
    ):
        self._max_open_stories = _unwrap(max_open_stories)
        self._sweep_interval = _unwrap(sweep_interval)
        self._clock = clock or time.monotonic
        self._stories: OrderedDict[str, _OpenStory] = OrderedDict()
        self._sink_callbacks: list[Callable] = []
        self._sweep_task: asyncio.Task | None = None

    def register_sink(self, callback: Callable) -> None:
        """Register an async callback for closed stories."""
        self._sink_callbacks.append(callback)

    async def register_story(
        self,
        story_id,
        rule_name,
        group_key: dict,
        timeout,
        parent_story_id = "",
        created_at = "",
    ) -> None:
        """Register a new open story."""
        story_id = str(_unwrap(story_id))
        rule_name = str(_unwrap(rule_name))
        timeout = float(_unwrap(timeout))
        parent_story_id = str(_unwrap(parent_story_id)) if parent_story_id else ""
        created_at = str(_unwrap(created_at)) if created_at else ""

        if story_id in self._stories:
            raise ValueError(f"Story {story_id} already registered")

        # Evict if at capacity
        while len(self._stories) >= self._max_open_stories:
            oldest_id, oldest = next(iter(self._stories.items()))
            self._stories.pop(oldest_id)
            closed = oldest.to_story(
                StoryStatus.evicted,
                datetime.now(timezone.utc).isoformat(),
            )
            await self._emit_to_sinks(closed)

        story = _OpenStory(
            story_id=story_id,
            rule_name=rule_name,
            group_key=group_key,
            timeout=timeout,
            parent_story_id=parent_story_id,
            created_at=created_at,
        )
        story.last_activity_time = self._clock()
        self._stories[story_id] = story

    async def append_event(self, story_id, event: Event) -> None:
        """Append an event to an existing open story."""
        story_id = str(_unwrap(story_id))
        if story_id not in self._stories:
            raise KeyError(f"Story {story_id} not found in open stories")
        story = self._stories[story_id]
        story.events.append(event)
        story.last_activity_time = self._clock()
        self._stories.move_to_end(story_id)

    async def touch_story(self, story_id) -> None:
        """Move story to most-recently-used position."""
        story_id = str(_unwrap(story_id))
        if story_id not in self._stories:
            raise KeyError(f"Story {story_id} not found in open stories")
        self._stories[story_id].last_activity_time = self._clock()
        self._stories.move_to_end(story_id)

    async def close_story(self, story_id, status: StoryStatus) -> Story:
        """Close an open story with the given status."""
        story_id = str(_unwrap(story_id))
        if story_id not in self._stories:
            raise KeyError(f"Story {story_id} not found in open stories")
        open_story = self._stories.pop(story_id)
        closed = open_story.to_story(
            status,
            datetime.now(timezone.utc).isoformat(),
        )
        await self._emit_to_sinks(closed)
        return closed

    async def _emit_to_sinks(self, story: Story) -> None:
        """Emit to all registered sink callbacks."""
        results = await asyncio.gather(
            *(self._safe_emit(cb, story) for cb in self._sink_callbacks),
            return_exceptions=True,
        )
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error("Sink %d error for story %s: %s", i, story.story_id, result)

    async def _safe_emit(self, callback: Callable, story: Story) -> None:
        try:
            await callback(story)
        except Exception as e:
            logger.error("Sink callback error: %s", e)

    async def start_sweep_task(self) -> None:
        """Start the periodic timeout sweep task."""
        if self._sweep_task is not None:
            raise RuntimeError("Sweep task is already running")
        self._sweep_task = asyncio.create_task(self._sweep_loop())

    async def stop_sweep_task(self) -> None:
        """Cancel the periodic sweep task."""
        if self._sweep_task is not None:
            self._sweep_task.cancel()
            try:
                await self._sweep_task
            except asyncio.CancelledError:
                pass
            self._sweep_task = None

    async def _sweep_loop(self) -> None:
        """Periodic sweep loop."""
        try:
            while True:
                await asyncio.sleep(self._sweep_interval)
                await self.sweep_once()
        except asyncio.CancelledError:
            pass

    async def sweep_once(self) -> int:
        """Perform a single timeout sweep pass."""
        now = self._clock()
        to_close = []
        for story_id, story in self._stories.items():
            if now - story.last_activity_time > story.timeout:
                to_close.append(story_id)

        count = 0
        for story_id in to_close:
            try:
                await self.close_story(story_id, StoryStatus.timed_out)
                count += 1
            except Exception as e:
                logger.error("Error closing timed-out story %s: %s", story_id, e)
        return count

    def get_open_story_count(self) -> int:
        """Return the current number of open stories."""
        return len(self._stories)

    async def load_recovered_stories(self, recovery_result: RecoveryResult) -> int:
        """Load recovered stories from state recovery."""
        loaded = 0
        for story_data in recovery_result.open_stories:
            try:
                await self.register_story(
                    story_id=story_data.get('story_id', ''),
                    rule_name=story_data.get('rule_name', ''),
                    group_key=story_data.get('group_key', {}),
                    timeout=story_data.get('timeout', 300.0),
                    parent_story_id=story_data.get('parent_story_id', ''),
                    created_at=story_data.get('created_at', ''),
                )
                # Replay events
                for event_data in story_data.get('events', []):
                    try:
                        evt = Event.model_validate(event_data)
                        await self.append_event(story_data['story_id'], evt)
                    except Exception:
                        logger.warning("Skipping malformed event in recovered story")
                loaded += 1
            except Exception as e:
                logger.warning("Malformed recovered story skipped: %s", e)
        return loaded


# ── Correlation Engine ──────────────────────────────────────────────────────

class CorrelationEngine:
    """Matches incoming events against correlation rules."""

    def __init__(
        self,
        config: CorrelationEngineConfig,
        story_manager: StoryManager,
        clock: Callable[[], float] | None = None,
        id_factory: Callable[[], str] | None = None,
    ):
        # Validate unique rule names
        names = [r.name.value for r in config.rules]
        seen = set()
        dupes = set()
        for n in names:
            if n in seen:
                dupes.add(n)
            seen.add(n)
        if dupes:
            raise ValueError(f"Duplicate rule names found: {dupes}")

        if not config.rules:
            raise ValueError("At least one correlation rule is required")

        self._rules = {r.name.value: r for r in config.rules}
        self._story_manager = story_manager
        self._clock = clock or config.clock or time.monotonic
        self._id_factory = id_factory or config.id_factory or (lambda: str(uuid.uuid4()))
        self._state_file = config.state_file.value

        self._correlation_dict: dict[tuple, str] = {}  # (rule_name, key_tuple) -> story_id
        self._chain_registry: dict[tuple, ChainEntry] = {}
        self._sequence: int = 0

        self._total_events_processed: int = 0
        self._total_stories_created: int = 0
        self._total_stories_closed: int = 0
        self._total_stories_evicted: int = 0
        self._total_chain_links: int = 0

    def _matches_rule(self, event: Event, rule: CorrelationRule) -> bool:
        """Check if an event matches a rule's match predicates."""
        for pred in rule.match:
            field_val = self._get_field(event, pred.field)
            if field_val is None:
                return False
            if pred.is_regex:
                if not re.match(pred.value, str(field_val)):
                    return False
            else:
                if str(field_val) != pred.value:
                    return False
        return True

    def _get_field(self, event: Event, field_path: str) -> Any:
        """Get a field value from an event by dot-delimited path."""
        if field_path == 'source_type':
            return event.source_type
        elif field_path.startswith('payload.'):
            key = field_path[len('payload.'):]
            return event.payload.get(key)
        elif field_path in event.payload:
            return event.payload.get(field_path)
        elif hasattr(event, field_path):
            val = getattr(event, field_path)
            if isinstance(val, (EventId, EventTime, SequenceNumber)):
                return val.value
            return val
        return None

    def _extract_group_key(self, event: Event, group_by: list[str]) -> tuple | None:
        """Extract group key values from event."""
        values = []
        for field_path in group_by:
            val = self._get_field(event, field_path)
            if val is None:
                logger.warning("Event dropped — missing group_by field '%s'", field_path)
                return None
            values.append((field_path, str(val)))
        return tuple(sorted(values))

    def _extract_group_key_dict(self, event: Event, group_by: list[str]) -> dict | None:
        """Extract group key as dict."""
        result = {}
        for field_path in group_by:
            val = self._get_field(event, field_path)
            if val is None:
                return None
            result[field_path] = str(val)
        return result

    def _check_terminal(self, event: Event, conditions: list[TerminalCondition]) -> bool:
        """Check if event matches any terminal condition."""
        if not conditions:
            return False
        for cond in conditions:
            field_val = self._get_field(event, cond.field)
            if field_val is None:
                continue
            if cond.is_regex:
                if re.match(cond.value, str(field_val)):
                    return True
            else:
                if str(field_val) == cond.value:
                    return True
        return False

    async def process_event(self, event: Event) -> None:
        """Match an incoming event against all correlation rules."""
        try:
            self._sequence += 1
            self._total_events_processed += 1

            matched_any = False
            for rule_name, rule in self._rules.items():
                if not self._matches_rule(event, rule):
                    continue
                matched_any = True

                key_tuple = self._extract_group_key(event, rule.group_by)
                if key_tuple is None:
                    continue

                lookup_key = (rule_name, key_tuple)
                group_key_dict = self._extract_group_key_dict(event, rule.group_by) or {}

                if lookup_key in self._correlation_dict:
                    # Append to existing story
                    story_id = self._correlation_dict[lookup_key]
                    await self._story_manager.append_event(story_id, event)

                    # Check terminal conditions
                    if self._check_terminal(event, rule.terminal_conditions):
                        closed = await self._story_manager.close_story(
                            story_id, StoryStatus.completed
                        )
                        del self._correlation_dict[lookup_key]
                        self._total_stories_closed += 1
                        if rule.chain_by:
                            self.register_chain_entry(closed, rule)
                else:
                    # Check chain registry using chain_by fields
                    parent_story_id = ""
                    if rule.chain_by:
                        chain_key_tuple = self._extract_group_key(event, rule.chain_by)
                    else:
                        chain_key_tuple = None
                    chain_key = (rule_name, chain_key_tuple) if chain_key_tuple else None
                    if chain_key and chain_key in self._chain_registry:
                        entry = self._chain_registry[chain_key]
                        if self._clock() < entry.expires_at:
                            parent_story_id = entry.parent_story_id
                            self._total_chain_links += 1
                        del self._chain_registry[chain_key]

                    # Create new story
                    story_id = self._id_factory()
                    now = datetime.now(timezone.utc).isoformat()
                    await self._story_manager.register_story(
                        story_id=story_id,
                        rule_name=rule_name,
                        group_key=group_key_dict,
                        timeout=rule.timeout.value,
                        parent_story_id=parent_story_id,
                        created_at=now,
                    )
                    await self._story_manager.append_event(story_id, event)
                    self._correlation_dict[lookup_key] = story_id
                    self._total_stories_created += 1

                    # Check terminal on first event
                    if self._check_terminal(event, rule.terminal_conditions):
                        closed = await self._story_manager.close_story(
                            story_id, StoryStatus.completed
                        )
                        del self._correlation_dict[lookup_key]
                        self._total_stories_closed += 1
                        if rule.chain_by:
                            self.register_chain_entry(closed, rule)

            if not matched_any:
                logger.debug("Event dropped — no matching rule")

        except Exception as e:
            logger.error("Exception during event processing: %s", e)

    def register_chain_entry(self, story: Story, rule: CorrelationRule) -> None:
        """Register a chain-by entry when a story closes."""
        if not rule.chain_by:
            return
        if not story.events:
            logger.error("Chain entry not registered — story has no events")
            return

        last_event = story.events[-1]
        values = []
        for field_path in rule.chain_by:
            val = self._get_field(last_event, field_path)
            if val is None:
                logger.warning(
                    "Chain entry not registered — missing field '%s' in last event",
                    field_path,
                )
                return
            values.append((field_path, str(val)))

        chain_key = (rule.name.value, tuple(sorted(values)))
        self._chain_registry[chain_key] = ChainEntry(
            parent_story_id=story.story_id,
            expires_at=self._clock() + rule.chain_ttl.value,
        )

    def purge_expired_chains(self) -> int:
        """Remove all expired chain entries."""
        now = self._clock()
        expired = [k for k, v in self._chain_registry.items() if v.expires_at < now]
        for k in expired:
            del self._chain_registry[k]
        return len(expired)

    def get_stats(self) -> EngineStats:
        """Return current engine statistics."""
        return EngineStats(
            open_story_count=self._story_manager.get_open_story_count(),
            total_events_processed=self._total_events_processed,
            total_stories_created=self._total_stories_created,
            total_stories_closed=self._total_stories_closed,
            total_stories_evicted=self._total_stories_evicted,
            total_chain_links=self._total_chain_links,
            current_sequence=self._sequence,
            active_chain_entries=len(self._chain_registry),
        )


# ── State Persistence ───────────────────────────────────────────────────────

async def append_record(path, record: StateRecord) -> None:
    """Append a single state record to the JSONL state file."""
    try:
        path_str = str(_unwrap(path))
        data = record.model_dump(mode='json')
        line = json.dumps(data, ensure_ascii=True, default=str)
        with open(path_str, 'a') as f:
            f.write(line + '\n')
            f.flush()
    except Exception as e:
        logger.error("I/O error writing state record: %s", e)


async def recover_state(
    path,
    clock: Callable[[], float] | None = None,
) -> RecoveryResult:
    """Read the JSONL state file and reconstruct open stories."""
    path = str(_unwrap(path))
    if not Path(path).exists():
        logger.info("State file not found: %s", path)
        return RecoveryResult()

    stories: dict[str, dict] = {}
    records_read = 0
    records_skipped = 0

    try:
        with open(path, 'r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    record_type = data.get('record_type')
                    if record_type == RecordType.story_opened.value:
                        stories[data['story_id']] = {
                            'story_id': data['story_id'],
                            'rule_name': data['rule_name'],
                            'group_key': data.get('group_key', {}),
                            'parent_story_id': data.get('parent_story_id', ''),
                            'created_at': data.get('created_at', ''),
                            'timeout': data.get('timeout', 300.0),
                            'events': [],
                        }
                    elif record_type == RecordType.event_appended.value:
                        sid = data['story_id']
                        if sid in stories:
                            stories[sid]['events'].append(data.get('event', {}))
                    elif record_type == RecordType.story_closed.value:
                        stories.pop(data['story_id'], None)
                    records_read += 1
                except (json.JSONDecodeError, KeyError, Exception) as e:
                    logger.warning("Corrupt line %d in state file: %s", line_num, e)
                    records_skipped += 1
    except OSError as e:
        raise OSError(f"Unrecoverable I/O error during state recovery: {e}")

    return RecoveryResult(
        open_stories=list(stories.values()),
        records_read=records_read,
        records_skipped=records_skipped,
        chain_entries=[],
    )




# ── Exports ─────────────────────────────────────────────────────────────────

__all__ = [
    'MatchPredicate', 'TerminalCondition', 'CorrelationRule', 'Event',
    'StoryStatus', 'Story', 'CompositeKey', 'ChainEntry', 'RecordType',
    'StoryOpenedRecord', 'EventAppendedRecord', 'StoryClosedRecord',
    'StateRecord', 'RecoveryResult', 'CorrelationEngineConfig',
    'EngineStats', 'append_record', 'recover_state',
    'CorrelationEngine', 'StoryManager',
    'EventTime', 'StoryId', 'EventId', 'RuleName', 'SequenceNumber',
    'MaxOpenStories', 'TimeoutSeconds', 'SweepIntervalSeconds', 'FilePath',
]

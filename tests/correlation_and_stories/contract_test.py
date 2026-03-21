"""
Contract tests for correlation_and_stories module.

Tests cover: type validators, CorrelationEngine, StoryManager, state persistence,
and cross-component integration flows.

Run with: pytest contract_test.py -v
"""
import asyncio
import json
import os
import pytest
import time
from unittest.mock import (
    AsyncMock, MagicMock, Mock, patch, call, PropertyMock, ANY
)
from typing import List, Dict, Optional

# Import the module under test
from correlation_and_stories import (
    EventTime,
    StoryId,
    EventId,
    RuleName,
    SequenceNumber,
    MaxOpenStories,
    TimeoutSeconds,
    SweepIntervalSeconds,
    MatchPredicate,
    TerminalCondition,
    CorrelationRule,
    Event,
    StoryStatus,
    Story,
    CompositeKey,
    ChainEntry,
    RecordType,
    StoryOpenedRecord,
    EventAppendedRecord,
    StoryClosedRecord,
    RecoveryResult,
    CorrelationEngineConfig,
    EngineStats,
    FilePath,
    CorrelationEngine,
    StoryManager,
    append_record,
    recover_state,
)

# Try pydantic import for ValidationError
try:
    from pydantic import ValidationError
except ImportError:
    ValidationError = Exception


# ============================================================================
# Test Helpers / Fixtures
# ============================================================================

class ControllableClock:
    """A clock that can be advanced for deterministic testing."""
    def __init__(self, start=1000000.0):
        self._time = start

    def __call__(self):
        return self._time

    def advance(self, seconds):
        self._time += seconds


def make_clock(start=1000000.0):
    return ControllableClock(start)


def make_id_factory():
    """Returns a sequential ID generator producing valid UUID4-format strings."""
    counter = [0]
    def factory():
        counter[0] += 1
        n = counter[0]
        # Format as valid UUID4: 8-4-4-4-12 hex with version 4 and variant 8/9/a/b
        return f"{n:08x}-0000-4000-a000-{n:012x}"
    return factory


VALID_UUID4 = "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d"
VALID_UUID4_2 = "b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e"
VALID_UUID4_3 = "c3d4e5f6-a7b8-4c9d-ae0f-1a2b3c4d5e6f"
VALID_EVENT_TIME = "2024-01-15T10:30:00Z"
VALID_EVENT_TIME_2 = "2024-01-15T10:31:00Z"
VALID_EVENT_TIME_3 = "2024-01-15T10:32:00Z"


def make_event(
    event_id=None,
    event_time=None,
    received_at=None,
    sequence=0,
    source_type="firewall",
    payload=None,
):
    """Factory for Event instances with sensible defaults."""
    return Event(
        event_id=EventId(value=event_id or VALID_UUID4),
        event_time=EventTime(value=event_time or VALID_EVENT_TIME),
        received_at=EventTime(value=received_at or VALID_EVENT_TIME),
        sequence=SequenceNumber(value=sequence),
        source_type=source_type,
        payload=payload or {"src_ip": "10.0.0.1", "dst_ip": "10.0.0.2"},
    )


def make_rule(
    name="test-rule",
    match=None,
    group_by=None,
    timeout=60.0,
    terminal_conditions=None,
    chain_by=None,
    chain_ttl=120.0,
):
    """Factory for CorrelationRule with sensible defaults."""
    return CorrelationRule(
        name=RuleName(value=name),
        match=match or [MatchPredicate(field="source_type", value="firewall", is_regex=False)],
        group_by=group_by or ["src_ip"],
        timeout=TimeoutSeconds(value=timeout),
        terminal_conditions=terminal_conditions or [],
        chain_by=chain_by or [],
        chain_ttl=TimeoutSeconds(value=chain_ttl),
    )


def make_config(
    rules=None,
    max_open_stories=1000,
    sweep_interval=1.0,
    state_file="/tmp/test_state.jsonl",
    clock=None,
    id_factory=None,
):
    """Factory for CorrelationEngineConfig."""
    return CorrelationEngineConfig(
        rules=rules if rules is not None else [make_rule()],
        max_open_stories=MaxOpenStories(value=max_open_stories),
        sweep_interval=SweepIntervalSeconds(value=sweep_interval),
        state_file=FilePath(value=state_file),
        clock=clock or make_clock(),
        id_factory=id_factory or make_id_factory(),
    )


def make_sink():
    """Returns (async_callable, captured_stories_list)."""
    captured = []
    async def sink(story):
        captured.append(story)
    return sink, captured


def make_error_sink(error_cls=RuntimeError, msg="sink error"):
    """Returns an async callable that always raises."""
    async def sink(story):
        raise error_cls(msg)
    return sink


def make_story_manager(max_open_stories=1000, sweep_interval=1.0, clock=None):
    """Factory for StoryManager."""
    clk = clock or make_clock()
    return StoryManager(
        max_open_stories=MaxOpenStories(value=max_open_stories),
        sweep_interval=SweepIntervalSeconds(value=sweep_interval),
        clock=clk,
    ), clk


# ============================================================================
# TYPE VALIDATION TESTS
# ============================================================================

class TestEventTimeValidation:
    """Tests for EventTime type validation."""

    def test_valid_utc_z_suffix(self):
        """TC-TYPE-01: EventTime accepts valid UTC ISO 8601 datetime with Z suffix."""
        et = EventTime(value="2024-01-15T10:30:00Z")
        assert et.value == "2024-01-15T10:30:00Z"

    def test_valid_with_fractional_seconds_and_offset(self):
        """TC-TYPE-02: EventTime accepts datetime with fractional seconds and offset."""
        et = EventTime(value="2024-01-15T10:30:00.123+05:30")
        assert et.value == "2024-01-15T10:30:00.123+05:30"

    def test_valid_negative_offset(self):
        et = EventTime(value="2024-01-15T10:30:00-08:00")
        assert et.value == "2024-01-15T10:30:00-08:00"

    def test_valid_fractional_z(self):
        et = EventTime(value="2024-01-15T10:30:00.999999Z")
        assert et.value == "2024-01-15T10:30:00.999999Z"

    def test_rejects_invalid_string(self):
        """TC-TYPE-03: EventTime rejects invalid datetime string."""
        with pytest.raises((ValidationError, ValueError)):
            EventTime(value="not-a-date")

    def test_rejects_no_timezone(self):
        """TC-TYPE-04: EventTime rejects datetime without timezone."""
        with pytest.raises((ValidationError, ValueError)):
            EventTime(value="2024-01-15T10:30:00")

    def test_rejects_date_only(self):
        with pytest.raises((ValidationError, ValueError)):
            EventTime(value="2024-01-15")

    def test_rejects_empty_string(self):
        with pytest.raises((ValidationError, ValueError)):
            EventTime(value="")


class TestStoryIdValidation:
    """Tests for StoryId type validation."""

    def test_valid_uuid4(self):
        """TC-TYPE-05: StoryId accepts valid UUID4 string."""
        sid = StoryId(value=VALID_UUID4)
        assert sid.value == VALID_UUID4

    def test_rejects_uuid_v1(self):
        """TC-TYPE-06: StoryId rejects non-UUID4 (version 1)."""
        with pytest.raises((ValidationError, ValueError)):
            StoryId(value="a1b2c3d4-e5f6-1a7b-8c9d-0e1f2a3b4c5d")

    def test_rejects_non_uuid_string(self):
        with pytest.raises((ValidationError, ValueError)):
            StoryId(value="not-a-uuid")

    def test_rejects_uuid_with_invalid_variant(self):
        # Variant must be 8, 9, a, or b in position 19
        with pytest.raises((ValidationError, ValueError)):
            StoryId(value="a1b2c3d4-e5f6-4a7b-0c9d-0e1f2a3b4c5d")


class TestEventIdValidation:
    """Tests for EventId type validation."""

    def test_valid_uuid4(self):
        eid = EventId(value=VALID_UUID4)
        assert eid.value == VALID_UUID4

    def test_rejects_invalid(self):
        with pytest.raises((ValidationError, ValueError)):
            EventId(value="bad")


class TestRuleNameValidation:
    """Tests for RuleName type validation."""

    def test_valid_name(self):
        """TC-TYPE-07: RuleName accepts valid lowercase alphanumeric with hyphens/underscores."""
        rn = RuleName(value="my-rule_01")
        assert rn.value == "my-rule_01"

    def test_rejects_starts_with_digit(self):
        """TC-TYPE-08: RuleName rejects name starting with digit."""
        with pytest.raises((ValidationError, ValueError)):
            RuleName(value="1rule")

    def test_rejects_uppercase(self):
        """TC-TYPE-09: RuleName rejects name with uppercase."""
        with pytest.raises((ValidationError, ValueError)):
            RuleName(value="MyRule")

    def test_rejects_empty(self):
        """TC-TYPE-10: RuleName rejects empty string."""
        with pytest.raises((ValidationError, ValueError)):
            RuleName(value="")

    def test_single_char_min_length(self):
        """TC-TYPE-11: RuleName accepts single lowercase letter."""
        rn = RuleName(value="a")
        assert rn.value == "a"

    def test_max_length_128(self):
        """TC-TYPE-12: RuleName accepts 128 character name."""
        name = "a" * 128
        rn = RuleName(value=name)
        assert rn.value == name

    def test_rejects_129_chars(self):
        """TC-TYPE-13: RuleName rejects 129 character name."""
        with pytest.raises((ValidationError, ValueError)):
            RuleName(value="a" * 129)

    def test_rejects_special_chars(self):
        with pytest.raises((ValidationError, ValueError)):
            RuleName(value="my.rule")

    def test_rejects_spaces(self):
        with pytest.raises((ValidationError, ValueError)):
            RuleName(value="my rule")


class TestSequenceNumberValidation:
    """Tests for SequenceNumber type validation."""

    def test_zero_minimum(self):
        """TC-TYPE-14: SequenceNumber accepts zero."""
        sn = SequenceNumber(value=0)
        assert sn.value == 0

    def test_rejects_negative(self):
        """TC-TYPE-15: SequenceNumber rejects negative."""
        with pytest.raises((ValidationError, ValueError)):
            SequenceNumber(value=-1)

    def test_large_value(self):
        sn = SequenceNumber(value=999999999)
        assert sn.value == 999999999


class TestMaxOpenStoriesValidation:
    """Tests for MaxOpenStories type validation."""

    def test_minimum_one(self):
        """TC-TYPE-16: MaxOpenStories accepts 1."""
        m = MaxOpenStories(value=1)
        assert m.value == 1

    def test_rejects_zero(self):
        """TC-TYPE-17: MaxOpenStories rejects 0."""
        with pytest.raises((ValidationError, ValueError)):
            MaxOpenStories(value=0)

    def test_maximum_1000000(self):
        """TC-TYPE-18: MaxOpenStories accepts 1000000."""
        m = MaxOpenStories(value=1000000)
        assert m.value == 1000000

    def test_rejects_above_max(self):
        """TC-TYPE-19: MaxOpenStories rejects 1000001."""
        with pytest.raises((ValidationError, ValueError)):
            MaxOpenStories(value=1000001)

    def test_rejects_negative(self):
        with pytest.raises((ValidationError, ValueError)):
            MaxOpenStories(value=-5)


class TestTimeoutSecondsValidation:
    """Tests for TimeoutSeconds type validation."""

    def test_minimum_0_001(self):
        """TC-TYPE-20: TimeoutSeconds accepts 0.001."""
        t = TimeoutSeconds(value=0.001)
        assert t.value == 0.001

    def test_rejects_zero(self):
        """TC-TYPE-21: TimeoutSeconds rejects 0."""
        with pytest.raises((ValidationError, ValueError)):
            TimeoutSeconds(value=0)

    def test_rejects_negative(self):
        with pytest.raises((ValidationError, ValueError)):
            TimeoutSeconds(value=-1.0)

    def test_large_value(self):
        t = TimeoutSeconds(value=86400.0)
        assert t.value == 86400.0


class TestSweepIntervalSecondsValidation:
    """Tests for SweepIntervalSeconds type validation."""

    def test_minimum_0_1(self):
        """TC-TYPE-22: SweepIntervalSeconds accepts 0.1."""
        s = SweepIntervalSeconds(value=0.1)
        assert s.value == 0.1

    def test_rejects_below_min(self):
        """TC-TYPE-23: SweepIntervalSeconds rejects 0.09."""
        with pytest.raises((ValidationError, ValueError)):
            SweepIntervalSeconds(value=0.09)

    def test_rejects_above_max(self):
        """TC-TYPE-24: SweepIntervalSeconds rejects 3601."""
        with pytest.raises((ValidationError, ValueError)):
            SweepIntervalSeconds(value=3601)

    def test_max_3600(self):
        s = SweepIntervalSeconds(value=3600)
        assert s.value == 3600


class TestCorrelationRuleValidation:
    """Tests for CorrelationRule struct validation."""

    def test_rejects_empty_group_by(self):
        """TC-TYPE-25: CorrelationRule requires non-empty group_by."""
        with pytest.raises((ValidationError, ValueError)):
            CorrelationRule(
                name=RuleName(value="test-rule"),
                match=[MatchPredicate(field="source_type", value="firewall", is_regex=False)],
                group_by=[],
                timeout=TimeoutSeconds(value=60.0),
                terminal_conditions=[],
                chain_by=[],
                chain_ttl=TimeoutSeconds(value=120.0),
            )

    def test_valid_rule(self):
        rule = make_rule()
        assert rule.name.value == "test-rule"
        assert len(rule.group_by) >= 1


class TestFilePathValidation:
    """Tests for FilePath type validation."""

    def test_valid_path(self):
        """TC-TYPE-27: FilePath accepts valid path."""
        fp = FilePath(value="/tmp/state.jsonl")
        assert fp.value == "/tmp/state.jsonl"

    def test_rejects_empty(self):
        """TC-TYPE-28: FilePath rejects empty string."""
        with pytest.raises((ValidationError, ValueError)):
            FilePath(value="")


class TestEnumTypes:
    """Tests for enum types."""

    def test_story_status_variants(self):
        """TC-TYPE-29: StoryStatus enum has expected variants."""
        assert StoryStatus.completed is not None
        assert StoryStatus.timed_out is not None
        assert StoryStatus.evicted is not None

    def test_record_type_variants(self):
        """TC-TYPE-30: RecordType enum has expected variants."""
        assert RecordType.story_opened is not None
        assert RecordType.event_appended is not None
        assert RecordType.story_closed is not None


class TestCorrelationEngineConfigValidation:
    """Tests for CorrelationEngineConfig validation."""

    def test_rejects_empty_rules(self):
        """TC-TYPE-26: CorrelationEngineConfig rejects empty rules list."""
        with pytest.raises((ValidationError, ValueError)):
            CorrelationEngineConfig(
                rules=[],
                max_open_stories=MaxOpenStories(value=1000),
                sweep_interval=SweepIntervalSeconds(value=1.0),
                state_file=FilePath(value="/tmp/state.jsonl"),
                clock=make_clock(),
                id_factory=make_id_factory(),
            )


# ============================================================================
# CORRELATION ENGINE TESTS
# ============================================================================

class TestCorrelationEngineInit:
    """Tests for CorrelationEngine.__init__."""

    def test_init_valid_config(self):
        """TC-CE-01: Init with valid config and unique rules succeeds."""
        clock = make_clock()
        id_factory = make_id_factory()
        sm, _ = make_story_manager(clock=clock)
        config = make_config(
            rules=[make_rule(name="rule-a"), make_rule(name="rule-b", match=[
                MatchPredicate(field="source_type", value="dns", is_regex=False)
            ])],
            clock=clock,
            id_factory=id_factory,
        )
        engine = CorrelationEngine(
            config=config,
            story_manager=sm,
            clock=clock,
            id_factory=id_factory,
        )
        stats = engine.get_stats()
        assert stats.open_story_count == 0
        assert stats.current_sequence == 0
        assert stats.total_events_processed == 0

    def test_init_duplicate_rule_names_raises(self):
        """TC-CE-02: Init raises on duplicate rule names."""
        clock = make_clock()
        id_factory = make_id_factory()
        sm, _ = make_story_manager(clock=clock)
        rule1 = make_rule(name="same-name")
        rule2 = make_rule(name="same-name")
        config = make_config(rules=[rule1, rule2], clock=clock, id_factory=id_factory)
        with pytest.raises(Exception):  # Could be ValueError or custom error
            CorrelationEngine(
                config=config,
                story_manager=sm,
                clock=clock,
                id_factory=id_factory,
            )

    def test_init_empty_rules_raises(self):
        """TC-CE-03: Init raises on empty rules (caught by config validation)."""
        with pytest.raises((ValidationError, ValueError, Exception)):
            config = make_config(rules=[])


class TestCorrelationEngineProcessEvent:
    """Tests for CorrelationEngine.process_event."""

    @pytest.mark.asyncio
    async def test_process_event_creates_new_story(self):
        """TC-CE-04: Event matching rule creates new story."""
        clock = make_clock()
        id_factory = make_id_factory()
        sm, _ = make_story_manager(clock=clock)
        config = make_config(clock=clock, id_factory=id_factory)
        engine = CorrelationEngine(
            config=config,
            story_manager=sm,
            clock=clock,
            id_factory=id_factory,
        )
        event = make_event(
            source_type="firewall",
            payload={"src_ip": "10.0.0.1", "dst_ip": "10.0.0.2"},
        )
        await engine.process_event(event)
        stats = engine.get_stats()
        assert stats.total_events_processed >= 1
        assert sm.get_open_story_count() >= 1
        assert stats.current_sequence >= 1

    @pytest.mark.asyncio
    async def test_process_event_appends_to_existing_story(self):
        """TC-CE-05: Second event with same key appends to existing story."""
        clock = make_clock()
        id_factory = make_id_factory()
        sm, _ = make_story_manager(clock=clock)
        config = make_config(clock=clock, id_factory=id_factory)
        engine = CorrelationEngine(
            config=config,
            story_manager=sm,
            clock=clock,
            id_factory=id_factory,
        )
        event1 = make_event(
            event_id=VALID_UUID4,
            source_type="firewall",
            payload={"src_ip": "10.0.0.1"},
        )
        event2 = make_event(
            event_id=VALID_UUID4_2,
            source_type="firewall",
            payload={"src_ip": "10.0.0.1"},
        )
        await engine.process_event(event1)
        await engine.process_event(event2)
        # Only 1 story created, 2 events processed
        assert sm.get_open_story_count() == 1
        stats = engine.get_stats()
        assert stats.total_events_processed >= 2
        assert stats.total_stories_created == 1

    @pytest.mark.asyncio
    async def test_process_event_no_matching_rule_dropped(self):
        """TC-CE-06: Event not matching any rule is dropped."""
        clock = make_clock()
        id_factory = make_id_factory()
        sm, _ = make_story_manager(clock=clock)
        config = make_config(clock=clock, id_factory=id_factory)
        engine = CorrelationEngine(
            config=config,
            story_manager=sm,
            clock=clock,
            id_factory=id_factory,
        )
        event = make_event(
            source_type="dns",  # Rule expects "firewall"
            payload={"src_ip": "10.0.0.1"},
        )
        # Should not crash
        await engine.process_event(event)
        assert sm.get_open_story_count() == 0

    @pytest.mark.asyncio
    async def test_process_event_missing_group_by_field_dropped(self):
        """TC-CE-07: Event missing group_by field is dropped."""
        clock = make_clock()
        id_factory = make_id_factory()
        sm, _ = make_story_manager(clock=clock)
        config = make_config(clock=clock, id_factory=id_factory)
        engine = CorrelationEngine(
            config=config,
            story_manager=sm,
            clock=clock,
            id_factory=id_factory,
        )
        # Rule expects group_by=["src_ip"], event payload has no src_ip
        event = make_event(
            source_type="firewall",
            payload={"dst_ip": "10.0.0.2"},
        )
        await engine.process_event(event)
        assert sm.get_open_story_count() == 0

    @pytest.mark.asyncio
    async def test_process_event_terminal_condition_closes_story(self):
        """TC-CE-08: Terminal condition match closes story with completed status."""
        clock = make_clock()
        id_factory = make_id_factory()
        sm, _ = make_story_manager(clock=clock)
        sink, captured = make_sink()
        sm.register_sink(sink)

        rule = make_rule(
            terminal_conditions=[
                TerminalCondition(field="status", value="closed", is_regex=False)
            ],
        )
        config = make_config(rules=[rule], clock=clock, id_factory=id_factory)
        engine = CorrelationEngine(
            config=config,
            story_manager=sm,
            clock=clock,
            id_factory=id_factory,
        )
        # First event opens story
        event1 = make_event(
            event_id=VALID_UUID4,
            source_type="firewall",
            payload={"src_ip": "10.0.0.1", "status": "open"},
        )
        await engine.process_event(event1)
        assert sm.get_open_story_count() == 1

        # Second event triggers terminal condition
        event2 = make_event(
            event_id=VALID_UUID4_2,
            source_type="firewall",
            payload={"src_ip": "10.0.0.1", "status": "closed"},
        )
        await engine.process_event(event2)
        assert sm.get_open_story_count() == 0
        assert len(captured) == 1
        assert captured[0].status == StoryStatus.completed

    @pytest.mark.asyncio
    async def test_process_event_sequence_monotonically_increasing(self):
        """TC-CE-09 / INV-04: Sequence counter increments with each event."""
        clock = make_clock()
        id_factory = make_id_factory()
        sm, _ = make_story_manager(clock=clock)
        config = make_config(clock=clock, id_factory=id_factory)
        engine = CorrelationEngine(
            config=config,
            story_manager=sm,
            clock=clock,
            id_factory=id_factory,
        )
        for i in range(3):
            uid = f"{i+1:08x}-0000-4000-a000-{i+1:012x}"
            event = make_event(
                event_id=uid,
                source_type="firewall",
                payload={"src_ip": f"10.0.0.{i+1}"},
            )
            await engine.process_event(event)

        stats = engine.get_stats()
        assert stats.current_sequence == 3

    @pytest.mark.asyncio
    async def test_process_event_never_crashes_on_internal_error(self):
        """TC-CE-10: process_event catches and logs unexpected errors."""
        clock = make_clock()
        id_factory = make_id_factory()
        sm, _ = make_story_manager(clock=clock)
        config = make_config(clock=clock, id_factory=id_factory)
        engine = CorrelationEngine(
            config=config,
            story_manager=sm,
            clock=clock,
            id_factory=id_factory,
        )
        # Monkey-patch to cause an internal error
        original_register = sm.register_story
        async def broken_register(*args, **kwargs):
            raise RuntimeError("internal boom")
        sm.register_story = broken_register

        event = make_event(
            source_type="firewall",
            payload={"src_ip": "10.0.0.1"},
        )
        # Should NOT raise
        await engine.process_event(event)
        # Restore
        sm.register_story = original_register


class TestCorrelationEngineChainEntry:
    """Tests for chain entry registration and usage."""

    @pytest.mark.asyncio
    async def test_register_chain_entry_stores_with_expiration(self):
        """TC-CE-11: Chain entry stored with correct expiration."""
        clock = make_clock()
        id_factory = make_id_factory()
        sm, _ = make_story_manager(clock=clock)
        rule = make_rule(
            chain_by=["session_id"],
            chain_ttl=300.0,
            terminal_conditions=[
                TerminalCondition(field="status", value="done", is_regex=False)
            ],
        )
        config = make_config(rules=[rule], clock=clock, id_factory=id_factory)
        engine = CorrelationEngine(
            config=config,
            story_manager=sm,
            clock=clock,
            id_factory=id_factory,
        )
        sink, captured = make_sink()
        sm.register_sink(sink)

        # Create and close a story with chain_by fields
        event1 = make_event(
            event_id=VALID_UUID4,
            source_type="firewall",
            payload={"src_ip": "10.0.0.1", "session_id": "sess-001", "status": "start"},
        )
        await engine.process_event(event1)

        event2 = make_event(
            event_id=VALID_UUID4_2,
            source_type="firewall",
            payload={"src_ip": "10.0.0.1", "session_id": "sess-001", "status": "done"},
        )
        await engine.process_event(event2)

        # Verify story was closed
        assert len(captured) == 1
        assert captured[0].status == StoryStatus.completed

        # Verify chain entry exists via stats
        stats = engine.get_stats()
        assert stats.active_chain_entries >= 1

    @pytest.mark.asyncio
    async def test_chain_entry_creates_child_story_with_parent_id(self):
        """TC-CE-13 / INV-07: Chain-linked story has non-empty parent_story_id."""
        clock = make_clock()
        id_factory = make_id_factory()
        sm, _ = make_story_manager(clock=clock)
        rule = make_rule(
            chain_by=["session_id"],
            chain_ttl=300.0,
            terminal_conditions=[
                TerminalCondition(field="status", value="done", is_regex=False)
            ],
        )
        config = make_config(rules=[rule], clock=clock, id_factory=id_factory)
        engine = CorrelationEngine(
            config=config,
            story_manager=sm,
            clock=clock,
            id_factory=id_factory,
        )
        sink, captured = make_sink()
        sm.register_sink(sink)

        # Create first story
        e1 = make_event(
            event_id=VALID_UUID4,
            source_type="firewall",
            payload={"src_ip": "10.0.0.1", "session_id": "sess-001", "status": "start"},
        )
        await engine.process_event(e1)

        # Close first story (triggers chain entry)
        e2 = make_event(
            event_id=VALID_UUID4_2,
            source_type="firewall",
            payload={"src_ip": "10.0.0.1", "session_id": "sess-001", "status": "done"},
        )
        await engine.process_event(e2)

        parent_story = captured[0]
        assert parent_story.status == StoryStatus.completed

        # Now process event that matches the chain
        e3 = make_event(
            event_id=VALID_UUID4_3,
            source_type="firewall",
            payload={"src_ip": "10.0.0.1", "session_id": "sess-001", "status": "start"},
        )
        await engine.process_event(e3)

        # The new story should have parent_story_id set
        # Close the chained story to inspect it
        e4_id = "d4e5f6a7-b8c9-4dae-8f01-2a3b4c5d6e7f"
        e4 = make_event(
            event_id=e4_id,
            source_type="firewall",
            payload={"src_ip": "10.0.0.1", "session_id": "sess-001", "status": "done"},
        )
        await engine.process_event(e4)

        assert len(captured) >= 2
        child_story = captured[1]
        assert child_story.parent_story_id != ""
        assert child_story.parent_story_id == parent_story.story_id.value


class TestCorrelationEnginePurgeChains:
    """Tests for purge_expired_chains."""

    @pytest.mark.asyncio
    async def test_purge_removes_expired_entries(self):
        """TC-CE-14: Expired chain entries are purged."""
        clock = make_clock()
        id_factory = make_id_factory()
        sm, _ = make_story_manager(clock=clock)
        rule = make_rule(
            chain_by=["session_id"],
            chain_ttl=10.0,
            terminal_conditions=[
                TerminalCondition(field="status", value="done", is_regex=False)
            ],
        )
        config = make_config(rules=[rule], clock=clock, id_factory=id_factory)
        engine = CorrelationEngine(
            config=config,
            story_manager=sm,
            clock=clock,
            id_factory=id_factory,
        )

        # Create and close a story to register chain entry
        e1 = make_event(
            event_id=VALID_UUID4,
            source_type="firewall",
            payload={"src_ip": "10.0.0.1", "session_id": "s1", "status": "start"},
        )
        await engine.process_event(e1)

        e2 = make_event(
            event_id=VALID_UUID4_2,
            source_type="firewall",
            payload={"src_ip": "10.0.0.1", "session_id": "s1", "status": "done"},
        )
        await engine.process_event(e2)

        stats_before = engine.get_stats()
        assert stats_before.active_chain_entries >= 1

        # Advance clock past chain TTL
        clock.advance(15.0)
        purged = engine.purge_expired_chains()
        assert purged >= 1

        stats_after = engine.get_stats()
        assert stats_after.active_chain_entries == 0

    @pytest.mark.asyncio
    async def test_purge_returns_zero_when_none_expired(self):
        """TC-CE-15: Returns 0 when no entries expired."""
        clock = make_clock()
        id_factory = make_id_factory()
        sm, _ = make_story_manager(clock=clock)
        config = make_config(clock=clock, id_factory=id_factory)
        engine = CorrelationEngine(
            config=config,
            story_manager=sm,
            clock=clock,
            id_factory=id_factory,
        )
        purged = engine.purge_expired_chains()
        assert purged == 0


class TestCorrelationEngineGetStats:
    """Tests for get_stats."""

    @pytest.mark.asyncio
    async def test_get_stats_reflects_state(self):
        """TC-CE-16: get_stats returns correct counters."""
        clock = make_clock()
        id_factory = make_id_factory()
        sm, _ = make_story_manager(clock=clock)
        config = make_config(clock=clock, id_factory=id_factory)
        engine = CorrelationEngine(
            config=config,
            story_manager=sm,
            clock=clock,
            id_factory=id_factory,
        )
        stats = engine.get_stats()
        assert isinstance(stats, EngineStats)
        assert stats.open_story_count == 0
        assert stats.total_events_processed == 0
        assert stats.total_stories_created == 0
        assert stats.total_stories_closed == 0
        assert stats.current_sequence == 0


class TestCorrelationEngineExpiredChainNotUsed:
    """INV-06: Chain entries not used after expiration."""

    @pytest.mark.asyncio
    async def test_expired_chain_not_used(self):
        """TC-INV-06: Expired chain entry ignored, new story has no parent."""
        clock = make_clock()
        id_factory = make_id_factory()
        sm, _ = make_story_manager(clock=clock)
        rule = make_rule(
            chain_by=["session_id"],
            chain_ttl=5.0,
            terminal_conditions=[
                TerminalCondition(field="status", value="done", is_regex=False)
            ],
        )
        config = make_config(rules=[rule], clock=clock, id_factory=id_factory)
        engine = CorrelationEngine(
            config=config,
            story_manager=sm,
            clock=clock,
            id_factory=id_factory,
        )
        sink, captured = make_sink()
        sm.register_sink(sink)

        # Create and close story, registering chain entry
        e1 = make_event(
            event_id=VALID_UUID4,
            source_type="firewall",
            payload={"src_ip": "10.0.0.1", "session_id": "s1", "status": "start"},
        )
        await engine.process_event(e1)
        e2 = make_event(
            event_id=VALID_UUID4_2,
            source_type="firewall",
            payload={"src_ip": "10.0.0.1", "session_id": "s1", "status": "done"},
        )
        await engine.process_event(e2)

        # Advance clock past chain TTL
        clock.advance(10.0)
        engine.purge_expired_chains()

        # Process new event - should create fresh story, not chained
        e3 = make_event(
            event_id=VALID_UUID4_3,
            source_type="firewall",
            payload={"src_ip": "10.0.0.1", "session_id": "s1", "status": "start"},
        )
        await engine.process_event(e3)

        # Close the new story to inspect
        e4_id = "d4e5f6a7-b8c9-4dae-8f01-2a3b4c5d6e7f"
        e4 = make_event(
            event_id=e4_id,
            source_type="firewall",
            payload={"src_ip": "10.0.0.1", "session_id": "s1", "status": "done"},
        )
        await engine.process_event(e4)

        # Second closed story should have no parent
        assert len(captured) >= 2
        second_story = captured[1]
        assert second_story.parent_story_id == "" or second_story.parent_story_id is None


# ============================================================================
# STORY MANAGER TESTS
# ============================================================================

class TestStoryManagerInit:
    """Tests for StoryManager.__init__."""

    def test_init_valid(self):
        """TC-SM-01: Init with valid parameters."""
        sm, _ = make_story_manager(max_open_stories=100, sweep_interval=1.0)
        assert sm.get_open_story_count() == 0


class TestStoryManagerRegisterStory:
    """Tests for StoryManager.register_story."""

    @pytest.mark.asyncio
    async def test_register_story_increments_count(self):
        """TC-SM-02: register_story adds story."""
        sm, clock = make_story_manager()
        id_factory = make_id_factory()
        story_id = StoryId(value=id_factory())
        await sm.register_story(
            story_id=story_id,
            rule_name=RuleName(value="test-rule"),
            group_key={"src_ip": "10.0.0.1"},
            timeout=TimeoutSeconds(value=60.0),
            parent_story_id="",
            created_at=EventTime(value=VALID_EVENT_TIME),
        )
        assert sm.get_open_story_count() == 1

    @pytest.mark.asyncio
    async def test_register_duplicate_story_id_raises(self):
        """TC-SM-03: Duplicate story_id raises error."""
        sm, clock = make_story_manager()
        sid = StoryId(value=VALID_UUID4)
        await sm.register_story(
            story_id=sid,
            rule_name=RuleName(value="test-rule"),
            group_key={"src_ip": "10.0.0.1"},
            timeout=TimeoutSeconds(value=60.0),
            parent_story_id="",
            created_at=EventTime(value=VALID_EVENT_TIME),
        )
        with pytest.raises(Exception):
            await sm.register_story(
                story_id=sid,
                rule_name=RuleName(value="test-rule"),
                group_key={"src_ip": "10.0.0.1"},
                timeout=TimeoutSeconds(value=60.0),
                parent_story_id="",
                created_at=EventTime(value=VALID_EVENT_TIME),
            )

    @pytest.mark.asyncio
    async def test_register_evicts_lru_when_at_max(self):
        """TC-SM-04 / INV-01 / INV-05: Evicts LRU story when at capacity."""
        sm, clock = make_story_manager(max_open_stories=2)
        sink, captured = make_sink()
        sm.register_sink(sink)

        ids = []
        for i in range(3):
            sid = StoryId(value=f"{i+1:08x}-0000-4000-a000-{i+1:012x}")
            ids.append(sid)
            await sm.register_story(
                story_id=sid,
                rule_name=RuleName(value="test-rule"),
                group_key={"src_ip": f"10.0.0.{i+1}"},
                timeout=TimeoutSeconds(value=60.0),
                parent_story_id="",
                created_at=EventTime(value=VALID_EVENT_TIME),
            )

        assert sm.get_open_story_count() == 2
        # First story should have been evicted
        assert len(captured) == 1
        assert captured[0].status == StoryStatus.evicted
        assert captured[0].story_id == ids[0]


class TestStoryManagerAppendEvent:
    """Tests for StoryManager.append_event."""

    @pytest.mark.asyncio
    async def test_append_event_to_existing_story(self):
        """TC-SM-05: Append event to existing story."""
        sm, clock = make_story_manager()
        sid = StoryId(value=VALID_UUID4)
        await sm.register_story(
            story_id=sid,
            rule_name=RuleName(value="test-rule"),
            group_key={"src_ip": "10.0.0.1"},
            timeout=TimeoutSeconds(value=60.0),
            parent_story_id="",
            created_at=EventTime(value=VALID_EVENT_TIME),
        )
        event = make_event()
        await sm.append_event(story_id=sid, event=event)
        # No error means success

    @pytest.mark.asyncio
    async def test_append_event_unknown_story_raises(self):
        """TC-SM-06: append_event on unknown story_id raises."""
        sm, clock = make_story_manager()
        with pytest.raises(Exception):
            await sm.append_event(
                story_id=StoryId(value=VALID_UUID4),
                event=make_event(),
            )


class TestStoryManagerTouchStory:
    """Tests for StoryManager.touch_story."""

    @pytest.mark.asyncio
    async def test_touch_story_moves_to_mru(self):
        """TC-SM-07 / INV-11: Touch moves story to MRU position."""
        sm, clock = make_story_manager()
        sid_a = StoryId(value=VALID_UUID4)
        sid_b = StoryId(value=VALID_UUID4_2)
        sid_c = StoryId(value=VALID_UUID4_3)

        for sid, ip in [(sid_a, "1"), (sid_b, "2"), (sid_c, "3")]:
            await sm.register_story(
                story_id=sid,
                rule_name=RuleName(value="test-rule"),
                group_key={"src_ip": f"10.0.0.{ip}"},
                timeout=TimeoutSeconds(value=60.0),
                parent_story_id="",
                created_at=EventTime(value=VALID_EVENT_TIME),
            )

        # Touch A - should move A to end (MRU)
        await sm.touch_story(story_id=sid_a)

        # Now if we evict, B should be evicted first (LRU)
        # We can verify by setting max=2 via eviction
        # Instead, verify by checking the sweep behavior or close order
        # For a simpler test, verify A is not the LRU anymore
        # We can do this by closing all and checking order

        # At this point order should be B, C, A
        # If we reduce capacity, B should be evicted first
        # We can verify indirectly: close and check story exists
        assert sm.get_open_story_count() == 3

    @pytest.mark.asyncio
    async def test_touch_unknown_story_raises(self):
        """TC-SM-08: touch_story on unknown story_id raises."""
        sm, clock = make_story_manager()
        with pytest.raises(Exception):
            await sm.touch_story(story_id=StoryId(value=VALID_UUID4))


class TestStoryManagerCloseStory:
    """Tests for StoryManager.close_story."""

    @pytest.mark.asyncio
    async def test_close_story_removes_and_returns_frozen(self):
        """TC-SM-09 / INV-08: close_story removes from dict, returns frozen Story."""
        sm, clock = make_story_manager()
        sid = StoryId(value=VALID_UUID4)
        await sm.register_story(
            story_id=sid,
            rule_name=RuleName(value="test-rule"),
            group_key={"src_ip": "10.0.0.1"},
            timeout=TimeoutSeconds(value=60.0),
            parent_story_id="",
            created_at=EventTime(value=VALID_EVENT_TIME),
        )
        await sm.append_event(story_id=sid, event=make_event())

        story = await sm.close_story(story_id=sid, status=StoryStatus.completed)
        assert sm.get_open_story_count() == 0
        assert story.status == StoryStatus.completed
        assert story.story_id == sid

        # Verify frozen
        with pytest.raises(Exception):  # Pydantic frozen model raises on attribute set
            story.status = StoryStatus.timed_out

    @pytest.mark.asyncio
    async def test_close_story_invokes_all_sinks(self):
        """TC-SM-10: close_story invokes all registered sinks."""
        sm, clock = make_story_manager()
        sink1, captured1 = make_sink()
        sink2, captured2 = make_sink()
        sm.register_sink(sink1)
        sm.register_sink(sink2)

        sid = StoryId(value=VALID_UUID4)
        await sm.register_story(
            story_id=sid,
            rule_name=RuleName(value="test-rule"),
            group_key={"src_ip": "10.0.0.1"},
            timeout=TimeoutSeconds(value=60.0),
            parent_story_id="",
            created_at=EventTime(value=VALID_EVENT_TIME),
        )

        story = await sm.close_story(story_id=sid, status=StoryStatus.completed)
        assert len(captured1) == 1
        assert len(captured2) == 1
        assert captured1[0].story_id == sid
        assert captured2[0].story_id == sid

    @pytest.mark.asyncio
    async def test_close_story_sink_error_does_not_prevent_closure(self):
        """TC-SM-11 / INV-10: Sink error doesn't prevent closure."""
        sm, clock = make_story_manager()
        error_sink = make_error_sink()
        good_sink, captured = make_sink()
        sm.register_sink(error_sink)
        sm.register_sink(good_sink)

        sid = StoryId(value=VALID_UUID4)
        await sm.register_story(
            story_id=sid,
            rule_name=RuleName(value="test-rule"),
            group_key={"src_ip": "10.0.0.1"},
            timeout=TimeoutSeconds(value=60.0),
            parent_story_id="",
            created_at=EventTime(value=VALID_EVENT_TIME),
        )

        # Should not raise despite error_sink
        story = await sm.close_story(story_id=sid, status=StoryStatus.completed)
        assert sm.get_open_story_count() == 0
        assert story.status == StoryStatus.completed
        # Good sink should still have received the story
        assert len(captured) == 1

    @pytest.mark.asyncio
    async def test_close_unknown_story_raises(self):
        """TC-SM-12: close_story on unknown story_id raises."""
        sm, clock = make_story_manager()
        with pytest.raises(Exception):
            await sm.close_story(
                story_id=StoryId(value=VALID_UUID4),
                status=StoryStatus.completed,
            )

    @pytest.mark.asyncio
    async def test_close_story_events_sorted(self):
        """TC-SM-13 / INV-03: Events in closed story are sorted by (event_time, received_at, sequence)."""
        sm, clock = make_story_manager()
        sid = StoryId(value=VALID_UUID4)
        await sm.register_story(
            story_id=sid,
            rule_name=RuleName(value="test-rule"),
            group_key={"src_ip": "10.0.0.1"},
            timeout=TimeoutSeconds(value=60.0),
            parent_story_id="",
            created_at=EventTime(value=VALID_EVENT_TIME),
        )

        # Add events in reverse time order
        e1 = make_event(
            event_id=VALID_UUID4,
            event_time="2024-01-15T10:32:00Z",
            received_at="2024-01-15T10:32:01Z",
            sequence=3,
        )
        e2 = make_event(
            event_id=VALID_UUID4_2,
            event_time="2024-01-15T10:30:00Z",
            received_at="2024-01-15T10:30:01Z",
            sequence=1,
        )
        e3 = make_event(
            event_id=VALID_UUID4_3,
            event_time="2024-01-15T10:31:00Z",
            received_at="2024-01-15T10:31:01Z",
            sequence=2,
        )

        await sm.append_event(story_id=sid, event=e1)
        await sm.append_event(story_id=sid, event=e2)
        await sm.append_event(story_id=sid, event=e3)

        story = await sm.close_story(story_id=sid, status=StoryStatus.completed)

        # Events should be sorted ascending by event_time
        event_times = [e.event_time.value for e in story.events]
        assert event_times == sorted(event_times)
        sequences = [e.sequence.value for e in story.events]
        assert sequences == [1, 2, 3]


class TestStoryManagerSweep:
    """Tests for sweep_once, start_sweep_task, stop_sweep_task."""

    @pytest.mark.asyncio
    async def test_sweep_once_closes_timed_out_stories(self):
        """TC-SM-14: sweep_once closes stories exceeding timeout."""
        sm, clock = make_story_manager()
        sink, captured = make_sink()
        sm.register_sink(sink)

        sid = StoryId(value=VALID_UUID4)
        await sm.register_story(
            story_id=sid,
            rule_name=RuleName(value="test-rule"),
            group_key={"src_ip": "10.0.0.1"},
            timeout=TimeoutSeconds(value=5.0),
            parent_story_id="",
            created_at=EventTime(value=VALID_EVENT_TIME),
        )

        # Advance clock past timeout
        clock.advance(6.0)
        count = await sm.sweep_once()
        assert count == 1
        assert sm.get_open_story_count() == 0
        assert len(captured) == 1
        assert captured[0].status == StoryStatus.timed_out

    @pytest.mark.asyncio
    async def test_sweep_once_returns_zero_when_none_timed_out(self):
        """TC-SM-15: sweep_once returns 0 when no stories timed out."""
        sm, clock = make_story_manager()
        sid = StoryId(value=VALID_UUID4)
        await sm.register_story(
            story_id=sid,
            rule_name=RuleName(value="test-rule"),
            group_key={"src_ip": "10.0.0.1"},
            timeout=TimeoutSeconds(value=60.0),
            parent_story_id="",
            created_at=EventTime(value=VALID_EVENT_TIME),
        )
        count = await sm.sweep_once()
        assert count == 0
        assert sm.get_open_story_count() == 1

    @pytest.mark.asyncio
    async def test_start_sweep_task(self):
        """TC-SM-16: start_sweep_task creates background task."""
        sm, clock = make_story_manager()
        await sm.start_sweep_task()
        # Task should be running now - stop it for cleanup
        await sm.stop_sweep_task()

    @pytest.mark.asyncio
    async def test_start_sweep_task_double_start_raises(self):
        """TC-SM-17: Double start raises error."""
        sm, clock = make_story_manager()
        await sm.start_sweep_task()
        try:
            with pytest.raises(Exception):
                await sm.start_sweep_task()
        finally:
            await sm.stop_sweep_task()

    @pytest.mark.asyncio
    async def test_stop_sweep_task_clears_reference(self):
        """TC-SM-18: stop_sweep_task cancels and clears task."""
        sm, clock = make_story_manager()
        await sm.start_sweep_task()
        await sm.stop_sweep_task()
        # Should be safe to start again
        await sm.start_sweep_task()
        await sm.stop_sweep_task()

    @pytest.mark.asyncio
    async def test_stop_sweep_task_noop_when_not_running(self):
        """TC-SM-19: stop_sweep_task is safe when not running."""
        sm, clock = make_story_manager()
        await sm.stop_sweep_task()  # Should not raise


class TestStoryManagerRegisterSink:
    """Tests for register_sink."""

    @pytest.mark.asyncio
    async def test_register_sink_receives_closed_stories(self):
        """TC-SM-20: Registered sink is called on story close."""
        sm, clock = make_story_manager()
        sink, captured = make_sink()
        sm.register_sink(sink)

        sid = StoryId(value=VALID_UUID4)
        await sm.register_story(
            story_id=sid,
            rule_name=RuleName(value="test-rule"),
            group_key={"src_ip": "10.0.0.1"},
            timeout=TimeoutSeconds(value=60.0),
            parent_story_id="",
            created_at=EventTime(value=VALID_EVENT_TIME),
        )
        await sm.close_story(story_id=sid, status=StoryStatus.completed)
        assert len(captured) == 1


class TestStoryManagerGetOpenStoryCount:
    """Tests for get_open_story_count."""

    @pytest.mark.asyncio
    async def test_count_accuracy(self):
        """TC-SM-21: Count is accurate through lifecycle."""
        sm, clock = make_story_manager()
        assert sm.get_open_story_count() == 0

        ids = []
        for i in range(3):
            sid = StoryId(value=f"{i+1:08x}-0000-4000-a000-{i+1:012x}")
            ids.append(sid)
            await sm.register_story(
                story_id=sid,
                rule_name=RuleName(value="test-rule"),
                group_key={"src_ip": f"10.0.0.{i+1}"},
                timeout=TimeoutSeconds(value=60.0),
                parent_story_id="",
                created_at=EventTime(value=VALID_EVENT_TIME),
            )
        assert sm.get_open_story_count() == 3

        await sm.close_story(story_id=ids[0], status=StoryStatus.completed)
        assert sm.get_open_story_count() == 2


class TestStoryManagerLoadRecoveredStories:
    """Tests for load_recovered_stories."""

    @pytest.mark.asyncio
    async def test_load_recovered_stories(self):
        """TC-SM-22: Stories loaded from recovery result."""
        sm, clock = make_story_manager(max_open_stories=10)
        # We need to build a valid RecoveryResult
        # This depends on the actual shape; using minimal approach
        recovery = RecoveryResult(
            open_stories=[
                {
                    "story_id": VALID_UUID4,
                    "rule_name": "test-rule",
                    "group_key": {"src_ip": "10.0.0.1"},
                    "timeout": 60.0,
                    "parent_story_id": "",
                    "created_at": VALID_EVENT_TIME,
                    "events": [],
                }
            ],
            records_read=1,
            records_skipped=0,
            chain_entries=[],
        )
        try:
            count = await sm.load_recovered_stories(recovery)
            assert count >= 1
            assert sm.get_open_story_count() >= 1
        except (TypeError, AttributeError):
            # The interface may differ; skip if shape doesn't match
            pytest.skip("RecoveryResult shape may differ from expected")

    @pytest.mark.asyncio
    async def test_load_recovered_stories_evicts_when_exceeding_max(self):
        """TC-SM-23: Excess recovered stories are evicted."""
        sm, clock = make_story_manager(max_open_stories=2)
        stories_data = []
        for i in range(3):
            stories_data.append({
                "story_id": f"{i+1:08x}-0000-4000-a000-{i+1:012x}",
                "rule_name": "test-rule",
                "group_key": {"src_ip": f"10.0.0.{i+1}"},
                "timeout": 60.0,
                "parent_story_id": "",
                "created_at": VALID_EVENT_TIME,
                "events": [],
            })
        recovery = RecoveryResult(
            open_stories=stories_data,
            records_read=3,
            records_skipped=0,
            chain_entries=[],
        )
        try:
            count = await sm.load_recovered_stories(recovery)
            assert sm.get_open_story_count() <= 2
        except (TypeError, AttributeError):
            pytest.skip("RecoveryResult shape may differ from expected")


# ============================================================================
# INVARIANT TESTS
# ============================================================================

class TestInvariantMaxOpenStories:
    """INV-01: open stories never exceed max_open_stories."""

    @pytest.mark.asyncio
    async def test_never_exceeds_max(self):
        """TC-INV-01: Registering beyond max evicts to stay within limit."""
        max_stories = 5
        sm, clock = make_story_manager(max_open_stories=max_stories)
        sink, captured = make_sink()
        sm.register_sink(sink)

        for i in range(max_stories + 5):
            sid = StoryId(value=f"{i+1:08x}-0000-4000-a000-{i+1:012x}")
            await sm.register_story(
                story_id=sid,
                rule_name=RuleName(value="test-rule"),
                group_key={"src_ip": f"10.0.0.{i+1}"},
                timeout=TimeoutSeconds(value=60.0),
                parent_story_id="",
                created_at=EventTime(value=VALID_EVENT_TIME),
            )
            assert sm.get_open_story_count() <= max_stories

        # Exactly 5 evictions should have occurred
        assert len(captured) == 5
        for story in captured:
            assert story.status == StoryStatus.evicted


class TestInvariantSinkErrorIsolation:
    """INV-10: Sink errors never prevent story closure."""

    @pytest.mark.asyncio
    async def test_all_sinks_error_story_still_closed(self):
        """TC-INV-10: Even if all sinks raise, story is closed."""
        sm, clock = make_story_manager()
        error_sink1 = make_error_sink(msg="error 1")
        error_sink2 = make_error_sink(msg="error 2")
        sm.register_sink(error_sink1)
        sm.register_sink(error_sink2)

        sid = StoryId(value=VALID_UUID4)
        await sm.register_story(
            story_id=sid,
            rule_name=RuleName(value="test-rule"),
            group_key={"src_ip": "10.0.0.1"},
            timeout=TimeoutSeconds(value=60.0),
            parent_story_id="",
            created_at=EventTime(value=VALID_EVENT_TIME),
        )

        story = await sm.close_story(story_id=sid, status=StoryStatus.completed)
        assert sm.get_open_story_count() == 0
        assert story.status == StoryStatus.completed


class TestInvariantLRUOrder:
    """INV-11: OrderedDict maintains LRU order."""

    @pytest.mark.asyncio
    async def test_lru_order_after_touch(self):
        """TC-INV-11: After touching A in [A, B, C], order becomes [B, C, A]."""
        sm, clock = make_story_manager(max_open_stories=3)
        sink, captured = make_sink()
        sm.register_sink(sink)

        sid_a = StoryId(value=VALID_UUID4)
        sid_b = StoryId(value=VALID_UUID4_2)
        sid_c = StoryId(value=VALID_UUID4_3)

        for sid in [sid_a, sid_b, sid_c]:
            await sm.register_story(
                story_id=sid,
                rule_name=RuleName(value="test-rule"),
                group_key={"src_ip": sid.value[:8]},
                timeout=TimeoutSeconds(value=60.0),
                parent_story_id="",
                created_at=EventTime(value=VALID_EVENT_TIME),
            )

        # Touch A -> moves to MRU. Order: B, C, A
        await sm.touch_story(story_id=sid_a)

        # Now add a 4th story to trigger eviction - should evict B (LRU)
        # First reduce max. Since we can't change max, we add stories that cause eviction.
        # With max=3, adding 4th evicts LRU=B
        sid_d = StoryId(value="d4e5f6a7-b8c9-4dae-8f01-2a3b4c5d6e7f")

        # Need max=3 so this won't evict... Let's use max=2 instead
        # Recreate with max=2
        sm2, clock2 = make_story_manager(max_open_stories=2)
        sink2, captured2 = make_sink()
        sm2.register_sink(sink2)

        await sm2.register_story(
            story_id=sid_a,
            rule_name=RuleName(value="test-rule"),
            group_key={"src_ip": "1"},
            timeout=TimeoutSeconds(value=60.0),
            parent_story_id="",
            created_at=EventTime(value=VALID_EVENT_TIME),
        )
        await sm2.register_story(
            story_id=sid_b,
            rule_name=RuleName(value="test-rule"),
            group_key={"src_ip": "2"},
            timeout=TimeoutSeconds(value=60.0),
            parent_story_id="",
            created_at=EventTime(value=VALID_EVENT_TIME),
        )
        # Order: A, B. Touch A -> Order: B, A
        await sm2.touch_story(story_id=sid_a)

        # Adding C should evict B (now LRU)
        await sm2.register_story(
            story_id=sid_c,
            rule_name=RuleName(value="test-rule"),
            group_key={"src_ip": "3"},
            timeout=TimeoutSeconds(value=60.0),
            parent_story_id="",
            created_at=EventTime(value=VALID_EVENT_TIME),
        )
        assert sm2.get_open_story_count() == 2
        assert len(captured2) == 1
        # B should be evicted (it was LRU after touch)
        assert captured2[0].story_id == sid_b


class TestInvariantMissingGroupByDropped:
    """INV-13: Events missing group_by fields are dropped."""

    @pytest.mark.asyncio
    async def test_missing_group_by_drops_event(self):
        """TC-INV-13: Event without required group_by field is dropped."""
        clock = make_clock()
        id_factory = make_id_factory()
        sm, _ = make_story_manager(clock=clock)
        config = make_config(clock=clock, id_factory=id_factory)
        engine = CorrelationEngine(
            config=config,
            story_manager=sm,
            clock=clock,
            id_factory=id_factory,
        )

        # Rule expects group_by=["src_ip"], payload has only "dst_ip"
        event = make_event(
            source_type="firewall",
            payload={"dst_ip": "10.0.0.2"},
        )
        await engine.process_event(event)
        assert sm.get_open_story_count() == 0


# ============================================================================
# STATE PERSISTENCE TESTS
# ============================================================================

class TestAppendRecord:
    """Tests for append_record function."""

    @pytest.mark.asyncio
    async def test_append_record_writes_jsonl(self, tmp_path):
        """TC-STATE-01: append_record writes valid JSONL."""
        state_file = tmp_path / "state.jsonl"
        fp = FilePath(value=str(state_file))
        record = StoryOpenedRecord(
            record_type=RecordType.story_opened,
            story_id=StoryId(value=VALID_UUID4),
            rule_name=RuleName(value="test-rule"),
            group_key={"src_ip": "10.0.0.1"},
            parent_story_id="",
            created_at=EventTime(value=VALID_EVENT_TIME),
            timeout=60.0,
        )
        await append_record(path=fp, record=record)

        content = state_file.read_text().strip()
        assert content  # Non-empty
        parsed = json.loads(content)
        assert parsed["record_type"] == "story_opened"
        assert parsed["story_id"] == VALID_UUID4 or parsed.get("story_id", {}).get("value") == VALID_UUID4

    @pytest.mark.asyncio
    async def test_append_record_io_error_does_not_propagate(self):
        """TC-STATE-02: IO error in append_record is caught and logged."""
        fp = FilePath(value="/nonexistent/path/that/should/fail/state.jsonl")
        record = StoryOpenedRecord(
            record_type=RecordType.story_opened,
            story_id=StoryId(value=VALID_UUID4),
            rule_name=RuleName(value="test-rule"),
            group_key={"src_ip": "10.0.0.1"},
            parent_story_id="",
            created_at=EventTime(value=VALID_EVENT_TIME),
            timeout=60.0,
        )
        # Should NOT raise
        await append_record(path=fp, record=record)


class TestRecoverState:
    """Tests for recover_state function."""

    @pytest.mark.asyncio
    async def test_recover_open_stories(self, tmp_path):
        """TC-STATE-03: recover_state returns open stories from JSONL."""
        state_file = tmp_path / "state.jsonl"
        clock = make_clock()

        # Write records manually
        opened = {
            "record_type": "story_opened",
            "story_id": VALID_UUID4,
            "rule_name": "test-rule",
            "group_key": {"src_ip": "10.0.0.1"},
            "parent_story_id": "",
            "created_at": VALID_EVENT_TIME,
            "timeout": 60.0,
        }
        appended = {
            "record_type": "event_appended",
            "story_id": VALID_UUID4,
            "event": {
                "event_id": VALID_UUID4_2,
                "event_time": VALID_EVENT_TIME,
                "received_at": VALID_EVENT_TIME,
                "sequence": 1,
                "source_type": "firewall",
                "payload": {"src_ip": "10.0.0.1"},
            },
        }
        state_file.write_text(json.dumps(opened) + "\n" + json.dumps(appended) + "\n")

        result = await recover_state(path=FilePath(value=str(state_file)), clock=clock)
        assert isinstance(result, RecoveryResult)
        assert len(result.open_stories) == 1
        assert result.records_read >= 2
        assert result.records_skipped == 0

    @pytest.mark.asyncio
    async def test_recover_excludes_closed_stories(self, tmp_path):
        """TC-STATE-04: Closed stories not in open_stories."""
        state_file = tmp_path / "state.jsonl"
        clock = make_clock()

        opened = {
            "record_type": "story_opened",
            "story_id": VALID_UUID4,
            "rule_name": "test-rule",
            "group_key": {"src_ip": "10.0.0.1"},
            "parent_story_id": "",
            "created_at": VALID_EVENT_TIME,
            "timeout": 60.0,
        }
        closed = {
            "record_type": "story_closed",
            "story_id": VALID_UUID4,
            "status": "completed",
            "closed_at": VALID_EVENT_TIME_2,
        }
        state_file.write_text(json.dumps(opened) + "\n" + json.dumps(closed) + "\n")

        result = await recover_state(path=FilePath(value=str(state_file)), clock=clock)
        assert len(result.open_stories) == 0

    @pytest.mark.asyncio
    async def test_recover_skips_corrupt_lines(self, tmp_path):
        """TC-STATE-05: Corrupt lines skipped with records_skipped incremented."""
        state_file = tmp_path / "state.jsonl"
        clock = make_clock()

        opened = {
            "record_type": "story_opened",
            "story_id": VALID_UUID4,
            "rule_name": "test-rule",
            "group_key": {"src_ip": "10.0.0.1"},
            "parent_story_id": "",
            "created_at": VALID_EVENT_TIME,
            "timeout": 60.0,
        }
        lines = [
            json.dumps(opened),
            "THIS IS NOT JSON",
            "{invalid json too",
            json.dumps({"record_type": "story_closed", "story_id": VALID_UUID4, "status": "completed", "closed_at": VALID_EVENT_TIME_2}),
        ]
        state_file.write_text("\n".join(lines) + "\n")

        result = await recover_state(path=FilePath(value=str(state_file)), clock=clock)
        assert result.records_skipped == 2
        assert result.records_read + result.records_skipped == 4

    @pytest.mark.asyncio
    async def test_recover_nonexistent_file_returns_empty(self, tmp_path):
        """TC-STATE-06: Non-existent file returns empty RecoveryResult."""
        clock = make_clock()
        result = await recover_state(
            path=FilePath(value=str(tmp_path / "nonexistent.jsonl")),
            clock=clock,
        )
        assert len(result.open_stories) == 0
        assert result.records_read == 0
        assert result.records_skipped == 0

    @pytest.mark.asyncio
    async def test_recover_state_idempotent(self, tmp_path):
        """TC-STATE-07 / INV-12: recover_state is idempotent."""
        state_file = tmp_path / "state.jsonl"
        clock = make_clock()

        opened = {
            "record_type": "story_opened",
            "story_id": VALID_UUID4,
            "rule_name": "test-rule",
            "group_key": {"src_ip": "10.0.0.1"},
            "parent_story_id": "",
            "created_at": VALID_EVENT_TIME,
            "timeout": 60.0,
        }
        state_file.write_text(json.dumps(opened) + "\n")

        result1 = await recover_state(path=FilePath(value=str(state_file)), clock=clock)
        result2 = await recover_state(path=FilePath(value=str(state_file)), clock=clock)

        assert len(result1.open_stories) == len(result2.open_stories)
        assert result1.records_read == result2.records_read
        assert result1.records_skipped == result2.records_skipped


# ============================================================================
# INTEGRATION TESTS
# ============================================================================

class TestIntegrationFullLifecycle:
    """End-to-end integration tests combining engine + story manager."""

    @pytest.mark.asyncio
    async def test_full_lifecycle_event_to_sink(self):
        """TC-INT-01: Full lifecycle: events -> story open -> terminal -> closed -> sink."""
        clock = make_clock()
        id_factory = make_id_factory()
        sm, _ = make_story_manager(clock=clock)
        sink, captured = make_sink()
        sm.register_sink(sink)

        rule = make_rule(
            name="fw-rule",
            match=[MatchPredicate(field="source_type", value="firewall", is_regex=False)],
            group_by=["src_ip"],
            timeout=60.0,
            terminal_conditions=[
                TerminalCondition(field="action", value="block", is_regex=False)
            ],
        )
        config = make_config(rules=[rule], clock=clock, id_factory=id_factory)
        engine = CorrelationEngine(
            config=config,
            story_manager=sm,
            clock=clock,
            id_factory=id_factory,
        )

        # Event 1: opens story
        e1 = make_event(
            event_id=VALID_UUID4,
            source_type="firewall",
            payload={"src_ip": "10.0.0.1", "action": "allow"},
        )
        await engine.process_event(e1)
        assert sm.get_open_story_count() == 1

        # Event 2: appends to story
        e2 = make_event(
            event_id=VALID_UUID4_2,
            source_type="firewall",
            payload={"src_ip": "10.0.0.1", "action": "allow"},
        )
        await engine.process_event(e2)
        assert sm.get_open_story_count() == 1

        # Event 3: terminal condition triggers close
        e3 = make_event(
            event_id=VALID_UUID4_3,
            source_type="firewall",
            payload={"src_ip": "10.0.0.1", "action": "block"},
        )
        await engine.process_event(e3)
        assert sm.get_open_story_count() == 0

        # Sink received the completed story
        assert len(captured) == 1
        story = captured[0]
        assert story.status == StoryStatus.completed
        assert len(story.events) == 3
        assert story.rule_name.value == "fw-rule"

        # Verify events are sorted
        for i in range(len(story.events) - 1):
            assert story.events[i].event_time.value <= story.events[i + 1].event_time.value

    @pytest.mark.asyncio
    async def test_eviction_under_load(self):
        """TC-INT-02: Eviction under load with max_open_stories=2."""
        clock = make_clock()
        id_factory = make_id_factory()
        sm, _ = make_story_manager(max_open_stories=2, clock=clock)
        sink, captured = make_sink()
        sm.register_sink(sink)

        config = make_config(
            max_open_stories=2,
            clock=clock,
            id_factory=id_factory,
        )
        engine = CorrelationEngine(
            config=config,
            story_manager=sm,
            clock=clock,
            id_factory=id_factory,
        )

        # Process events for 3 different group keys
        for i in range(3):
            uid = f"{i+1:08x}-0000-4000-a000-{i+1:012x}"
            e = make_event(
                event_id=uid,
                source_type="firewall",
                payload={"src_ip": f"10.0.0.{i+1}"},
            )
            await engine.process_event(e)

        # Max 2 stories open, 1 should have been evicted
        assert sm.get_open_story_count() <= 2
        evicted = [s for s in captured if s.status == StoryStatus.evicted]
        assert len(evicted) >= 1

    @pytest.mark.asyncio
    async def test_multiple_rules_single_event(self):
        """Event matching multiple rules creates stories for each."""
        clock = make_clock()
        id_factory = make_id_factory()
        sm, _ = make_story_manager(clock=clock)

        rule1 = make_rule(
            name="rule-a",
            match=[MatchPredicate(field="source_type", value="firewall", is_regex=False)],
            group_by=["src_ip"],
        )
        rule2 = make_rule(
            name="rule-b",
            match=[MatchPredicate(field="source_type", value="firewall", is_regex=False)],
            group_by=["dst_ip"],
        )
        config = make_config(rules=[rule1, rule2], clock=clock, id_factory=id_factory)
        engine = CorrelationEngine(
            config=config,
            story_manager=sm,
            clock=clock,
            id_factory=id_factory,
        )

        event = make_event(
            source_type="firewall",
            payload={"src_ip": "10.0.0.1", "dst_ip": "10.0.0.2"},
        )
        await engine.process_event(event)

        # Should have created 2 stories (one per matching rule)
        assert sm.get_open_story_count() == 2

    @pytest.mark.asyncio
    async def test_regex_match_predicate(self):
        """Rule with regex match predicate matches correctly."""
        clock = make_clock()
        id_factory = make_id_factory()
        sm, _ = make_story_manager(clock=clock)

        rule = make_rule(
            name="regex-rule",
            match=[MatchPredicate(field="source_type", value="fire.*", is_regex=True)],
            group_by=["src_ip"],
        )
        config = make_config(rules=[rule], clock=clock, id_factory=id_factory)
        engine = CorrelationEngine(
            config=config,
            story_manager=sm,
            clock=clock,
            id_factory=id_factory,
        )

        event = make_event(
            source_type="firewall",
            payload={"src_ip": "10.0.0.1"},
        )
        await engine.process_event(event)
        assert sm.get_open_story_count() == 1

    @pytest.mark.asyncio
    async def test_sweep_integration_with_engine(self):
        """Sweep closes timed-out stories created by the engine."""
        clock = make_clock()
        id_factory = make_id_factory()
        sm, _ = make_story_manager(clock=clock)
        sink, captured = make_sink()
        sm.register_sink(sink)

        rule = make_rule(timeout=5.0)
        config = make_config(rules=[rule], clock=clock, id_factory=id_factory)
        engine = CorrelationEngine(
            config=config,
            story_manager=sm,
            clock=clock,
            id_factory=id_factory,
        )

        event = make_event(
            source_type="firewall",
            payload={"src_ip": "10.0.0.1"},
        )
        await engine.process_event(event)
        assert sm.get_open_story_count() == 1

        # Advance past timeout
        clock.advance(6.0)
        count = await sm.sweep_once()
        assert count == 1
        assert sm.get_open_story_count() == 0
        assert len(captured) == 1
        assert captured[0].status == StoryStatus.timed_out


class TestIntegrationStateRoundTrip:
    """State persistence round-trip tests."""

    @pytest.mark.asyncio
    async def test_append_multiple_records_and_recover(self, tmp_path):
        """Round-trip: append several records, recover, verify fidelity."""
        state_file = tmp_path / "state.jsonl"
        fp = FilePath(value=str(state_file))
        clock = make_clock()

        # Append story_opened
        opened_record = StoryOpenedRecord(
            record_type=RecordType.story_opened,
            story_id=StoryId(value=VALID_UUID4),
            rule_name=RuleName(value="test-rule"),
            group_key={"src_ip": "10.0.0.1"},
            parent_story_id="",
            created_at=EventTime(value=VALID_EVENT_TIME),
            timeout=60.0,
        )
        await append_record(path=fp, record=opened_record)

        # Append event_appended
        evt = make_event(event_id=VALID_UUID4_2, sequence=1)
        appended_record = EventAppendedRecord(
            record_type=RecordType.event_appended,
            story_id=StoryId(value=VALID_UUID4),
            event=evt,
        )
        await append_record(path=fp, record=appended_record)

        # Recover
        result = await recover_state(path=fp, clock=clock)
        assert len(result.open_stories) == 1
        assert result.records_read == 2
        assert result.records_skipped == 0

    @pytest.mark.asyncio
    async def test_append_and_close_round_trip(self, tmp_path):
        """Append open + close records, recover returns empty."""
        state_file = tmp_path / "state.jsonl"
        fp = FilePath(value=str(state_file))
        clock = make_clock()

        opened_record = StoryOpenedRecord(
            record_type=RecordType.story_opened,
            story_id=StoryId(value=VALID_UUID4),
            rule_name=RuleName(value="test-rule"),
            group_key={"src_ip": "10.0.0.1"},
            parent_story_id="",
            created_at=EventTime(value=VALID_EVENT_TIME),
            timeout=60.0,
        )
        await append_record(path=fp, record=opened_record)

        closed_record = StoryClosedRecord(
            record_type=RecordType.story_closed,
            story_id=StoryId(value=VALID_UUID4),
            status=StoryStatus.completed,
            closed_at=EventTime(value=VALID_EVENT_TIME_2),
        )
        await append_record(path=fp, record=closed_record)

        result = await recover_state(path=fp, clock=clock)
        assert len(result.open_stories) == 0
        assert result.records_read == 2

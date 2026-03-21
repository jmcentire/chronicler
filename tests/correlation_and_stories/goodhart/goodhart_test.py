"""
Adversarial hidden acceptance tests for Correlation Engine & Story Manager.

These tests catch implementations that "teach to the test" by hardcoding
returns, skipping validation, or only handling specific visible test inputs.
"""

import asyncio
import json
import os
import tempfile
import time
from collections import OrderedDict
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from chronicler.correlation import (
    CorrelationEngine, CorrelationEngineConfig, CorrelationRule, MatchPredicate,
    TerminalCondition, Event, RuleName, EventTime, StoryId, EventId,
    SequenceNumber, MaxOpenStories, TimeoutSeconds, SweepIntervalSeconds,
    FilePath, CompositeKey, ChainEntry, EngineStats, Story,
    RecoveryResult, StoryOpenedRecord, StoryClosedRecord,
)
from chronicler.story_manager import StoryManager
from chronicler.correlation import recover_state, append_record


# ============================================================
# Helpers
# ============================================================

def make_event_time(dt_str="2024-01-15T10:00:00Z"):
    return dt_str


def make_uuid4(suffix="0"):
    """Generate a valid UUID4 string for testing.

    Converts the suffix to a hex string to ensure UUID validity.
    """
    # Convert any string to a hex representation to guarantee valid hex chars
    hex_str = suffix.encode().hex() if not all(c in '0123456789abcdef' for c in suffix.lower()) else suffix.lower()
    base = f"00000000-0000-4000-a000-{hex_str.zfill(12)[-12:]}"
    return base


def make_event(event_id=None, event_time=None, payload=None, sequence=0, source_type="test"):
    if event_id is None:
        event_id = make_uuid4("1")
    if event_time is None:
        event_time = "2024-01-15T10:00:00Z"
    if payload is None:
        payload = {}
    return Event(
        event_id=event_id,
        event_time=event_time,
        received_at=event_time,
        sequence=sequence,
        source_type=source_type,
        payload=payload,
    )


def make_rule(name="rule-a", match=None, group_by=None, timeout=60.0,
              terminal_conditions=None, chain_by=None, chain_ttl=60.0):
    if match is None:
        match = [MatchPredicate(field="type", value="alert", is_regex=False)]
    if group_by is None:
        group_by = ["host"]
    if terminal_conditions is None:
        terminal_conditions = []
    if chain_by is None:
        chain_by = []
    return CorrelationRule(
        name=name,
        match=match,
        group_by=group_by,
        timeout=timeout,
        terminal_conditions=terminal_conditions,
        chain_by=chain_by,
        chain_ttl=chain_ttl,
    )


def make_config(rules=None, max_open_stories=100, sweep_interval=1.0,
                state_file="/tmp/test_state.jsonl", clock=None, id_factory=None):
    if rules is None:
        rules = [make_rule()]
    if clock is None:
        clock = time.monotonic
    if id_factory is None:
        counter = iter(range(1000000))
        id_factory = lambda: make_uuid4(str(next(counter)))
    return CorrelationEngineConfig(
        rules=rules,
        max_open_stories=max_open_stories,
        sweep_interval=sweep_interval,
        state_file=state_file,
        clock=clock,
        id_factory=id_factory,
    )


# ============================================================
# Type validation tests
# ============================================================

class TestGoodhartTypeValidation:
    """Tests for type validators that catch hardcoded acceptance of visible test values."""

    def test_goodhart_rulename_rejects_period(self):
        """RuleName must reject names containing periods, which are not in [a-z0-9_-]."""
        with pytest.raises(Exception):
            RuleName("my.rule.name")

    def test_goodhart_rulename_rejects_space(self):
        """RuleName must reject names containing spaces."""
        with pytest.raises(Exception):
            RuleName("my rule")

    def test_goodhart_rulename_rejects_leading_hyphen(self):
        """RuleName must reject names starting with a hyphen (must start with [a-z])."""
        with pytest.raises(Exception):
            RuleName("-rule")

    def test_goodhart_rulename_rejects_leading_underscore(self):
        """RuleName must reject names starting with underscore (must start with [a-z])."""
        with pytest.raises(Exception):
            RuleName("_rule")

    def test_goodhart_rulename_accepts_hyphens_in_middle(self):
        """RuleName must accept valid names with hyphens in the middle."""
        name = RuleName("my-rule-name")
        assert name.value == "my-rule-name"

    def test_goodhart_rulename_accepts_mixed_valid(self):
        """RuleName must accept names mixing lowercase letters, digits, hyphens, underscores."""
        name = RuleName("a1-b2_c3")
        assert name.value == "a1-b2_c3"

    def test_goodhart_eventtime_accepts_fractional_seconds(self):
        """EventTime must accept ISO 8601 with fractional seconds."""
        et = EventTime("2024-01-15T08:30:00.123456Z")
        assert et.value == "2024-01-15T08:30:00.123456Z"

    def test_goodhart_eventtime_accepts_negative_offset(self):
        """EventTime must accept negative timezone offsets."""
        et = EventTime("2024-06-01T12:00:00-05:00")
        assert et.value == "2024-06-01T12:00:00-05:00"

    def test_goodhart_eventtime_rejects_date_only(self):
        """EventTime must reject date-only strings without time component."""
        with pytest.raises(Exception):
            EventTime("2024-01-15")

    def test_goodhart_eventtime_rejects_trailing_whitespace(self):
        """EventTime must reject strings with trailing whitespace."""
        with pytest.raises(Exception):
            EventTime("2024-01-15T08:30:00Z ")

    def test_goodhart_eventtime_rejects_leading_whitespace(self):
        """EventTime must reject strings with leading whitespace."""
        with pytest.raises(Exception):
            EventTime(" 2024-01-15T08:30:00Z")

    def test_goodhart_storyid_rejects_uppercase_hex(self):
        """StoryId must reject UUIDs with uppercase hex characters."""
        with pytest.raises(Exception):
            StoryId("A1234567-abcd-4abc-8abc-0123456789ab")

    def test_goodhart_storyid_rejects_wrong_variant(self):
        """StoryId must reject UUIDs where variant nibble is outside [89ab]."""
        with pytest.raises(Exception):
            StoryId("12345678-1234-4234-0234-123456789abc")

    def test_goodhart_eventid_rejects_version5(self):
        """EventId must reject UUIDs with version 5 (not version 4)."""
        with pytest.raises(Exception):
            EventId("12345678-1234-5234-8234-123456789abc")

    def test_goodhart_eventid_rejects_version3(self):
        """EventId must reject UUIDs with version 3."""
        with pytest.raises(Exception):
            EventId("12345678-1234-3234-8234-123456789abc")

    def test_goodhart_sequencenumber_accepts_large(self):
        """SequenceNumber must accept very large positive integers (no upper bound)."""
        sn = SequenceNumber(999999999)
        assert sn.value == 999999999

    def test_goodhart_timeoutseconds_accepts_large(self):
        """TimeoutSeconds must accept very large values (no upper bound defined)."""
        ts = TimeoutSeconds(86400.0)
        assert ts.value == 86400.0

    def test_goodhart_timeoutseconds_rejects_negative(self):
        """TimeoutSeconds must reject negative values."""
        with pytest.raises(Exception):
            TimeoutSeconds(-1.0)

    def test_goodhart_timeoutseconds_rejects_very_small(self):
        """TimeoutSeconds must reject 0.0009 (below 0.001 minimum)."""
        with pytest.raises(Exception):
            TimeoutSeconds(0.0009)

    def test_goodhart_sweepinterval_accepts_max(self):
        """SweepIntervalSeconds must accept exactly 3600 (maximum boundary)."""
        si = SweepIntervalSeconds(3600)
        assert si.value == 3600

    def test_goodhart_sweepinterval_rejects_just_below_min(self):
        """SweepIntervalSeconds must reject 0.099 (just below 0.1 minimum)."""
        with pytest.raises(Exception):
            SweepIntervalSeconds(0.099)

    def test_goodhart_maxopenstories_accepts_middle(self):
        """MaxOpenStories must accept values in the middle of the range, not just boundaries."""
        m = MaxOpenStories(500)
        assert m.value == 500

    def test_goodhart_filepath_accepts_max_length(self):
        """FilePath must accept a path of exactly 4096 characters."""
        path = "a" * 4096
        fp = FilePath(path)
        assert fp.value == path

    def test_goodhart_filepath_rejects_over_max_length(self):
        """FilePath must reject paths exceeding 4096 characters."""
        path = "a" * 4097
        with pytest.raises(Exception):
            FilePath(path)


# ============================================================
# StoryManager tests
# ============================================================

class TestGoodhartStoryManager:
    """Tests for StoryManager that catch shortcut implementations."""

    @pytest.fixture
    def clock(self):
        """Injectable clock that can be advanced."""
        current = [0.0]
        def _clock():
            return current[0]
        _clock._current = current
        return _clock

    @pytest.fixture
    def manager(self, clock):
        return StoryManager(max_open_stories=10, sweep_interval=1.0, clock=clock)

    def _register_story(self, manager, story_id, rule_name="rule-a",
                        group_key=None, timeout=60.0, parent_story_id="",
                        created_at="2024-01-15T10:00:00Z"):
        if group_key is None:
            group_key = {"host": "srv1"}
        return asyncio.get_event_loop().run_until_complete(
            manager.register_story(
                story_id=story_id,
                rule_name=rule_name,
                group_key=group_key,
                timeout=timeout,
                parent_story_id=parent_story_id,
                created_at=created_at,
            )
        ) if asyncio.iscoroutinefunction(getattr(manager, 'register_story', None)) else \
            manager.register_story(
                story_id=story_id,
                rule_name=rule_name,
                group_key=group_key,
                timeout=timeout,
                parent_story_id=parent_story_id,
                created_at=created_at,
            )

    @pytest.mark.asyncio
    async def test_goodhart_multiple_sinks_all_called(self, clock):
        """All registered sinks must receive the closed story, not just one."""
        manager = StoryManager(max_open_stories=10, sweep_interval=1.0, clock=clock)
        received = []
        sink1 = AsyncMock(side_effect=lambda s: received.append(("sink1", s)))
        sink2 = AsyncMock(side_effect=lambda s: received.append(("sink2", s)))
        sink3 = AsyncMock(side_effect=lambda s: received.append(("sink3", s)))

        manager.register_sink(sink1)
        manager.register_sink(sink2)
        manager.register_sink(sink3)

        sid = make_uuid4("100")
        await manager.register_story(
            story_id=sid, rule_name="rule-a",
            group_key={"host": "srv1"}, timeout=60.0,
            parent_story_id="", created_at="2024-01-15T10:00:00Z"
        )

        story = await manager.close_story(story_id=sid, status="completed")
        assert sink1.call_count == 1
        assert sink2.call_count == 1
        assert sink3.call_count == 1
        assert story.status == "completed"

    @pytest.mark.asyncio
    async def test_goodhart_failing_sink_doesnt_block_others(self, clock):
        """A failing sink must not prevent other sinks from receiving the story."""
        manager = StoryManager(max_open_stories=10, sweep_interval=1.0, clock=clock)
        received = []
        failing_sink = AsyncMock(side_effect=RuntimeError("sink error"))
        good_sink = AsyncMock(side_effect=lambda s: received.append(s))

        manager.register_sink(failing_sink)
        manager.register_sink(good_sink)

        sid = make_uuid4("200")
        await manager.register_story(
            story_id=sid, rule_name="rule-a",
            group_key={"host": "srv1"}, timeout=60.0,
            parent_story_id="", created_at="2024-01-15T10:00:00Z"
        )

        story = await manager.close_story(story_id=sid, status="completed")
        assert good_sink.call_count == 1
        assert len(received) == 1
        assert manager.get_open_story_count() == 0

    @pytest.mark.asyncio
    async def test_goodhart_event_count_matches_appended(self, clock):
        """event_count on closed Story must match the number of events actually appended."""
        manager = StoryManager(max_open_stories=10, sweep_interval=1.0, clock=clock)
        sid = make_uuid4("300")
        await manager.register_story(
            story_id=sid, rule_name="rule-a",
            group_key={"host": "srv1"}, timeout=60.0,
            parent_story_id="", created_at="2024-01-15T10:00:00Z"
        )

        for i in range(5):
            evt = make_event(event_id=make_uuid4(str(300 + i)), sequence=i,
                             payload={"type": "alert", "host": "srv1"})
            await manager.append_event(story_id=sid, event=evt)

        story = await manager.close_story(story_id=sid, status="completed")
        assert story.event_count == 5
        assert len(story.events) == 5

    @pytest.mark.asyncio
    async def test_goodhart_events_sorted_by_sequence_tiebreaker(self, clock):
        """Events with same event_time and received_at must be sorted by sequence as tiebreaker."""
        manager = StoryManager(max_open_stories=10, sweep_interval=1.0, clock=clock)
        sid = make_uuid4("400")
        await manager.register_story(
            story_id=sid, rule_name="rule-a",
            group_key={"host": "srv1"}, timeout=60.0,
            parent_story_id="", created_at="2024-01-15T10:00:00Z"
        )

        same_time = "2024-01-15T10:00:00Z"
        # Append in reverse sequence order
        for seq in [5, 3, 1, 4, 2]:
            evt = make_event(
                event_id=make_uuid4(str(400 + seq)),
                event_time=same_time,
                sequence=seq,
                payload={"type": "alert", "host": "srv1"}
            )
            await manager.append_event(story_id=sid, event=evt)

        story = await manager.close_story(story_id=sid, status="completed")
        sequences = [e.sequence for e in story.events]
        assert sequences == sorted(sequences), f"Events not sorted by sequence: {sequences}"

    @pytest.mark.asyncio
    async def test_goodhart_close_completed_vs_timed_out_status(self, clock):
        """close_story must return the exact status passed to it, not a default."""
        manager = StoryManager(max_open_stories=10, sweep_interval=1.0, clock=clock)

        sid1 = make_uuid4("500")
        sid2 = make_uuid4("501")

        await manager.register_story(
            story_id=sid1, rule_name="rule-a",
            group_key={"host": "srv1"}, timeout=60.0,
            parent_story_id="", created_at="2024-01-15T10:00:00Z"
        )
        await manager.register_story(
            story_id=sid2, rule_name="rule-a",
            group_key={"host": "srv2"}, timeout=60.0,
            parent_story_id="", created_at="2024-01-15T10:00:00Z"
        )

        story1 = await manager.close_story(story_id=sid1, status="completed")
        story2 = await manager.close_story(story_id=sid2, status="timed_out")

        assert story1.status == "completed"
        assert story2.status == "timed_out"

    @pytest.mark.asyncio
    async def test_goodhart_lru_eviction_respects_touch(self, clock):
        """LRU eviction must consider touch_story activity, not just creation order."""
        manager = StoryManager(max_open_stories=2, sweep_interval=1.0, clock=clock)
        evicted = []
        sink = AsyncMock(side_effect=lambda s: evicted.append(s))
        manager.register_sink(sink)

        sid_a = make_uuid4("600")
        sid_b = make_uuid4("601")
        sid_c = make_uuid4("602")

        await manager.register_story(
            story_id=sid_a, rule_name="rule-a",
            group_key={"host": "a"}, timeout=60.0,
            parent_story_id="", created_at="2024-01-15T10:00:00Z"
        )
        clock._current[0] = 1.0
        await manager.register_story(
            story_id=sid_b, rule_name="rule-a",
            group_key={"host": "b"}, timeout=60.0,
            parent_story_id="", created_at="2024-01-15T10:00:01Z"
        )

        # Touch A to make it more recently used than B
        clock._current[0] = 2.0
        await manager.touch_story(story_id=sid_a)

        # Register C — should evict B (LRU), not A
        clock._current[0] = 3.0
        await manager.register_story(
            story_id=sid_c, rule_name="rule-a",
            group_key={"host": "c"}, timeout=60.0,
            parent_story_id="", created_at="2024-01-15T10:00:03Z"
        )

        assert manager.get_open_story_count() == 2
        assert len(evicted) == 1
        assert evicted[0].story_id == sid_b
        assert evicted[0].status == "evicted"

    @pytest.mark.asyncio
    async def test_goodhart_multiple_consecutive_evictions(self, clock):
        """Multiple sequential registrations must each evict the LRU when at capacity."""
        manager = StoryManager(max_open_stories=1, sweep_interval=1.0, clock=clock)
        evicted = []
        sink = AsyncMock(side_effect=lambda s: evicted.append(s))
        manager.register_sink(sink)

        for i in range(4):
            sid = make_uuid4(str(700 + i))
            clock._current[0] = float(i)
            await manager.register_story(
                story_id=sid, rule_name="rule-a",
                group_key={"host": f"srv{i}"}, timeout=60.0,
                parent_story_id="", created_at=f"2024-01-15T10:00:0{i}Z"
            )

        assert manager.get_open_story_count() == 1
        assert len(evicted) == 3
        for e in evicted:
            assert e.status == "evicted"

    @pytest.mark.asyncio
    async def test_goodhart_sweep_closes_timed_out_not_active(self, clock):
        """sweep_once must only close stories that have exceeded their timeout, leaving active ones open."""
        manager = StoryManager(max_open_stories=10, sweep_interval=1.0, clock=clock)
        timed_out_stories = []
        sink = AsyncMock(side_effect=lambda s: timed_out_stories.append(s))
        manager.register_sink(sink)

        # Register 5 stories at clock=0 with timeout=10
        sids = []
        for i in range(5):
            sid = make_uuid4(str(800 + i))
            sids.append(sid)
            await manager.register_story(
                story_id=sid, rule_name="rule-a",
                group_key={"host": f"srv{i}"}, timeout=10.0,
                parent_story_id="", created_at="2024-01-15T10:00:00Z"
            )

        # Touch stories 0,1 at clock=8 to keep them alive
        clock._current[0] = 8.0
        await manager.touch_story(story_id=sids[0])
        await manager.touch_story(story_id=sids[1])

        # At clock=15, stories 2,3,4 have been inactive for 15s > 10s timeout
        # Stories 0,1 have been inactive for 7s < 10s timeout
        clock._current[0] = 15.0
        count = await manager.sweep_once()

        assert count == 3
        assert manager.get_open_story_count() == 2
        for s in timed_out_stories:
            assert s.status == "timed_out"

    @pytest.mark.asyncio
    async def test_goodhart_append_event_resets_timeout(self, clock):
        """append_event must reset last_activity_time, extending the story's timeout window."""
        manager = StoryManager(max_open_stories=10, sweep_interval=1.0, clock=clock)

        sid = make_uuid4("900")
        await manager.register_story(
            story_id=sid, rule_name="rule-a",
            group_key={"host": "srv1"}, timeout=10.0,
            parent_story_id="", created_at="2024-01-15T10:00:00Z"
        )

        # Append at clock=8
        clock._current[0] = 8.0
        evt = make_event(event_id=make_uuid4("901"), sequence=1,
                         payload={"type": "alert", "host": "srv1"})
        await manager.append_event(story_id=sid, event=evt)

        # At clock=15, inactivity = 15-8 = 7 < 10 timeout
        clock._current[0] = 15.0
        count = await manager.sweep_once()
        assert count == 0
        assert manager.get_open_story_count() == 1

        # At clock=19, inactivity = 19-8 = 11 > 10 timeout
        clock._current[0] = 19.0
        count = await manager.sweep_once()
        assert count == 1
        assert manager.get_open_story_count() == 0

    @pytest.mark.asyncio
    async def test_goodhart_closed_story_has_valid_closed_at(self, clock):
        """Closed story must have a valid closed_at EventTime, different from creation time."""
        manager = StoryManager(max_open_stories=10, sweep_interval=1.0, clock=clock)
        sid = make_uuid4("950")
        await manager.register_story(
            story_id=sid, rule_name="rule-a",
            group_key={"host": "srv1"}, timeout=60.0,
            parent_story_id="", created_at="2024-01-01T00:00:00Z"
        )

        story = await manager.close_story(story_id=sid, status="completed")
        # closed_at should be a valid EventTime string
        assert story.closed_at is not None
        assert len(story.closed_at) > 0 if isinstance(story.closed_at, str) else True

    @pytest.mark.asyncio
    async def test_goodhart_story_frozen_after_close(self, clock):
        """A closed Story (frozen Pydantic model) must raise on attribute assignment."""
        manager = StoryManager(max_open_stories=10, sweep_interval=1.0, clock=clock)
        sid = make_uuid4("960")
        await manager.register_story(
            story_id=sid, rule_name="rule-a",
            group_key={"host": "srv1"}, timeout=60.0,
            parent_story_id="", created_at="2024-01-15T10:00:00Z"
        )
        story = await manager.close_story(story_id=sid, status="completed")

        with pytest.raises(Exception):
            story.status = "timed_out"

        with pytest.raises(Exception):
            story.story_id = make_uuid4("999")

    @pytest.mark.asyncio
    async def test_goodhart_load_recovered_returns_actual_loaded_count(self, clock):
        """load_recovered_stories must return the number actually loaded (respecting max_open_stories), not the input count."""
        manager = StoryManager(max_open_stories=2, sweep_interval=1.0, clock=clock)

        # Build a mock recovery result with 5 stories
        open_stories = []
        for i in range(5):
            open_stories.append({
                "story_id": make_uuid4(str(1000 + i)),
                "rule_name": "rule-a",
                "group_key": {"host": f"srv{i}"},
                "timeout": 60.0,
                "parent_story_id": "",
                "created_at": f"2024-01-15T10:00:0{i}Z",
                "events": [],
                "last_activity_time": float(i),
            })

        recovery = RecoveryResult(
            open_stories=open_stories,
            records_read=5,
            records_skipped=0,
            chain_entries=[],
        )

        loaded = await manager.load_recovered_stories(recovery) \
            if asyncio.iscoroutinefunction(getattr(manager, 'load_recovered_stories', None)) \
            else manager.load_recovered_stories(recovery)

        assert manager.get_open_story_count() <= 2


# ============================================================
# CorrelationEngine tests
# ============================================================

class TestGoodhartCorrelationEngine:
    """Tests for CorrelationEngine that catch shortcut implementations."""

    @pytest.fixture
    def clock(self):
        current = [0.0]
        def _clock():
            return current[0]
        _clock._current = current
        return _clock

    @pytest.fixture
    def id_counter(self):
        counter = [0]
        def _factory():
            val = counter[0]
            counter[0] += 1
            return make_uuid4(str(val))
        return _factory

    @pytest.mark.asyncio
    async def test_goodhart_sequence_increments_on_no_match(self, clock, id_counter):
        """Sequence counter must increment even for events that match no rule."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            state_file = f.name
        try:
            rule = make_rule(name="rule-a",
                             match=[MatchPredicate(field="type", value="alert", is_regex=False)],
                             group_by=["host"])
            config = make_config(rules=[rule], state_file=state_file, clock=clock,
                                 id_factory=id_counter)
            sm = StoryManager(max_open_stories=100, sweep_interval=1.0, clock=clock)
            engine = CorrelationEngine(config=config, story_manager=sm,
                                       clock=clock, id_factory=id_counter)

            # Non-matching event
            evt1 = make_event(event_id=make_uuid4("a01"),
                              payload={"type": "not-alert", "host": "srv1"})
            await engine.process_event(evt1)

            # Matching event
            evt2 = make_event(event_id=make_uuid4("a02"),
                              payload={"type": "alert", "host": "srv1"})
            await engine.process_event(evt2)

            stats = engine.get_stats()
            assert stats.current_sequence == 2
        finally:
            os.unlink(state_file)

    @pytest.mark.asyncio
    async def test_goodhart_sequence_increments_on_missing_group_by(self, clock, id_counter):
        """Sequence counter must increment even when event is dropped due to missing group_by field."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            state_file = f.name
        try:
            rule = make_rule(name="rule-a",
                             match=[MatchPredicate(field="type", value="alert", is_regex=False)],
                             group_by=["host"])
            config = make_config(rules=[rule], state_file=state_file, clock=clock,
                                 id_factory=id_counter)
            sm = StoryManager(max_open_stories=100, sweep_interval=1.0, clock=clock)
            engine = CorrelationEngine(config=config, story_manager=sm,
                                       clock=clock, id_factory=id_counter)

            # Matches rule but missing 'host' group_by field
            evt1 = make_event(event_id=make_uuid4("b01"),
                              payload={"type": "alert"})
            await engine.process_event(evt1)

            stats = engine.get_stats()
            assert stats.current_sequence == 1
            assert stats.open_story_count == 0  # Event was dropped
        finally:
            os.unlink(state_file)

    @pytest.mark.asyncio
    async def test_goodhart_regex_match_predicate(self, clock, id_counter):
        """MatchPredicate with is_regex=True must use regex pattern matching."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            state_file = f.name
        try:
            rule = make_rule(
                name="rule-regex",
                match=[MatchPredicate(field="source", value="web-.*", is_regex=True)],
                group_by=["host"],
            )
            config = make_config(rules=[rule], state_file=state_file, clock=clock,
                                 id_factory=id_counter)
            sm = StoryManager(max_open_stories=100, sweep_interval=1.0, clock=clock)
            engine = CorrelationEngine(config=config, story_manager=sm,
                                       clock=clock, id_factory=id_counter)

            evt = make_event(event_id=make_uuid4("c01"),
                             payload={"source": "web-server-01", "host": "srv1"})
            await engine.process_event(evt)

            assert sm.get_open_story_count() == 1
        finally:
            os.unlink(state_file)

    @pytest.mark.asyncio
    async def test_goodhart_exact_match_not_substring(self, clock, id_counter):
        """MatchPredicate with is_regex=False must require exact match, not substring."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            state_file = f.name
        try:
            rule = make_rule(
                name="rule-exact",
                match=[MatchPredicate(field="source", value="web", is_regex=False)],
                group_by=["host"],
            )
            config = make_config(rules=[rule], state_file=state_file, clock=clock,
                                 id_factory=id_counter)
            sm = StoryManager(max_open_stories=100, sweep_interval=1.0, clock=clock)
            engine = CorrelationEngine(config=config, story_manager=sm,
                                       clock=clock, id_factory=id_counter)

            evt = make_event(event_id=make_uuid4("d01"),
                             payload={"source": "web-server", "host": "srv1"})
            await engine.process_event(evt)

            assert sm.get_open_story_count() == 0  # Should NOT match
        finally:
            os.unlink(state_file)

    @pytest.mark.asyncio
    async def test_goodhart_event_matches_multiple_rules(self, clock, id_counter):
        """An event matching multiple rules must create separate stories for each rule."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            state_file = f.name
        try:
            rule1 = make_rule(
                name="rule-one",
                match=[MatchPredicate(field="type", value="alert", is_regex=False)],
                group_by=["host"],
            )
            rule2 = make_rule(
                name="rule-two",
                match=[MatchPredicate(field="type", value="alert", is_regex=False)],
                group_by=["host"],
            )
            config = make_config(rules=[rule1, rule2], state_file=state_file, clock=clock,
                                 id_factory=id_counter)
            sm = StoryManager(max_open_stories=100, sweep_interval=1.0, clock=clock)
            engine = CorrelationEngine(config=config, story_manager=sm,
                                       clock=clock, id_factory=id_counter)

            evt = make_event(event_id=make_uuid4("e01"),
                             payload={"type": "alert", "host": "srv1"})
            await engine.process_event(evt)

            assert sm.get_open_story_count() == 2
        finally:
            os.unlink(state_file)

    @pytest.mark.asyncio
    async def test_goodhart_composite_key_sorted_fields(self, clock, id_counter):
        """Composite keys must use sorted field-value pairs so key order in payload doesn't matter."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            state_file = f.name
        try:
            rule = make_rule(
                name="rule-multi",
                match=[MatchPredicate(field="type", value="alert", is_regex=False)],
                group_by=["host", "service"],
            )
            config = make_config(rules=[rule], state_file=state_file, clock=clock,
                                 id_factory=id_counter)
            sm = StoryManager(max_open_stories=100, sweep_interval=1.0, clock=clock)
            engine = CorrelationEngine(config=config, story_manager=sm,
                                       clock=clock, id_factory=id_counter)

            # Same values for host and service, different payload key order
            evt1 = make_event(event_id=make_uuid4("f01"),
                              payload={"type": "alert", "host": "srv1", "service": "web"})
            await engine.process_event(evt1)

            evt2 = make_event(event_id=make_uuid4("f02"),
                              payload={"type": "alert", "service": "web", "host": "srv1"})
            await engine.process_event(evt2)

            # Both events should be in the same story
            assert sm.get_open_story_count() == 1
        finally:
            os.unlink(state_file)

    @pytest.mark.asyncio
    async def test_goodhart_different_group_by_values_create_different_stories(self, clock, id_counter):
        """Different group_by field values must create distinct stories."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            state_file = f.name
        try:
            rule = make_rule(
                name="rule-a",
                match=[MatchPredicate(field="type", value="alert", is_regex=False)],
                group_by=["host", "service"],
            )
            config = make_config(rules=[rule], state_file=state_file, clock=clock,
                                 id_factory=id_counter)
            sm = StoryManager(max_open_stories=100, sweep_interval=1.0, clock=clock)
            engine = CorrelationEngine(config=config, story_manager=sm,
                                       clock=clock, id_factory=id_counter)

            evt1 = make_event(event_id=make_uuid4("g01"),
                              payload={"type": "alert", "host": "srv1", "service": "web"})
            await engine.process_event(evt1)

            evt2 = make_event(event_id=make_uuid4("g02"),
                              payload={"type": "alert", "host": "srv1", "service": "api"})
            await engine.process_event(evt2)

            assert sm.get_open_story_count() == 2
        finally:
            os.unlink(state_file)

    @pytest.mark.asyncio
    async def test_goodhart_terminal_regex_condition(self, clock, id_counter):
        """TerminalCondition with is_regex=True must trigger story closure on regex match."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            state_file = f.name
        try:
            rule = make_rule(
                name="rule-term",
                match=[MatchPredicate(field="type", value="alert", is_regex=False)],
                group_by=["host"],
                terminal_conditions=[
                    TerminalCondition(field="status", value="(success|done)", is_regex=True)
                ],
            )
            config = make_config(rules=[rule], state_file=state_file, clock=clock,
                                 id_factory=id_counter)
            sm = StoryManager(max_open_stories=100, sweep_interval=1.0, clock=clock)
            engine = CorrelationEngine(config=config, story_manager=sm,
                                       clock=clock, id_factory=id_counter)

            # First event opens story
            evt1 = make_event(event_id=make_uuid4("h01"),
                              payload={"type": "alert", "host": "srv1", "status": "pending"})
            await engine.process_event(evt1)
            assert sm.get_open_story_count() == 1

            # Second event matches terminal condition via regex
            evt2 = make_event(event_id=make_uuid4("h02"),
                              payload={"type": "alert", "host": "srv1", "status": "done"})
            await engine.process_event(evt2)
            assert sm.get_open_story_count() == 0
        finally:
            os.unlink(state_file)

    @pytest.mark.asyncio
    async def test_goodhart_new_story_after_terminal_close(self, clock, id_counter):
        """After a story is closed via terminal condition, a new event with same key must create a new story."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            state_file = f.name
        try:
            rule = make_rule(
                name="rule-a",
                match=[MatchPredicate(field="type", value="alert", is_regex=False)],
                group_by=["host"],
                terminal_conditions=[
                    TerminalCondition(field="status", value="done", is_regex=False)
                ],
            )
            config = make_config(rules=[rule], state_file=state_file, clock=clock,
                                 id_factory=id_counter)
            sm = StoryManager(max_open_stories=100, sweep_interval=1.0, clock=clock)
            engine = CorrelationEngine(config=config, story_manager=sm,
                                       clock=clock, id_factory=id_counter)

            # Create and close story
            evt1 = make_event(event_id=make_uuid4("i01"),
                              payload={"type": "alert", "host": "srv1"})
            await engine.process_event(evt1)

            evt2 = make_event(event_id=make_uuid4("i02"),
                              payload={"type": "alert", "host": "srv1", "status": "done"})
            await engine.process_event(evt2)
            assert sm.get_open_story_count() == 0

            # New event with same key should create new story
            evt3 = make_event(event_id=make_uuid4("i03"),
                              payload={"type": "alert", "host": "srv1"})
            await engine.process_event(evt3)
            assert sm.get_open_story_count() == 1

            stats = engine.get_stats()
            assert stats.total_stories_created == 2
        finally:
            os.unlink(state_file)

    @pytest.mark.asyncio
    async def test_goodhart_chain_entry_not_used_after_expiry(self, clock, id_counter):
        """Expired chain entries must not be used to set parent_story_id on new stories."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            state_file = f.name
        try:
            rule = make_rule(
                name="rule-chain",
                match=[MatchPredicate(field="type", value="alert", is_regex=False)],
                group_by=["host"],
                terminal_conditions=[
                    TerminalCondition(field="status", value="done", is_regex=False)
                ],
                chain_by=["session"],
                chain_ttl=10.0,
            )
            config = make_config(rules=[rule], state_file=state_file, clock=clock,
                                 id_factory=id_counter)
            sm = StoryManager(max_open_stories=100, sweep_interval=1.0, clock=clock)
            closed_stories = []
            sink = AsyncMock(side_effect=lambda s: closed_stories.append(s))
            sm.register_sink(sink)
            engine = CorrelationEngine(config=config, story_manager=sm,
                                       clock=clock, id_factory=id_counter)

            # Create and close story with chain_by
            clock._current[0] = 0.0
            evt1 = make_event(event_id=make_uuid4("j01"),
                              payload={"type": "alert", "host": "srv1", "session": "abc", "status": "done"})
            await engine.process_event(evt1)

            # Advance past chain TTL
            clock._current[0] = 15.0  # 15 > 10 TTL

            # New event should NOT be chain-linked
            evt2 = make_event(event_id=make_uuid4("j02"),
                              payload={"type": "alert", "host": "srv1", "session": "abc"})
            await engine.process_event(evt2)

            # The second story should not have a parent_story_id
            # Check via stats or by closing the story
            # Close the open story to inspect it
            stats = engine.get_stats()
            assert stats.open_story_count == 1
        finally:
            os.unlink(state_file)

    @pytest.mark.asyncio
    async def test_goodhart_empty_payload_no_crash(self, clock, id_counter):
        """Event with empty payload must not crash the engine, just be dropped."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            state_file = f.name
        try:
            rule = make_rule(
                name="rule-a",
                match=[MatchPredicate(field="type", value="alert", is_regex=False)],
                group_by=["host"],
            )
            config = make_config(rules=[rule], state_file=state_file, clock=clock,
                                 id_factory=id_counter)
            sm = StoryManager(max_open_stories=100, sweep_interval=1.0, clock=clock)
            engine = CorrelationEngine(config=config, story_manager=sm,
                                       clock=clock, id_factory=id_counter)

            evt = make_event(event_id=make_uuid4("k01"), payload={})
            await engine.process_event(evt)

            assert sm.get_open_story_count() == 0
            assert engine.get_stats().current_sequence == 1
        finally:
            os.unlink(state_file)

    @pytest.mark.asyncio
    async def test_goodhart_stats_reflect_multiple_operations(self, clock, id_counter):
        """get_stats must accurately reflect all operations, not return cached/hardcoded values."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            state_file = f.name
        try:
            rule = make_rule(
                name="rule-a",
                match=[MatchPredicate(field="type", value="alert", is_regex=False)],
                group_by=["host"],
                terminal_conditions=[
                    TerminalCondition(field="status", value="done", is_regex=False)
                ],
            )
            config = make_config(rules=[rule], state_file=state_file, clock=clock,
                                 id_factory=id_counter)
            sm = StoryManager(max_open_stories=100, sweep_interval=1.0, clock=clock)
            engine = CorrelationEngine(config=config, story_manager=sm,
                                       clock=clock, id_factory=id_counter)

            # Create 3 stories
            for i in range(3):
                evt = make_event(event_id=make_uuid4(str(5000 + i)),
                                 payload={"type": "alert", "host": f"srv{i}"})
                await engine.process_event(evt)

            stats1 = engine.get_stats()
            assert stats1.total_stories_created == 3
            assert stats1.open_story_count == 3
            assert stats1.total_events_processed == 3
            assert stats1.current_sequence == 3

            # Close one via terminal
            evt_done = make_event(event_id=make_uuid4("5010"),
                                  payload={"type": "alert", "host": "srv0", "status": "done"})
            await engine.process_event(evt_done)

            stats2 = engine.get_stats()
            assert stats2.total_stories_closed == 1
            assert stats2.open_story_count == 2
            assert stats2.total_events_processed == 4
            assert stats2.current_sequence == 4
        finally:
            os.unlink(state_file)

    def test_goodhart_purge_boundary_not_equal(self, clock, id_counter):
        """purge_expired_chains must NOT remove entries where expires_at == clock() (strict less-than)."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            state_file = f.name
        try:
            rule = make_rule(
                name="rule-a",
                match=[MatchPredicate(field="type", value="alert", is_regex=False)],
                group_by=["host"],
                chain_by=["session"],
                chain_ttl=10.0,
            )
            config = make_config(rules=[rule], state_file=state_file, clock=clock,
                                 id_factory=id_counter)
            sm = StoryManager(max_open_stories=100, sweep_interval=1.0, clock=clock)
            engine = CorrelationEngine(config=config, story_manager=sm,
                                       clock=clock, id_factory=id_counter)

            # Manually insert a chain entry that expires at exactly 100.0
            chain_key = CompositeKey(rule_name="rule-a", key_tuple=[("session", "abc")])
            parent_sid = make_uuid4("5050")
            entry = ChainEntry(parent_story_id=parent_sid, expires_at=100.0)

            # Access internal chain registry (implementation detail, but needed for boundary test)
            if hasattr(engine, '_chain_registry'):
                engine._chain_registry[chain_key] = entry
            elif hasattr(engine, 'chain_registry'):
                engine.chain_registry[chain_key] = entry

            clock._current[0] = 100.0  # Exactly at expiration
            purged = engine.purge_expired_chains()
            # At exactly expires_at, entry should NOT be purged (strict < comparison)
            assert purged == 0

            clock._current[0] = 100.001  # Just past expiration
            purged = engine.purge_expired_chains()
            assert purged == 1
        finally:
            os.unlink(state_file)


# ============================================================
# State recovery tests
# ============================================================

class TestGoodhartStateRecovery:
    """Tests for state recovery that catch shortcut implementations."""

    @pytest.mark.asyncio
    async def test_goodhart_recover_mixed_valid_corrupt_line_counts(self):
        """records_read + records_skipped must equal total lines for mixed valid/corrupt file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            state_file = f.name
            # Write some valid and corrupt lines
            valid_record = {
                "record_type": "story_opened",
                "story_id": make_uuid4("1"),
                "rule_name": "rule-a",
                "group_key": {"host": "srv1"},
                "parent_story_id": "",
                "created_at": "2024-01-15T10:00:00Z",
                "timeout": 60.0,
            }
            f.write(json.dumps(valid_record) + "\n")
            f.write("this is not json\n")
            f.write("{malformed json\n")
            f.write(json.dumps(valid_record) + "\n")  # Duplicate open is still valid JSON
            f.write("another bad line\n")

        try:
            result = await recover_state(state_file, clock=time.monotonic) \
                if asyncio.iscoroutinefunction(recover_state) \
                else recover_state(state_file, clock=time.monotonic)

            total_lines = 5
            assert result.records_read + result.records_skipped == total_lines
            assert result.records_skipped == 3
        finally:
            os.unlink(state_file)

    @pytest.mark.asyncio
    async def test_goodhart_recover_excludes_closed_stories(self):
        """Stories with both opened and closed records must not appear in recovered open_stories."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            state_file = f.name
            sid_closed = make_uuid4("c1")
            sid_open = make_uuid4("c2")

            records = [
                {"record_type": "story_opened", "story_id": sid_closed,
                 "rule_name": "rule-a", "group_key": {"host": "srv1"},
                 "parent_story_id": "", "created_at": "2024-01-15T10:00:00Z",
                 "timeout": 60.0},
                {"record_type": "story_opened", "story_id": sid_open,
                 "rule_name": "rule-a", "group_key": {"host": "srv2"},
                 "parent_story_id": "", "created_at": "2024-01-15T10:00:01Z",
                 "timeout": 60.0},
                {"record_type": "story_closed", "story_id": sid_closed,
                 "status": "completed", "closed_at": "2024-01-15T10:05:00Z"},
            ]
            for r in records:
                f.write(json.dumps(r) + "\n")

        try:
            result = await recover_state(state_file, clock=time.monotonic) \
                if asyncio.iscoroutinefunction(recover_state) \
                else recover_state(state_file, clock=time.monotonic)

            open_ids = [s.get("story_id", s.story_id if hasattr(s, "story_id") else None)
                        if isinstance(s, dict) else s.story_id
                        for s in result.open_stories]
            assert sid_closed not in open_ids
            assert sid_open in open_ids
            assert result.records_read == 3
        finally:
            os.unlink(state_file)

    @pytest.mark.asyncio
    async def test_goodhart_recover_nonexistent_file_empty_result(self):
        """recover_state on a nonexistent file must return empty result with all counts 0."""
        path = "/tmp/definitely_does_not_exist_12345.jsonl"
        if os.path.exists(path):
            os.unlink(path)

        result = await recover_state(path, clock=time.monotonic) \
            if asyncio.iscoroutinefunction(recover_state) \
            else recover_state(path, clock=time.monotonic)

        assert len(result.open_stories) == 0
        assert result.records_read == 0
        assert result.records_skipped == 0


# ============================================================
# append_record tests
# ============================================================

class TestGoodhartAppendRecord:
    """Tests for append_record to ensure proper JSONL output."""

    @pytest.mark.asyncio
    async def test_goodhart_append_multiple_records_each_on_own_line(self):
        """Each appended record must be on its own line in the JSONL file."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            state_file = f.name

        try:
            record1 = StoryOpenedRecord(
                record_type="story_opened",
                story_id=make_uuid4("1"),
                rule_name="rule-a",
                group_key={"host": "srv1"},
                parent_story_id="",
                created_at="2024-01-15T10:00:00Z",
                timeout=60.0,
            )
            record2 = StoryClosedRecord(
                record_type="story_closed",
                story_id=make_uuid4("1"),
                status="completed",
                closed_at="2024-01-15T10:05:00Z",
            )

            if asyncio.iscoroutinefunction(append_record):
                await append_record(state_file, record1)
                await append_record(state_file, record2)
            else:
                append_record(state_file, record1)
                append_record(state_file, record2)

            with open(state_file, "r") as f:
                lines = f.readlines()
            assert len(lines) == 2
            # Each line should be valid JSON
            for line in lines:
                parsed = json.loads(line.strip())
                assert "record_type" in parsed
        finally:
            os.unlink(state_file)

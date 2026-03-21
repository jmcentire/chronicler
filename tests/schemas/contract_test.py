"""
Contract tests for the Chronicler schemas module.
Tests cover types, functions, invariants, and configuration validation.

Run with: pytest tests/test_schemas.py -v
"""
import json
import re
import hashlib
import uuid
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

import pytest

# Import everything from the schemas module
from schemas import (
    EventKind,
    EntityKind,
    EntityId,
    ActorId,
    Sha256Hex,
    StoryId,
    AwareDatetime,
    ContextDict,
    CorrelationKeys,
    StoryStatus,
    CloseReason,
    Event,
    Story,
    TerminalEventPattern,
    GroupByKeys,
    ChainByKeys,
    EventKindSet,
    CorrelationRule,
    SourceType,
    WebhookSourceConfig,
    OtlpSourceConfig,
    FileSourceConfig,
    SentinelSourceConfig,
    StigmergySinkConfig,
    ApprenticeSinkConfig,
    DiskSinkConfig,
    KindexSinkConfig,
    NoteworthinessFilter,
    MemoryLimitsConfig,
    ChroniclerConfig,
    SinkType,
    compute_event_id,
    create_event,
    create_story,
    append_event_to_story,
    close_story,
    validate_story_consistency,
    matches_terminal_pattern,
    extract_group_key,
    load_chronicler_config,
    event_to_dict,
    event_from_dict,
    story_to_dict,
    story_from_dict,
)

# ─── Fixtures ────────────────────────────────────────────────────────────────

VALID_SHA256 = "a" * 64
VALID_SHA256_B = "b" * 64
VALID_SHA256_C = "c" * 64

VALID_UUID4 = "12345678-1234-4123-8123-123456789abc"

NOW = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
LATER = datetime(2024, 1, 15, 13, 0, 0, tzinfo=timezone.utc)
EARLIER = datetime(2024, 1, 15, 11, 0, 0, tzinfo=timezone.utc)
NAIVE_DT = datetime(2024, 1, 15, 12, 0, 0)


def make_valid_event_kwargs():
    """Return kwargs suitable for create_event."""
    return dict(
        event_kind="ci.build.completed",
        entity_kind="repository",
        entity_id="repo-123",
        actor="user@example.com",
        timestamp=NOW,
        context={"status": "success", "count": 42},
        correlation_keys={"repo": "myrepo", "branch": "main"},
        source="webhook",
    )


def make_valid_event():
    """Create a valid Event instance."""
    return create_event(**make_valid_event_kwargs())


def make_valid_open_story(event=None):
    """Create a valid open Story."""
    if event is None:
        event = make_valid_event()
    return create_story(
        story_type="ci.pipeline",
        correlation_rule="ci_rule",
        group_key={"repo": "myrepo", "branch": "main"},
        first_event_id=event.id,
        start_ts=NOW,
        parent_story_id=None,
    )


def make_valid_closed_story():
    """Create a valid closed Story."""
    story = make_valid_open_story()
    return close_story(story, CloseReason.terminal, LATER)


# ─── Test Types ──────────────────────────────────────────────────────────────


class TestTypes:
    """Test primitive/constrained type validators."""

    # --- EventKind ---
    def test_event_kind_valid(self):
        ek = EventKind("ci.build.completed")
        # Access underlying value; may be .value or direct str depending on impl
        assert str(ek) or ek  # At minimum, construction succeeds

    def test_event_kind_single_char(self):
        """Minimum valid EventKind is a single lowercase letter."""
        ek = EventKind("a")
        assert ek

    def test_event_kind_with_underscores_and_dots(self):
        ek = EventKind("ci_build.step.completed")
        assert ek

    def test_event_kind_max_length_255(self):
        val = "a" + "b" * 254  # 255 chars total
        ek = EventKind(val)
        assert ek

    def test_event_kind_rejects_256_chars(self):
        val = "a" + "b" * 255  # 256 chars total
        with pytest.raises(Exception):  # ValidationError
            EventKind(val)

    def test_event_kind_rejects_uppercase(self):
        with pytest.raises(Exception):
            EventKind("CI.build")

    def test_event_kind_rejects_empty(self):
        with pytest.raises(Exception):
            EventKind("")

    def test_event_kind_rejects_starts_with_digit(self):
        with pytest.raises(Exception):
            EventKind("1ci.build")

    def test_event_kind_rejects_special_chars(self):
        with pytest.raises(Exception):
            EventKind("ci-build")  # hyphen not allowed

    def test_event_kind_rejects_spaces(self):
        with pytest.raises(Exception):
            EventKind("ci build")

    # --- EntityKind ---
    def test_entity_kind_valid(self):
        ek = EntityKind("repository")
        assert ek

    def test_entity_kind_rejects_uppercase(self):
        with pytest.raises(Exception):
            EntityKind("Repository")

    def test_entity_kind_rejects_empty(self):
        with pytest.raises(Exception):
            EntityKind("")

    # --- EntityId ---
    def test_entity_id_valid(self):
        eid = EntityId("repo-123")
        assert eid

    def test_entity_id_rejects_empty(self):
        with pytest.raises(Exception):
            EntityId("")

    def test_entity_id_max_length_1024(self):
        eid = EntityId("x" * 1024)
        assert eid

    def test_entity_id_rejects_1025_chars(self):
        with pytest.raises(Exception):
            EntityId("x" * 1025)

    # --- ActorId ---
    def test_actor_id_valid(self):
        aid = ActorId("user@example.com")
        assert aid

    def test_actor_id_rejects_empty(self):
        with pytest.raises(Exception):
            ActorId("")

    def test_actor_id_max_length_512(self):
        aid = ActorId("x" * 512)
        assert aid

    def test_actor_id_rejects_513_chars(self):
        with pytest.raises(Exception):
            ActorId("x" * 513)

    # --- Sha256Hex ---
    def test_sha256_hex_valid(self):
        s = Sha256Hex("a" * 64)
        assert s

    def test_sha256_hex_valid_mixed_hex(self):
        s = Sha256Hex("0123456789abcdef" * 4)
        assert s

    def test_sha256_hex_rejects_short(self):
        with pytest.raises(Exception):
            Sha256Hex("abcdef")

    def test_sha256_hex_rejects_65_chars(self):
        with pytest.raises(Exception):
            Sha256Hex("a" * 65)

    def test_sha256_hex_rejects_uppercase(self):
        with pytest.raises(Exception):
            Sha256Hex("A" * 64)

    def test_sha256_hex_rejects_non_hex(self):
        with pytest.raises(Exception):
            Sha256Hex("g" * 64)

    # --- StoryId ---
    def test_story_id_valid_uuid4(self):
        sid = StoryId(VALID_UUID4)
        assert sid

    def test_story_id_rejects_non_uuid(self):
        with pytest.raises(Exception):
            StoryId("not-a-uuid")

    def test_story_id_rejects_uuid_version_1(self):
        # UUID v1 has a different version nibble
        with pytest.raises(Exception):
            StoryId("12345678-1234-1123-8123-123456789abc")

    # --- AwareDatetime ---
    def test_aware_datetime_valid(self):
        # Should accept timezone-aware datetime
        adt = AwareDatetime(NOW)
        assert adt

    def test_aware_datetime_rejects_naive(self):
        with pytest.raises(Exception):
            AwareDatetime(NAIVE_DT)

    def test_aware_datetime_non_utc_tz(self):
        """Non-UTC but still timezone-aware should be accepted."""
        tz_plus5 = timezone(timedelta(hours=5))
        dt = datetime(2024, 1, 15, 12, 0, 0, tzinfo=tz_plus5)
        adt = AwareDatetime(dt)
        assert adt

    # --- ContextDict ---
    def test_context_dict_valid_all_types(self):
        cd = ContextDict({"s": "val", "i": 42, "f": 3.14, "b": True, "n": None})
        assert cd

    def test_context_dict_empty(self):
        cd = ContextDict({})
        assert cd

    def test_context_dict_rejects_list_value(self):
        with pytest.raises(Exception):
            ContextDict({"key": [1, 2, 3]})

    def test_context_dict_rejects_dict_value(self):
        with pytest.raises(Exception):
            ContextDict({"key": {"nested": "bad"}})

    def test_context_dict_rejects_empty_key(self):
        with pytest.raises(Exception):
            ContextDict({"": "val"})

    # --- CorrelationKeys ---
    def test_correlation_keys_valid(self):
        ck = CorrelationKeys({"repo": "myrepo", "branch": "main"})
        assert ck

    def test_correlation_keys_rejects_empty_value(self):
        with pytest.raises(Exception):
            CorrelationKeys({"repo": ""})

    def test_correlation_keys_rejects_empty_key(self):
        with pytest.raises(Exception):
            CorrelationKeys({"": "val"})

    def test_correlation_keys_empty_dict_accepted(self):
        """Empty CorrelationKeys dict should be valid (no entries is ok)."""
        # Contract says all k,v must be non-empty strings - vacuously true for empty dict
        ck = CorrelationKeys({})
        assert ck

    # --- StoryStatus ---
    def test_story_status_open(self):
        assert StoryStatus.open

    def test_story_status_closed(self):
        assert StoryStatus.closed

    # --- CloseReason ---
    def test_close_reason_terminal(self):
        assert CloseReason.terminal

    def test_close_reason_timeout(self):
        assert CloseReason.timeout

    def test_close_reason_evicted(self):
        assert CloseReason.evicted

    # --- TerminalEventPattern ---
    def test_terminal_event_pattern_valid(self):
        tp = TerminalEventPattern({"event_kind": "ci.build.completed"})
        assert tp

    def test_terminal_event_pattern_rejects_empty_key(self):
        with pytest.raises(Exception):
            TerminalEventPattern({"": "value"})

    # --- EventKindSet ---
    def test_event_kind_set_valid(self):
        eks = EventKindSet(["ci.build.completed", "ci.build.started"])
        assert eks

    def test_event_kind_set_single_item(self):
        eks = EventKindSet(["ci.build.completed"])
        assert eks

    def test_event_kind_set_rejects_empty(self):
        with pytest.raises(Exception):
            EventKindSet([])


# ─── Test compute_event_id ───────────────────────────────────────────────────


class TestComputeEventId:
    """Test the content-addressable event ID computation."""

    def _base_kwargs(self):
        return dict(
            event_kind="ci.build.completed",
            entity_kind="repository",
            entity_id="repo-123",
            actor="user@example.com",
            timestamp=NOW,
            context={"status": "success"},
            correlation_keys={"repo": "myrepo"},
        )

    def test_returns_64_hex_chars(self):
        result = compute_event_id(**self._base_kwargs())
        assert len(result) == 64
        assert re.match(r"^[0-9a-f]{64}$", result)

    def test_deterministic(self):
        """Same inputs always produce same output."""
        id1 = compute_event_id(**self._base_kwargs())
        id2 = compute_event_id(**self._base_kwargs())
        assert id1 == id2

    def test_sensitivity_event_kind(self):
        base_id = compute_event_id(**self._base_kwargs())
        kwargs = self._base_kwargs()
        kwargs["event_kind"] = "ci.build.started"
        changed_id = compute_event_id(**kwargs)
        assert base_id != changed_id

    def test_sensitivity_entity_kind(self):
        base_id = compute_event_id(**self._base_kwargs())
        kwargs = self._base_kwargs()
        kwargs["entity_kind"] = "pipeline"
        changed_id = compute_event_id(**kwargs)
        assert base_id != changed_id

    def test_sensitivity_entity_id(self):
        base_id = compute_event_id(**self._base_kwargs())
        kwargs = self._base_kwargs()
        kwargs["entity_id"] = "repo-456"
        changed_id = compute_event_id(**kwargs)
        assert base_id != changed_id

    def test_sensitivity_actor(self):
        base_id = compute_event_id(**self._base_kwargs())
        kwargs = self._base_kwargs()
        kwargs["actor"] = "other@example.com"
        changed_id = compute_event_id(**kwargs)
        assert base_id != changed_id

    def test_sensitivity_timestamp(self):
        base_id = compute_event_id(**self._base_kwargs())
        kwargs = self._base_kwargs()
        kwargs["timestamp"] = LATER
        changed_id = compute_event_id(**kwargs)
        assert base_id != changed_id

    def test_sensitivity_context(self):
        base_id = compute_event_id(**self._base_kwargs())
        kwargs = self._base_kwargs()
        kwargs["context"] = {"status": "failure"}
        changed_id = compute_event_id(**kwargs)
        assert base_id != changed_id

    def test_sensitivity_correlation_keys(self):
        base_id = compute_event_id(**self._base_kwargs())
        kwargs = self._base_kwargs()
        kwargs["correlation_keys"] = {"repo": "otherrepo"}
        changed_id = compute_event_id(**kwargs)
        assert base_id != changed_id

    def test_naive_timestamp_rejected(self):
        kwargs = self._base_kwargs()
        kwargs["timestamp"] = NAIVE_DT
        with pytest.raises(Exception):
            compute_event_id(**kwargs)

    def test_empty_context_and_correlation_keys(self):
        """Edge case: empty context and empty correlation_keys."""
        result = compute_event_id(
            event_kind="ci.build.completed",
            entity_kind="repository",
            entity_id="repo-123",
            actor="user@example.com",
            timestamp=NOW,
            context={},
            correlation_keys={},
        )
        assert len(result) == 64
        assert re.match(r"^[0-9a-f]{64}$", result)


# ─── Test create_event ───────────────────────────────────────────────────────


class TestCreateEvent:
    """Test event creation including automatic ID computation."""

    def test_happy_path(self):
        event = make_valid_event()
        # Event.id should be a valid SHA-256 hex
        assert len(event.id) == 64
        assert re.match(r"^[0-9a-f]{64}$", event.id)
        # Fields should match inputs
        assert event.event_kind == "ci.build.completed"
        assert event.entity_kind == "repository"
        assert event.entity_id == "repo-123"
        assert event.actor == "user@example.com"
        assert event.source == "webhook"

    def test_id_matches_compute_event_id(self):
        """Event.id must match what compute_event_id returns."""
        kwargs = make_valid_event_kwargs()
        event = create_event(**kwargs)
        expected_id = compute_event_id(
            event_kind=kwargs["event_kind"],
            entity_kind=kwargs["entity_kind"],
            entity_id=kwargs["entity_id"],
            actor=kwargs["actor"],
            timestamp=kwargs["timestamp"],
            context=kwargs["context"],
            correlation_keys=kwargs["correlation_keys"],
        )
        assert event.id == expected_id

    def test_same_inputs_same_id(self):
        """Creating the same event twice produces identical IDs."""
        event1 = create_event(**make_valid_event_kwargs())
        event2 = create_event(**make_valid_event_kwargs())
        assert event1.id == event2.id

    def test_source_excluded_from_hash(self):
        """Changing source does not change the event id."""
        kwargs1 = make_valid_event_kwargs()
        kwargs1["source"] = "webhook"
        kwargs2 = make_valid_event_kwargs()
        kwargs2["source"] = "otlp"
        event1 = create_event(**kwargs1)
        event2 = create_event(**kwargs2)
        assert event1.id == event2.id

    def test_event_is_frozen(self):
        """Event instances are frozen (immutable)."""
        event = make_valid_event()
        with pytest.raises(Exception):
            event.source = "changed"

    def test_invalid_event_kind(self):
        kwargs = make_valid_event_kwargs()
        kwargs["event_kind"] = "INVALID"
        with pytest.raises(Exception):
            create_event(**kwargs)

    def test_invalid_entity_kind(self):
        kwargs = make_valid_event_kwargs()
        kwargs["entity_kind"] = "INVALID-KIND"
        with pytest.raises(Exception):
            create_event(**kwargs)

    def test_empty_entity_id(self):
        kwargs = make_valid_event_kwargs()
        kwargs["entity_id"] = ""
        with pytest.raises(Exception):
            create_event(**kwargs)

    def test_empty_actor(self):
        kwargs = make_valid_event_kwargs()
        kwargs["actor"] = ""
        with pytest.raises(Exception):
            create_event(**kwargs)

    def test_naive_timestamp(self):
        kwargs = make_valid_event_kwargs()
        kwargs["timestamp"] = NAIVE_DT
        with pytest.raises(Exception):
            create_event(**kwargs)

    def test_invalid_context_value(self):
        kwargs = make_valid_event_kwargs()
        kwargs["context"] = {"key": [1, 2, 3]}
        with pytest.raises(Exception):
            create_event(**kwargs)

    def test_invalid_correlation_key_empty_value(self):
        kwargs = make_valid_event_kwargs()
        kwargs["correlation_keys"] = {"repo": ""}
        with pytest.raises(Exception):
            create_event(**kwargs)

    def test_invalid_correlation_key_empty_key(self):
        kwargs = make_valid_event_kwargs()
        kwargs["correlation_keys"] = {"": "myrepo"}
        with pytest.raises(Exception):
            create_event(**kwargs)


# ─── Test Story Creation & Lifecycle ─────────────────────────────────────────


class TestCreateStory:
    """Test story creation."""

    def test_happy_path(self):
        event = make_valid_event()
        story = create_story(
            story_type="ci.pipeline",
            correlation_rule="ci_rule",
            group_key={"repo": "myrepo", "branch": "main"},
            first_event_id=event.id,
            start_ts=NOW,
            parent_story_id=None,
        )
        assert story.status == StoryStatus.open
        assert story.events == (event.id,)
        assert story.event_count == 1
        assert story.end_ts is None
        assert story.duration is None
        assert story.close_reason is None
        assert story.story_type == "ci.pipeline"
        assert story.correlation_rule == "ci_rule"
        # Verify story_id is a valid UUID4
        assert re.match(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
            story.story_id,
        )

    def test_with_parent_story_id(self):
        event = make_valid_event()
        story = create_story(
            story_type="ci.pipeline",
            correlation_rule="ci_rule",
            group_key={"repo": "myrepo"},
            first_event_id=event.id,
            start_ts=NOW,
            parent_story_id=VALID_UUID4,
        )
        assert story.parent_story_id == VALID_UUID4

    def test_no_parent(self):
        event = make_valid_event()
        story = create_story(
            story_type="ci.pipeline",
            correlation_rule="ci_rule",
            group_key={"repo": "myrepo"},
            first_event_id=event.id,
            start_ts=NOW,
            parent_story_id=None,
        )
        assert story.parent_story_id is None

    def test_empty_story_type_rejected(self):
        event = make_valid_event()
        with pytest.raises(Exception):
            create_story(
                story_type="",
                correlation_rule="ci_rule",
                group_key={"repo": "myrepo"},
                first_event_id=event.id,
                start_ts=NOW,
                parent_story_id=None,
            )

    def test_empty_correlation_rule_rejected(self):
        event = make_valid_event()
        with pytest.raises(Exception):
            create_story(
                story_type="ci.pipeline",
                correlation_rule="",
                group_key={"repo": "myrepo"},
                first_event_id=event.id,
                start_ts=NOW,
                parent_story_id=None,
            )

    def test_invalid_first_event_id_rejected(self):
        with pytest.raises(Exception):
            create_story(
                story_type="ci.pipeline",
                correlation_rule="ci_rule",
                group_key={"repo": "myrepo"},
                first_event_id="not-a-hash",
                start_ts=NOW,
                parent_story_id=None,
            )


class TestStoryLifecycle:
    """Test append_event_to_story and close_story."""

    # --- append_event_to_story ---
    def test_append_event_happy_path(self):
        story = make_valid_open_story()
        original_events = story.events
        original_count = story.event_count
        new_story = append_event_to_story(story, VALID_SHA256_B)
        assert new_story.events == original_events + (VALID_SHA256_B,)
        assert new_story.event_count == original_count + 1
        assert new_story.status == StoryStatus.open
        assert new_story.story_id == story.story_id

    def test_append_event_original_unchanged(self):
        story = make_valid_open_story()
        original_events = story.events
        original_count = story.event_count
        _ = append_event_to_story(story, VALID_SHA256_B)
        # Original unchanged
        assert story.events == original_events
        assert story.event_count == original_count

    def test_append_event_multiple(self):
        story = make_valid_open_story()
        story2 = append_event_to_story(story, VALID_SHA256_B)
        story3 = append_event_to_story(story2, VALID_SHA256_C)
        assert story3.event_count == 3
        assert len(story3.events) == 3

    def test_append_event_story_not_open(self):
        closed_story = make_valid_closed_story()
        with pytest.raises(Exception):
            append_event_to_story(closed_story, VALID_SHA256_B)

    def test_append_event_duplicate_id(self):
        story = make_valid_open_story()
        # The first event id is already in story.events
        first_event_id = story.events[0]
        with pytest.raises(Exception):
            append_event_to_story(story, first_event_id)

    # --- close_story ---
    def test_close_story_happy_path(self):
        story = make_valid_open_story()
        closed = close_story(story, CloseReason.terminal, LATER)
        assert closed.status == StoryStatus.closed
        assert closed.close_reason == CloseReason.terminal
        assert closed.end_ts == LATER
        expected_duration = (LATER - NOW).total_seconds()
        assert closed.duration == expected_duration
        assert closed.duration >= 0.0
        assert closed.story_id == story.story_id
        assert closed.events == story.events

    def test_close_story_original_unchanged(self):
        story = make_valid_open_story()
        _ = close_story(story, CloseReason.terminal, LATER)
        assert story.status == StoryStatus.open
        assert story.end_ts is None
        assert story.close_reason is None

    def test_close_story_already_closed(self):
        closed_story = make_valid_closed_story()
        with pytest.raises(Exception):
            close_story(closed_story, CloseReason.timeout, LATER)

    def test_close_story_end_before_start(self):
        story = make_valid_open_story()
        with pytest.raises(Exception):
            close_story(story, CloseReason.terminal, EARLIER)

    def test_close_story_zero_duration(self):
        """end_ts == start_ts produces duration 0.0."""
        story = make_valid_open_story()
        closed = close_story(story, CloseReason.terminal, story.start_ts)
        assert closed.duration == 0.0

    def test_close_story_timeout_reason(self):
        story = make_valid_open_story()
        closed = close_story(story, CloseReason.timeout, LATER)
        assert closed.close_reason == CloseReason.timeout

    def test_close_story_evicted_reason(self):
        story = make_valid_open_story()
        closed = close_story(story, CloseReason.evicted, LATER)
        assert closed.close_reason == CloseReason.evicted


# ─── Test validate_story_consistency ─────────────────────────────────────────


class TestValidateStoryConsistency:
    """Test the story consistency validator."""

    def test_valid_open_story(self):
        story = make_valid_open_story()
        result = validate_story_consistency(story)
        assert result is True

    def test_valid_closed_story(self):
        story = make_valid_closed_story()
        result = validate_story_consistency(story)
        assert result is True

    def test_open_story_with_end_ts_rejected(self):
        """Open story with end_ts set should fail consistency."""
        # We must create an inconsistent story; since frozen, we use model construction
        # that bypasses validators or use construct/model_construct
        story = make_valid_open_story()
        with pytest.raises(Exception):
            # Attempt to create inconsistent state via model_copy
            story.model_copy(update={"end_ts": LATER})

    def test_open_story_with_close_reason_rejected(self):
        story = make_valid_open_story()
        with pytest.raises(Exception):
            story.model_copy(update={"close_reason": CloseReason.terminal})

    def test_open_story_with_duration_rejected(self):
        story = make_valid_open_story()
        with pytest.raises(Exception):
            story.model_copy(update={"duration": 100.0})

    def test_closed_story_missing_end_ts_rejected(self):
        """Closed story without end_ts should fail."""
        story = make_valid_open_story()
        with pytest.raises(Exception):
            story.model_copy(
                update={
                    "status": StoryStatus.closed,
                    "close_reason": CloseReason.terminal,
                    "duration": 100.0,
                    # end_ts intentionally missing (None)
                }
            )

    def test_closed_story_missing_close_reason_rejected(self):
        story = make_valid_open_story()
        with pytest.raises(Exception):
            story.model_copy(
                update={
                    "status": StoryStatus.closed,
                    "end_ts": LATER,
                    "duration": 100.0,
                    # close_reason intentionally missing (None)
                }
            )

    def test_closed_story_missing_duration_rejected(self):
        story = make_valid_open_story()
        with pytest.raises(Exception):
            story.model_copy(
                update={
                    "status": StoryStatus.closed,
                    "end_ts": LATER,
                    "close_reason": CloseReason.terminal,
                    # duration intentionally missing (None)
                }
            )

    def test_event_count_mismatch_rejected(self):
        """Story with event_count != len(events) should fail."""
        story = make_valid_open_story()
        with pytest.raises(Exception):
            story.model_copy(update={"event_count": 99})


# ─── Test Correlation Helpers ────────────────────────────────────────────────


class TestCorrelationHelpers:
    """Test matches_terminal_pattern and extract_group_key."""

    def _make_event(self):
        return make_valid_event()

    # --- matches_terminal_pattern ---
    def test_empty_pattern_matches_all(self):
        event = self._make_event()
        pattern = TerminalEventPattern({})
        assert matches_terminal_pattern(event, pattern) is True

    def test_event_kind_match(self):
        event = self._make_event()
        pattern = TerminalEventPattern({"event_kind": "ci.build.completed"})
        assert matches_terminal_pattern(event, pattern) is True

    def test_event_kind_no_match(self):
        event = self._make_event()
        pattern = TerminalEventPattern({"event_kind": "ci.build.started"})
        assert matches_terminal_pattern(event, pattern) is False

    def test_entity_kind_match(self):
        event = self._make_event()
        pattern = TerminalEventPattern({"entity_kind": "repository"})
        assert matches_terminal_pattern(event, pattern) is True

    def test_context_key_match(self):
        event = self._make_event()
        pattern = TerminalEventPattern({"context.status": "success"})
        assert matches_terminal_pattern(event, pattern) is True

    def test_context_key_no_match(self):
        event = self._make_event()
        pattern = TerminalEventPattern({"context.status": "failure"})
        assert matches_terminal_pattern(event, pattern) is False

    def test_missing_context_key_returns_false(self):
        event = self._make_event()
        pattern = TerminalEventPattern({"context.nonexistent": "value"})
        assert matches_terminal_pattern(event, pattern) is False

    def test_correlation_key_match(self):
        event = self._make_event()
        pattern = TerminalEventPattern({"correlation_keys.repo": "myrepo"})
        assert matches_terminal_pattern(event, pattern) is True

    def test_multi_field_all_match(self):
        event = self._make_event()
        pattern = TerminalEventPattern({
            "event_kind": "ci.build.completed",
            "context.status": "success",
        })
        assert matches_terminal_pattern(event, pattern) is True

    def test_multi_field_partial_match_returns_false(self):
        event = self._make_event()
        pattern = TerminalEventPattern({
            "event_kind": "ci.build.completed",
            "context.status": "failure",  # This won't match
        })
        assert matches_terminal_pattern(event, pattern) is False

    def test_actor_match(self):
        event = self._make_event()
        pattern = TerminalEventPattern({"actor": "user@example.com"})
        assert matches_terminal_pattern(event, pattern) is True

    def test_entity_id_match(self):
        event = self._make_event()
        pattern = TerminalEventPattern({"entity_id": "repo-123"})
        assert matches_terminal_pattern(event, pattern) is True

    # --- extract_group_key ---
    def test_extract_group_key_correlation_keys(self):
        event = self._make_event()
        group_by = GroupByKeys(["correlation_keys.repo", "correlation_keys.branch"])
        result = extract_group_key(event, group_by)
        assert len(result) == 2
        assert result["correlation_keys.repo"] == "myrepo"
        assert result["correlation_keys.branch"] == "main"

    def test_extract_group_key_top_level_field(self):
        event = self._make_event()
        group_by = GroupByKeys(["event_kind"])
        result = extract_group_key(event, group_by)
        assert result["event_kind"] == "ci.build.completed"

    def test_extract_group_key_context_field(self):
        event = self._make_event()
        group_by = GroupByKeys(["context.status"])
        result = extract_group_key(event, group_by)
        assert result["context.status"] == "success"

    def test_extract_group_key_missing_field(self):
        event = self._make_event()
        group_by = GroupByKeys(["correlation_keys.nonexistent"])
        with pytest.raises(Exception):
            extract_group_key(event, group_by)


# ─── Test Serialization ─────────────────────────────────────────────────────


class TestSerialization:
    """Test event_to_dict, event_from_dict, story_to_dict, story_from_dict."""

    # --- Event round trip ---
    def test_event_round_trip(self):
        event = make_valid_event()
        d = event_to_dict(event)
        restored = event_from_dict(d)
        assert restored.id == event.id
        assert restored.event_kind == event.event_kind
        assert restored.entity_kind == event.entity_kind
        assert restored.entity_id == event.entity_id
        assert restored.actor == event.actor
        assert restored.source == event.source

    def test_event_to_dict_all_fields_present(self):
        event = make_valid_event()
        d = event_to_dict(event)
        assert "id" in d
        assert "event_kind" in d
        assert "entity_kind" in d
        assert "entity_id" in d
        assert "actor" in d
        assert "timestamp" in d
        assert "context" in d
        assert "correlation_keys" in d
        assert "source" in d

    def test_event_to_dict_json_serializable(self):
        event = make_valid_event()
        d = event_to_dict(event)
        # Should not raise
        json.dumps(d)

    def test_event_from_dict_missing_required_field(self):
        event = make_valid_event()
        d = event_to_dict(event)
        del d["event_kind"]
        with pytest.raises(Exception):
            event_from_dict(d)

    def test_event_from_dict_id_mismatch(self):
        event = make_valid_event()
        d = event_to_dict(event)
        d["id"] = "0" * 64  # Wrong hash
        with pytest.raises(Exception):
            event_from_dict(d)

    def test_event_from_dict_no_id_computes_it(self):
        event = make_valid_event()
        d = event_to_dict(event)
        original_id = d.pop("id")
        restored = event_from_dict(d)
        assert restored.id == original_id

    def test_event_from_dict_invalid_field_value(self):
        event = make_valid_event()
        d = event_to_dict(event)
        d["event_kind"] = "INVALID_KIND"
        with pytest.raises(Exception):
            event_from_dict(d)

    # --- Story round trip ---
    def test_story_round_trip_open(self):
        story = make_valid_open_story()
        d = story_to_dict(story)
        restored = story_from_dict(d)
        assert restored.story_id == story.story_id
        assert restored.status == story.status
        assert restored.event_count == story.event_count
        assert restored.story_type == story.story_type

    def test_story_round_trip_closed(self):
        story = make_valid_closed_story()
        d = story_to_dict(story)
        restored = story_from_dict(d)
        assert restored.story_id == story.story_id
        assert restored.status == StoryStatus.closed
        assert restored.close_reason == story.close_reason
        assert restored.duration == story.duration

    def test_story_to_dict_all_fields_present(self):
        story = make_valid_open_story()
        d = story_to_dict(story)
        assert "story_id" in d
        assert "story_type" in d
        assert "correlation_rule" in d
        assert "group_key" in d
        assert "events" in d
        assert "status" in d
        assert "start_ts" in d
        assert "event_count" in d

    def test_story_to_dict_events_is_list(self):
        story = make_valid_open_story()
        d = story_to_dict(story)
        assert isinstance(d["events"], list)

    def test_story_to_dict_json_serializable(self):
        story = make_valid_closed_story()
        d = story_to_dict(story)
        json.dumps(d)

    def test_story_from_dict_lifecycle_inconsistency(self):
        """Open story with end_ts set should fail lifecycle validation."""
        story = make_valid_open_story()
        d = story_to_dict(story)
        d["end_ts"] = LATER.isoformat()
        with pytest.raises(Exception):
            story_from_dict(d)

    def test_story_from_dict_event_count_mismatch(self):
        story = make_valid_open_story()
        d = story_to_dict(story)
        d["event_count"] = 99
        with pytest.raises(Exception):
            story_from_dict(d)

    def test_story_from_dict_missing_required_field(self):
        story = make_valid_open_story()
        d = story_to_dict(story)
        del d["story_type"]
        with pytest.raises(Exception):
            story_from_dict(d)


# ─── Test Configuration ─────────────────────────────────────────────────────


class TestConfig:
    """Test configuration types and load_chronicler_config."""

    # --- Individual config types ---
    def test_webhook_source_config_valid(self):
        cfg = WebhookSourceConfig(
            type="webhook",
            host="0.0.0.0",
            port=8080,
            path="/events",
            source_label="main_webhook",
        )
        assert cfg.port == 8080
        assert cfg.path == "/events"

    def test_webhook_source_config_port_zero(self):
        with pytest.raises(Exception):
            WebhookSourceConfig(
                type="webhook", host="0.0.0.0", port=0, path="/events", source_label="x"
            )

    def test_webhook_source_config_port_65536(self):
        with pytest.raises(Exception):
            WebhookSourceConfig(
                type="webhook", host="0.0.0.0", port=65536, path="/events", source_label="x"
            )

    def test_webhook_source_config_invalid_path(self):
        with pytest.raises(Exception):
            WebhookSourceConfig(
                type="webhook", host="0.0.0.0", port=8080, path="no-leading-slash", source_label="x"
            )

    def test_webhook_source_config_valid_path_with_segments(self):
        cfg = WebhookSourceConfig(
            type="webhook", host="0.0.0.0", port=8080, path="/api/v1/events", source_label="x"
        )
        assert cfg.path == "/api/v1/events"

    def test_otlp_source_config_valid(self):
        cfg = OtlpSourceConfig(
            type="otlp", host="0.0.0.0", port=4317, source_label="otlp_source"
        )
        assert cfg.port == 4317

    def test_file_source_config_valid(self):
        cfg = FileSourceConfig(
            type="file",
            path="/var/log/events.jsonl",
            poll_interval_seconds=1.0,
            source_label="file_source",
        )
        assert cfg.poll_interval_seconds == 1.0

    def test_file_source_config_poll_too_low(self):
        with pytest.raises(Exception):
            FileSourceConfig(
                type="file",
                path="/var/log/events.jsonl",
                poll_interval_seconds=0.01,
                source_label="x",
            )

    def test_file_source_config_poll_too_high(self):
        with pytest.raises(Exception):
            FileSourceConfig(
                type="file",
                path="/var/log/events.jsonl",
                poll_interval_seconds=3601,
                source_label="x",
            )

    def test_file_source_config_empty_path(self):
        with pytest.raises(Exception):
            FileSourceConfig(
                type="file", path="", poll_interval_seconds=1.0, source_label="x"
            )

    def test_sentinel_source_config_valid(self):
        cfg = SentinelSourceConfig(
            type="sentinel", host="0.0.0.0", port=9090, source_label="sentinel"
        )
        assert cfg.port == 9090

    def test_stigmergy_sink_config_valid(self):
        cfg = StigmergySinkConfig(
            type="stigmergy", url="https://stigmergy.example.com", timeout_seconds=30.0
        )
        assert cfg.url.startswith("https://")

    def test_stigmergy_sink_config_invalid_url(self):
        with pytest.raises(Exception):
            StigmergySinkConfig(
                type="stigmergy", url="ftp://not-http.com", timeout_seconds=30.0
            )

    def test_stigmergy_sink_config_timeout_too_low(self):
        with pytest.raises(Exception):
            StigmergySinkConfig(
                type="stigmergy", url="https://example.com", timeout_seconds=0.1
            )

    def test_stigmergy_sink_config_timeout_too_high(self):
        with pytest.raises(Exception):
            StigmergySinkConfig(
                type="stigmergy", url="https://example.com", timeout_seconds=301
            )

    def test_apprentice_sink_config_valid(self):
        cfg = ApprenticeSinkConfig(
            type="apprentice", url="http://apprentice.local", timeout_seconds=5.0
        )
        assert cfg

    def test_disk_sink_config_valid(self):
        cfg = DiskSinkConfig(
            type="disk", events_path="/data/events.jsonl", stories_path="/data/stories.jsonl"
        )
        assert cfg

    def test_disk_sink_config_empty_events_path(self):
        with pytest.raises(Exception):
            DiskSinkConfig(type="disk", events_path="", stories_path="/data/stories.jsonl")

    def test_disk_sink_config_empty_stories_path(self):
        with pytest.raises(Exception):
            DiskSinkConfig(type="disk", events_path="/data/events.jsonl", stories_path="")

    def test_kindex_sink_config_valid(self):
        nf = NoteworthinessFilter(
            min_event_count=2,
            min_duration_seconds=0.0,
            story_types=["ci.pipeline"],
            exclude_close_reasons=["evicted"],
        )
        cfg = KindexSinkConfig(
            type="kindex",
            url="https://kindex.example.com",
            timeout_seconds=10.0,
            noteworthiness=nf,
        )
        assert cfg

    def test_noteworthiness_filter_valid(self):
        nf = NoteworthinessFilter(
            min_event_count=1,
            min_duration_seconds=0.0,
            story_types=[],
            exclude_close_reasons=[],
        )
        assert nf.min_event_count == 1

    def test_noteworthiness_filter_min_event_count_zero(self):
        with pytest.raises(Exception):
            NoteworthinessFilter(
                min_event_count=0,
                min_duration_seconds=0.0,
                story_types=[],
                exclude_close_reasons=[],
            )

    def test_noteworthiness_filter_negative_duration(self):
        with pytest.raises(Exception):
            NoteworthinessFilter(
                min_event_count=1,
                min_duration_seconds=-1.0,
                story_types=[],
                exclude_close_reasons=[],
            )

    def test_memory_limits_config_valid(self):
        cfg = MemoryLimitsConfig(max_open_stories=10000, max_events_per_story=1000)
        assert cfg.max_open_stories == 10000

    def test_memory_limits_config_zero_stories(self):
        with pytest.raises(Exception):
            MemoryLimitsConfig(max_open_stories=0, max_events_per_story=1000)

    def test_memory_limits_config_zero_events(self):
        with pytest.raises(Exception):
            MemoryLimitsConfig(max_open_stories=1000, max_events_per_story=0)

    def test_memory_limits_config_max_boundary(self):
        cfg = MemoryLimitsConfig(max_open_stories=1000000, max_events_per_story=100000)
        assert cfg.max_open_stories == 1000000

    def test_memory_limits_config_over_max(self):
        with pytest.raises(Exception):
            MemoryLimitsConfig(max_open_stories=1000001, max_events_per_story=1000)

    # --- CorrelationRule ---
    def test_correlation_rule_valid(self):
        rule = CorrelationRule(
            name="ci.pipeline.rule",
            event_kinds=["ci.build.started", "ci.build.completed"],
            group_by=["correlation_keys.repo"],
            chain_by=[],
            terminal_event={"event_kind": "ci.build.completed"},
            timeout=300.0,
            story_type="ci.pipeline",
        )
        assert rule.name == "ci.pipeline.rule"

    def test_correlation_rule_frozen(self):
        rule = CorrelationRule(
            name="testRule",
            event_kinds=["ci.build.completed"],
            group_by=["correlation_keys.repo"],
            chain_by=[],
            terminal_event={"event_kind": "ci.build.completed"},
            timeout=300.0,
            story_type="ci.pipeline",
        )
        with pytest.raises(Exception):
            rule.name = "changed"

    def test_correlation_rule_invalid_name_starts_with_digit(self):
        with pytest.raises(Exception):
            CorrelationRule(
                name="1invalid",
                event_kinds=["ci.build.completed"],
                group_by=["correlation_keys.repo"],
                chain_by=[],
                terminal_event={},
                timeout=300.0,
                story_type="ci.pipeline",
            )

    def test_correlation_rule_timeout_too_low(self):
        with pytest.raises(Exception):
            CorrelationRule(
                name="testRule",
                event_kinds=["ci.build.completed"],
                group_by=["correlation_keys.repo"],
                chain_by=[],
                terminal_event={},
                timeout=0.0001,
                story_type="ci.pipeline",
            )

    def test_correlation_rule_empty_story_type(self):
        with pytest.raises(Exception):
            CorrelationRule(
                name="testRule",
                event_kinds=["ci.build.completed"],
                group_by=["correlation_keys.repo"],
                chain_by=[],
                terminal_event={},
                timeout=300.0,
                story_type="",
            )


class TestLoadChroniclerConfig:
    """Test load_chronicler_config with file fixtures."""

    def _write_valid_config(self, tmp_path):
        """Write a valid YAML config file and return its path."""
        config_content = """
sources:
  - type: webhook
    host: "0.0.0.0"
    port: 8080
    path: "/events"
    source_label: "main_webhook"
sinks:
  - type: disk
    events_path: "/data/events.jsonl"
    stories_path: "/data/stories.jsonl"
correlation_rules:
  - name: ciPipeline
    event_kinds:
      - "ci.build.started"
      - "ci.build.completed"
    group_by:
      - "correlation_keys.repo"
    chain_by: []
    terminal_event:
      event_kind: "ci.build.completed"
    timeout: 300.0
    story_type: "ci.pipeline"
memory_limits:
  max_open_stories: 10000
  max_events_per_story: 1000
state_path: "/data/state"
"""
        config_path = tmp_path / "config.yaml"
        config_path.write_text(config_content)
        return str(config_path)

    def test_happy_path(self, tmp_path):
        config_path = self._write_valid_config(tmp_path)
        config = load_chronicler_config(config_path)
        assert len(config.sources) >= 1
        assert len(config.sinks) >= 1
        assert len(config.correlation_rules) >= 1
        assert config.memory_limits.max_open_stories == 10000

    def test_file_not_found(self):
        with pytest.raises(Exception):
            load_chronicler_config("/nonexistent/path/config.yaml")

    def test_invalid_yaml(self, tmp_path):
        config_path = tmp_path / "bad.yaml"
        config_path.write_text("{{{{invalid yaml content!!!:::")
        with pytest.raises(Exception):
            load_chronicler_config(str(config_path))

    def test_empty_sources(self, tmp_path):
        config_content = """
sources: []
sinks:
  - type: disk
    events_path: "/data/events.jsonl"
    stories_path: "/data/stories.jsonl"
correlation_rules:
  - name: ciPipeline
    event_kinds: ["ci.build.started"]
    group_by: ["correlation_keys.repo"]
    chain_by: []
    terminal_event: {}
    timeout: 300.0
    story_type: "ci.pipeline"
memory_limits:
  max_open_stories: 10000
  max_events_per_story: 1000
state_path: "/data/state"
"""
        config_path = tmp_path / "config.yaml"
        config_path.write_text(config_content)
        with pytest.raises(Exception):
            load_chronicler_config(str(config_path))

    def test_empty_sinks(self, tmp_path):
        config_content = """
sources:
  - type: webhook
    host: "0.0.0.0"
    port: 8080
    path: "/events"
    source_label: "x"
sinks: []
correlation_rules:
  - name: ciPipeline
    event_kinds: ["ci.build.started"]
    group_by: ["correlation_keys.repo"]
    chain_by: []
    terminal_event: {}
    timeout: 300.0
    story_type: "ci.pipeline"
memory_limits:
  max_open_stories: 10000
  max_events_per_story: 1000
state_path: "/data/state"
"""
        config_path = tmp_path / "config.yaml"
        config_path.write_text(config_content)
        with pytest.raises(Exception):
            load_chronicler_config(str(config_path))

    def test_empty_rules(self, tmp_path):
        config_content = """
sources:
  - type: webhook
    host: "0.0.0.0"
    port: 8080
    path: "/events"
    source_label: "x"
sinks:
  - type: disk
    events_path: "/data/events.jsonl"
    stories_path: "/data/stories.jsonl"
correlation_rules: []
memory_limits:
  max_open_stories: 10000
  max_events_per_story: 1000
state_path: "/data/state"
"""
        config_path = tmp_path / "config.yaml"
        config_path.write_text(config_content)
        with pytest.raises(Exception):
            load_chronicler_config(str(config_path))

    def test_validation_error_invalid_port(self, tmp_path):
        config_content = """
sources:
  - type: webhook
    host: "0.0.0.0"
    port: 99999
    path: "/events"
    source_label: "x"
sinks:
  - type: disk
    events_path: "/data/events.jsonl"
    stories_path: "/data/stories.jsonl"
correlation_rules:
  - name: ciPipeline
    event_kinds: ["ci.build.started"]
    group_by: ["correlation_keys.repo"]
    chain_by: []
    terminal_event: {}
    timeout: 300.0
    story_type: "ci.pipeline"
memory_limits:
  max_open_stories: 10000
  max_events_per_story: 1000
state_path: "/data/state"
"""
        config_path = tmp_path / "config.yaml"
        config_path.write_text(config_content)
        with pytest.raises(Exception):
            load_chronicler_config(str(config_path))

    def test_empty_state_path(self, tmp_path):
        config_content = """
sources:
  - type: webhook
    host: "0.0.0.0"
    port: 8080
    path: "/events"
    source_label: "x"
sinks:
  - type: disk
    events_path: "/data/events.jsonl"
    stories_path: "/data/stories.jsonl"
correlation_rules:
  - name: ciPipeline
    event_kinds: ["ci.build.started"]
    group_by: ["correlation_keys.repo"]
    chain_by: []
    terminal_event: {}
    timeout: 300.0
    story_type: "ci.pipeline"
memory_limits:
  max_open_stories: 10000
  max_events_per_story: 1000
state_path: ""
"""
        config_path = tmp_path / "config.yaml"
        config_path.write_text(config_content)
        with pytest.raises(Exception):
            load_chronicler_config(str(config_path))


# ─── Test Enum Values ────────────────────────────────────────────────────────


class TestEnums:
    """Test enum types have the expected variants."""

    def test_source_type_variants(self):
        assert SourceType.webhook
        assert SourceType.otlp
        assert SourceType.file
        assert SourceType.sentinel

    def test_sink_type_variants(self):
        assert SinkType.stigmergy
        assert SinkType.apprentice
        assert SinkType.disk
        assert SinkType.kindex

    def test_story_status_variants(self):
        assert StoryStatus.open
        assert StoryStatus.closed

    def test_close_reason_variants(self):
        assert CloseReason.terminal
        assert CloseReason.timeout
        assert CloseReason.evicted


# ─── Test Model Immutability ────────────────────────────────────────────────


class TestModelImmutability:
    """Test that frozen models reject attribute mutation."""

    def test_event_frozen(self):
        event = make_valid_event()
        with pytest.raises(Exception):
            event.source = "changed"
        with pytest.raises(Exception):
            event.event_kind = "changed"
        with pytest.raises(Exception):
            event.id = "0" * 64

    def test_story_frozen(self):
        story = make_valid_open_story()
        with pytest.raises(Exception):
            story.status = StoryStatus.closed
        with pytest.raises(Exception):
            story.story_type = "changed"
        with pytest.raises(Exception):
            story.event_count = 99

    def test_correlation_rule_frozen(self):
        rule = CorrelationRule(
            name="testRule",
            event_kinds=["ci.build.completed"],
            group_by=["correlation_keys.repo"],
            chain_by=[],
            terminal_event={"event_kind": "ci.build.completed"},
            timeout=300.0,
            story_type="ci.pipeline",
        )
        with pytest.raises(Exception):
            rule.name = "changed"
        with pytest.raises(Exception):
            rule.timeout = 1.0

    def test_webhook_source_config_frozen(self):
        cfg = WebhookSourceConfig(
            type="webhook", host="0.0.0.0", port=8080, path="/events", source_label="x"
        )
        with pytest.raises(Exception):
            cfg.port = 9090

    def test_memory_limits_config_frozen(self):
        cfg = MemoryLimitsConfig(max_open_stories=10000, max_events_per_story=1000)
        with pytest.raises(Exception):
            cfg.max_open_stories = 5000


# ─── Test Invariants ────────────────────────────────────────────────────────


class TestInvariants:
    """Test system-wide invariants from the contract."""

    def test_event_id_is_sha256_of_canonical_json(self):
        """Event.id matches SHA-256 of canonical JSON of identity fields."""
        event = make_valid_event()
        # Manually compute the expected hash — must normalize timestamp
        # to UTC with 'Z' suffix, matching compute_event_id behavior.
        ts_str = NOW.isoformat()
        if ts_str.endswith("+00:00"):
            ts_str = ts_str[:-6] + "Z"
        identity = {
            "actor": "user@example.com",
            "context": {"status": "success", "count": 42},
            "correlation_keys": {"repo": "myrepo", "branch": "main"},
            "entity_id": "repo-123",
            "entity_kind": "repository",
            "event_kind": "ci.build.completed",
            "timestamp": ts_str,
        }
        canonical = json.dumps(
            identity,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            default=str,
        )
        expected_hash = hashlib.sha256(canonical.encode()).hexdigest()
        assert event.id == expected_hash

    def test_two_events_identical_identity_same_id(self):
        """Content-addressable: identical identity fields -> same id."""
        e1 = create_event(**make_valid_event_kwargs())
        e2 = create_event(**make_valid_event_kwargs())
        assert e1.id == e2.id

    def test_source_excluded_from_hash(self):
        """source field is provenance metadata, excluded from hash."""
        kwargs1 = make_valid_event_kwargs()
        kwargs1["source"] = "webhook"
        kwargs2 = make_valid_event_kwargs()
        kwargs2["source"] = "file"
        e1 = create_event(**kwargs1)
        e2 = create_event(**kwargs2)
        assert e1.id == e2.id

    def test_open_story_invariants(self):
        """Open story: status=open, end_ts=None, duration=None, close_reason=None."""
        story = make_valid_open_story()
        assert story.status == StoryStatus.open
        assert story.end_ts is None
        assert story.duration is None
        assert story.close_reason is None

    def test_closed_story_invariants(self):
        """Closed story: status=closed, end_ts set, duration >=0, close_reason set."""
        story = make_valid_closed_story()
        assert story.status == StoryStatus.closed
        assert story.end_ts is not None
        assert story.duration is not None
        assert story.duration >= 0.0
        assert story.close_reason is not None

    def test_event_count_equals_len_events(self):
        """Story.event_count always equals len(Story.events)."""
        story = make_valid_open_story()
        assert story.event_count == len(story.events)
        story2 = append_event_to_story(story, VALID_SHA256_B)
        assert story2.event_count == len(story2.events)

    def test_story_events_is_tuple(self):
        """Story.events is stored as a tuple."""
        story = make_valid_open_story()
        assert isinstance(story.events, tuple)

    def test_duration_matches_computation(self):
        """Story.duration equals (end_ts - start_ts).total_seconds()."""
        story = make_valid_open_story()
        closed = close_story(story, CloseReason.terminal, LATER)
        expected = (LATER - NOW).total_seconds()
        assert closed.duration == expected

    def test_story_model_copy_produces_new_instance(self):
        """model_copy(update=...) produces a distinct Story instance."""
        story = make_valid_open_story()
        new_story = append_event_to_story(story, VALID_SHA256_B)
        assert story is not new_story
        # Original is unchanged
        assert story.event_count == 1
        assert new_story.event_count == 2

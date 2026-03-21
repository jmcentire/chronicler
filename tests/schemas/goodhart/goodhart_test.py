"""
Adversarial hidden acceptance tests for Core Data Models (schemas).
These tests catch implementations that pass visible tests through shortcuts
rather than truly satisfying the contract.
"""
import json
import hashlib
import uuid
import re
from datetime import datetime, timezone, timedelta

import pytest

from chronicler.schemas import (
    EventKind, EntityKind, EntityId, ActorId, Sha256Hex, StoryId,
    AwareDatetime, ContextDict, CorrelationKeys, StoryStatus, CloseReason,
    TerminalEventPattern, EventKindSet, GroupByKeys, ChainByKeys,
    CorrelationRule, WebhookSourceConfig, OtlpSourceConfig, FileSourceConfig,
    SentinelSourceConfig, StigmergySinkConfig, ApprenticeSinkConfig,
    DiskSinkConfig, KindexSinkConfig, NoteworthinessFilter, MemoryLimitsConfig,
    compute_event_id, create_event, create_story, append_event_to_story,
    close_story, validate_story_consistency, matches_terminal_pattern,
    extract_group_key, event_to_dict, event_from_dict, story_to_dict,
    story_from_dict,
)


# ============================================================
# Helpers
# ============================================================
def _utc_now():
    return datetime.now(timezone.utc)

def _make_ts(year=2024, month=6, day=15, hour=12, minute=0, second=0, micro=0, tz=timezone.utc):
    return datetime(year, month, day, hour, minute, second, micro, tzinfo=tz)

def _valid_sha256():
    return hashlib.sha256(b"test").hexdigest()

def _another_sha256(seed=b"other"):
    return hashlib.sha256(seed).hexdigest()

def _make_open_story(**overrides):
    defaults = dict(
        story_type="ci_pipeline",
        correlation_rule="rule1",
        group_key={"repo": "myrepo"},
        first_event_id=_valid_sha256(),
        start_ts=_make_ts(),
        parent_story_id=None,
    )
    defaults.update(overrides)
    return create_story(**defaults)

def _make_event(**overrides):
    defaults = dict(
        event_kind="ci.build.completed",
        entity_kind="repository",
        entity_id="repo-1",
        actor="user-1",
        timestamp=_make_ts(),
        context={"status": "success"},
        correlation_keys={"repo": "myrepo"},
        source="webhook-1",
    )
    defaults.update(overrides)
    return create_event(**defaults)


# ============================================================
# compute_event_id
# ============================================================

class TestGoodhartComputeEventId:

    def test_goodhart_canonical_json_format(self):
        """The event ID must be computed from canonical JSON with sort_keys=True, compact separators,
        ensure_ascii=True, and timestamp serialized with 'Z' suffix — verifiable by independently hashing."""
        ts = _make_ts(2024, 1, 15, 10, 30, 0)
        event_kind = "ci.build.completed"
        entity_kind = "repository"
        entity_id = "repo-1"
        actor = "user-1"
        context = {"status": "success", "count": 42}
        correlation_keys = {"repo": "myrepo"}

        result = compute_event_id(
            event_kind=event_kind, entity_kind=entity_kind, entity_id=entity_id,
            actor=actor, timestamp=ts, context=context, correlation_keys=correlation_keys,
        )

        # Independently compute the expected hash
        ts_str = ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        if ts_str.endswith("+00:00"):
            ts_str = ts_str[:-6] + "Z"
        canonical_dict = {
            "actor": actor,
            "context": context,
            "correlation_keys": correlation_keys,
            "entity_id": entity_id,
            "entity_kind": entity_kind,
            "event_kind": event_kind,
            "timestamp": ts_str,
        }
        canonical_json = json.dumps(canonical_dict, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str)
        expected = hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()

        assert result == expected, f"Expected {expected}, got {result}"
        assert len(result) == 64
        assert re.match(r'^[0-9a-f]{64}$', result)

    def test_goodhart_non_utc_timezone_same_moment(self):
        """Timestamps in non-UTC timezones must produce the same event ID as the equivalent UTC timestamp."""
        ts_utc = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        ts_offset = datetime(2024, 6, 15, 17, 30, 0, tzinfo=timezone(timedelta(hours=5, minutes=30)))

        id_utc = compute_event_id(
            event_kind="ci.build", entity_kind="repo", entity_id="r1",
            actor="a1", timestamp=ts_utc, context={}, correlation_keys={},
        )
        id_offset = compute_event_id(
            event_kind="ci.build", entity_kind="repo", entity_id="r1",
            actor="a1", timestamp=ts_offset, context={}, correlation_keys={},
        )
        assert id_utc == id_offset

    def test_goodhart_context_none_vs_missing(self):
        """Context with {'key': None} must produce a different ID than empty context {}."""
        ts = _make_ts()
        base = dict(event_kind="test.event", entity_kind="entity", entity_id="id1", actor="actor1", timestamp=ts, correlation_keys={})

        id_with_none = compute_event_id(**base, context={"key": None})
        id_empty = compute_event_id(**base, context={})
        assert id_with_none != id_empty

    def test_goodhart_context_bool_vs_int(self):
        """Boolean True in context must produce different ID than integer 1."""
        ts = _make_ts()
        base = dict(event_kind="test.event", entity_kind="entity", entity_id="id1", actor="actor1", timestamp=ts, correlation_keys={})

        id_bool = compute_event_id(**base, context={"flag": True})
        id_int = compute_event_id(**base, context={"flag": 1})
        assert id_bool != id_int

    def test_goodhart_context_ordering_irrelevant(self):
        """Context dict key ordering must not affect the computed event ID."""
        ts = _make_ts()
        base = dict(event_kind="test.event", entity_kind="entity", entity_id="id1", actor="actor1", timestamp=ts, correlation_keys={})

        ctx_a = {"alpha": "1", "beta": "2", "gamma": "3"}
        ctx_b = {"gamma": "3", "alpha": "1", "beta": "2"}

        id_a = compute_event_id(**base, context=ctx_a)
        id_b = compute_event_id(**base, context=ctx_b)
        assert id_a == id_b

    def test_goodhart_correlation_keys_ordering_irrelevant(self):
        """Correlation keys dict ordering must not affect the computed event ID."""
        ts = _make_ts()
        base = dict(event_kind="test.event", entity_kind="entity", entity_id="id1", actor="actor1", timestamp=ts, context={})

        ck_a = {"repo": "myrepo", "branch": "main"}
        ck_b = {"branch": "main", "repo": "myrepo"}

        id_a = compute_event_id(**base, correlation_keys=ck_a)
        id_b = compute_event_id(**base, correlation_keys=ck_b)
        assert id_a == id_b

    def test_goodhart_unicode_in_context(self):
        """Unicode characters in context must be handled deterministically via ensure_ascii=True."""
        ts = _make_ts()
        base = dict(event_kind="test.event", entity_kind="entity", entity_id="id1", actor="actor1", timestamp=ts, correlation_keys={})

        ctx = {"message": "Hello 🌍 世界"}
        id1 = compute_event_id(**base, context=ctx)
        id2 = compute_event_id(**base, context=ctx)
        assert id1 == id2
        assert len(id1) == 64
        assert re.match(r'^[0-9a-f]{64}$', id1)

    def test_goodhart_empty_context_and_corr_keys(self):
        """Empty context and empty correlation_keys must produce a deterministic hash."""
        ts = _make_ts()
        result = compute_event_id(
            event_kind="test.event", entity_kind="entity", entity_id="id1",
            actor="actor1", timestamp=ts, context={}, correlation_keys={},
        )
        assert len(result) == 64
        assert re.match(r'^[0-9a-f]{64}$', result)

    def test_goodhart_float_context_value(self):
        """Float values in context must be serialized distinctly from their string representation."""
        ts = _make_ts()
        base = dict(event_kind="test.event", entity_kind="entity", entity_id="id1", actor="actor1", timestamp=ts, correlation_keys={})

        id_float = compute_event_id(**base, context={"score": 3.14})
        id_str = compute_event_id(**base, context={"score": "3.14"})
        assert id_float != id_str


# ============================================================
# EventKind / EntityKind boundary cases
# ============================================================

class TestGoodhartEventKindBoundary:

    def test_goodhart_rejects_hyphen(self):
        """EventKind pattern only allows lowercase alphanumeric, dots, and underscores — hyphens must be rejected."""
        with pytest.raises(Exception):
            # Depending on implementation, this could be a Pydantic ValidationError
            create_event(
                event_kind="ci-build", entity_kind="repo", entity_id="r1",
                actor="a1", timestamp=_make_ts(), context={}, correlation_keys={}, source="test"
            )

    def test_goodhart_rejects_space(self):
        """EventKind must reject strings containing whitespace characters."""
        with pytest.raises(Exception):
            create_event(
                event_kind="ci build", entity_kind="repo", entity_id="r1",
                actor="a1", timestamp=_make_ts(), context={}, correlation_keys={}, source="test"
            )

    def test_goodhart_single_char(self):
        """EventKind must accept a single lowercase letter as the minimum valid identifier."""
        event = create_event(
            event_kind="a", entity_kind="b", entity_id="r1",
            actor="a1", timestamp=_make_ts(), context={}, correlation_keys={}, source="test"
        )
        assert event.event_kind == "a"

    def test_goodhart_rejects_dot_start(self):
        """EventKind must start with a lowercase letter — dot as first character is invalid."""
        with pytest.raises(Exception):
            create_event(
                event_kind=".build.completed", entity_kind="repo", entity_id="r1",
                actor="a1", timestamp=_make_ts(), context={}, correlation_keys={}, source="test"
            )

    def test_goodhart_rejects_underscore_start(self):
        """EventKind must start with a lowercase letter — underscore as first character is invalid."""
        with pytest.raises(Exception):
            create_event(
                event_kind="_build", entity_kind="repo", entity_id="r1",
                actor="a1", timestamp=_make_ts(), context={}, correlation_keys={}, source="test"
            )

    def test_goodhart_allows_trailing_dots(self):
        """EventKind regex allows dots in non-first positions."""
        event = create_event(
            event_kind="ci.build.", entity_kind="repo", entity_id="r1",
            actor="a1", timestamp=_make_ts(), context={}, correlation_keys={}, source="test"
        )
        assert event.event_kind == "ci.build."

    def test_goodhart_entity_kind_rejects_hyphen(self):
        """EntityKind follows the same pattern as EventKind — hyphens must be rejected."""
        with pytest.raises(Exception):
            create_event(
                event_kind="ci.build", entity_kind="my-repo", entity_id="r1",
                actor="a1", timestamp=_make_ts(), context={}, correlation_keys={}, source="test"
            )


# ============================================================
# ActorId boundary
# ============================================================

class TestGoodhartActorIdBoundary:

    def test_goodhart_max_length(self):
        """ActorId must accept strings at the maximum length boundary of 512 characters."""
        long_actor = "a" * 512
        event = create_event(
            event_kind="ci.build", entity_kind="repo", entity_id="r1",
            actor=long_actor, timestamp=_make_ts(), context={}, correlation_keys={}, source="test"
        )
        assert event.actor == long_actor

    def test_goodhart_over_max(self):
        """ActorId must reject strings exceeding 512 characters."""
        with pytest.raises(Exception):
            create_event(
                event_kind="ci.build", entity_kind="repo", entity_id="r1",
                actor="a" * 513, timestamp=_make_ts(), context={}, correlation_keys={}, source="test"
            )


# ============================================================
# Sha256Hex boundary
# ============================================================

class TestGoodhartSha256Boundary:

    def test_goodhart_non_hex_chars(self):
        """Sha256Hex must reject strings of correct length containing non-hex characters."""
        with pytest.raises(Exception):
            # 'g' is not a valid hex character
            create_story(
                story_type="test", correlation_rule="rule1",
                group_key={"k": "v"},
                first_event_id="g" * 64,
                start_ts=_make_ts(), parent_story_id=None,
            )


# ============================================================
# StoryId UUID version
# ============================================================

class TestGoodhartStoryIdVersion:

    def test_goodhart_uuid_v1_rejected(self):
        """StoryId must only accept UUID version 4 — UUID v1 must be rejected."""
        uuid_v1_str = str(uuid.uuid1())
        # UUID v1 has version digit '1' in 13th char of the UUID
        with pytest.raises(Exception):
            story_from_dict({
                "story_id": uuid_v1_str,
                "story_type": "test",
                "correlation_rule": "rule1",
                "group_key": {"k": "v"},
                "events": [_valid_sha256()],
                "status": "open",
                "start_ts": _make_ts().isoformat(),
                "end_ts": None,
                "duration": None,
                "event_count": 1,
                "close_reason": None,
                "parent_story_id": None,
            })


# ============================================================
# ContextDict boundary
# ============================================================

class TestGoodhartContextDictBoundary:

    def test_goodhart_rejects_nested_dict(self):
        """ContextDict must reject dict values (nested dicts)."""
        with pytest.raises(Exception):
            create_event(
                event_kind="ci.build", entity_kind="repo", entity_id="r1",
                actor="a1", timestamp=_make_ts(), context={"inner": {"nested": "val"}},
                correlation_keys={}, source="test"
            )

    def test_goodhart_accepts_all_primitive_types(self):
        """ContextDict must accept str, int, float, bool, and None simultaneously."""
        ctx = {"s": "val", "i": 42, "f": 3.14, "b": True, "n": None}
        event = create_event(
            event_kind="ci.build", entity_kind="repo", entity_id="r1",
            actor="a1", timestamp=_make_ts(), context=ctx,
            correlation_keys={}, source="test"
        )
        assert event.context["s"] == "val"
        assert event.context["i"] == 42
        assert event.context["f"] == 3.14
        assert event.context["b"] is True
        assert event.context["n"] is None


# ============================================================
# CorrelationKeys boundary
# ============================================================

class TestGoodhartCorrelationKeysBoundary:

    def test_goodhart_rejects_non_string_value(self):
        """CorrelationKeys must reject integer values — only non-empty string values allowed."""
        with pytest.raises(Exception):
            create_event(
                event_kind="ci.build", entity_kind="repo", entity_id="r1",
                actor="a1", timestamp=_make_ts(), context={},
                correlation_keys={"repo": 123}, source="test"
            )


# ============================================================
# create_event
# ============================================================

class TestGoodhartCreateEvent:

    def test_goodhart_id_matches_compute(self):
        """Event.id must exactly match compute_event_id with the same identity fields."""
        ts = _make_ts()
        ctx = {"env": "prod", "count": 5}
        ck = {"repo": "myrepo", "branch": "main"}

        event = create_event(
            event_kind="ci.deploy.started", entity_kind="pipeline",
            entity_id="pipe-42", actor="deploy-bot", timestamp=ts,
            context=ctx, correlation_keys=ck, source="webhook",
        )

        computed = compute_event_id(
            event_kind="ci.deploy.started", entity_kind="pipeline",
            entity_id="pipe-42", actor="deploy-bot", timestamp=ts,
            context=ctx, correlation_keys=ck,
        )

        assert event.id == computed

    def test_goodhart_with_rich_context(self):
        """Event with context containing multiple value types must compute a deterministic ID."""
        ts = _make_ts()
        ctx = {"msg": "ok", "count": 10, "ratio": 0.95, "active": False, "extra": None}

        e1 = create_event(
            event_kind="ci.test", entity_kind="repo", entity_id="r1",
            actor="bot", timestamp=ts, context=ctx, correlation_keys={}, source="s1",
        )
        e2 = create_event(
            event_kind="ci.test", entity_kind="repo", entity_id="r1",
            actor="bot", timestamp=ts, context=ctx, correlation_keys={}, source="s2",
        )
        assert e1.id == e2.id

    def test_goodhart_empty_context_and_corr_keys(self):
        """create_event must accept empty context dict and empty correlation_keys dict."""
        event = create_event(
            event_kind="ci.build", entity_kind="repo", entity_id="r1",
            actor="a1", timestamp=_make_ts(), context={}, correlation_keys={}, source="test",
        )
        assert len(event.id) == 64
        assert re.match(r'^[0-9a-f]{64}$', event.id)

    def test_goodhart_context_special_json_chars(self):
        """Events with context values containing JSON-special characters must produce deterministic IDs."""
        ts = _make_ts()
        ctx = {"msg": 'He said "hello"', "path": "C:\\Users\\test"}
        e1 = create_event(
            event_kind="ci.build", entity_kind="repo", entity_id="r1",
            actor="a1", timestamp=ts, context=ctx, correlation_keys={}, source="s1",
        )
        e2 = create_event(
            event_kind="ci.build", entity_kind="repo", entity_id="r1",
            actor="a1", timestamp=ts, context=ctx, correlation_keys={}, source="s2",
        )
        assert e1.id == e2.id


# ============================================================
# create_story
# ============================================================

class TestGoodhartCreateStory:

    def test_goodhart_unique_ids(self):
        """Each call to create_story must produce a unique UUID4 story_id."""
        eid = _valid_sha256()
        ts = _make_ts()
        s1 = create_story(story_type="test", correlation_rule="r1", group_key={"k": "v"}, first_event_id=eid, start_ts=ts, parent_story_id=None)
        s2 = create_story(story_type="test", correlation_rule="r1", group_key={"k": "v"}, first_event_id=eid, start_ts=ts, parent_story_id=None)
        assert s1.story_id != s2.story_id

    def test_goodhart_with_parent(self):
        """create_story must correctly store a non-None parent_story_id."""
        parent_id = str(uuid.uuid4())
        story = create_story(
            story_type="test", correlation_rule="r1",
            group_key={"k": "v"}, first_event_id=_valid_sha256(),
            start_ts=_make_ts(), parent_story_id=parent_id,
        )
        assert story.parent_story_id == parent_id

    def test_goodhart_preserves_group_key(self):
        """create_story must store the provided group_key unchanged."""
        gk = {"repo": "myrepo", "branch": "main", "env": "prod"}
        story = create_story(
            story_type="test", correlation_rule="r1",
            group_key=gk, first_event_id=_valid_sha256(),
            start_ts=_make_ts(), parent_story_id=None,
        )
        assert dict(story.group_key) == gk

    def test_goodhart_start_ts_preserved(self):
        """create_story must store the provided start_ts unchanged."""
        ts = _make_ts(2023, 3, 14, 9, 26, 53)
        story = create_story(
            story_type="test", correlation_rule="r1",
            group_key={"k": "v"}, first_event_id=_valid_sha256(),
            start_ts=ts, parent_story_id=None,
        )
        assert story.start_ts == ts


# ============================================================
# append_event_to_story
# ============================================================

class TestGoodhartAppendEvent:

    def test_goodhart_multiple_appends(self):
        """Appending multiple distinct events produces correct ordered tuple and count."""
        story = _make_open_story()
        ids = [hashlib.sha256(f"event-{i}".encode()).hexdigest() for i in range(3)]
        for eid in ids:
            story = append_event_to_story(story, eid)

        assert story.event_count == 4  # 1 initial + 3 appended
        assert len(story.events) == 4
        # Check ordering: initial + appended in order
        for i, eid in enumerate(ids):
            assert story.events[i + 1] == eid

    def test_goodhart_preserves_all_fields(self):
        """Appending an event must not alter any Story fields other than events and event_count."""
        story = _make_open_story()
        new_id = _another_sha256(b"new-event")
        updated = append_event_to_story(story, new_id)

        assert updated.story_id == story.story_id
        assert updated.story_type == story.story_type
        assert updated.correlation_rule == story.correlation_rule
        assert updated.group_key == story.group_key
        assert updated.status == story.status
        assert updated.start_ts == story.start_ts
        assert updated.end_ts == story.end_ts
        assert updated.close_reason == story.close_reason
        assert updated.parent_story_id == story.parent_story_id
        assert updated.duration == story.duration


# ============================================================
# close_story
# ============================================================

class TestGoodhartCloseStory:

    def test_goodhart_timeout_reason(self):
        """close_story must accept 'timeout' as a valid close_reason."""
        story = _make_open_story()
        end = story.start_ts + timedelta(hours=1)
        closed = close_story(story, "timeout", end)
        assert closed.close_reason == "timeout"
        assert closed.status == "closed"

    def test_goodhart_evicted_reason(self):
        """close_story must accept 'evicted' as a valid close_reason."""
        story = _make_open_story()
        end = story.start_ts + timedelta(minutes=5)
        closed = close_story(story, "evicted", end)
        assert closed.close_reason == "evicted"
        assert closed.status == "closed"

    def test_goodhart_duration_precision(self):
        """close_story must accurately compute sub-second duration."""
        ts_start = _make_ts(2024, 6, 15, 12, 0, 0, 0)
        story = _make_open_story(start_ts=ts_start)
        end = ts_start + timedelta(milliseconds=500)
        closed = close_story(story, "terminal", end)
        assert closed.duration == pytest.approx(0.5)

    def test_goodhart_large_duration(self):
        """close_story must correctly compute duration for 24-hour spans."""
        ts_start = _make_ts(2024, 6, 15, 0, 0, 0)
        story = _make_open_story(start_ts=ts_start)
        end = ts_start + timedelta(hours=24)
        closed = close_story(story, "terminal", end)
        assert closed.duration == pytest.approx(86400.0)

    def test_goodhart_preserves_events(self):
        """Closing a story must not modify events or event_count."""
        story = _make_open_story()
        eid2 = _another_sha256(b"evt2")
        eid3 = _another_sha256(b"evt3")
        story = append_event_to_story(story, eid2)
        story = append_event_to_story(story, eid3)

        original_events = story.events
        original_count = story.event_count

        end = story.start_ts + timedelta(hours=1)
        closed = close_story(story, "terminal", end)

        assert closed.events == original_events
        assert closed.event_count == original_count

    def test_goodhart_cross_timezone_duration(self):
        """Duration must be correctly computed when start and end are in different timezones."""
        ts_start = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        # End at UTC+5, 18:00 which is 13:00 UTC — 1 hour after start
        ts_end = datetime(2024, 6, 15, 18, 0, 0, tzinfo=timezone(timedelta(hours=5)))
        story = _make_open_story(start_ts=ts_start)
        closed = close_story(story, "terminal", ts_end)
        assert closed.duration == pytest.approx(3600.0)


# ============================================================
# matches_terminal_pattern
# ============================================================

class TestGoodhartMatchesTerminal:

    def test_goodhart_entity_id_field(self):
        """matches_terminal_pattern must support matching on entity_id."""
        event = _make_event(entity_id="repo-123")
        assert matches_terminal_pattern(event, {"entity_id": "repo-123"}) is True
        assert matches_terminal_pattern(event, {"entity_id": "repo-999"}) is False

    def test_goodhart_actor_field(self):
        """matches_terminal_pattern must support matching on actor."""
        event = _make_event(actor="user-x")
        assert matches_terminal_pattern(event, {"actor": "user-x"}) is True
        assert matches_terminal_pattern(event, {"actor": "user-y"}) is False

    def test_goodhart_entity_kind_field(self):
        """matches_terminal_pattern must support matching on entity_kind."""
        event = _make_event(entity_kind="repository")
        assert matches_terminal_pattern(event, {"entity_kind": "repository"}) is True
        assert matches_terminal_pattern(event, {"entity_kind": "pipeline"}) is False

    def test_goodhart_multi_partial_mismatch(self):
        """When a pattern has multiple fields and only some match, must return False."""
        event = _make_event(event_kind="ci.build.completed", context={"status": "success"})
        pattern = {"event_kind": "ci.build.completed", "context.status": "failed"}
        assert matches_terminal_pattern(event, pattern) is False

    def test_goodhart_missing_correlation_key(self):
        """Pattern referencing a missing correlation_keys subkey must return False."""
        event = _make_event(correlation_keys={"repo": "myrepo"})
        assert matches_terminal_pattern(event, {"correlation_keys.nonexistent": "val"}) is False

    def test_goodhart_invalid_field_path(self):
        """matches_terminal_pattern must raise error for unrecognized field paths."""
        event = _make_event()
        with pytest.raises(Exception):
            matches_terminal_pattern(event, {"unknown_field": "val"})


# ============================================================
# extract_group_key
# ============================================================

class TestGoodhartExtractGroupKey:

    def test_goodhart_context_field(self):
        """extract_group_key must support extracting values from context.<key>."""
        event = _make_event(context={"env": "prod"})
        result = extract_group_key(event, ("context.env",))
        assert result == {"context.env": "prod"}

    def test_goodhart_multiple_fields(self):
        """extract_group_key must return exactly len(group_by) entries for multiple field paths."""
        event = _make_event(
            event_kind="ci.build",
            correlation_keys={"repo": "myrepo", "branch": "main"},
            context={"env": "prod"},
        )
        result = extract_group_key(event, ("correlation_keys.repo", "correlation_keys.branch", "context.env"))
        assert len(result) == 3
        assert result["correlation_keys.repo"] == "myrepo"
        assert result["correlation_keys.branch"] == "main"
        assert result["context.env"] == "prod"

    def test_goodhart_entity_kind_field(self):
        """extract_group_key must support extracting entity_kind as a top-level field."""
        event = _make_event(entity_kind="pipeline")
        result = extract_group_key(event, ("entity_kind",))
        assert result == {"entity_kind": "pipeline"}


# ============================================================
# event_to_dict / event_from_dict
# ============================================================

class TestGoodhartEventSerialization:

    def test_goodhart_contains_all_fields(self):
        """event_to_dict must include all Event fields."""
        event = _make_event()
        d = event_to_dict(event)
        for key in ["id", "event_kind", "entity_kind", "entity_id", "actor", "timestamp", "context", "correlation_keys", "source"]:
            assert key in d, f"Missing key: {key}"

    def test_goodhart_json_serializable(self):
        """event_to_dict output must be JSON-serializable."""
        event = _make_event(context={"s": "val", "i": 42, "f": 3.14, "b": True, "n": None})
        d = event_to_dict(event)
        # This should not raise
        json_str = json.dumps(d)
        assert isinstance(json_str, str)

    def test_goodhart_from_dict_with_correct_id(self):
        """event_from_dict must accept a dict where the 'id' matches the computed hash."""
        event = _make_event()
        d = event_to_dict(event)
        # d already has the correct 'id'
        restored = event_from_dict(d)
        assert restored.id == event.id

    def test_goodhart_from_dict_preserves_source(self):
        """event_from_dict must preserve the 'source' field."""
        event = _make_event(source="my-webhook-source")
        d = event_to_dict(event)
        restored = event_from_dict(d)
        assert restored.source == "my-webhook-source"


# ============================================================
# story_to_dict / story_from_dict
# ============================================================

class TestGoodhartStorySerialization:

    def test_goodhart_open_story_null_fields(self):
        """story_to_dict for an open story must serialize end_ts, duration, close_reason as None."""
        story = _make_open_story()
        d = story_to_dict(story)
        assert d["end_ts"] is None
        assert d["duration"] is None
        assert d["close_reason"] is None

    def test_goodhart_events_is_list(self):
        """story_to_dict must serialize events as a list."""
        story = _make_open_story()
        eid2 = _another_sha256(b"extra")
        story = append_event_to_story(story, eid2)
        d = story_to_dict(story)
        assert isinstance(d["events"], list)
        assert len(d["events"]) == 2
        for eid in d["events"]:
            assert re.match(r'^[0-9a-f]{64}$', eid)

    def test_goodhart_closed_story_round_trip(self):
        """A closed Story must survive round-trip serialization."""
        story = _make_open_story()
        end = story.start_ts + timedelta(hours=2)
        closed = close_story(story, "terminal", end)

        d = story_to_dict(closed)
        restored = story_from_dict(d)

        assert restored.status == "closed"
        assert restored.close_reason == "terminal"
        assert restored.event_count == closed.event_count
        assert restored.duration == pytest.approx(closed.duration)
        assert len(restored.events) == len(closed.events)
        assert restored.story_id == closed.story_id

    def test_goodhart_from_dict_valid_closed(self):
        """story_from_dict must accept a valid closed story dict."""
        story = _make_open_story()
        end = story.start_ts + timedelta(minutes=30)
        closed = close_story(story, "timeout", end)
        d = story_to_dict(closed)
        restored = story_from_dict(d)
        assert restored.status == "closed"
        assert restored.close_reason == "timeout"

    def test_goodhart_from_dict_closed_missing_duration(self):
        """story_from_dict must reject a closed story dict with duration=None."""
        story = _make_open_story()
        end = story.start_ts + timedelta(hours=1)
        closed = close_story(story, "terminal", end)
        d = story_to_dict(closed)
        d["duration"] = None  # violate lifecycle invariant
        with pytest.raises(Exception):
            story_from_dict(d)


# ============================================================
# Config boundary tests
# ============================================================

class TestGoodhartConfigBoundary:

    def test_goodhart_webhook_port_max(self):
        """WebhookSourceConfig must accept port 65535."""
        cfg = WebhookSourceConfig(type="webhook", host="0.0.0.0", port=65535, path="/hook", source_label="wh")
        assert cfg.port == 65535

    def test_goodhart_webhook_port_over_max(self):
        """WebhookSourceConfig must reject port 65536."""
        with pytest.raises(Exception):
            WebhookSourceConfig(type="webhook", host="0.0.0.0", port=65536, path="/hook", source_label="wh")

    def test_goodhart_file_poll_max(self):
        """FileSourceConfig must accept poll_interval_seconds at 3600."""
        cfg = FileSourceConfig(type="file", path="/var/log/events.jsonl", poll_interval_seconds=3600.0, source_label="f1")
        assert cfg.poll_interval_seconds == 3600.0

    def test_goodhart_file_poll_over_max(self):
        """FileSourceConfig must reject poll_interval_seconds exceeding 3600."""
        with pytest.raises(Exception):
            FileSourceConfig(type="file", path="/var/log/events.jsonl", poll_interval_seconds=3601.0, source_label="f1")

    def test_goodhart_stigmergy_timeout_lower_boundary(self):
        """StigmergySinkConfig must accept timeout_seconds=0.5 and reject 0.4."""
        cfg = StigmergySinkConfig(type="stigmergy", url="https://stig.example.com", timeout_seconds=0.5)
        assert cfg.timeout_seconds == 0.5

        with pytest.raises(Exception):
            StigmergySinkConfig(type="stigmergy", url="https://stig.example.com", timeout_seconds=0.4)

    def test_goodhart_stigmergy_timeout_over_max(self):
        """StigmergySinkConfig must reject timeout_seconds exceeding 300."""
        with pytest.raises(Exception):
            StigmergySinkConfig(type="stigmergy", url="https://stig.example.com", timeout_seconds=301.0)

    def test_goodhart_memory_limits_max_boundary(self):
        """MemoryLimitsConfig must accept max_open_stories=1000000, max_events_per_story=100000."""
        cfg = MemoryLimitsConfig(max_open_stories=1000000, max_events_per_story=100000)
        assert cfg.max_open_stories == 1000000
        assert cfg.max_events_per_story == 100000

    def test_goodhart_memory_limits_stories_over_max(self):
        """MemoryLimitsConfig must reject max_open_stories=1000001."""
        with pytest.raises(Exception):
            MemoryLimitsConfig(max_open_stories=1000001, max_events_per_story=100)

    def test_goodhart_memory_limits_events_over_max(self):
        """MemoryLimitsConfig must reject max_events_per_story=100001."""
        with pytest.raises(Exception):
            MemoryLimitsConfig(max_open_stories=100, max_events_per_story=100001)

    def test_goodhart_disk_sink_empty_stories_path(self):
        """DiskSinkConfig must reject empty stories_path."""
        with pytest.raises(Exception):
            DiskSinkConfig(type="disk", events_path="/data/events.jsonl", stories_path="")

    def test_goodhart_apprentice_sink_valid(self):
        """ApprenticeSinkConfig must accept valid URL and timeout."""
        cfg = ApprenticeSinkConfig(type="apprentice", url="https://apprentice.example.com", timeout_seconds=30.0)
        assert cfg.url == "https://apprentice.example.com"

    def test_goodhart_apprentice_sink_invalid_url(self):
        """ApprenticeSinkConfig must reject URLs not starting with http(s)://."""
        with pytest.raises(Exception):
            ApprenticeSinkConfig(type="apprentice", url="ftp://invalid", timeout_seconds=30.0)

    def test_goodhart_otlp_source_valid(self):
        """OtlpSourceConfig must accept valid configuration."""
        cfg = OtlpSourceConfig(type="otlp", host="0.0.0.0", port=4317, source_label="otel")
        assert cfg.port == 4317

    def test_goodhart_sentinel_source_valid(self):
        """SentinelSourceConfig must accept valid configuration."""
        cfg = SentinelSourceConfig(type="sentinel", host="0.0.0.0", port=8443, source_label="sentinel-1")
        assert cfg.port == 8443

    def test_goodhart_noteworthiness_negative_duration(self):
        """NoteworthinessFilter must reject negative min_duration_seconds."""
        with pytest.raises(Exception):
            NoteworthinessFilter(min_event_count=1, min_duration_seconds=-1.0, story_types=[], exclude_close_reasons=[])

    def test_goodhart_webhook_path_underscore_hyphen(self):
        """WebhookSourceConfig path must accept underscores and hyphens."""
        cfg = WebhookSourceConfig(type="webhook", host="0.0.0.0", port=8080, path="/my_webhook-endpoint/v2", source_label="wh")
        assert cfg.path == "/my_webhook-endpoint/v2"

    def test_goodhart_webhook_path_rejects_query_params(self):
        """WebhookSourceConfig path must reject paths with query parameters."""
        with pytest.raises(Exception):
            WebhookSourceConfig(type="webhook", host="0.0.0.0", port=8080, path="/webhooks?key=val", source_label="wh")


# ============================================================
# CorrelationRule boundary
# ============================================================

class TestGoodhartCorrelationRule:

    def test_goodhart_name_special_chars(self):
        """CorrelationRule name must accept dots and hyphens."""
        rule = CorrelationRule(
            name="my-rule.v2",
            event_kinds=["ci.build"],
            group_by=("correlation_keys.repo",),
            chain_by=(),
            terminal_event={"event_kind": "ci.build.completed"},
            timeout=60.0,
            story_type="ci_pipeline",
        )
        assert rule.name == "my-rule.v2"

    def test_goodhart_name_max_length(self):
        """CorrelationRule name must accept names up to 255 characters."""
        name = "a" + "b" * 254  # 255 chars starting with letter
        rule = CorrelationRule(
            name=name,
            event_kinds=["ci.build"],
            group_by=("correlation_keys.repo",),
            chain_by=(),
            terminal_event={"event_kind": "ci.build.completed"},
            timeout=60.0,
            story_type="ci_pipeline",
        )
        assert rule.name == name

    def test_goodhart_name_over_max_length(self):
        """CorrelationRule name must reject names exceeding 255 characters."""
        name = "a" + "b" * 255  # 256 chars
        with pytest.raises(Exception):
            CorrelationRule(
                name=name,
                event_kinds=["ci.build"],
                group_by=("correlation_keys.repo",),
                chain_by=(),
                terminal_event={"event_kind": "ci.build.completed"},
                timeout=60.0,
                story_type="ci_pipeline",
            )

    def test_goodhart_frozen(self):
        """CorrelationRule must be frozen — attribute assignment must raise error."""
        rule = CorrelationRule(
            name="myRule",
            event_kinds=["ci.build"],
            group_by=("correlation_keys.repo",),
            chain_by=(),
            terminal_event={},
            timeout=60.0,
            story_type="ci_pipeline",
        )
        with pytest.raises(Exception):
            rule.timeout = 120.0


# ============================================================
# Config: duplicate rule names
# ============================================================

class TestGoodhartConfigDuplicateRules:

    def test_goodhart_duplicate_rule_names(self, tmp_path):
        """ChroniclerConfig must reject configurations with duplicate CorrelationRule names."""
        import yaml
        config = {
            "sources": [
                {"type": "webhook", "host": "0.0.0.0", "port": 8080, "path": "/hook", "source_label": "wh1"}
            ],
            "sinks": [
                {"type": "disk", "events_path": "/data/events.jsonl", "stories_path": "/data/stories.jsonl"}
            ],
            "correlation_rules": [
                {
                    "name": "myRule",
                    "event_kinds": ["ci.build"],
                    "group_by": ["correlation_keys.repo"],
                    "chain_by": [],
                    "terminal_event": {"event_kind": "ci.build.completed"},
                    "timeout": 60.0,
                    "story_type": "ci_pipeline",
                },
                {
                    "name": "myRule",  # duplicate!
                    "event_kinds": ["ci.deploy"],
                    "group_by": ["correlation_keys.repo"],
                    "chain_by": [],
                    "terminal_event": {"event_kind": "ci.deploy.completed"},
                    "timeout": 120.0,
                    "story_type": "deploy_pipeline",
                },
            ],
            "memory_limits": {"max_open_stories": 1000, "max_events_per_story": 100},
            "state_path": "/data/state",
        }

        config_file = tmp_path / "config.yaml"
        config_file.write_text(yaml.dump(config))

        from chronicler.schemas import load_chronicler_config
        with pytest.raises(Exception):
            load_chronicler_config(str(config_file))

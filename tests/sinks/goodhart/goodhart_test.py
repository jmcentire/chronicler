"""
Hidden adversarial acceptance tests for the Story Sinks component.

These tests target gaps in visible test coverage to catch implementations
that pass visible tests through shortcuts (hardcoded returns, missing validation, etc.)
"""

import asyncio
import json
import logging
import os
import tempfile
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

# Import everything from the sinks module
from chronicler.sinks.disk import DiskSink, DiskSinkConfig, read_stories
from chronicler.sinks.kindex import KindexSinkConfig, is_noteworthy
from chronicler.sinks.stigmergy import StigmergySink
from chronicler.sinks.apprentice import ApprenticeSink
from chronicler.sinks.types import (
    FilePath, PositiveInt, NonNegativeFloat,
    SinkEmitResult, SafeEmitOutcome,
)
from chronicler.sinks.engine import safe_emit


# ─── Helpers ───────────────────────────────────────────────────────────────────

def make_story(
    story_id="test-story-001",
    story_type="http_request",
    events=None,
    opened_at=None,
    closed_at=None,
    metadata=None,
):
    """Helper to construct a Story-like object for testing."""
    if events is None:
        events = [{"event": "e1"}, {"event": "e2"}, {"event": "e3"}]
    if opened_at is None:
        opened_at = "2024-01-01T00:00:00+00:00"
    if closed_at is None:
        closed_at = "2024-01-01T00:10:00+00:00"
    if metadata is None:
        metadata = {}
    # Try to use the actual Story class if available
    try:
        from chronicler.schemas import Story as SchemaStory
        return SchemaStory(
            story_id=story_id,
            story_type=story_type,
            events=events,
            opened_at=opened_at,
            closed_at=closed_at,
            metadata=metadata,
        )
    except (ImportError, Exception):
        # Fallback: create a simple namespace object
        class _Story:
            def __init__(self, **kwargs):
                for k, v in kwargs.items():
                    setattr(self, k, v)
            def model_dump(self):
                return {
                    "story_id": self.story_id,
                    "story_type": self.story_type,
                    "events": self.events,
                    "opened_at": self.opened_at,
                    "closed_at": self.closed_at,
                    "metadata": self.metadata,
                }
            def model_dump_json(self):
                return json.dumps(self.model_dump())
        return _Story(
            story_id=story_id,
            story_type=story_type,
            events=events,
            opened_at=opened_at,
            closed_at=closed_at,
            metadata=metadata,
        )


def make_kindex_config(story_types=None, min_event_count=1, min_duration_seconds=0.0):
    """Helper to construct a KindexSinkConfig."""
    return KindexSinkConfig(
        story_types=story_types,
        min_event_count=min_event_count,
        min_duration_seconds=min_duration_seconds,
    )


def make_disk_config(path, flush_on_write=False):
    """Helper to construct a DiskSinkConfig."""
    return DiskSinkConfig(path=path, flush_on_write=flush_on_write)


# ─── is_noteworthy tests ──────────────────────────────────────────────────────

def test_goodhart_noteworthy_all_fail():
    """is_noteworthy must return False when ALL three conditions fail simultaneously."""
    config = make_kindex_config(
        story_types={"database", "grpc"},
        min_event_count=10,
        min_duration_seconds=300.0,
    )
    story = make_story(
        story_type="http_request",  # not in set
        events=[{"e": 1}],  # 1 < 10
        opened_at="2024-01-01T00:00:00+00:00",
        closed_at="2024-01-01T00:00:30+00:00",  # 30s < 300s
    )
    assert is_noteworthy(story, config) is False


def test_goodhart_noteworthy_duration_fractional():
    """is_noteworthy correctly computes sub-second duration differences."""
    config = make_kindex_config(
        story_types=None,
        min_event_count=1,
        min_duration_seconds=1.5,
    )
    # Duration = 1.0s < 1.5s → False
    story_below = make_story(
        events=[{"e": 1}],
        opened_at="2024-06-15T12:00:00.000+00:00",
        closed_at="2024-06-15T12:00:01.000+00:00",
    )
    assert is_noteworthy(story_below, config) is False

    # Duration = 1.6s >= 1.5s → True
    story_above = make_story(
        events=[{"e": 1}],
        opened_at="2024-06-15T12:00:00.000+00:00",
        closed_at="2024-06-15T12:00:01.600+00:00",
    )
    assert is_noteworthy(story_above, config) is True


def test_goodhart_noteworthy_large_event_count():
    """is_noteworthy handles large event counts and high thresholds correctly."""
    config = make_kindex_config(
        story_types=None,
        min_event_count=100,
        min_duration_seconds=0.0,
    )
    events_100 = [{"e": i} for i in range(100)]
    events_99 = [{"e": i} for i in range(99)]

    story_exact = make_story(events=events_100)
    story_below = make_story(events=events_99)

    assert is_noteworthy(story_exact, config) is True
    assert is_noteworthy(story_below, config) is False


def test_goodhart_noteworthy_zero_duration():
    """is_noteworthy handles zero-duration stories with min_duration_seconds=0.0."""
    config = make_kindex_config(
        story_types=None,
        min_event_count=1,
        min_duration_seconds=0.0,
    )
    ts = "2024-01-01T00:00:00+00:00"
    story = make_story(events=[{"e": 1}], opened_at=ts, closed_at=ts)
    assert is_noteworthy(story, config) is True


def test_goodhart_noteworthy_timezone_aware():
    """is_noteworthy correctly computes duration from timezone-aware timestamps."""
    config = make_kindex_config(
        story_types=None,
        min_event_count=1,
        min_duration_seconds=3600.0,  # 1 hour
    )
    # These are the same absolute instant separated by 2 hours,
    # but expressed in different timezones
    story = make_story(
        events=[{"e": 1}],
        opened_at="2024-01-01T10:00:00+05:00",   # = 05:00 UTC
        closed_at="2024-01-01T08:00:00+01:00",    # = 07:00 UTC → 2 hours later
    )
    assert is_noteworthy(story, config) is True


def test_goodhart_noteworthy_empty_story_types_set():
    """is_noteworthy returns False when story_types is an empty set (not None)."""
    config = make_kindex_config(
        story_types=set(),  # empty set → nothing matches
        min_event_count=1,
        min_duration_seconds=0.0,
    )
    story = make_story(events=[{"e": 1}])
    assert is_noteworthy(story, config) is False


def test_goodhart_noteworthy_multiple_story_types():
    """is_noteworthy checks membership in multi-element story_types sets."""
    config = make_kindex_config(
        story_types={"http_request", "grpc_call", "database_query", "websocket"},
        min_event_count=1,
        min_duration_seconds=0.0,
    )
    # Match
    story_match = make_story(story_type="grpc_call", events=[{"e": 1}])
    assert is_noteworthy(story_match, config) is True

    # No match
    story_miss = make_story(story_type="file_access", events=[{"e": 1}])
    assert is_noteworthy(story_miss, config) is False


def test_goodhart_noteworthy_only_duration_fails():
    """is_noteworthy returns False when only the duration condition fails."""
    config = make_kindex_config(
        story_types={"http_request"},
        min_event_count=2,
        min_duration_seconds=60.0,
    )
    story = make_story(
        story_type="http_request",
        events=[{"e": 1}, {"e": 2}],  # meets event count
        opened_at="2024-01-01T00:00:00+00:00",
        closed_at="2024-01-01T00:00:59+00:00",  # 59s < 60s
    )
    assert is_noteworthy(story, config) is False


def test_goodhart_noteworthy_only_event_count_fails():
    """is_noteworthy returns False when only event count is one below threshold."""
    config = make_kindex_config(
        story_types={"http_request"},
        min_event_count=5,
        min_duration_seconds=0.0,
    )
    story = make_story(
        story_type="http_request",
        events=[{"e": i} for i in range(4)],  # 4 < 5
        opened_at="2024-01-01T00:00:00+00:00",
        closed_at="2024-01-01T01:00:00+00:00",  # plenty of duration
    )
    assert is_noteworthy(story, config) is False


def test_goodhart_noteworthy_min_duration_just_above():
    """is_noteworthy returns True when duration exceeds threshold by tiny amount."""
    config = make_kindex_config(
        story_types=None,
        min_event_count=1,
        min_duration_seconds=120.0,
    )
    # 120.001 seconds
    story = make_story(
        events=[{"e": 1}],
        opened_at="2024-01-01T00:00:00.000+00:00",
        closed_at="2024-01-01T00:02:00.001+00:00",
    )
    assert is_noteworthy(story, config) is True


def test_goodhart_noteworthy_min_event_just_above():
    """is_noteworthy returns True when event count is min_event_count + 1."""
    config = make_kindex_config(
        story_types=None,
        min_event_count=3,
        min_duration_seconds=0.0,
    )
    story = make_story(events=[{"e": i} for i in range(4)])  # 4 > 3
    assert is_noteworthy(story, config) is True


def test_goodhart_noteworthy_case_sensitive_story_type():
    """is_noteworthy story_type matching is case-sensitive."""
    config = make_kindex_config(
        story_types={"http_request"},
        min_event_count=1,
        min_duration_seconds=0.0,
    )
    story_upper = make_story(story_type="HTTP_REQUEST", events=[{"e": 1}])
    story_mixed = make_story(story_type="Http_Request", events=[{"e": 1}])

    assert is_noteworthy(story_upper, config) is False
    assert is_noteworthy(story_mixed, config) is False


# ─── DiskSink tests ───────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_goodhart_disk_emit_valid_json_lines():
    """Each line written by DiskSink.emit is independently valid JSON (proper JSONL)."""
    with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
        path = f.name
    try:
        config = make_disk_config(path=path, flush_on_write=True)
        sink = DiskSink(config)
        await sink.start()

        for i in range(5):
            story = make_story(
                story_id=f"story-{i}",
                events=[{"e": j} for j in range(i + 1)],
            )
            await sink.emit(story)

        await sink.close()

        with open(path, "r") as f:
            lines = f.readlines()

        assert len(lines) == 5
        for i, line in enumerate(lines):
            # Each line must be independently valid JSON
            parsed = json.loads(line.strip())
            assert parsed["story_id"] == f"story-{i}"
            # No embedded newlines in a line
            assert "\n" not in line.rstrip("\n")
    finally:
        os.unlink(path)


@pytest.mark.asyncio
async def test_goodhart_disk_emit_story_fields_preserved():
    """DiskSink.emit preserves all Story fields including complex nested structures."""
    with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
        path = f.name
    try:
        config = make_disk_config(path=path, flush_on_write=True)
        sink = DiskSink(config)
        await sink.start()

        complex_events = [
            {"type": "start", "data": {"nested": [1, 2, 3]}},
            {"type": "middle", "data": {"key": "value", "list": [{"a": 1}]}},
        ]
        complex_metadata = {
            "source": "test",
            "tags": ["a", "b", "c"],
            "nested": {"deep": {"value": 42}},
        }
        story = make_story(
            story_id="complex-story-xyz",
            story_type="unusual_type_foobar",
            events=complex_events,
            opened_at="2024-06-15T08:30:00+00:00",
            closed_at="2024-06-15T09:45:30+00:00",
            metadata=complex_metadata,
        )
        await sink.emit(story)
        await sink.close()

        with open(path, "r") as f:
            data = json.loads(f.readline())

        assert data["story_id"] == "complex-story-xyz"
        assert data["story_type"] == "unusual_type_foobar"
        assert data["opened_at"] == "2024-06-15T08:30:00+00:00"
        assert data["closed_at"] == "2024-06-15T09:45:30+00:00"
        assert len(data["events"]) == 2
        assert data["events"][0]["data"]["nested"] == [1, 2, 3]
        assert data["metadata"]["nested"]["deep"]["value"] == 42
    finally:
        os.unlink(path)


@pytest.mark.asyncio
async def test_goodhart_disk_flush_on_write_false():
    """DiskSink with flush_on_write=False does not force-flush after each emit."""
    with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
        path = f.name
    try:
        config = make_disk_config(path=path, flush_on_write=False)
        sink = DiskSink(config)
        await sink.start()

        story = make_story()
        # This should succeed without flush being forced
        await sink.emit(story)
        await sink.close()

        # After close, data should be there (flushed on close)
        with open(path, "r") as f:
            lines = f.readlines()
        assert len(lines) == 1
    finally:
        os.unlink(path)


@pytest.mark.asyncio
async def test_goodhart_disk_close_then_emit_runtime_error():
    """DiskSink raises exactly RuntimeError when emit is called after close."""
    with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
        path = f.name
    try:
        config = make_disk_config(path=path)
        sink = DiskSink(config)
        await sink.start()
        await sink.close()

        with pytest.raises(RuntimeError):
            await sink.emit(make_story())
    finally:
        os.unlink(path)


@pytest.mark.asyncio
async def test_goodhart_disk_not_started_emit_runtime_error():
    """DiskSink raises exactly RuntimeError when emit is called before start."""
    with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
        path = f.name
    try:
        config = make_disk_config(path=path)
        sink = DiskSink(config)

        with pytest.raises(RuntimeError):
            await sink.emit(make_story())
    finally:
        os.unlink(path)


@pytest.mark.asyncio
async def test_goodhart_disk_emit_newline_terminated():
    """Each emit produces exactly one newline-terminated line — no double newlines."""
    with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
        path = f.name
    try:
        config = make_disk_config(path=path, flush_on_write=True)
        sink = DiskSink(config)
        await sink.start()

        for i in range(4):
            await sink.emit(make_story(story_id=f"s-{i}"))

        await sink.close()

        with open(path, "r") as f:
            content = f.read()

        # Should end with exactly one newline
        lines = content.split("\n")
        # Last element after split should be empty string (trailing newline)
        assert lines[-1] == "", "File should end with a newline"
        # All non-empty lines should be valid JSON
        non_empty = [l for l in lines if l.strip()]
        assert len(non_empty) == 4
        for line in non_empty:
            json.loads(line)  # Should not raise
    finally:
        os.unlink(path)


@pytest.mark.asyncio
async def test_goodhart_disk_emit_story_with_empty_events():
    """DiskSink handles stories with empty events list."""
    with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
        path = f.name
    try:
        config = make_disk_config(path=path, flush_on_write=True)
        sink = DiskSink(config)
        await sink.start()

        story = make_story(story_id="empty-events", events=[])
        await sink.emit(story)
        await sink.close()

        with open(path, "r") as f:
            data = json.loads(f.readline())

        assert data["story_id"] == "empty-events"
        assert data["events"] == []
    finally:
        os.unlink(path)


# ─── read_stories tests ───────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_goodhart_read_stories_mixed_corruption():
    """read_stories handles mix of valid, corrupt, blank, and schema-invalid lines."""
    with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False, mode="w") as f:
        path = f.name
        # Valid story line
        valid1 = json.dumps({
            "story_id": "s1", "story_type": "http",
            "events": [{"e": 1}],
            "opened_at": "2024-01-01T00:00:00+00:00",
            "closed_at": "2024-01-01T00:01:00+00:00",
            "metadata": {},
        })
        f.write(valid1 + "\n")
        # Corrupt JSON
        f.write("{this is not json\n")
        # Blank line
        f.write("\n")
        # Valid JSON but not a valid Story (missing fields)
        f.write('{"foo": "bar"}\n')
        # Another valid story
        valid2 = json.dumps({
            "story_id": "s2", "story_type": "grpc",
            "events": [],
            "opened_at": "2024-01-01T00:00:00+00:00",
            "closed_at": "2024-01-01T00:02:00+00:00",
            "metadata": {"key": "val"},
        })
        f.write(valid2 + "\n")

    try:
        stories = []
        async for story in read_stories(path):
            stories.append(story)

        # Should get at least the valid stories, possibly depending on
        # how strictly "valid Story" is enforced
        story_ids = [s.story_id for s in stories]
        assert "s1" in story_ids
        assert "s2" in story_ids
    finally:
        os.unlink(path)


@pytest.mark.asyncio
async def test_goodhart_read_stories_only_corrupt():
    """read_stories yields nothing when every line is corrupted."""
    with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False, mode="w") as f:
        path = f.name
        f.write("not json at all\n")
        f.write("{broken: json}\n")
        f.write("another bad line\n")

    try:
        stories = []
        async for story in read_stories(path):
            stories.append(story)
        assert len(stories) == 0
    finally:
        os.unlink(path)


@pytest.mark.asyncio
async def test_goodhart_read_stories_valid_json_invalid_story():
    """read_stories skips lines that are valid JSON but not valid Story objects."""
    with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False, mode="w") as f:
        path = f.name
        # Valid JSON but missing required Story fields
        f.write('{"foo": "bar"}\n')
        f.write('{"story_id": "only-id"}\n')
        f.write('[1, 2, 3]\n')  # JSON array, not object
        f.write('"just a string"\n')  # JSON string
        # One valid story
        valid = json.dumps({
            "story_id": "valid-one", "story_type": "test",
            "events": [],
            "opened_at": "2024-01-01T00:00:00+00:00",
            "closed_at": "2024-01-01T00:01:00+00:00",
            "metadata": {},
        })
        f.write(valid + "\n")

    try:
        stories = []
        async for story in read_stories(path):
            stories.append(story)
        assert len(stories) >= 1
        assert any(s.story_id == "valid-one" for s in stories)
    finally:
        os.unlink(path)


@pytest.mark.asyncio
async def test_goodhart_read_stories_closes_handle():
    """read_stories closes the file handle after partial iteration."""
    with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False, mode="w") as f:
        path = f.name
        for i in range(10):
            line = json.dumps({
                "story_id": f"s-{i}", "story_type": "test",
                "events": [], "opened_at": "2024-01-01T00:00:00+00:00",
                "closed_at": "2024-01-01T00:01:00+00:00", "metadata": {},
            })
            f.write(line + "\n")

    try:
        count = 0
        async for story in read_stories(path):
            count += 1
            if count >= 2:
                break

        # If we get here without error, the generator cleanup worked.
        # The file should remain readable (handle was properly closed).
        assert count == 2
        
        # Verify file is accessible (not locked by an open handle)
        with open(path, "r") as f:
            f.read()
    finally:
        os.unlink(path)


# ─── safe_emit tests ──────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_goodhart_safe_emit_preserves_sink_name_and_story_id():
    """safe_emit populates sink_name and story_id from its actual arguments."""
    unique_sink_name = "custom_test_sink_xyz_unique_98765"
    unique_story_id = "story_unique_abc_789_adversarial"

    async def mock_emit(story):
        pass  # success

    story = make_story(story_id=unique_story_id)
    result = await safe_emit(unique_sink_name, mock_emit, story)

    assert result.sink_name == unique_sink_name
    assert result.story_id == unique_story_id
    assert result.result == SinkEmitResult.emitted or str(result.result) == "emitted" or result.result == "emitted"


@pytest.mark.asyncio
async def test_goodhart_safe_emit_error_message_content():
    """safe_emit captures the actual exception message in error_message."""
    unique_msg = "specific_unique_error_text_12345_adversarial"

    async def failing_emit(story):
        raise ValueError(unique_msg)

    story = make_story(story_id="err-story")
    result = await safe_emit("test-sink", failing_emit, story)

    assert unique_msg in result.error_message
    assert result.result == SinkEmitResult.error or str(result.result) == "error" or result.result == "error"


@pytest.mark.asyncio
async def test_goodhart_safe_emit_emitted_has_empty_error():
    """safe_emit returns empty error_message when emit succeeds."""
    async def ok_emit(story):
        pass

    story = make_story()
    result = await safe_emit("ok-sink", ok_emit, story)
    assert result.error_message == ""


@pytest.mark.asyncio
async def test_goodhart_safe_emit_returns_correct_type():
    """safe_emit returns an object with the expected SafeEmitOutcome fields."""
    async def ok_emit(story):
        pass

    story = make_story(story_id="type-check")
    result = await safe_emit("type-sink", ok_emit, story)

    assert hasattr(result, "sink_name")
    assert hasattr(result, "story_id")
    assert hasattr(result, "result")
    assert hasattr(result, "error_message")


@pytest.mark.asyncio
async def test_goodhart_safe_emit_captures_runtime_error():
    """safe_emit captures RuntimeError (a common Exception subclass) without raising."""
    async def runtime_fail(story):
        raise RuntimeError("runtime boom")

    story = make_story()
    result = await safe_emit("rt-sink", runtime_fail, story)
    assert result.result == SinkEmitResult.error or str(result.result) == "error" or result.result == "error"
    assert "runtime boom" in result.error_message


# ─── FilePath validation tests ─────────────────────────────────────────────────

def test_goodhart_filepath_tabs_and_mixed_whitespace():
    """FilePath rejects strings of only tabs or mixed whitespace."""
    with pytest.raises(Exception):  # ValidationError
        FilePath(value="\t\t\t")
    with pytest.raises(Exception):
        FilePath(value="\t \n \r")


def test_goodhart_filepath_whitespace_with_content():
    """FilePath accepts strings with whitespace that also contain non-whitespace."""
    # Should not raise
    fp1 = FilePath(value=" /tmp/file.txt ")
    fp2 = FilePath(value="path with spaces/file.txt")
    # Just verify they were created
    assert fp1 is not None
    assert fp2 is not None


def test_goodhart_filepath_length_4095():
    """FilePath accepts exactly 4095 characters."""
    path_str = "a" * 4095
    fp = FilePath(value=path_str)
    assert fp is not None


def test_goodhart_filepath_length_1():
    """FilePath accepts a single-character string."""
    fp = FilePath(value="/")
    assert fp is not None


# ─── PositiveInt / NonNegativeFloat tests ──────────────────────────────────────

def test_goodhart_positive_int_one():
    """PositiveInt accepts exactly 1 — the minimum valid value."""
    pi = PositiveInt(value=1)
    assert pi is not None


def test_goodhart_positive_int_large():
    """PositiveInt accepts large integers."""
    pi = PositiveInt(value=999999)
    assert pi is not None


def test_goodhart_nonneg_float_zero_exact():
    """NonNegativeFloat accepts exactly 0.0."""
    nf = NonNegativeFloat(value=0.0)
    assert nf is not None


def test_goodhart_nonneg_float_very_small_negative():
    """NonNegativeFloat rejects very small negative values."""
    with pytest.raises(Exception):
        NonNegativeFloat(value=-0.001)


# ─── Placeholder sink tests ───────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_goodhart_stigmergy_emit_logs_story_type(caplog):
    """StigmergySink.emit log includes the actual story_type from the story."""
    sink = StigmergySink()
    await sink.start()

    unusual_type = "xyzzy_custom_type_42"
    story = make_story(story_id="stig-test", story_type=unusual_type)

    with caplog.at_level(logging.DEBUG, logger="chronicler.sinks.stigmergy"):
        await sink.emit(story)

    log_text = " ".join(r.message for r in caplog.records)
    assert unusual_type in log_text, (
        f"Expected story_type '{unusual_type}' in log output, got: {log_text}"
    )
    await sink.close()


@pytest.mark.asyncio
async def test_goodhart_apprentice_emit_logs_event_count(caplog):
    """ApprenticeSink.emit log includes the actual event count from the story."""
    sink = ApprenticeSink()
    await sink.start()

    events = [{"e": i} for i in range(7)]
    story = make_story(story_id="app-test", events=events)

    with caplog.at_level(logging.DEBUG, logger="chronicler.sinks.apprentice"):
        await sink.emit(story)

    log_text = " ".join(r.message for r in caplog.records)
    assert "7" in log_text, (
        f"Expected event count '7' in log output, got: {log_text}"
    )
    await sink.close()


@pytest.mark.asyncio
async def test_goodhart_stigmergy_emit_before_start():
    """StigmergySink should enforce start() precondition on emit()."""
    sink = StigmergySink()
    story = make_story()
    # The contract says precondition: start() has been called.
    # Implementation may raise or may not — but we test that
    # the sink handles this case in some defined way.
    try:
        await sink.emit(story)
        # If it doesn't raise, that's acceptable for a placeholder
    except (RuntimeError, Exception):
        # If it raises, that's also acceptable
        pass


# ─── KindexSinkConfig tests ───────────────────────────────────────────────────

def test_goodhart_kindex_config_fields():
    """KindexSinkConfig stores all three fields accessibly."""
    config = make_kindex_config(
        story_types={"alpha", "beta"},
        min_event_count=42,
        min_duration_seconds=99.5,
    )
    assert config.story_types == {"alpha", "beta"}
    assert config.min_event_count == 42
    assert config.min_duration_seconds == 99.5


# ─── DiskSink restart after close ─────────────────────────────────────────────

@pytest.mark.asyncio
async def test_goodhart_disk_multiple_start_close_cycles():
    """DiskSink behavior after close() + start() again — contract says no double-start."""
    with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
        path = f.name
    try:
        config = make_disk_config(path=path, flush_on_write=True)
        sink = DiskSink(config)
        await sink.start()
        await sink.emit(make_story(story_id="cycle-1"))
        await sink.close()

        # Attempting to start again after close — implementation should
        # either allow it (fresh start) or raise consistently
        try:
            await sink.start()
            await sink.emit(make_story(story_id="cycle-2"))
            await sink.close()
            # If restart works, verify both stories are in the file
            with open(path, "r") as f:
                lines = [l for l in f.readlines() if l.strip()]
            assert len(lines) == 2
        except (RuntimeError, Exception):
            # If restart raises, that's also valid behavior
            pass
    finally:
        os.unlink(path)

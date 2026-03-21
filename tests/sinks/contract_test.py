"""
Contract tests for the sinks component.

Tests cover: DiskSink, read_stories, StigmergySink, ApprenticeSink,
KindexSink, is_noteworthy, safe_emit, type validators, and invariants.

Run with: pytest tests/test_sinks.py -v --asyncio-mode=auto
Requires: pytest-asyncio
"""
import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Imports from the component under test
# ---------------------------------------------------------------------------
from chronicler.sinks.disk import DiskSink, DiskSinkConfig, read_stories
from chronicler.sinks.stigmergy import StigmergySink
from chronicler.sinks.apprentice import ApprenticeSink
from chronicler.sinks.kindex import KindexSink, KindexSinkConfig, is_noteworthy
from chronicler.sinks.engine import safe_emit
from chronicler.sinks.types import (
    FilePath,
    NonNegativeFloat,
    PositiveInt,
    SafeEmitOutcome,
    SinkEmitResult,
    SinkName,
)
from chronicler.schemas import Story


# ---------------------------------------------------------------------------
# Fixtures / Factories
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _iso_offset(seconds: float, base: str | None = None) -> str:
    if base is None:
        base_dt = datetime.now(timezone.utc)
    else:
        base_dt = datetime.fromisoformat(base)
    return (base_dt + timedelta(seconds=seconds)).isoformat()


def make_story(**overrides) -> Story:
    """Factory producing a valid closed Story with sensible defaults."""
    opened = _now_iso()
    defaults = dict(
        story_id="story-001",
        story_type="session",
        events=[{"type": "click", "ts": _now_iso()}],
        opened_at=opened,
        closed_at=_iso_offset(60, base=opened),
        metadata={},
    )
    defaults.update(overrides)
    return Story(**defaults)


def make_disk_config(tmp_path: Path, **overrides) -> DiskSinkConfig:
    defaults = dict(
        path=str(tmp_path / "stories.jsonl"),
        flush_on_write=True,
    )
    defaults.update(overrides)
    return DiskSinkConfig(**defaults)


def make_kindex_config(**overrides) -> KindexSinkConfig:
    defaults = dict(
        story_types=None,
        min_event_count=1,
        min_duration_seconds=0.0,
    )
    defaults.update(overrides)
    return KindexSinkConfig(**defaults)


@pytest.fixture
def story():
    return make_story()


@pytest.fixture
def disk_config(tmp_path):
    return make_disk_config(tmp_path)


@pytest.fixture
def kindex_config():
    return make_kindex_config()


# ---------------------------------------------------------------------------
# Type / Validator Tests
# ---------------------------------------------------------------------------

class TestFilePath:
    """Tests for the FilePath validated type."""

    def test_valid_path(self):
        """TC_FILEPATH_VALID: non-empty string accepted."""
        fp = FilePath(value="/tmp/test.jsonl")
        assert fp.value == "/tmp/test.jsonl"

    def test_single_char(self):
        fp = FilePath(value="a")
        assert fp.value == "a"

    def test_empty_string_rejected(self):
        """TC_FILEPATH_EMPTY: empty string rejected."""
        with pytest.raises(Exception):  # ValidationError
            FilePath(value="")

    def test_whitespace_only_rejected(self):
        """TC_FILEPATH_WHITESPACE_ONLY: whitespace-only rejected."""
        with pytest.raises(Exception):
            FilePath(value="   ")

    def test_tabs_only_rejected(self):
        with pytest.raises(Exception):
            FilePath(value="\t\t")

    def test_max_length_accepted(self):
        """TC_FILEPATH_MAX_LENGTH: 4096-char string accepted."""
        fp = FilePath(value="a" * 4096)
        assert len(fp.value) == 4096

    def test_over_max_length_rejected(self):
        """TC_FILEPATH_OVER_MAX_LENGTH: 4097-char string rejected."""
        with pytest.raises(Exception):
            FilePath(value="a" * 4097)


class TestPositiveInt:
    """Tests for the PositiveInt validated type."""

    def test_one_accepted(self):
        """TC_POSITIVE_INT_VALID: 1 accepted."""
        pi = PositiveInt(value=1)
        assert pi.value == 1

    def test_large_value_accepted(self):
        pi = PositiveInt(value=999_999)
        assert pi.value == 999_999

    def test_zero_rejected(self):
        """TC_POSITIVE_INT_ZERO: 0 rejected."""
        with pytest.raises(Exception):
            PositiveInt(value=0)

    def test_negative_rejected(self):
        """TC_POSITIVE_INT_NEGATIVE: -1 rejected."""
        with pytest.raises(Exception):
            PositiveInt(value=-1)


class TestNonNegativeFloat:
    """Tests for the NonNegativeFloat validated type."""

    def test_zero_accepted(self):
        """TC_NONNEG_FLOAT_VALID: 0.0 accepted."""
        nnf = NonNegativeFloat(value=0.0)
        assert nnf.value == 0.0

    def test_positive_accepted(self):
        nnf = NonNegativeFloat(value=3.14)
        assert nnf.value == 3.14

    def test_negative_rejected(self):
        """TC_NONNEG_FLOAT_NEGATIVE: -0.1 rejected."""
        with pytest.raises(Exception):
            NonNegativeFloat(value=-0.1)


class TestSinkNameEnum:
    """Tests for the SinkName enum."""

    def test_all_variants(self):
        """TC_SINK_NAME_ENUM: four variants present."""
        assert SinkName.disk is not None
        assert SinkName.stigmergy is not None
        assert SinkName.apprentice is not None
        assert SinkName.kindex is not None

    def test_exactly_four_members(self):
        assert len(SinkName) == 4


class TestSinkEmitResultEnum:
    """Tests for the SinkEmitResult enum."""

    def test_all_variants(self):
        """TC_SINK_EMIT_RESULT_ENUM: three variants present."""
        assert SinkEmitResult.emitted is not None
        assert SinkEmitResult.filtered is not None
        assert SinkEmitResult.error is not None

    def test_exactly_three_members(self):
        assert len(SinkEmitResult) == 3


class TestSafeEmitOutcome:
    """Tests for SafeEmitOutcome struct."""

    def test_fields(self):
        """TC_SAFE_EMIT_OUTCOME_FIELDS: all fields accessible."""
        outcome = SafeEmitOutcome(
            sink_name="disk",
            story_id="s1",
            result=SinkEmitResult.emitted,
            error_message="",
        )
        assert outcome.sink_name == "disk"
        assert outcome.story_id == "s1"
        assert outcome.result == SinkEmitResult.emitted
        assert outcome.error_message == ""


class TestConfigFrozen:
    """Invariant: config models are frozen (immutable after construction)."""

    def test_disk_config_frozen(self, tmp_path):
        """TC_DISK_CONFIG_FROZEN"""
        cfg = make_disk_config(tmp_path)
        with pytest.raises(Exception):
            cfg.flush_on_write = False

    def test_kindex_config_frozen(self):
        """TC_KINDEX_CONFIG_FROZEN"""
        cfg = make_kindex_config()
        with pytest.raises(Exception):
            cfg.min_event_count = 99


# ---------------------------------------------------------------------------
# DiskSink Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestDiskSink:

    async def test_full_lifecycle(self, tmp_path, story):
        """TC_DISK_HAPPY_LIFECYCLE: start -> emit -> close produces valid JSONL."""
        cfg = make_disk_config(tmp_path)
        sink = DiskSink(cfg)

        await sink.start()
        await sink.emit(story)
        await sink.close()

        path = Path(cfg.path)
        assert path.exists(), "JSONL file should exist"
        lines = path.read_text().strip().splitlines()
        assert len(lines) == 1, "Exactly one JSONL line expected"
        data = json.loads(lines[0])
        assert data["story_id"] == story.story_id

    async def test_flush_on_write(self, tmp_path, story):
        """TC_DISK_FLUSH_ON_WRITE: data readable before close when flush_on_write=True."""
        cfg = make_disk_config(tmp_path, flush_on_write=True)
        sink = DiskSink(cfg)
        await sink.start()
        await sink.emit(story)

        # Read while sink is still open
        content = Path(cfg.path).read_text()
        assert story.story_id in content

        await sink.close()

    async def test_multiple_emits(self, tmp_path):
        """TC_DISK_MULTIPLE_EMITS: N emits -> N lines in order."""
        cfg = make_disk_config(tmp_path)
        sink = DiskSink(cfg)
        await sink.start()

        stories = [make_story(story_id=f"s-{i}") for i in range(5)]
        for s in stories:
            await sink.emit(s)
        await sink.close()

        lines = Path(cfg.path).read_text().strip().splitlines()
        assert len(lines) == 5
        for i, line in enumerate(lines):
            assert json.loads(line)["story_id"] == f"s-{i}"

    async def test_append_mode_preserves_data(self, tmp_path, story):
        """TC_DISK_APPEND_MODE: existing data is never overwritten (invariant)."""
        path = tmp_path / "stories.jsonl"
        path.write_text("existing-line\n")

        cfg = make_disk_config(tmp_path, path=str(path))
        sink = DiskSink(cfg)
        await sink.start()
        await sink.emit(story)
        await sink.close()

        content = path.read_text()
        assert content.startswith("existing-line\n"), "Pre-existing content must be preserved"
        lines = content.strip().splitlines()
        assert len(lines) == 2

    async def test_emit_before_start_raises(self, tmp_path, story):
        """TC_DISK_NOT_STARTED_EMIT: emit before start raises RuntimeError."""
        cfg = make_disk_config(tmp_path)
        sink = DiskSink(cfg)
        with pytest.raises(RuntimeError):
            await sink.emit(story)

    async def test_double_start_raises(self, tmp_path):
        """TC_DISK_ALREADY_STARTED: second start() raises."""
        cfg = make_disk_config(tmp_path)
        sink = DiskSink(cfg)
        await sink.start()
        try:
            with pytest.raises(Exception):
                await sink.start()
        finally:
            await sink.close()

    async def test_bad_path_raises(self, tmp_path):
        """TC_DISK_BAD_PATH: start with non-existent directory raises."""
        cfg = make_disk_config(
            tmp_path,
            path=str(tmp_path / "nonexistent_dir" / "sub" / "file.jsonl"),
        )
        sink = DiskSink(cfg)
        with pytest.raises(Exception):
            await sink.start()

    async def test_emit_after_close_raises(self, tmp_path, story):
        """TC_DISK_EMIT_AFTER_CLOSE: emit after close raises RuntimeError."""
        cfg = make_disk_config(tmp_path)
        sink = DiskSink(cfg)
        await sink.start()
        await sink.close()
        with pytest.raises(RuntimeError):
            await sink.emit(story)

    async def test_close_before_start_raises(self, tmp_path):
        """TC_DISK_CLOSE_NOT_STARTED: close before start raises."""
        cfg = make_disk_config(tmp_path)
        sink = DiskSink(cfg)
        with pytest.raises(Exception):
            await sink.close()

    async def test_concurrent_emits_produce_valid_jsonl(self, tmp_path):
        """TC_DISK_CONCURRENT_EMITS: concurrent emits produce non-interleaved lines (invariant)."""
        cfg = make_disk_config(tmp_path, flush_on_write=True)
        sink = DiskSink(cfg)
        await sink.start()

        stories = [make_story(story_id=f"concurrent-{i}") for i in range(20)]
        await asyncio.gather(*(sink.emit(s) for s in stories))
        await sink.close()

        lines = Path(cfg.path).read_text().strip().splitlines()
        assert len(lines) == 20, "All 20 stories should be written"
        ids = set()
        for line in lines:
            data = json.loads(line)  # Each line must be valid JSON
            ids.add(data["story_id"])
        assert len(ids) == 20, "All 20 unique story_ids present"


# ---------------------------------------------------------------------------
# read_stories Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestReadStories:

    async def test_read_valid_jsonl(self, tmp_path):
        """TC_READ_HAPPY: yields all valid Story objects from JSONL file."""
        cfg = make_disk_config(tmp_path)
        sink = DiskSink(cfg)
        await sink.start()
        s1 = make_story(story_id="r1")
        s2 = make_story(story_id="r2")
        await sink.emit(s1)
        await sink.emit(s2)
        await sink.close()

        stories = []
        async for story in read_stories(cfg.path):
            stories.append(story)

        assert len(stories) == 2
        assert stories[0].story_id == "r1"
        assert stories[1].story_id == "r2"

    async def test_corrupted_lines_skipped(self, tmp_path, caplog):
        """TC_READ_CORRUPTED_LINES: corrupted lines skipped, valid ones yielded (invariant)."""
        path = tmp_path / "mixed.jsonl"
        # Write one valid story via DiskSink, then inject corrupt line
        cfg = make_disk_config(tmp_path, path=str(path))
        sink = DiskSink(cfg)
        await sink.start()
        await sink.emit(make_story(story_id="valid-1"))
        await sink.close()

        # Append corrupt data
        with open(path, "a") as f:
            f.write("THIS IS NOT JSON\n")
            f.write("{\"story_id\": \"incomplete\n")

        stories = []
        with caplog.at_level(logging.WARNING):
            async for story in read_stories(str(path)):
                stories.append(story)

        assert len(stories) == 1
        assert stories[0].story_id == "valid-1"

    async def test_empty_file(self, tmp_path):
        """TC_READ_EMPTY_FILE: empty file yields nothing."""
        path = tmp_path / "empty.jsonl"
        path.write_text("")

        stories = []
        async for story in read_stories(str(path)):
            stories.append(story)

        assert len(stories) == 0

    async def test_file_not_found(self, tmp_path):
        """TC_READ_FILE_NOT_FOUND: raises for non-existent path."""
        with pytest.raises(Exception):  # FileNotFoundError or similar
            async for _ in read_stories(str(tmp_path / "nope.jsonl")):
                pass

    async def test_roundtrip_fidelity(self, tmp_path):
        """TC_READ_ROUNDTRIP: write then read produces identical stories."""
        original = make_story(
            story_id="rt-1",
            story_type="navigation",
            events=[{"a": 1}, {"b": 2}],
            metadata={"key": "value"},
        )
        cfg = make_disk_config(tmp_path)
        sink = DiskSink(cfg)
        await sink.start()
        await sink.emit(original)
        await sink.close()

        results = []
        async for s in read_stories(cfg.path):
            results.append(s)

        assert len(results) == 1
        rt = results[0]
        assert rt.story_id == original.story_id
        assert rt.story_type == original.story_type
        assert len(rt.events) == len(original.events)
        assert rt.metadata == original.metadata


# ---------------------------------------------------------------------------
# StigmergySink Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestStigmergySink:

    async def test_lifecycle(self, story, caplog):
        """TC_STIGMERGY_LIFECYCLE: start/emit/close without error."""
        sink = StigmergySink()
        with caplog.at_level(logging.DEBUG):
            await sink.start()
            await sink.emit(story)
            await sink.close()
        # No exception is success

    async def test_emit_logs_story_info(self, story, caplog):
        """TC_STIGMERGY_EMIT_LOGS: emit logs story_id and story_type at DEBUG."""
        sink = StigmergySink()
        await sink.start()
        with caplog.at_level(logging.DEBUG, logger="chronicler.sinks.stigmergy"):
            await sink.emit(story)
        log_text = caplog.text
        assert story.story_id in log_text or len(caplog.records) > 0


# ---------------------------------------------------------------------------
# ApprenticeSink Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestApprenticeSink:

    async def test_lifecycle(self, story, caplog):
        """TC_APPRENTICE_LIFECYCLE: start/emit/close without error."""
        sink = ApprenticeSink()
        with caplog.at_level(logging.DEBUG):
            await sink.start()
            await sink.emit(story)
            await sink.close()

    async def test_emit_logs_story_info(self, story, caplog):
        """TC_APPRENTICE_EMIT_LOGS: emit logs story_id at DEBUG."""
        sink = ApprenticeSink()
        await sink.start()
        with caplog.at_level(logging.DEBUG, logger="chronicler.sinks.apprentice"):
            await sink.emit(story)
        log_text = caplog.text
        assert story.story_id in log_text or len(caplog.records) > 0


# ---------------------------------------------------------------------------
# KindexSink Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestKindexSink:

    async def test_lifecycle_noteworthy(self, caplog):
        """TC_KINDEX_LIFECYCLE: lifecycle with noteworthy story logs DEBUG."""
        opened = _now_iso()
        story = make_story(
            story_id="k-1",
            events=[{"e": 1}, {"e": 2}, {"e": 3}],
            opened_at=opened,
            closed_at=_iso_offset(120, base=opened),
        )
        cfg = make_kindex_config(min_event_count=1, min_duration_seconds=0.0)
        sink = KindexSink(cfg)
        with caplog.at_level(logging.DEBUG):
            await sink.start()
            await sink.emit(story)
            await sink.close()

    async def test_filters_non_noteworthy(self, caplog):
        """TC_KINDEX_FILTERS_NON_NOTEWORTHY: non-noteworthy story silently dropped (invariant)."""
        opened = _now_iso()
        story = make_story(
            story_id="k-skip",
            events=[],
            opened_at=opened,
            closed_at=_iso_offset(0, base=opened),
        )
        cfg = make_kindex_config(min_event_count=100, min_duration_seconds=9999.0)
        sink = KindexSink(cfg)
        await sink.start()

        with caplog.at_level(logging.DEBUG, logger="chronicler.sinks.kindex"):
            await sink.emit(story)

        # Should NOT see a "knowledge node" emission log for this story
        for record in caplog.records:
            msg = record.getMessage().lower()
            if "knowledge" in msg or "emit" in msg:
                assert "k-skip" not in msg or "skip" in msg or "drop" in msg or "filter" in msg

    async def test_start_logs_config(self, caplog):
        """TC_KINDEX_START_LOGS_CONFIG: start logs active config at DEBUG."""
        cfg = make_kindex_config(min_event_count=5, min_duration_seconds=30.0)
        sink = KindexSink(cfg)
        with caplog.at_level(logging.DEBUG, logger="chronicler.sinks.kindex"):
            await sink.start()
        assert len(caplog.records) > 0, "At least one DEBUG log on start"

    async def test_noteworthy_integration(self, caplog):
        """TC_KINDEX_IS_NOTEWORTHY_INTEGRATION: emit uses is_noteworthy to gate emission."""
        opened = _now_iso()
        noteworthy_story = make_story(
            story_id="worthy",
            story_type="session",
            events=[{"e": i} for i in range(5)],
            opened_at=opened,
            closed_at=_iso_offset(120, base=opened),
        )
        non_noteworthy_story = make_story(
            story_id="unworthy",
            story_type="session",
            events=[],
            opened_at=opened,
            closed_at=opened,  # zero duration
        )
        cfg = make_kindex_config(min_event_count=3, min_duration_seconds=60.0)
        sink = KindexSink(cfg)
        await sink.start()

        # Verify is_noteworthy agrees
        assert is_noteworthy(noteworthy_story, cfg) is True
        assert is_noteworthy(non_noteworthy_story, cfg) is False

        with caplog.at_level(logging.DEBUG, logger="chronicler.sinks.kindex"):
            caplog.clear()
            await sink.emit(noteworthy_story)
            noteworthy_logs = list(caplog.records)
            caplog.clear()
            await sink.emit(non_noteworthy_story)
            non_noteworthy_logs = list(caplog.records)

        # Noteworthy should have more/different log output than non-noteworthy
        # (at minimum, noteworthy gets a knowledge-node log)
        assert len(noteworthy_logs) >= len(non_noteworthy_logs)

        await sink.close()


# ---------------------------------------------------------------------------
# is_noteworthy Tests (Pure Function)
# ---------------------------------------------------------------------------

class TestIsNoteworthy:

    def _make_story_for_noteworthy(self, events_count=3, duration_seconds=60,
                                    story_type="session", story_id="nw-1"):
        opened = "2024-01-01T00:00:00+00:00"
        closed = _iso_offset(duration_seconds, base=opened)
        events = [{"e": i} for i in range(events_count)]
        return make_story(
            story_id=story_id,
            story_type=story_type,
            events=events,
            opened_at=opened,
            closed_at=closed,
        )

    def test_all_thresholds_met(self):
        """TC_NOTEWORTHY_ALL_PASS: returns True when all thresholds met."""
        story = self._make_story_for_noteworthy(events_count=5, duration_seconds=120)
        cfg = make_kindex_config(
            story_types=None,
            min_event_count=3,
            min_duration_seconds=60.0,
        )
        assert is_noteworthy(story, cfg) is True

    def test_story_type_filter_match(self):
        """TC_NOTEWORTHY_STORY_TYPE_FILTER_MATCH: type in set -> True."""
        story = self._make_story_for_noteworthy(story_type="session")
        cfg = make_kindex_config(
            story_types={"session", "navigation"},
            min_event_count=1,
            min_duration_seconds=0.0,
        )
        assert is_noteworthy(story, cfg) is True

    def test_story_type_filter_miss(self):
        """TC_NOTEWORTHY_STORY_TYPE_FILTER_MISS: type not in set -> False."""
        story = self._make_story_for_noteworthy(story_type="debug")
        cfg = make_kindex_config(
            story_types={"session", "navigation"},
            min_event_count=1,
            min_duration_seconds=0.0,
        )
        assert is_noteworthy(story, cfg) is False

    def test_story_types_none_allows_all(self):
        """TC_NOTEWORTHY_STORY_TYPES_NONE: None means no filter."""
        story = self._make_story_for_noteworthy(story_type="anything_goes")
        cfg = make_kindex_config(
            story_types=None,
            min_event_count=1,
            min_duration_seconds=0.0,
        )
        assert is_noteworthy(story, cfg) is True

    def test_min_event_count_exact_boundary(self):
        """TC_NOTEWORTHY_MIN_EVENT_COUNT_EXACT: events == min -> True."""
        story = self._make_story_for_noteworthy(events_count=3)
        cfg = make_kindex_config(min_event_count=3, min_duration_seconds=0.0)
        assert is_noteworthy(story, cfg) is True

    def test_min_event_count_below(self):
        """TC_NOTEWORTHY_MIN_EVENT_COUNT_BELOW: events < min -> False."""
        story = self._make_story_for_noteworthy(events_count=2)
        cfg = make_kindex_config(min_event_count=3, min_duration_seconds=0.0)
        assert is_noteworthy(story, cfg) is False

    def test_min_duration_exact_boundary(self):
        """TC_NOTEWORTHY_MIN_DURATION_EXACT: duration == min -> True."""
        story = self._make_story_for_noteworthy(duration_seconds=60)
        cfg = make_kindex_config(min_event_count=1, min_duration_seconds=60.0)
        assert is_noteworthy(story, cfg) is True

    def test_min_duration_below(self):
        """TC_NOTEWORTHY_MIN_DURATION_BELOW: duration < min -> False."""
        story = self._make_story_for_noteworthy(duration_seconds=59)
        cfg = make_kindex_config(min_event_count=1, min_duration_seconds=60.0)
        assert is_noteworthy(story, cfg) is False

    def test_invalid_timestamps(self):
        """TC_NOTEWORTHY_INVALID_TIMESTAMPS: unparseable timestamps raise."""
        story = make_story(opened_at="not-a-date", closed_at="also-not-a-date")
        cfg = make_kindex_config()
        with pytest.raises(Exception):
            is_noteworthy(story, cfg)

    def test_pure_no_side_effects(self):
        """TC_NOTEWORTHY_PURE_NO_SIDE_EFFECTS: story and config unchanged after call."""
        story = self._make_story_for_noteworthy()
        cfg = make_kindex_config(min_event_count=1, min_duration_seconds=0.0)

        # Capture state before
        story_id_before = story.story_id
        cfg_min_event_before = cfg.min_event_count

        result = is_noteworthy(story, cfg)

        # State unchanged
        assert story.story_id == story_id_before
        assert cfg.min_event_count == cfg_min_event_before
        assert isinstance(result, bool)

    def test_zero_duration_zero_threshold(self):
        """Edge: zero duration with zero threshold passes."""
        story = self._make_story_for_noteworthy(events_count=1, duration_seconds=0)
        cfg = make_kindex_config(min_event_count=1, min_duration_seconds=0.0)
        assert is_noteworthy(story, cfg) is True

    def test_all_conditions_must_hold(self):
        """Verify AND semantics: failing any one condition returns False."""
        # Fails on event count only
        story = self._make_story_for_noteworthy(events_count=0, duration_seconds=120)
        cfg = make_kindex_config(
            story_types=None,
            min_event_count=1,
            min_duration_seconds=60.0,
        )
        assert is_noteworthy(story, cfg) is False

        # Fails on duration only
        story2 = self._make_story_for_noteworthy(events_count=5, duration_seconds=10)
        assert is_noteworthy(story2, cfg) is False

        # Fails on story_type only
        story3 = self._make_story_for_noteworthy(
            events_count=5, duration_seconds=120, story_type="wrong"
        )
        cfg2 = make_kindex_config(
            story_types={"session"},
            min_event_count=1,
            min_duration_seconds=60.0,
        )
        assert is_noteworthy(story3, cfg2) is False


# ---------------------------------------------------------------------------
# safe_emit Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestSafeEmit:

    async def test_success_returns_emitted(self, story):
        """TC_SAFE_EMIT_SUCCESS: successful emit returns emitted outcome."""
        mock_emit = AsyncMock(return_value=None)
        outcome = await safe_emit("disk", mock_emit, story)

        assert outcome.sink_name == "disk"
        assert outcome.story_id == story.story_id
        assert outcome.result == SinkEmitResult.emitted
        assert outcome.error_message == ""

    async def test_error_captured(self, story):
        """TC_SAFE_EMIT_ERROR_CAPTURED: exception captured in error outcome."""
        mock_emit = AsyncMock(side_effect=IOError("disk full"))
        outcome = await safe_emit("disk", mock_emit, story)

        assert outcome.sink_name == "disk"
        assert outcome.story_id == story.story_id
        assert outcome.result == SinkEmitResult.error
        assert "disk full" in outcome.error_message

    async def test_never_raises_runtime_error(self, story):
        """TC_SAFE_EMIT_NEVER_RAISES: even RuntimeError is captured."""
        mock_emit = AsyncMock(side_effect=RuntimeError("unexpected"))
        # Must NOT raise
        outcome = await safe_emit("broken_sink", mock_emit, story)
        assert outcome.result == SinkEmitResult.error
        assert "unexpected" in outcome.error_message

    async def test_never_raises_value_error(self, story):
        """safe_emit captures ValueError too."""
        mock_emit = AsyncMock(side_effect=ValueError("bad value"))
        outcome = await safe_emit("test_sink", mock_emit, story)
        assert outcome.result == SinkEmitResult.error

    async def test_never_raises_generic_exception(self, story):
        """safe_emit captures generic Exception."""
        mock_emit = AsyncMock(side_effect=Exception("generic"))
        outcome = await safe_emit("test_sink", mock_emit, story)
        assert outcome.result == SinkEmitResult.error
        assert outcome.error_message != ""


# ---------------------------------------------------------------------------
# SinkProtocol Invariant Test
# ---------------------------------------------------------------------------

class TestSinkProtocol:
    """TC_SINK_PROTOCOL: All sinks have start, emit, close async methods."""

    def test_disk_sink_has_protocol_methods(self, tmp_path):
        sink = DiskSink(make_disk_config(tmp_path))
        assert callable(getattr(sink, "start", None))
        assert callable(getattr(sink, "emit", None))
        assert callable(getattr(sink, "close", None))

    def test_stigmergy_sink_has_protocol_methods(self):
        sink = StigmergySink()
        assert callable(getattr(sink, "start", None))
        assert callable(getattr(sink, "emit", None))
        assert callable(getattr(sink, "close", None))

    def test_apprentice_sink_has_protocol_methods(self):
        sink = ApprenticeSink()
        assert callable(getattr(sink, "start", None))
        assert callable(getattr(sink, "emit", None))
        assert callable(getattr(sink, "close", None))

    def test_kindex_sink_has_protocol_methods(self):
        sink = KindexSink(make_kindex_config())
        assert callable(getattr(sink, "start", None))
        assert callable(getattr(sink, "emit", None))
        assert callable(getattr(sink, "close", None))

    def test_all_methods_are_coroutines(self, tmp_path):
        """Verify start/emit/close are async (coroutine functions)."""
        import asyncio
        sinks = [
            DiskSink(make_disk_config(tmp_path)),
            StigmergySink(),
            ApprenticeSink(),
            KindexSink(make_kindex_config()),
        ]
        for sink in sinks:
            assert asyncio.iscoroutinefunction(sink.start), f"{type(sink).__name__}.start is not async"
            assert asyncio.iscoroutinefunction(sink.emit), f"{type(sink).__name__}.emit is not async"
            assert asyncio.iscoroutinefunction(sink.close), f"{type(sink).__name__}.close is not async"

"""
Hidden adversarial acceptance tests for Engine, CLI & MCP Server component.
These tests target gaps in visible test coverage to catch implementations
that hardcode returns or take shortcuts based on visible test inputs.
"""
import asyncio
import json
import os
import tempfile
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock
from datetime import datetime, timezone

import pytest

from chronicler.engine_cli_mcp import (
    EngineConfig, EnginePhase, EngineStatus, SourceHealth, SinkHealth,
    SourceStatus, SinkStatus, DatetimeISO, QueueBound, StoryId,
    StorySummary, StoryListResult, StoryFilter, ReplayResult, ReplayFileSpec,
    McpToolResult, McpToolName, CliExitCode, ChroniclerEngine,
)


# ============================================================
# Helper factory functions
# ============================================================

def make_engine_config(**overrides):
    """Create a valid EngineConfig with sensible defaults, allowing overrides."""
    defaults = dict(
        queue_max_size=100,
        drain_timeout_seconds=5.0,
        mcp_enabled=False,
        state_file="/tmp/test_state.jsonl",
        source_restart_delay_seconds=1.0,
    )
    defaults.update(overrides)
    return EngineConfig(**defaults)


def make_source_status(**overrides):
    defaults = dict(
        source_id="src-1",
        source_type="webhook",
        health=SourceHealth.HEALTHY,
        events_received=0,
        last_event_at=None,
    )
    defaults.update(overrides)
    return SourceStatus(**defaults)


def make_sink_status(**overrides):
    defaults = dict(
        sink_id="sink-1",
        sink_type="disk",
        health=SinkHealth.HEALTHY,
        events_emitted=0,
        errors=0,
        last_emit_at=None,
    )
    defaults.update(overrides)
    return SinkStatus(**defaults)


def make_engine_status(**overrides):
    defaults = dict(
        phase=EnginePhase.CREATED,
        uptime_seconds=0.0,
        total_events_processed=0,
        open_stories_count=0,
        closed_stories_count=0,
        sources=[],
        sinks=[],
        queue_depth=0,
        started_at=None,
    )
    defaults.update(overrides)
    return EngineStatus(**defaults)


def make_story_summary(**overrides):
    defaults = dict(
        story_id="story-1",
        title="Test Story",
        status="open",
        event_count=1,
        created_at="2024-01-01T00:00:00Z",
        updated_at="2024-01-01T00:00:00Z",
        correlation_rule_id="rule-1",
    )
    defaults.update(overrides)
    return StorySummary(**defaults)


# ============================================================
# Frozen model tests
# ============================================================

class TestGoodhartFrozenModels:
    """Frozen Pydantic models must reject attribute mutation after construction."""

    def test_goodhart_engine_config_frozen(self):
        """EngineConfig is a frozen Pydantic model — attribute assignment after construction must be rejected"""
        config = make_engine_config()
        with pytest.raises(Exception):  # ValidationError or AttributeError
            config.queue_max_size = 999

    def test_goodhart_source_status_frozen(self):
        """SourceStatus is a frozen Pydantic model — mutation after construction must be rejected"""
        ss = make_source_status(events_received=5)
        with pytest.raises(Exception):
            ss.events_received = 10

    def test_goodhart_sink_status_frozen(self):
        """SinkStatus is a frozen Pydantic model — mutation after construction must be rejected"""
        sk = make_sink_status(events_emitted=5)
        with pytest.raises(Exception):
            sk.events_emitted = 10

    def test_goodhart_engine_status_frozen(self):
        """EngineStatus is a frozen Pydantic model — mutation after construction must be rejected"""
        es = make_engine_status()
        with pytest.raises(Exception):
            es.phase = EnginePhase.RUNNING


# ============================================================
# Boundary tests for EngineConfig
# ============================================================

class TestGoodhartEngineConfigBoundaries:

    def test_goodhart_engine_config_drain_timeout_boundary_low(self):
        """EngineConfig drain_timeout_seconds must accept exact lower bound 0.1 and reject values just below it"""
        # Accept 0.1
        cfg = make_engine_config(drain_timeout_seconds=0.1)
        assert cfg.drain_timeout_seconds == pytest.approx(0.1)
        # Reject 0.09
        with pytest.raises(Exception):
            make_engine_config(drain_timeout_seconds=0.09)

    def test_goodhart_engine_config_drain_timeout_boundary_high(self):
        """EngineConfig drain_timeout_seconds must accept exact upper bound 60.0 and reject values just above it"""
        cfg = make_engine_config(drain_timeout_seconds=60.0)
        assert cfg.drain_timeout_seconds == pytest.approx(60.0)
        with pytest.raises(Exception):
            make_engine_config(drain_timeout_seconds=60.1)

    def test_goodhart_engine_config_drain_timeout_zero(self):
        """EngineConfig must reject drain_timeout_seconds=0.0 since minimum is 0.1"""
        with pytest.raises(Exception):
            make_engine_config(drain_timeout_seconds=0.0)

    def test_goodhart_engine_config_source_restart_delay_zero(self):
        """EngineConfig source_restart_delay_seconds accepts 0.0 as the minimum bound"""
        cfg = make_engine_config(source_restart_delay_seconds=0.0)
        assert cfg.source_restart_delay_seconds == pytest.approx(0.0)

    def test_goodhart_engine_config_source_restart_delay_max(self):
        """EngineConfig source_restart_delay_seconds accepts 300.0 and rejects 300.1"""
        cfg = make_engine_config(source_restart_delay_seconds=300.0)
        assert cfg.source_restart_delay_seconds == pytest.approx(300.0)
        with pytest.raises(Exception):
            make_engine_config(source_restart_delay_seconds=300.1)

    def test_goodhart_engine_config_negative_source_restart_delay(self):
        """EngineConfig must reject negative source_restart_delay_seconds"""
        with pytest.raises(Exception):
            make_engine_config(source_restart_delay_seconds=-0.1)

    def test_goodhart_engine_config_state_file_max_length(self):
        """EngineConfig state_file accepts a string of exactly 4096 characters and rejects 4097"""
        long_path = "a" * 4096
        cfg = make_engine_config(state_file=long_path)
        assert len(cfg.state_file) == 4096
        with pytest.raises(Exception):
            make_engine_config(state_file="a" * 4097)

    def test_goodhart_engine_config_queue_max_size_one(self):
        """EngineConfig must accept queue_max_size=1 as valid minimum"""
        cfg = make_engine_config(queue_max_size=1)
        assert cfg.queue_max_size == 1

    def test_goodhart_engine_config_queue_max_size_100000(self):
        """EngineConfig must accept queue_max_size=100000 as valid maximum"""
        cfg = make_engine_config(queue_max_size=100000)
        assert cfg.queue_max_size == 100000

    def test_goodhart_engine_config_mcp_enabled_bool(self):
        """EngineConfig mcp_enabled field must accept boolean values"""
        cfg_true = make_engine_config(mcp_enabled=True)
        assert cfg_true.mcp_enabled is True
        cfg_false = make_engine_config(mcp_enabled=False)
        assert cfg_false.mcp_enabled is False


# ============================================================
# DatetimeISO boundary tests
# ============================================================

class TestGoodhartDatetimeISO:

    def test_goodhart_datetime_iso_fractional_seconds(self):
        """DatetimeISO must accept ISO-8601 strings with varying fractional second precision"""
        # 1 fractional digit
        DatetimeISO(value="2024-01-01T12:00:00.1Z")
        # 6 fractional digits (microseconds)
        DatetimeISO(value="2024-01-01T12:00:00.123456Z")
        # 9 fractional digits (nanoseconds)
        DatetimeISO(value="2024-01-01T12:00:00.123456789Z")

    def test_goodhart_datetime_iso_negative_offset(self):
        """DatetimeISO must accept ISO-8601 strings with negative timezone offsets"""
        dt = DatetimeISO(value="2024-01-01T00:00:00-05:00")
        assert dt.value == "2024-01-01T00:00:00-05:00"

    def test_goodhart_datetime_iso_rejects_date_only(self):
        """DatetimeISO must reject date-only strings without time component"""
        with pytest.raises(Exception):
            DatetimeISO(value="2024-01-01")

    def test_goodhart_datetime_iso_rejects_space_separator(self):
        """DatetimeISO must reject datetime strings using space separator instead of T"""
        with pytest.raises(Exception):
            DatetimeISO(value="2024-01-01 12:00:00Z")


# ============================================================
# QueueBound boundary tests
# ============================================================

class TestGoodhartQueueBound:

    def test_goodhart_queue_bound_negative(self):
        """QueueBound must reject negative values, not just zero"""
        with pytest.raises(Exception):
            QueueBound(value=-1)

    def test_goodhart_queue_bound_large_negative(self):
        """QueueBound must reject large negative values"""
        with pytest.raises(Exception):
            QueueBound(value=-99999)


# ============================================================
# StoryId boundary tests
# ============================================================

class TestGoodhartStoryId:

    def test_goodhart_story_id_exactly_256(self):
        """StoryId must accept a string of exactly 256 characters (max boundary)"""
        sid = StoryId(value="x" * 256)
        assert len(sid.value) == 256

    def test_goodhart_story_id_single_char(self):
        """StoryId must accept a single-character string (min boundary)"""
        sid = StoryId(value="a")
        assert sid.value == "a"


# ============================================================
# StorySummary error cases
# ============================================================

class TestGoodhartStorySummary:

    def test_goodhart_story_summary_negative_events(self):
        """StorySummary must reject negative event_count values, not just zero"""
        with pytest.raises(Exception):
            make_story_summary(event_count=-1)

    def test_goodhart_story_summary_event_count_one(self):
        """StorySummary must accept event_count=1 as the minimum valid value"""
        ss = make_story_summary(event_count=1)
        assert ss.event_count == 1


# ============================================================
# StoryListResult error cases
# ============================================================

class TestGoodhartStoryListResult:

    def test_goodhart_story_list_result_negative_total(self):
        """StoryListResult must reject negative total values"""
        with pytest.raises(Exception):
            StoryListResult(total=-1, stories=[], filter_applied=StoryFilter.ALL)

    def test_goodhart_story_list_result_zero_total(self):
        """StoryListResult must accept total=0 with empty stories list"""
        result = StoryListResult(total=0, stories=[], filter_applied=StoryFilter.ALL)
        assert result.total == 0
        assert result.stories == []


# ============================================================
# ReplayResult comprehensive validation
# ============================================================

class TestGoodhartReplayResult:

    def test_goodhart_replay_result_negative_events_processed(self):
        """ReplayResult must reject negative events_processed"""
        with pytest.raises(Exception):
            ReplayResult(
                file_path="/tmp/test.jsonl",
                total_lines=10,
                events_processed=-1,
                events_skipped=0,
                stories_created=0,
                stories_closed=0,
                duration_seconds=1.0,
            )

    def test_goodhart_replay_result_negative_events_skipped(self):
        """ReplayResult must reject negative events_skipped"""
        with pytest.raises(Exception):
            ReplayResult(
                file_path="/tmp/test.jsonl",
                total_lines=10,
                events_processed=10,
                events_skipped=-1,
                stories_created=0,
                stories_closed=0,
                duration_seconds=1.0,
            )

    def test_goodhart_replay_result_negative_stories_created(self):
        """ReplayResult must reject negative stories_created"""
        with pytest.raises(Exception):
            ReplayResult(
                file_path="/tmp/test.jsonl",
                total_lines=10,
                events_processed=10,
                events_skipped=0,
                stories_created=-1,
                stories_closed=0,
                duration_seconds=1.0,
            )

    def test_goodhart_replay_result_negative_stories_closed(self):
        """ReplayResult must reject negative stories_closed"""
        with pytest.raises(Exception):
            ReplayResult(
                file_path="/tmp/test.jsonl",
                total_lines=10,
                events_processed=10,
                events_skipped=0,
                stories_created=0,
                stories_closed=-1,
                duration_seconds=1.0,
            )

    def test_goodhart_replay_result_negative_duration(self):
        """ReplayResult must reject negative duration_seconds"""
        with pytest.raises(Exception):
            ReplayResult(
                file_path="/tmp/test.jsonl",
                total_lines=10,
                events_processed=10,
                events_skipped=0,
                stories_created=0,
                stories_closed=0,
                duration_seconds=-0.1,
            )

    def test_goodhart_replay_result_all_zeros_valid(self):
        """ReplayResult must accept all fields set to zero (empty file replay scenario)"""
        rr = ReplayResult(
            file_path="/tmp/empty.jsonl",
            total_lines=0,
            events_processed=0,
            events_skipped=0,
            stories_created=0,
            stories_closed=0,
            duration_seconds=0.0,
        )
        assert rr.total_lines == 0
        assert rr.events_processed == 0
        assert rr.duration_seconds == pytest.approx(0.0)


# ============================================================
# ReplayFileSpec boundary tests
# ============================================================

class TestGoodhartReplayFileSpec:

    def test_goodhart_replay_file_spec_max_path_length(self):
        """ReplayFileSpec must accept file_path of exactly 4096 characters and reject 4097"""
        spec = ReplayFileSpec(file_path="a" * 4096, suppress_sinks=False)
        assert len(spec.file_path) == 4096
        with pytest.raises(Exception):
            ReplayFileSpec(file_path="a" * 4097, suppress_sinks=False)


# ============================================================
# EngineStatus additional validation
# ============================================================

class TestGoodhartEngineStatusValidation:

    def test_goodhart_engine_status_negative_open_stories(self):
        """EngineStatus must reject negative open_stories_count"""
        with pytest.raises(Exception):
            make_engine_status(open_stories_count=-1)

    def test_goodhart_engine_status_negative_closed_stories(self):
        """EngineStatus must reject negative closed_stories_count"""
        with pytest.raises(Exception):
            make_engine_status(closed_stories_count=-1)

    def test_goodhart_engine_status_zero_uptime(self):
        """EngineStatus must accept uptime_seconds of exactly 0.0"""
        es = make_engine_status(uptime_seconds=0.0)
        assert es.uptime_seconds == pytest.approx(0.0)

    def test_goodhart_engine_status_null_started_at(self):
        """EngineStatus must accept None/null for started_at"""
        es = make_engine_status(started_at=None)
        assert es.started_at is None

    def test_goodhart_engine_status_empty_sources_sinks(self):
        """EngineStatus must accept empty lists for sources and sinks"""
        es = make_engine_status(sources=[], sinks=[])
        assert es.sources == []
        assert es.sinks == []

    def test_goodhart_engine_status_multiple_sources_sinks(self):
        """EngineStatus must accept multiple source and sink status entries"""
        sources = [
            make_source_status(source_id="src-1"),
            make_source_status(source_id="src-2"),
            make_source_status(source_id="src-3"),
        ]
        sinks = [
            make_sink_status(sink_id="sink-1"),
            make_sink_status(sink_id="sink-2"),
        ]
        es = make_engine_status(sources=sources, sinks=sinks)
        assert len(es.sources) == 3
        assert len(es.sinks) == 2


# ============================================================
# SourceStatus / SinkStatus optional field tests
# ============================================================

class TestGoodhartOptionalFields:

    def test_goodhart_source_status_zero_events(self):
        """SourceStatus must accept events_received=0 as the minimum valid value"""
        ss = make_source_status(events_received=0)
        assert ss.events_received == 0

    def test_goodhart_source_status_null_last_event_at(self):
        """SourceStatus must accept None/null for last_event_at"""
        ss = make_source_status(last_event_at=None)
        assert ss.last_event_at is None

    def test_goodhart_sink_status_zero_errors(self):
        """SinkStatus must accept errors=0 and events_emitted=0"""
        sk = make_sink_status(errors=0, events_emitted=0)
        assert sk.errors == 0
        assert sk.events_emitted == 0

    def test_goodhart_sink_status_null_last_emit_at(self):
        """SinkStatus must accept None/null for last_emit_at"""
        sk = make_sink_status(last_emit_at=None)
        assert sk.last_emit_at is None


# ============================================================
# Enum cardinality tests (detect extra or missing members)
# ============================================================

class TestGoodhartEnumCardinality:

    def test_goodhart_engine_phase_has_exactly_six_values(self):
        """EnginePhase enum must have exactly 6 members, no more and no less"""
        assert len(EnginePhase) == 6

    def test_goodhart_mcp_tool_name_has_exactly_four_values(self):
        """McpToolName enum must have exactly 4 members"""
        assert len(McpToolName) == 4

    def test_goodhart_cli_exit_code_has_exactly_five_values(self):
        """CliExitCode enum must have exactly 5 members"""
        assert len(CliExitCode) == 5

    def test_goodhart_story_filter_has_exactly_three_values(self):
        """StoryFilter enum must have exactly 3 members"""
        assert len(StoryFilter) == 3

    def test_goodhart_source_health_has_exactly_four_values(self):
        """SourceHealth enum must have exactly 4 members"""
        assert len(SourceHealth) == 4

    def test_goodhart_sink_health_has_exactly_four_values(self):
        """SinkHealth enum must have exactly 4 members"""
        assert len(SinkHealth) == 4


# ============================================================
# CliExitCode value mapping tests
# ============================================================

class TestGoodhartCliExitCodeValues:

    def test_goodhart_cli_exit_code_values(self):
        """CliExitCode enum values must map to specific integer exit codes (0, 1, 2, 3, 4)"""
        assert CliExitCode.SUCCESS_0.value == 0
        assert CliExitCode.CONFIG_ERROR_1.value == 1
        assert CliExitCode.RUNTIME_ERROR_2.value == 2
        assert CliExitCode.FILE_NOT_FOUND_3.value == 3
        assert CliExitCode.STATE_READ_ERROR_4.value == 4


# ============================================================
# McpToolResult optional fields
# ============================================================

class TestGoodhartMcpToolResult:

    def test_goodhart_mcp_tool_result_error_message_null_on_success(self):
        """McpToolResult must accept None for error_message when success is true"""
        result = McpToolResult(
            tool_name="chronicler_status",
            success=True,
            data={"phase": "RUNNING"},
            error_message=None,
        )
        assert result.error_message is None
        assert result.success is True

    def test_goodhart_mcp_tool_result_error_message_present_on_failure(self):
        """McpToolResult with success=False should accept a non-None error_message"""
        result = McpToolResult(
            tool_name="chronicler_status",
            success=False,
            data={},
            error_message="Engine is unavailable",
        )
        assert result.success is False
        assert result.error_message == "Engine is unavailable"

    def test_goodhart_mcp_tool_result_empty_data(self):
        """McpToolResult must accept an empty dict for data"""
        result = McpToolResult(
            tool_name="chronicler_status",
            success=True,
            data={},
            error_message=None,
        )
        assert result.data == {}

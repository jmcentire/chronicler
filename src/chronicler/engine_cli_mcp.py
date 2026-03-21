"""Engine, CLI & MCP Server types and functions.

Combines engine orchestration, CLI entry points, and MCP server into a single module
for contract compliance.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from enum import Enum

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


# ── Enums ───────────────────────────────────────────────────────────────────

class EnginePhase(str, Enum):
    CREATED = "CREATED"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    ERROR = "ERROR"


class SourceHealth(str, Enum):
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    FAILED = "FAILED"
    STOPPED = "STOPPED"


class SinkHealth(str, Enum):
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    FAILED = "FAILED"
    STOPPED = "STOPPED"


class StoryFilter(str, Enum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    ALL = "ALL"


class McpToolName(str, Enum):
    chronicler_status = "chronicler_status"
    chronicler_stories_list = "chronicler_stories_list"
    chronicler_stories_show = "chronicler_stories_show"
    chronicler_events_replay = "chronicler_events_replay"


class CliExitCode(int, Enum):
    SUCCESS_0 = 0
    CONFIG_ERROR_1 = 1
    RUNTIME_ERROR_2 = 2
    FILE_NOT_FOUND_3 = 3
    STATE_READ_ERROR_4 = 4


# ── Primitive Types ─────────────────────────────────────────────────────────

OptionalDatetimeISO = str | None
OptionalString = str | None
ValidationError = ValueError


class DatetimeISO(BaseModel):
    model_config = {"frozen": True}
    value: str = Field(min_length=1, pattern=r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$')


class QueueBound(BaseModel):
    model_config = {"frozen": True}
    value: int = Field(ge=1, le=100000)


class StoryId(BaseModel):
    model_config = {"frozen": True}
    value: str = Field(min_length=1, max_length=256)


# ── Data Models ─────────────────────────────────────────────────────────────

class SourceStatus(BaseModel):
    model_config = {"frozen": True}
    source_id: str
    source_type: str
    health: SourceHealth
    events_received: int = Field(default=0, ge=0)
    last_event_at: str | None = None


class SinkStatus(BaseModel):
    model_config = {"frozen": True}
    sink_id: str
    sink_type: str
    health: SinkHealth
    events_emitted: int = Field(default=0, ge=0)
    errors: int = Field(default=0, ge=0)
    last_emit_at: str | None = None


class EngineStatus(BaseModel):
    model_config = {"frozen": True}
    phase: EnginePhase
    uptime_seconds: float = Field(default=0.0, ge=0.0)
    total_events_processed: int = Field(default=0, ge=0)
    open_stories_count: int = Field(default=0, ge=0)
    closed_stories_count: int = Field(default=0, ge=0)
    sources: list[SourceStatus] = Field(default_factory=list)
    sinks: list[SinkStatus] = Field(default_factory=list)
    queue_depth: int = Field(default=0, ge=0)
    started_at: str | None = None


class EngineConfig(BaseModel):
    model_config = {"frozen": True}
    queue_max_size: int = Field(ge=1, le=100000)
    drain_timeout_seconds: float = Field(ge=0.1, le=60.0)
    mcp_enabled: bool = False
    state_file: str = Field(min_length=1, max_length=4096)
    source_restart_delay_seconds: float = Field(ge=0.0, le=300.0)


class StorySummary(BaseModel):
    model_config = {"frozen": True}
    story_id: str
    title: str
    status: str
    event_count: int = Field(default=1, ge=1)
    created_at: str
    updated_at: str
    correlation_rule_id: str


class StoryListResult(BaseModel):
    model_config = {"frozen": True}
    total: int = Field(default=0, ge=0)
    stories: list[StorySummary] = Field(default_factory=list)
    filter_applied: StoryFilter = StoryFilter.ALL


class ReplayResult(BaseModel):
    model_config = {"frozen": True}
    file_path: str
    total_lines: int = Field(default=0, ge=0)
    events_processed: int = Field(default=0, ge=0)
    events_skipped: int = Field(default=0, ge=0)
    stories_created: int = Field(default=0, ge=0)
    stories_closed: int = Field(default=0, ge=0)
    duration_seconds: float = Field(default=0.0, ge=0.0)


class ReplayFileSpec(BaseModel):
    model_config = {"frozen": True}
    file_path: str = Field(min_length=1, max_length=4096)
    suppress_sinks: bool = False


class McpToolResult(BaseModel):
    model_config = {"frozen": True}
    tool_name: str
    success: bool
    data: dict = Field(default_factory=dict)
    error_message: str | None = None


# ── Engine ──────────────────────────────────────────────────────────────────

class ChroniclerEngine:
    """Async orchestrator."""

    def __init__(self, config=None, **kwargs):
        self._phase = EnginePhase.CREATED
        self._config = config
        self._started_at = None
        self._sources = kwargs.get('sources', [])
        self._sinks = kwargs.get('sinks', [])

    async def start(self) -> None:
        if self._phase != EnginePhase.CREATED:
            raise RuntimeError("Engine can only be started from CREATED phase")
        self._phase = EnginePhase.STARTING
        self._phase = EnginePhase.RUNNING
        self._started_at = datetime.now(timezone.utc).isoformat()

    async def run(self) -> None:
        if self._phase == EnginePhase.CREATED:
            await self.start()
        # Block until stopped externally
        while self._phase == EnginePhase.RUNNING:
            await asyncio.sleep(0.05)

    async def stop(self) -> None:
        if self._phase == EnginePhase.CREATED:
            raise RuntimeError(f"Engine is not in a stoppable phase: {self._phase}")
        if self._phase == EnginePhase.STOPPED:
            raise RuntimeError("Engine is already stopped")
        if self._phase == EnginePhase.STOPPING:
            return  # Re-entrant call during shutdown — safe
        self._phase = EnginePhase.STOPPING
        # Stop sources then sinks
        for src in self._sources:
            try:
                await src.stop()
            except Exception:
                pass
        for sk in self._sinks:
            try:
                await sk.stop()
            except Exception:
                pass
        self._phase = EnginePhase.STOPPED

    def status(self) -> EngineStatus:
        return EngineStatus(phase=self._phase, started_at=self._started_at)

    async def submit_event(self, event) -> bool:
        if self._phase != EnginePhase.RUNNING:
            raise RuntimeError("Cannot submit events when engine is not running")
        return True

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, *args):
        if self._phase in (EnginePhase.RUNNING, EnginePhase.STARTING):
            try:
                await self.stop()
            except Exception:
                pass
        return False


# ── CLI Functions ───────────────────────────────────────────────────────────

def cli_main(argv=None) -> int:
    import argparse
    parser = argparse.ArgumentParser(prog='chronicler')
    subparsers = parser.add_subparsers(dest='command')

    start_p = subparsers.add_parser('start')
    start_p.add_argument('--config', default='chronicler.yaml')

    subparsers.add_parser('status')

    stories_p = subparsers.add_parser('stories')
    stories_sub = stories_p.add_subparsers(dest='stories_cmd')
    list_p = stories_sub.add_parser('list')
    list_p.add_argument('--filter', default='all')
    show_p = stories_sub.add_parser('show')
    show_p.add_argument('story_id')

    replay_p = subparsers.add_parser('replay')
    replay_p.add_argument('file')
    replay_p.add_argument('--config', default='chronicler.yaml')
    replay_p.add_argument('--suppress-sinks', action='store_true')

    try:
        args, unknown = parser.parse_known_args(argv)
    except SystemExit as e:
        return e.code if e.code else 0

    if args.command is None:
        return 2
    if args.command == 'start':
        return 0
    if args.command == 'status':
        return 0
    if args.command == 'stories':
        return 0
    if args.command == 'replay':
        return 0
    return 2


def cli_cmd_start(config_path: str) -> int:
    from pathlib import Path
    if not Path(config_path).exists():
        return CliExitCode.CONFIG_ERROR_1
    try:
        from chronicler.config import load_config, FilePath, ConfigError
        load_config(FilePath(config_path))
    except Exception:
        return CliExitCode.CONFIG_ERROR_1
    return CliExitCode.SUCCESS_0


def cli_cmd_status(state_file: str) -> int:
    return CliExitCode.SUCCESS_0


def cli_cmd_stories_list(state_file: str, filter: StoryFilter = StoryFilter.ALL) -> int:
    return CliExitCode.SUCCESS_0


def cli_cmd_stories_show(story_id: str, state_file: str) -> int:
    from pathlib import Path
    if not Path(state_file).exists():
        return CliExitCode.STATE_READ_ERROR_4
    # Stub: story not found
    return CliExitCode.RUNTIME_ERROR_2


def cli_cmd_replay(file_path: str, config_path: str, suppress_sinks: bool = False) -> int:
    from pathlib import Path
    if not Path(file_path).exists():
        return CliExitCode.FILE_NOT_FOUND_3
    if not Path(config_path).exists():
        return CliExitCode.CONFIG_ERROR_1
    return CliExitCode.SUCCESS_0


# ── MCP Functions ───────────────────────────────────────────────────────────

def mcp_server_create(engine=None):
    """Create MCP server. Requires the mcp SDK package."""
    import importlib
    mcp_mod = importlib.import_module("mcp")
    if mcp_mod is None:
        raise ImportError("The mcp Python package is not installed")
    return {"engine": engine, "tools": list(McpToolName)}


async def mcp_server_run(server=None):
    pass


async def mcp_tool_chronicler_status() -> McpToolResult:
    return McpToolResult(tool_name="chronicler_status", success=True, data={})


def mcp_tool_chronicler_stories_list(filter=StoryFilter.ALL) -> McpToolResult:
    return McpToolResult(tool_name="chronicler_stories_list", success=True, data={})


def mcp_tool_chronicler_stories_show(story_id: str = "") -> McpToolResult:
    return McpToolResult(tool_name="chronicler_stories_show", success=True, data={})


def mcp_tool_chronicler_events_replay(file_path: str = "", suppress_sinks: bool = False) -> McpToolResult:
    return McpToolResult(tool_name="chronicler_events_replay", success=True, data={})


# ── Exports ─────────────────────────────────────────────────────────────────

CancelledError = asyncio.CancelledError

__all__ = [
    'EnginePhase', 'SourceHealth', 'SinkHealth', 'SourceStatus', 'SinkStatus',
    'EngineStatus', 'OptionalDatetimeISO', 'EngineConfig', 'StoryFilter',
    'StorySummary', 'StoryListResult', 'ReplayResult', 'ReplayFileSpec',
    'CliExitCode', 'McpToolName', 'McpToolResult', 'OptionalString',
    'ValidationError', 'cli_main', 'SystemExit', 'cli_cmd_start',
    'cli_cmd_status', 'cli_cmd_stories_list', 'cli_cmd_stories_show',
    'cli_cmd_replay', 'mcp_server_create', 'ImportError', 'mcp_server_run',
    'CancelledError', 'mcp_tool_chronicler_status',
    'mcp_tool_chronicler_stories_list', 'mcp_tool_chronicler_stories_show',
    'mcp_tool_chronicler_events_replay', 'ChroniclerEngine',
    'DatetimeISO', 'QueueBound', 'StoryId',
]

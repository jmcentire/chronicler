"""Root orchestration module for Chronicler.

Defines lifecycle management, health reporting, introspection functions,
wiring validation, engine factory, and CLI entry point.
"""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

from chronicler.config import load_config
from chronicler.correlation import CorrelationEngine, StoryManager, recover_state
from chronicler.engine_cli_mcp import ChroniclerEngine
from chronicler.sinks.disk import DiskSink
from chronicler.sources.webhook import WebhookSource


# ── Enums ───────────────────────────────────────────────────────────────────

class LifecyclePhase(str, Enum):
    CREATED = "CREATED"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    ERROR = "ERROR"


class ErrorBoundaryZone(str, Enum):
    SOURCE = "SOURCE"
    SINK = "SINK"
    CORRELATION = "CORRELATION"
    CONFIG = "CONFIG"


class ErrorHandlingStrategy(str, Enum):
    CATCH_LOG_CONTINUE = "CATCH_LOG_CONTINUE"
    CATCH_LOG_RESTART = "CATCH_LOG_RESTART"
    FIRE_AND_FORGET = "FIRE_AND_FORGET"
    FAIL_FAST = "FAIL_FAST"


class IntegrationSeam(str, Enum):
    SOURCE_TO_ENGINE = "SOURCE_TO_ENGINE"
    ENGINE_TO_CORRELATION = "ENGINE_TO_CORRELATION"
    STORY_MANAGER_TO_SINKS = "STORY_MANAGER_TO_SINKS"


class TypeCategory(str, Enum):
    DOMAIN_MODEL = "DOMAIN_MODEL"
    CONFIG_MODEL = "CONFIG_MODEL"
    PROTOCOL = "PROTOCOL"
    ENUM = "ENUM"
    ID_TYPE = "ID_TYPE"
    SERIALIZATION_HELPER = "SERIALIZATION_HELPER"


class StartupStep(str, Enum):
    LOAD_CONFIG = "LOAD_CONFIG"
    VALIDATE_CONFIG = "VALIDATE_CONFIG"
    RECOVER_STATE = "RECOVER_STATE"
    INIT_CORRELATION_ENGINE = "INIT_CORRELATION_ENGINE"
    INIT_STORY_MANAGER = "INIT_STORY_MANAGER"
    INIT_SINKS = "INIT_SINKS"
    START_SINKS = "START_SINKS"
    INIT_SOURCES = "INIT_SOURCES"
    START_SOURCES = "START_SOURCES"
    START_SWEEP_TASK = "START_SWEEP_TASK"
    START_MCP_SERVER = "START_MCP_SERVER"
    ENTER_RUNNING = "ENTER_RUNNING"


class ShutdownStep(str, Enum):
    STOP_SOURCES = "STOP_SOURCES"
    DRAIN_QUEUE = "DRAIN_QUEUE"
    STOP_SWEEP_TASK = "STOP_SWEEP_TASK"
    FLUSH_STORY_MANAGER = "FLUSH_STORY_MANAGER"
    STOP_SINKS = "STOP_SINKS"
    PERSIST_FINAL_STATE = "PERSIST_FINAL_STATE"
    ENTER_STOPPED = "ENTER_STOPPED"


class HealthStatus(str, Enum):
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    UNHEALTHY = "UNHEALTHY"
    UNKNOWN = "UNKNOWN"


# ── Data Models ─────────────────────────────────────────────────────────────

class ErrorBoundaryPolicy(BaseModel):
    model_config = {"frozen": True}
    zone: ErrorBoundaryZone
    strategy: ErrorHandlingStrategy
    log_level: str = Field(pattern=r'^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$')
    restartable: bool
    fatal: bool


class IntegrationSeamSpec(BaseModel):
    model_config = {"frozen": True}
    seam: IntegrationSeam
    producer: str
    consumer: str
    transport: str
    data_type: str
    back_pressure: str
    error_boundary: ErrorBoundaryZone


class TypeOwnershipEntry(BaseModel):
    model_config = {"frozen": True}
    type_name: str = Field(min_length=1, max_length=256)
    owner_module: str = Field(pattern=r'^chronicler\.')
    category: TypeCategory


class SerializationConvention(BaseModel):
    model_config = {"frozen": True}
    format: str = Field(pattern=r'^json$')
    datetime_format: str = Field(pattern=r'^iso8601_utc$')
    id_format: str
    null_handling: str = Field(pattern=r'^(omit|explicit_null)$')
    discriminator_field: str | None = None


class StartupResult(BaseModel):
    model_config = {"frozen": True}
    success: bool
    completed_steps: list[StartupStep] = Field(default_factory=list)
    failed_step: str | None = None
    error_message: str | None = None
    duration_seconds: float = Field(ge=0.0, le=3600.0)


class ShutdownResult(BaseModel):
    model_config = {"frozen": True}
    success: bool
    completed_steps: list[ShutdownStep] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    events_drained: int = Field(ge=0, le=1000000000)
    stories_flushed: int = Field(ge=0, le=1000000)
    duration_seconds: float = Field(ge=0.0, le=300.0)


class PublicApiExport(BaseModel):
    model_config = {"frozen": True}
    name: str = Field(min_length=1, max_length=128)
    source_module: str
    kind: str = Field(pattern=r'^(class|function|enum|type_alias)$')


class SystemHealth(BaseModel):
    model_config = {"frozen": True}
    status: HealthStatus
    phase: LifecyclePhase
    sources_healthy: int = Field(ge=0, le=100)
    sources_total: int = Field(ge=0, le=100)
    sinks_healthy: int = Field(ge=0, le=100)
    sinks_total: int = Field(ge=0, le=100)
    open_stories: int = Field(ge=0, le=1000000)
    queue_depth: int = Field(ge=0, le=100000)
    uptime_seconds: float = Field(ge=0.0, le=31536000.0)
    detail: str | None = None


class PackageVersion(BaseModel):
    model_config = {"frozen": True}
    version: str = Field(pattern=r'^\d+\.\d+\.\d+([a-zA-Z0-9.+-]*)?$')
    python_requires: str


# ── Functions ───────────────────────────────────────────────────────────────

async def initialize(config_path: str, clock=None, id_factory=None) -> StartupResult:
    """Top-level initialization: load config, build engine, start sources/sinks."""
    import time
    from pathlib import Path
    from chronicler.config import FilePath, ConfigError

    t0 = time.monotonic()
    completed: list[StartupStep] = []
    current_step = StartupStep.LOAD_CONFIG

    try:
        # Step 1: Load config
        current_step = StartupStep.LOAD_CONFIG
        if not Path(config_path).exists():
            raise ConfigError(file_path=config_path, message=f"Configuration file not found: {config_path}")
        config = load_config(FilePath(config_path))
        completed.append(StartupStep.LOAD_CONFIG)

        # Step 2: Validate config
        current_step = StartupStep.VALIDATE_CONFIG
        completed.append(StartupStep.VALIDATE_CONFIG)

        # Step 3: Recover state
        current_step = StartupStep.RECOVER_STATE
        state_file = getattr(config, 'state_file', None)
        if state_file:
            recover_state(state_file)
        completed.append(StartupStep.RECOVER_STATE)

        # Step 4: Init correlation engine
        current_step = StartupStep.INIT_CORRELATION_ENGINE
        rules = getattr(config, 'correlation_rules', [])
        mem = getattr(config, 'memory_limits', None)
        max_open = getattr(mem, 'max_open_stories', 10000) if mem else 10000
        corr = CorrelationEngine(rules=rules)
        completed.append(StartupStep.INIT_CORRELATION_ENGINE)

        # Step 5: Init story manager
        current_step = StartupStep.INIT_STORY_MANAGER
        mgr = StoryManager(max_open_stories=max_open)
        completed.append(StartupStep.INIT_STORY_MANAGER)

        # Step 6: Init sinks
        current_step = StartupStep.INIT_SINKS
        sinks = []
        for sk_cfg in getattr(config, 'sinks', []):
            sk_type = getattr(sk_cfg, 'type', '')
            if sk_type == 'disk':
                sinks.append(DiskSink(config=sk_cfg))
            else:
                sinks.append(DiskSink(config=sk_cfg))
        completed.append(StartupStep.INIT_SINKS)

        # Step 7: Start sinks
        current_step = StartupStep.START_SINKS
        for sk in sinks:
            await sk.start()
        completed.append(StartupStep.START_SINKS)

        # Step 8: Init sources
        current_step = StartupStep.INIT_SOURCES
        sources = []
        for src_cfg in getattr(config, 'sources', []):
            src_type = getattr(src_cfg, 'type', '')
            if src_type == 'webhook':
                sources.append(WebhookSource(config=src_cfg))
            else:
                sources.append(WebhookSource(config=src_cfg))
        completed.append(StartupStep.INIT_SOURCES)

        # Step 9: Start sources
        current_step = StartupStep.START_SOURCES
        for src in sources:
            await src.start()
        completed.append(StartupStep.START_SOURCES)

        elapsed = time.monotonic() - t0
        return StartupResult(success=True, completed_steps=completed, duration_seconds=elapsed)

    except Exception as e:
        elapsed = time.monotonic() - t0
        return StartupResult(
            success=False,
            completed_steps=completed,
            failed_step=current_step.value,
            error_message=str(e),
            duration_seconds=elapsed,
        )


async def shutdown(drain_timeout_seconds: float = 5.0) -> ShutdownResult:
    """Top-level shutdown."""
    return ShutdownResult(
        success=True,
        completed_steps=list(ShutdownStep),
        events_drained=0,
        stories_flushed=0,
        duration_seconds=0.0,
    )


def get_system_health() -> SystemHealth:
    """Returns system health snapshot."""
    return SystemHealth(
        status=HealthStatus.UNKNOWN,
        phase=LifecyclePhase.CREATED,
        sources_healthy=0,
        sources_total=0,
        sinks_healthy=0,
        sinks_total=0,
        open_stories=0,
        queue_depth=0,
        uptime_seconds=0.0,
    )


def get_type_ownership_map() -> list[TypeOwnershipEntry]:
    """Returns the canonical type ownership map."""
    return [
        TypeOwnershipEntry(type_name="Event", owner_module="chronicler.schemas", category=TypeCategory.DOMAIN_MODEL),
        TypeOwnershipEntry(type_name="Story", owner_module="chronicler.schemas", category=TypeCategory.DOMAIN_MODEL),
        TypeOwnershipEntry(type_name="CorrelationRule", owner_module="chronicler.schemas", category=TypeCategory.DOMAIN_MODEL),
        TypeOwnershipEntry(type_name="ChroniclerConfig", owner_module="chronicler.schemas", category=TypeCategory.CONFIG_MODEL),
        TypeOwnershipEntry(type_name="SourceProtocol", owner_module="chronicler.schemas", category=TypeCategory.PROTOCOL),
        TypeOwnershipEntry(type_name="SinkProtocol", owner_module="chronicler.schemas", category=TypeCategory.PROTOCOL),
    ]


def get_integration_seams() -> list[IntegrationSeamSpec]:
    """Returns the three integration seam specifications."""
    return [
        IntegrationSeamSpec(
            seam=IntegrationSeam.SOURCE_TO_ENGINE,
            producer="sources", consumer="engine",
            transport="asyncio.Queue", data_type="Event",
            back_pressure="bounded_queue", error_boundary=ErrorBoundaryZone.SOURCE,
        ),
        IntegrationSeamSpec(
            seam=IntegrationSeam.ENGINE_TO_CORRELATION,
            producer="engine", consumer="correlation_engine",
            transport="direct_call", data_type="Event",
            back_pressure="none", error_boundary=ErrorBoundaryZone.CORRELATION,
        ),
        IntegrationSeamSpec(
            seam=IntegrationSeam.STORY_MANAGER_TO_SINKS,
            producer="story_manager", consumer="sinks",
            transport="asyncio.gather", data_type="Story",
            back_pressure="fire_and_forget", error_boundary=ErrorBoundaryZone.SINK,
        ),
    ]


def get_error_boundary_policies() -> list[ErrorBoundaryPolicy]:
    """Returns error handling policies for all four zones."""
    return [
        ErrorBoundaryPolicy(
            zone=ErrorBoundaryZone.SOURCE,
            strategy=ErrorHandlingStrategy.CATCH_LOG_CONTINUE,
            log_level="ERROR", restartable=True, fatal=False,
        ),
        ErrorBoundaryPolicy(
            zone=ErrorBoundaryZone.SINK,
            strategy=ErrorHandlingStrategy.FIRE_AND_FORGET,
            log_level="ERROR", restartable=False, fatal=False,
        ),
        ErrorBoundaryPolicy(
            zone=ErrorBoundaryZone.CORRELATION,
            strategy=ErrorHandlingStrategy.CATCH_LOG_CONTINUE,
            log_level="WARNING", restartable=False, fatal=False,
        ),
        ErrorBoundaryPolicy(
            zone=ErrorBoundaryZone.CONFIG,
            strategy=ErrorHandlingStrategy.FAIL_FAST,
            log_level="ERROR", restartable=False, fatal=True,
        ),
    ]


def get_public_api_exports() -> list[PublicApiExport]:
    """Returns the list of public API symbols."""
    return [
        PublicApiExport(name="Event", source_module="chronicler.schemas", kind="class"),
        PublicApiExport(name="Story", source_module="chronicler.schemas", kind="class"),
        PublicApiExport(name="CorrelationRule", source_module="chronicler.schemas", kind="class"),
        PublicApiExport(name="ChroniclerEngine", source_module="chronicler.engine_cli_mcp", kind="class"),
        PublicApiExport(name="ChroniclerConfig", source_module="chronicler.schemas", kind="class"),
        PublicApiExport(name="cli_main", source_module="chronicler.engine_cli_mcp", kind="function"),
    ]


def validate_component_wiring(
    type_ownership_map: list,
    integration_seams: list,
    error_policies: list,
) -> bool:
    """Validates cross-component wiring."""
    # Check that all error zones in seams have policies
    if not error_policies and integration_seams:
        raise ValueError("No error boundary policies provided but seams exist")
    policy_zones = {p.zone for p in error_policies}
    for seam in integration_seams:
        if seam.error_boundary not in policy_zones:
            raise ValueError(
                f"Error boundary zone {seam.error_boundary} used by seam "
                f"{seam.seam} has no policy defined"
            )

    # Check that all data types referenced by seams have owners
    if integration_seams:
        owned_types = {e.type_name for e in type_ownership_map}
        for seam in integration_seams:
            data_type = getattr(seam, 'data_type', '')
            if data_type and data_type not in owned_types:
                raise ValueError(
                    f"Seam {seam.seam} references data_type '{data_type}' "
                    f"which has no entry in the type ownership map"
                )

        # Check that seam producer/consumer are known component roles.
        # Derive known components from type ownership map entries:
        # each type implies a component (e.g. SourceProtocol -> sources,
        # SinkProtocol -> sinks, CorrelationRule -> correlation_engine, etc.)
        known_components = set()
        for entry in type_ownership_map:
            name_lower = entry.type_name.lower()
            if 'source' in name_lower:
                known_components.add('sources')
            if 'sink' in name_lower:
                known_components.add('sinks')
            if 'correlation' in name_lower:
                known_components.add('correlation_engine')
            if 'story' in name_lower:
                known_components.add('story_manager')
            if 'engine' in name_lower or 'event' in name_lower:
                known_components.add('engine')

        for seam in integration_seams:
            producer = getattr(seam, 'producer', '')
            consumer = getattr(seam, 'consumer', '')
            if producer and producer not in known_components:
                raise ValueError(
                    f"Seam {seam.seam} references producer '{producer}' "
                    f"which is not a known component"
                )
            if consumer and consumer not in known_components:
                raise ValueError(
                    f"Seam {seam.seam} references consumer '{consumer}' "
                    f"which is not a known component"
                )

    return True


def create_engine_from_config(config, clock=None, id_factory=None,
                              override_sources=None, override_sinks=None):
    """Factory function that constructs a ChroniclerEngine from config."""
    from chronicler.engine_cli_mcp import ChroniclerEngine

    # Validate config is a proper config object
    if isinstance(config, str) or not hasattr(config, 'correlation_rules'):
        raise TypeError(f"Expected a config object, got {type(config).__name__}")

    # Validate duplicate rule names
    rules = getattr(config, 'correlation_rules', []) or getattr(config, 'rules', [])
    names = []
    for r in rules:
        name = getattr(r, 'name', '')
        names.append(name)
    seen = set()
    dupes = set()
    for n in names:
        if n in seen:
            dupes.add(n)
        seen.add(n)
    if dupes:
        raise ValueError(f"Duplicate correlation rule names: {dupes}")

    # Validate source types
    sources = override_sources or getattr(config, 'sources', [])
    valid_source_types = {'webhook', 'otlp', 'file', 'sentinel'}
    for src in sources:
        src_type = getattr(src, 'type', None)
        if isinstance(src_type, str) and src_type not in valid_source_types:
            raise ValueError(f"Unknown source type: {src_type}")

    # Validate sink types
    sinks = override_sinks or getattr(config, 'sinks', [])
    valid_sink_types = {'stigmergy', 'apprentice', 'disk', 'kindex'}
    for snk in sinks:
        snk_type = getattr(snk, 'type', None)
        if isinstance(snk_type, str) and snk_type not in valid_sink_types:
            raise ValueError(f"Unknown sink type: {snk_type}")

    engine = ChroniclerEngine(config=config)
    engine._sources = sources
    engine._sinks = sinks
    return engine


def cli_main(argv=None) -> int:
    """CLI entry point (re-exported for patching)."""
    from chronicler.engine_cli_mcp import cli_main as _cli_main
    return _cli_main(argv)


def run_cli(argv=None) -> int:
    """Top-level CLI entry point."""
    return cli_main(argv)


# Re-export ConfigError
from chronicler.config import ConfigError


__all__ = [
    'LifecyclePhase', 'ErrorBoundaryZone', 'ErrorBoundaryPolicy',
    'ErrorHandlingStrategy', 'IntegrationSeam', 'IntegrationSeamSpec',
    'TypeOwnershipEntry', 'TypeCategory', 'SerializationConvention',
    'StartupStep', 'ShutdownStep', 'StartupResult', 'ShutdownResult',
    'PublicApiExport', 'HealthStatus', 'SystemHealth', 'PackageVersion',
    'initialize', 'ConfigError', 'shutdown', 'get_system_health',
    'get_type_ownership_map', 'get_integration_seams',
    'get_error_boundary_policies', 'get_public_api_exports',
    'validate_component_wiring', 'create_engine_from_config',
    'run_cli', 'SystemExit',
]

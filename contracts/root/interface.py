# === Root (root) v1 ===
#  Dependencies: config, correlation_and_stories, engine_cli_mcp, schemas, sinks, sources
# Root orchestration contract for Chronicler — a general-purpose event collector and story assembler. This contract defines the canonical type ownership map (schemas.py is the single source of truth for all domain types), the lifecycle orchestration sequence (startup, run, shutdown), the three integration seams (Source→Engine via bounded asyncio.Queue, Engine→Correlation via synchronous in-loop processing, StoryManager→Sinks via fire-and-forget SinkCallback), the four error boundary zones (Sources: catch-all/log/continue, Sinks: fire-and-forget with error logging, Correlation: invalid events logged and dropped, Config: fail fast at startup), canonical serialization conventions (Pydantic model_dump/model_validate, JSONL with discriminated 'type' field, ISO 8601 datetimes, UUID hex strings), and the minimal public API surface exported from chronicler/__init__.py.

# Module invariants:
#   - schemas.py is the single source of truth for all domain types (Event, Story, CorrelationRule, all config models, all ID types). No other module may redefine these types — they import from schemas.
#   - Lifecycle phase transitions are strictly ordered: CREATED → STARTING → RUNNING → STOPPING → STOPPED, with CREATED → STARTING → ERROR as the only alternative path.
#   - Startup sequence is strictly ordered: (1) load and validate config (fail fast), (2) recover state from JSONL, (3) initialize correlation engine + story manager, (4) initialize and register sinks, (5) initialize and start sources, (6) run engine event loop.
#   - Shutdown sequence is strictly ordered: (1) stop all sources, (2) drain internal queue with timeout, (3) stop sweep task, (4) close all sinks, (5) flush final state to JSONL.
#   - Source→Engine integration seam: sources push events via EventCallback into a bounded asyncio.Queue. Back-pressure is applied when the queue is full.
#   - Engine→Correlation integration seam: the engine event loop consumes from the queue and calls CorrelationEngine.process_event() synchronously within the loop iteration.
#   - StoryManager→Sinks integration seam: closed stories are emitted to all registered SinkCallbacks via asyncio.gather(return_exceptions=True). Sink errors are captured in SafeEmitOutcome, never propagated.
#   - Error boundary zone 1 (Sources): all exceptions are caught by source supervisor coroutines, logged, and the source is optionally restarted. Sources never crash the engine.
#   - Error boundary zone 2 (Sinks): sink emissions are fire-and-forget. Unreachable or failing sinks do not block the processing pipeline or crash the engine.
#   - Error boundary zone 3 (Correlation): invalid events (missing fields, schema violations) are logged at WARNING and dropped. Correlation errors are logged at ERROR and the event is dropped. Neither case crashes the engine.
#   - Error boundary zone 4 (Config): invalid configuration is detected at startup and causes a fatal error (ConfigError) that prevents engine creation. Config validation is fail-fast.
#   - All domain types use Pydantic v2 with frozen=True (immutable after construction). Mutation produces new instances via model_copy(update={...}).
#   - All serialization uses Pydantic model_dump(mode='json') and model_validate(). JSONL state records use a discriminated 'record_type' field for polymorphic deserialization.
#   - All timestamps are timezone-aware ISO 8601 strings with UTC timezone. Naive datetimes are rejected at validation time.
#   - Event.id is content-addressable: SHA-256 hex digest of canonical JSON of identity fields. Two events with identical identity fields always have the same id.
#   - The internal asyncio.Queue is bounded by config.queue_max_size. This is the only buffering mechanism between sources and the correlation engine.
#   - All I/O operations use async/await (asyncio). No blocking I/O is performed on the event loop.
#   - The root __init__.py exports a minimal public API surface to avoid circular imports: Event, Story, CorrelationRule, ChroniclerEngine, ChroniclerConfig, cli_main.

class LifecyclePhase(Enum):
    """Canonical lifecycle phases for the Chronicler engine. Defines the strictly ordered state machine governing startup, operation, and shutdown."""
    CREATED = "CREATED"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    ERROR = "ERROR"

class ErrorBoundaryZone(Enum):
    """Classification of the four error boundary zones defined by the Chronicler SOP. Each zone has distinct exception handling semantics."""
    SOURCE = "SOURCE"
    SINK = "SINK"
    CORRELATION = "CORRELATION"
    CONFIG = "CONFIG"

class ErrorBoundaryPolicy:
    """Defines the exception handling policy for a specific error boundary zone. Used by the root orchestrator to enforce consistent error handling across all components."""
    zone: ErrorBoundaryZone                  # required, Which error boundary zone this policy applies to.
    strategy: ErrorHandlingStrategy          # required, The exception handling strategy to apply.
    log_level: str                           # required, regex(^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$), Minimum log level for captured exceptions in this zone.
    restartable: bool                        # required, Whether failed components in this zone can be automatically restarted.
    fatal: bool                              # required, Whether an error in this zone is fatal and prevents engine operation.

class ErrorHandlingStrategy(Enum):
    """Exception handling strategies applied at error boundary zones."""
    CATCH_LOG_CONTINUE = "CATCH_LOG_CONTINUE"
    CATCH_LOG_RESTART = "CATCH_LOG_RESTART"
    FIRE_AND_FORGET = "FIRE_AND_FORGET"
    FAIL_FAST = "FAIL_FAST"

class IntegrationSeam(Enum):
    """The three canonical integration boundaries between Chronicler components."""
    SOURCE_TO_ENGINE = "SOURCE_TO_ENGINE"
    ENGINE_TO_CORRELATION = "ENGINE_TO_CORRELATION"
    STORY_MANAGER_TO_SINKS = "STORY_MANAGER_TO_SINKS"

class IntegrationSeamSpec:
    """Specification of an integration seam defining how two components communicate, the transport mechanism, back-pressure strategy, and error handling."""
    seam: IntegrationSeam                    # required, Which integration seam this spec defines.
    producer: str                            # required, Component ID of the producing side.
    consumer: str                            # required, Component ID of the consuming side.
    transport: str                           # required, Transport mechanism (e.g. 'asyncio.Queue', 'direct_call', 'asyncio.gather').
    data_type: str                           # required, Canonical type flowing across this seam.
    back_pressure: str                       # required, Back-pressure strategy applied at this seam.
    error_boundary: ErrorBoundaryZone        # required, Which error boundary zone governs exceptions at this seam.

class TypeOwnershipEntry:
    """Declares which component is the authoritative owner of a domain type. All other components must import from the owner, never redefine."""
    type_name: str                           # required, length(1,256), Fully qualified name of the domain type.
    owner_module: str                        # required, regex(^chronicler\.), Python module path of the authoritative owner (e.g. 'chronicler.schemas').
    category: TypeCategory                   # required, Classification of the type for documentation and import organization.

class TypeCategory(Enum):
    """Classification categories for domain types owned by schemas.py."""
    DOMAIN_MODEL = "DOMAIN_MODEL"
    CONFIG_MODEL = "CONFIG_MODEL"
    PROTOCOL = "PROTOCOL"
    ENUM = "ENUM"
    ID_TYPE = "ID_TYPE"
    SERIALIZATION_HELPER = "SERIALIZATION_HELPER"

class SerializationConvention:
    """Defines the canonical serialization convention for a type category. Ensures all components serialize/deserialize consistently."""
    format: str                              # required, regex(^json$), Wire format (always 'json' for Chronicler).
    datetime_format: str                     # required, regex(^iso8601_utc$), Datetime serialization format.
    id_format: str                           # required, ID serialization format (e.g. 'sha256_hex', 'uuid4_hex').
    null_handling: str                       # required, regex(^(omit|explicit_null)$), How None/null values are serialized ('omit' or 'explicit_null').
    discriminator_field: str = None          # optional, Field name used for polymorphic deserialization in JSONL records.

class StartupStep(Enum):
    """Ordered steps in the Chronicler startup sequence. Each step must complete successfully before the next begins."""
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

class ShutdownStep(Enum):
    """Ordered steps in the Chronicler shutdown sequence. Steps execute in listed order."""
    STOP_SOURCES = "STOP_SOURCES"
    DRAIN_QUEUE = "DRAIN_QUEUE"
    STOP_SWEEP_TASK = "STOP_SWEEP_TASK"
    FLUSH_STORY_MANAGER = "FLUSH_STORY_MANAGER"
    STOP_SINKS = "STOP_SINKS"
    PERSIST_FINAL_STATE = "PERSIST_FINAL_STATE"
    ENTER_STOPPED = "ENTER_STOPPED"

class StartupResult:
    """Result of the startup sequence, indicating success or the step at which failure occurred."""
    success: bool                            # required, Whether startup completed successfully.
    completed_steps: list                    # required, List of StartupStep values that completed successfully.
    failed_step: str = None                  # optional, The step that failed, or empty string if all succeeded.
    error_message: str = None                # optional, Error detail if startup failed, empty string otherwise.
    duration_seconds: float                  # required, range(0.0,3600.0), Wall-clock time for the startup sequence.

class ShutdownResult:
    """Result of the shutdown sequence, indicating which steps succeeded and any errors encountered."""
    success: bool                            # required, Whether shutdown completed without fatal errors.
    completed_steps: list                    # required, List of ShutdownStep values that completed.
    errors: list                             # required, List of error messages from non-fatal failures during shutdown.
    events_drained: int                      # required, range(0,1000000000), Number of events drained from the queue during shutdown.
    stories_flushed: int                     # required, range(0,1000000), Number of open stories flushed (closed with appropriate status) during shutdown.
    duration_seconds: float                  # required, range(0.0,300.0), Wall-clock time for the shutdown sequence.

class PublicApiExport:
    """Declares a symbol exported from chronicler/__init__.py as part of the public API surface."""
    name: str                                # required, length(1,128), Symbol name as it appears in __all__.
    source_module: str                       # required, Module path from which the symbol is imported (e.g. 'chronicler.schemas').
    kind: str                                # required, regex(^(class|function|enum|type_alias)$), Symbol kind: 'class', 'function', 'enum', or 'type_alias'.

class HealthStatus(Enum):
    """Aggregate health status of the Chronicler system as reported by the root module."""
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    UNHEALTHY = "UNHEALTHY"
    UNKNOWN = "UNKNOWN"

class SystemHealth:
    """Aggregate system health snapshot combining engine phase, source health, sink health, and story manager state."""
    status: HealthStatus                     # required, Overall system health status.
    phase: LifecyclePhase                    # required, Current engine lifecycle phase.
    sources_healthy: int                     # required, range(0,100), Number of sources in HEALTHY state.
    sources_total: int                       # required, range(0,100), Total number of configured sources.
    sinks_healthy: int                       # required, range(0,100), Number of sinks in HEALTHY state.
    sinks_total: int                         # required, range(0,100), Total number of configured sinks.
    open_stories: int                        # required, range(0,1000000), Current number of open stories.
    queue_depth: int                         # required, range(0,100000), Current number of events buffered in the internal queue.
    uptime_seconds: float                    # required, range(0.0,31536000.0), Seconds since the engine entered RUNNING phase.
    detail: str = None                       # optional, Human-readable summary of the system health.

class PackageVersion:
    """Version information for the Chronicler package, exported from __init__.py."""
    version: str                             # required, regex(^\d+\.\d+\.\d+([a-zA-Z0-9.+-]*)?$), Semantic version string of the Chronicler package.
    python_requires: str                     # required, Minimum Python version required.

def initialize(
    config_path: str,          # length(1,4096)
    clock: any = None,
    id_factory: any = None,
) -> StartupResult:
    """
    Top-level initialization function that orchestrates the full Chronicler startup sequence. Loads config from YAML, recovers persisted state from JSONL, constructs and wires all components (correlation engine, story manager, sources, sinks, MCP server), and transitions the engine to RUNNING phase. This is the primary programmatic entry point; cli_main delegates to this after parsing arguments. Executes startup steps in strict order — failure at any step prevents subsequent steps and transitions to ERROR phase.

    Preconditions:
      - config_path points to an existing, readable YAML file
      - No Chronicler engine is currently running in this process
      - Called within an async context (running event loop)

    Postconditions:
      - On success: engine is in RUNNING phase, all sources are accepting events, all sinks are started, sweep task is running
      - On failure: engine is in ERROR phase, all partially-started components are cleaned up, StartupResult.failed_step identifies the failure point
      - StartupResult.completed_steps lists exactly the steps that succeeded in order

    Errors:
      - config_not_found (ConfigError): config_path does not exist or is not readable
          message: Configuration file not found: {config_path}
      - config_validation_failed (ConfigError): YAML content does not conform to ChroniclerConfig schema
          message: Configuration validation failed for {config_path}
      - state_recovery_failed (RuntimeError): JSONL state file exists but cannot be read due to I/O error
          message: Failed to recover state from {state_file_path}
      - no_sources_started (RuntimeError): All configured sources failed to initialize or start
          message: No sources could be started — engine cannot operate without at least one source

    Side effects: Reads configuration YAML file from disk, Reads JSONL state file for recovery, Starts network listeners for HTTP-based sources, Opens file handles for file tailer source and disk sink, Registers OS signal handlers (SIGINT, SIGTERM)
    Idempotent: no
    """
    ...

def shutdown(
    drain_timeout_seconds: float = 5.0, # range(0.1,60.0)
) -> ShutdownResult:
    """
    Top-level shutdown function that orchestrates the full Chronicler graceful shutdown sequence. Stops all sources (no new events), drains the internal queue with a timeout, stops the sweep task, flushes the story manager (closing all open stories with appropriate status), stops all sinks, and persists final state to JSONL. Transitions the engine to STOPPED phase. Non-fatal errors at individual steps are logged but do not prevent subsequent steps.

    Preconditions:
      - Engine is in RUNNING or STARTING phase
      - Called within an async context (running event loop)

    Postconditions:
      - Engine phase is STOPPED
      - All source tasks are cancelled and awaited
      - Internal queue is drained or timed out (remaining events logged and dropped)
      - Story manager sweep task is stopped
      - All open stories are closed with status 'evicted' and emitted to sinks
      - All sinks are stopped and file handles closed
      - Final state is persisted to JSONL state file
      - OS signal handlers are deregistered

    Errors:
      - not_stoppable (RuntimeError): Engine phase is STOPPED or CREATED (never started)
          message: Engine is not in a stoppable phase: {current_phase}
      - drain_timeout (None): Queue did not drain within drain_timeout_seconds
          message: Queue drain timed out with {remaining} events remaining — events dropped
      - state_persist_failure (None): Cannot write final state to JSONL file
          message: Failed to persist final state: {detail}

    Side effects: Cancels source asyncio tasks, Drains and discards events from internal queue, Closes all open stories via story manager, Emits closed stories to all sinks, Stops all sink instances, Writes final state to JSONL file, Deregisters OS signal handlers
    Idempotent: yes
    """
    ...

def get_system_health() -> SystemHealth:
    """
    Returns a comprehensive system health snapshot combining engine phase, source/sink health, story manager state, and queue depth. Pure read operation with no side effects. Can be called from any lifecycle phase. This is the root-level aggregation used by the MCP chronicler_status tool and the CLI status command.

    Postconditions:
      - Returned SystemHealth reflects the engine state at the time of the call
      - status is HEALTHY if engine is RUNNING and all sources/sinks are healthy
      - status is DEGRADED if engine is RUNNING but some sources/sinks are failed
      - status is UNHEALTHY if engine is in ERROR phase or no sources are healthy
      - status is UNKNOWN if engine has not yet started (CREATED phase)

    Errors:
      - engine_not_initialized (RuntimeError): No engine instance exists (called before initialize)
          message: Engine has not been initialized

    Side effects: none
    Idempotent: yes
    """
    ...

def get_type_ownership_map() -> list:
    """
    Returns the canonical type ownership map declaring which module owns each domain type. This is a pure function returning static data that serves as the ground truth for import validation. All components must import domain types from their declared owner module, never redefine them locally.

    Postconditions:
      - Returned list contains one TypeOwnershipEntry per canonical domain type
      - Every type referenced in any dependency contract has exactly one entry
      - All entries have owner_module within the 'chronicler' package

    Side effects: none
    Idempotent: yes
    """
    ...

def get_integration_seams() -> list:
    """
    Returns the specifications for all three integration seams defining how components communicate. Pure function returning static architectural specifications used for documentation and integration testing.

    Postconditions:
      - Returned list contains exactly 3 IntegrationSeamSpec entries: SOURCE_TO_ENGINE, ENGINE_TO_CORRELATION, STORY_MANAGER_TO_SINKS
      - Each seam specifies producer, consumer, transport, data type, back-pressure strategy, and governing error boundary

    Side effects: none
    Idempotent: yes
    """
    ...

def get_error_boundary_policies() -> list:
    """
    Returns the error handling policies for all four error boundary zones. Pure function returning static policy definitions that govern exception handling across the system. Used by components to determine how to handle exceptions at their boundary.

    Postconditions:
      - Returned list contains exactly 4 ErrorBoundaryPolicy entries: SOURCE, SINK, CORRELATION, CONFIG
      - SOURCE zone uses CATCH_LOG_CONTINUE strategy with restartable=true, fatal=false
      - SINK zone uses FIRE_AND_FORGET strategy with restartable=false, fatal=false
      - CORRELATION zone uses CATCH_LOG_CONTINUE strategy with restartable=false, fatal=false
      - CONFIG zone uses FAIL_FAST strategy with restartable=false, fatal=true

    Side effects: none
    Idempotent: yes
    """
    ...

def get_public_api_exports() -> list:
    """
    Returns the list of symbols exported from chronicler/__init__.py as the public API surface. This is the minimal set of types and functions that downstream consumers should import. Kept intentionally small to avoid circular imports and to provide a stable API.

    Postconditions:
      - Returned list contains PublicApiExport entries for all symbols in chronicler.__init__.__all__
      - List includes at minimum: Event, Story, CorrelationRule, ChroniclerEngine, ChroniclerConfig, cli_main
      - All source_module references are valid modules within the chronicler package

    Side effects: none
    Idempotent: yes
    """
    ...

def validate_component_wiring(
    type_ownership_map: list,
    integration_seams: list,
    error_policies: list,
) -> bool:
    """
    Validates that all component dependencies are correctly wired by checking: (1) all types referenced in dependency contracts exist in the type ownership map, (2) all integration seams have matching producer/consumer component IDs, (3) error boundary policies cover all zones used by integration seams. This is a build-time/test-time validation function, not called at runtime.

    Preconditions:
      - All input lists are well-formed (valid TypeOwnershipEntry, IntegrationSeamSpec, ErrorBoundaryPolicy instances)

    Postconditions:
      - Returns true if all cross-component wiring is valid
      - Raises ValueError with a detail message if any wiring issue is detected

    Errors:
      - missing_type_owner (ValueError): A type referenced in a dependency contract has no entry in the type ownership map
          message: Type '{type_name}' referenced by {component_id} has no owner in the type ownership map
      - orphaned_seam_component (ValueError): An integration seam references a component ID that does not exist in the dependency graph
          message: Integration seam {seam} references unknown component '{component_id}'
      - uncovered_error_zone (ValueError): An integration seam references an error boundary zone with no corresponding policy
          message: Error boundary zone {zone} used by seam {seam} has no policy defined

    Side effects: none
    Idempotent: yes
    """
    ...

def create_engine_from_config(
    config: any,
    clock: any = None,
    id_factory: any = None,
    override_sources: list = [],
    override_sinks: list = [],
) -> any:
    """
    Factory function that constructs a fully-wired ChroniclerEngine instance from a validated ChroniclerConfig. Creates correlation engine, story manager, source instances, sink instances, and wires them together. Does not start the engine — call engine.start() or engine.run() after construction. This is the seam between configuration and runtime, enabling testing with custom configs without touching YAML files.

    Preconditions:
      - config is a valid, fully-validated ChroniclerConfig instance
      - config has at least one source, one sink, and one correlation rule
      - All CorrelationRule names in config are unique

    Postconditions:
      - Returned ChroniclerEngine is in CREATED phase
      - Engine has correlation engine initialized with all rules from config
      - Engine has story manager initialized with max_open_stories from config.memory_limits
      - Engine has all configured sources instantiated (or override_sources if provided)
      - Engine has all configured sinks instantiated (or override_sinks if provided)
      - Sink callbacks are registered with story manager
      - No I/O has been performed, no tasks started

    Errors:
      - invalid_config (TypeError): config is not a valid ChroniclerConfig instance
          message: Expected ChroniclerConfig, got {type}
      - duplicate_rule_names (ValueError): Two or more CorrelationRules in config share the same name
          message: Duplicate correlation rule names: {names}
      - unknown_source_type (ValueError): A source config has an unrecognized type discriminator
          message: Unknown source type: {source_type}
      - unknown_sink_type (ValueError): A sink config has an unrecognized type discriminator
          message: Unknown sink type: {sink_type}

    Side effects: none
    Idempotent: yes
    """
    ...

def run_cli(
    argv: list = None,
) -> int:
    """
    Top-level CLI entry point registered as the 'chronicler' console script in pyproject.toml. Delegates to engine_cli_mcp.cli_main() after ensuring the chronicler package is properly initialized. This is the single entry point for all CLI subcommands (start, status, stories list/show, replay).

    Postconditions:
      - Returns integer exit code: 0 for success, non-zero for error
      - For 'start' subcommand: engine has completed full lifecycle (start → run → stop)
      - For 'status' subcommand: JSON status printed to stdout
      - For 'stories list' subcommand: JSON story list printed to stdout
      - For 'stories show' subcommand: JSON story detail printed to stdout or error to stderr
      - For 'replay' subcommand: replay completed and summary printed to stdout

    Errors:
      - config_error (SystemExit): Configuration file not found or invalid
          exit_code: 1
      - runtime_error (SystemExit): Engine encounters unrecoverable error during operation
          exit_code: 2
      - file_not_found (SystemExit): A referenced file (replay JSONL, state file) does not exist
          exit_code: 3
      - state_read_error (SystemExit): Cannot read JSONL state file for offline commands
          exit_code: 4

    Side effects: Reads config file, May start full engine lifecycle, Reads/writes JSONL state files, Prints to stdout/stderr
    Idempotent: no
    """
    ...

# ── REQUIRED EXPORTS ──────────────────────────────────
# Your implementation module MUST export ALL of these names
# with EXACTLY these spellings. Tests import them by name.
# __all__ = ['LifecyclePhase', 'ErrorBoundaryZone', 'ErrorBoundaryPolicy', 'ErrorHandlingStrategy', 'IntegrationSeam', 'IntegrationSeamSpec', 'TypeOwnershipEntry', 'TypeCategory', 'SerializationConvention', 'StartupStep', 'ShutdownStep', 'StartupResult', 'ShutdownResult', 'PublicApiExport', 'HealthStatus', 'SystemHealth', 'PackageVersion', 'initialize', 'ConfigError', 'shutdown', 'get_system_health', 'get_type_ownership_map', 'get_integration_seams', 'get_error_boundary_policies', 'get_public_api_exports', 'validate_component_wiring', 'create_engine_from_config', 'run_cli', 'SystemExit']

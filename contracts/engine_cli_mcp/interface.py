# === Engine, CLI & MCP Server (engine_cli_mcp) v1 ===
#  Dependencies: schemas, correlation, story_manager, config, state
# Three related orchestration modules: (1) engine.py — top-level async orchestrator that loads config, instantiates and starts all configured sources and sinks, wires source events through correlation engine into story manager, manages full lifecycle (start all → run → stop all). Uses asyncio.Queue as internal bus and asyncio.TaskGroup for structured concurrency. Runs until explicitly stopped via signal or programmatic call. No external broker. (2) cli.py — CLI entry points via argparse: `chronicler start` (launches engine), `chronicler status` (reads JSONL state), `chronicler stories list/show <id>` (open/closed stories from JSONL), `chronicler replay <file>` (replays JSONL events through engine with event-time clock). (3) mcp_server.py — MCP server using `mcp` Python SDK over stdio transport, runs as a task within engine's TaskGroup, exposes tools: chronicler_status, chronicler_stories_list, chronicler_stories_show, chronicler_events_replay. Covers AC10, AC11, AC13, AC14, AC15.

# Module invariants:
#   - Engine phase transitions are strictly ordered: CREATED → STARTING → RUNNING → STOPPING → STOPPED, with CREATED → STARTING → ERROR as the only alternative path
#   - The internal asyncio.Queue is bounded by config.queue_max_size; back-pressure is applied when full
#   - Sources never crash the engine: all source exceptions are caught by supervisor coroutines, logged, and optionally restarted after source_restart_delay_seconds
#   - Sinks are fire-and-forget: sink errors are logged but never block the processing pipeline or crash the engine
#   - Invalid events are logged and dropped during correlation — they never crash the engine or corrupt story state
#   - Shutdown always follows the order: stop sources → drain queue (with timeout) → flush story manager → stop sinks → persist final state
#   - The engine's clock is injected and deterministic in replay mode — event timestamps drive time-dependent logic
#   - JSONL state file is append-only during normal operation; full state can be reconstructed by replaying the log
#   - CLI status and stories commands read JSONL state files directly without requiring a running engine
#   - MCP server lifecycle is tied to the engine: it starts with the engine and stops when the engine stops
#   - All I/O operations use async/await (asyncio); no blocking I/O on the event loop
#   - Config validation fails fast at startup — invalid configuration is a fatal error that prevents engine creation

class EnginePhase(Enum):
    """Lifecycle phase of the ChroniclerEngine. Transitions: CREATED → STARTING → RUNNING → STOPPING → STOPPED. Also CREATED → STARTING → ERROR if startup fails."""
    CREATED = "CREATED"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    ERROR = "ERROR"

class SourceHealth(Enum):
    """Health status of an individual source connector."""
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    FAILED = "FAILED"
    STOPPED = "STOPPED"

class SinkHealth(Enum):
    """Health status of an individual sink connector."""
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    FAILED = "FAILED"
    STOPPED = "STOPPED"

class SourceStatus:
    """Runtime status of a single source, reported in EngineStatus. Frozen Pydantic model."""
    source_id: str                           # required, Unique identifier of the source instance.
    source_type: str                         # required, Type discriminator, e.g. 'webhook', 'otlp', 'file', 'sentinel'.
    health: SourceHealth                     # required, Current health state.
    events_received: int                     # required, range(>=0), Total events received from this source since engine start.
    last_event_at: OptionalDatetimeISO       # required, ISO-8601 timestamp of last event received, or null if none yet.

class SinkStatus:
    """Runtime status of a single sink, reported in EngineStatus. Frozen Pydantic model."""
    sink_id: str                             # required, Unique identifier of the sink instance.
    sink_type: str                           # required, Type discriminator, e.g. 'stigmergy', 'apprentice', 'disk', 'kindex'.
    health: SinkHealth                       # required, Current health state.
    events_emitted: int                      # required, range(>=0), Total events successfully emitted to this sink.
    errors: int                              # required, range(>=0), Total errors encountered emitting to this sink.
    last_emit_at: OptionalDatetimeISO        # required, ISO-8601 timestamp of last successful emit, or null if none yet.

class EngineStatus:
    """Complete engine status snapshot. Frozen Pydantic model. Returned by engine.status() and exposed via CLI/MCP."""
    phase: EnginePhase                       # required, Current lifecycle phase of the engine.
    uptime_seconds: float                    # required, range(>=0.0), Seconds since engine entered RUNNING phase. 0.0 if not yet running.
    total_events_processed: int              # required, range(>=0), Total events consumed from the internal queue and processed.
    open_stories_count: int                  # required, range(>=0), Number of currently open stories.
    closed_stories_count: int                # required, range(>=0), Number of closed (completed/timed-out) stories.
    sources: list[SourceStatus]              # required, Status of each configured source.
    sinks: list[SinkStatus]                  # required, Status of each configured sink.
    queue_depth: int                         # required, range(>=0), Current number of events buffered in the internal asyncio.Queue.
    started_at: OptionalDatetimeISO          # required, ISO-8601 timestamp when engine entered RUNNING, or null if not yet started.

OptionalDatetimeISO = str | None

DatetimeISO = primitive  # An ISO-8601 formatted datetime string (UTC). Uses datetime.now(datetime.timezone.utc) internally — not the deprecated datetime.utcnow().

QueueBound = primitive  # Maximum size of the internal asyncio.Queue. Bounded to prevent memory exhaustion.

class EngineConfig:
    """Validated engine-specific configuration subset. Frozen Pydantic model. Extracted from the top-level ChroniclerConfig by the engine constructor."""
    queue_max_size: int                      # required, range(1..100000), Max events buffered in the internal asyncio.Queue before back-pressure.
    drain_timeout_seconds: float             # required, range(0.1..60.0), Max seconds to wait draining the queue during shutdown.
    mcp_enabled: bool                        # required, Whether to start the MCP server as a task within the engine.
    state_file: str                          # required, length(1..4096), Path to the JSONL state file for persistence.
    source_restart_delay_seconds: float      # required, range(0.0..300.0), Delay before restarting a failed source supervisor.

class StoryFilter(Enum):
    """Filter for listing stories: OPEN, CLOSED, or ALL."""
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    ALL = "ALL"

StoryId = primitive  # A unique story identifier. Non-empty string.

class StorySummary:
    """Lightweight summary of a story for list views. Does not include full event payloads."""
    story_id: str                            # required, Unique story identifier.
    title: str                               # required, Human-readable story title derived from correlation rule.
    status: str                              # required, Current status: 'open' or 'closed'.
    event_count: int                         # required, range(>=1), Number of events correlated into this story.
    created_at: str                          # required, ISO-8601 timestamp when story was created.
    updated_at: str                          # required, ISO-8601 timestamp of last event added or status change.
    correlation_rule_id: str                 # required, ID of the correlation rule that created this story.

class StoryListResult:
    """Result of listing stories, with total count and summaries."""
    total: int                               # required, range(>=0), Total number of stories matching the filter.
    stories: list[StorySummary]              # required, List of story summaries.
    filter_applied: StoryFilter              # required, The filter that was applied.

class ReplayResult:
    """Summary of a JSONL replay operation."""
    file_path: str                           # required, Path to the replayed JSONL file.
    total_lines: int                         # required, range(>=0), Total lines read from the file.
    events_processed: int                    # required, range(>=0), Events successfully parsed and processed.
    events_skipped: int                      # required, range(>=0), Lines that failed to parse as valid events (logged and dropped).
    stories_created: int                     # required, range(>=0), New stories created during replay.
    stories_closed: int                      # required, range(>=0), Stories that closed during replay.
    duration_seconds: float                  # required, range(>=0.0), Wall-clock duration of the replay.

class ReplayFileSpec:
    """Specification for a replay operation input."""
    file_path: str                           # required, length(1..4096), Path to the JSONL file to replay.
    suppress_sinks: bool                     # required, If true, suppress all sink emissions during replay (dry-run mode).

class CliExitCode(Enum):
    """Process exit codes returned by CLI commands."""
    SUCCESS_0 = "SUCCESS_0"
    CONFIG_ERROR_1 = "CONFIG_ERROR_1"
    RUNTIME_ERROR_2 = "RUNTIME_ERROR_2"
    FILE_NOT_FOUND_3 = "FILE_NOT_FOUND_3"
    STATE_READ_ERROR_4 = "STATE_READ_ERROR_4"

class McpToolName(Enum):
    """Tool names exposed by the MCP server."""
    chronicler_status = "chronicler_status"
    chronicler_stories_list = "chronicler_stories_list"
    chronicler_stories_show = "chronicler_stories_show"
    chronicler_events_replay = "chronicler_events_replay"

class McpToolResult:
    """Structured result returned by MCP tool invocations, serialized as JSON content blocks."""
    tool_name: str                           # required, Name of the tool that produced this result.
    success: bool                            # required, Whether the tool invocation succeeded.
    data: dict                               # required, Structured result data (tool-specific). Serialized as JSON.
    error_message: OptionalString            # required, Error message if success is false, null otherwise.

OptionalString = str | None

def ChroniclerEngine.__init__(
    config: EngineConfig,
    chronicler_config: any,
    sources: list = [],
    sinks: list = [],
    correlation_engine: any = None,
    story_manager: any = None,
    clock: any = None,
) -> None:
    """
    Construct the engine with validated configuration. Optionally accepts pre-built sources, sinks, correlation engine, and story manager for testing. Accepts an optional clock callable for replay mode (default: datetime.now(timezone.utc)). Creates a bounded asyncio.Queue. Engine starts in CREATED phase.

    Preconditions:
      - config is a valid EngineConfig instance
      - chronicler_config is a valid ChroniclerConfig instance
      - If sources/sinks are provided, they must implement SourceProtocol/SinkProtocol respectively

    Postconditions:
      - Engine is in CREATED phase
      - Internal asyncio.Queue is created with maxsize=config.queue_max_size
      - No I/O has been performed
      - No tasks have been started

    Errors:
      - invalid_config (ValidationError): config fails Pydantic validation
      - invalid_queue_size (ValidationError): queue_max_size is out of range

    Side effects: none
    Idempotent: no
    """
    ...

def ChroniclerEngine.start() -> None:
    """
    Async method that transitions engine from CREATED to RUNNING. Loads persisted state from JSONL, starts all source supervisor tasks in a TaskGroup, starts all sinks, optionally starts MCP server task if mcp_enabled. Registers SIGINT/SIGTERM handlers for graceful shutdown. Transitions to ERROR if any fatal startup failure occurs.

    Preconditions:
      - Engine phase is CREATED
      - Called within an async context (running event loop)

    Postconditions:
      - Engine phase is RUNNING (or ERROR on failure)
      - All configured sources have supervisor tasks running
      - All configured sinks are started
      - MCP server task is running if mcp_enabled=true
      - SIGINT/SIGTERM handlers are registered
      - started_at timestamp is set

    Errors:
      - already_started (RuntimeError): Engine phase is not CREATED
          message: Engine can only be started from CREATED phase
      - state_load_failure (RuntimeError): Cannot read JSONL state file
          message: Failed to load persisted state
      - source_init_failure (None): A source fails to initialize (logged, source marked FAILED, engine continues)
      - sink_init_failure (None): A sink fails to initialize (logged, sink marked FAILED, engine continues)

    Side effects: Reads JSONL state file, Starts network listeners for webhook/OTLP sources, Registers OS signal handlers
    Idempotent: no
    """
    ...

def ChroniclerEngine.run() -> None:
    """
    Async method that runs the main processing loop. Consumes events from the internal asyncio.Queue, passes each through CorrelationEngine.process(), feeds results to StoryManager.update(), then fans out to all sinks via asyncio.gather(return_exceptions=True). Runs until the shutdown event is set (by signal or stop()). Calls start() if not already started. This is the primary entry point for cli.py's `chronicler start`.

    Preconditions:
      - Called within an async context (running event loop)

    Postconditions:
      - Engine has processed all events until shutdown
      - Shutdown sequence has completed (stop() called internally)
      - Engine phase is STOPPED

    Errors:
      - unrecoverable_error (RuntimeError): Fatal error in processing loop that cannot be caught
          message: Engine encountered an unrecoverable error in the main loop
      - event_processing_error (None): Individual event fails correlation or story update (logged and dropped, never crashes)

    Side effects: Processes events through correlation and story manager, Emits to sinks, Persists state changes
    Idempotent: no
    """
    ...

def ChroniclerEngine.stop() -> None:
    """
    Async method that initiates graceful shutdown. Transitions to STOPPING phase. Stops all sources (no new events). Drains remaining events from the queue with a timeout (config.drain_timeout_seconds). Flushes the story manager. Stops all sinks. Persists final state to JSONL. Transitions to STOPPED.

    Preconditions:
      - Engine phase is RUNNING or STARTING

    Postconditions:
      - Engine phase is STOPPED
      - All source tasks are cancelled and awaited
      - Queue is drained (or timed out)
      - Story manager has been flushed
      - All sinks are stopped
      - Final state is persisted to JSONL
      - Signal handlers are deregistered

    Errors:
      - already_stopped (RuntimeError): Engine phase is STOPPED or CREATED
          message: Engine is not in a stoppable phase
      - drain_timeout (None): Queue did not drain within drain_timeout_seconds (remaining events logged and dropped)
      - state_persist_failure (None): Cannot write final state to JSONL (logged, not fatal)

    Side effects: Cancels source tasks, Drains queue, Flushes story manager, Stops sinks, Persists state
    Idempotent: yes
    """
    ...

def ChroniclerEngine.status() -> EngineStatus:
    """
    Returns a snapshot of the engine's current status as an EngineStatus struct. Thread-safe and lock-free (reads atomic counters). Can be called from any phase.

    Postconditions:
      - Returned EngineStatus reflects the engine state at the time of the call
      - No state mutation occurs

    Side effects: none
    Idempotent: yes
    """
    ...

def ChroniclerEngine.submit_event(
    event: any,
) -> bool:
    """
    Async method to programmatically submit an event to the engine's internal queue. Used by sources and by replay. Applies back-pressure if queue is full (blocks until space is available or timeout).

    Preconditions:
      - Engine phase is RUNNING
      - event is a valid Event instance

    Postconditions:
      - Event is enqueued in the internal asyncio.Queue (returns true)
      - Or queue was full and timeout expired (returns false, event is dropped)

    Errors:
      - engine_not_running (RuntimeError): Engine phase is not RUNNING
          message: Cannot submit events when engine is not running
      - invalid_event (ValidationError): Event fails validation

    Side effects: none
    Idempotent: no
    """
    ...

def ChroniclerEngine.__aenter__() -> any:
    """
    Async context manager entry. Calls start() and returns self. Enables `async with ChroniclerEngine(...) as engine:` usage pattern.

    Preconditions:
      - Engine phase is CREATED

    Postconditions:
      - Engine phase is RUNNING
      - Returns self

    Errors:
      - start_failure (RuntimeError): start() raises an error

    Side effects: none
    Idempotent: no
    """
    ...

def ChroniclerEngine.__aexit__(
    exc_type: any = None,
    exc_val: any = None,
    exc_tb: any = None,
) -> bool:
    """
    Async context manager exit. Calls stop() to perform graceful shutdown regardless of exception state.

    Postconditions:
      - Engine phase is STOPPED
      - Returns False (exceptions are not suppressed)

    Side effects: none
    Idempotent: yes
    """
    ...

def cli_main(
    argv: list = None,
) -> int:
    """
    CLI entry point registered as `chronicler` in pyproject.toml. Parses argv via argparse, dispatches to the appropriate subcommand handler. Returns an integer exit code. Subcommands: start, status, stories (list|show), replay.

    Postconditions:
      - Returns 0 on success, non-zero on error
      - For 'start': engine has run and stopped
      - For 'status'/'stories': output printed to stdout
      - For 'replay': replay completed and summary printed

    Errors:
      - config_not_found (SystemExit): Config YAML file does not exist or is unreadable
          exit_code: 1
      - invalid_config (SystemExit): Config YAML fails validation
          exit_code: 1
      - unknown_subcommand (SystemExit): Unrecognized subcommand
          exit_code: 2
      - replay_file_not_found (SystemExit): Replay JSONL file does not exist
          exit_code: 3
      - state_read_error (SystemExit): Cannot read JSONL state file for status/stories commands
          exit_code: 4

    Side effects: Reads config file, May start engine, Reads/writes state files, Prints to stdout/stderr
    Idempotent: no
    """
    ...

def cli_cmd_start(
    config_path: str,
) -> int:
    """
    Handler for `chronicler start`. Loads config from YAML (default: chronicler.yaml, overridable via --config), constructs ChroniclerEngine, calls asyncio.run(engine.run()). Blocks until engine stops (via signal or error).

    Preconditions:
      - config_path points to a readable YAML file

    Postconditions:
      - Engine has completed its full lifecycle (start → run → stop)
      - Returns 0 on clean shutdown, non-zero on error

    Errors:
      - config_load_error (SystemExit): Config file missing or invalid
          exit_code: 1
      - engine_runtime_error (SystemExit): Engine encounters unrecoverable error
          exit_code: 2

    Side effects: Full engine lifecycle
    Idempotent: no
    """
    ...

def cli_cmd_status(
    state_file: str,
) -> int:
    """
    Handler for `chronicler status`. Reads the JSONL state file directly (no running engine required) using the state module. Reconstructs and prints the engine status including last-known phase, event counts, and story counts. Outputs JSON to stdout.

    Postconditions:
      - Status JSON printed to stdout
      - Returns 0 on success, 4 on state read error

    Errors:
      - state_file_missing (None): State file does not exist (prints empty/default status)
      - state_file_corrupt (SystemExit): State file contains unparseable lines
          exit_code: 4

    Side effects: Reads JSONL state file, Prints to stdout
    Idempotent: yes
    """
    ...

def cli_cmd_stories_list(
    state_file: str,
    filter: StoryFilter,
) -> int:
    """
    Handler for `chronicler stories list`. Reads the JSONL state file, reconstructs stories, and prints a filtered list. Supports --filter (open/closed/all) flag. Outputs JSON array to stdout.

    Postconditions:
      - StoryListResult JSON printed to stdout
      - Returns 0 on success

    Errors:
      - state_file_missing (None): State file does not exist (prints empty list)
      - state_file_corrupt (SystemExit): State file contains unparseable lines
          exit_code: 4

    Side effects: Reads JSONL state file, Prints to stdout
    Idempotent: yes
    """
    ...

def cli_cmd_stories_show(
    story_id: str,             # length(1..256)
    state_file: str,
) -> int:
    """
    Handler for `chronicler stories show <id>`. Reads the JSONL state file, finds the story by ID, prints full story detail including all correlated events. Outputs JSON to stdout.

    Preconditions:
      - story_id is a non-empty string

    Postconditions:
      - Full Story JSON printed to stdout if found
      - Error message printed to stderr if not found
      - Returns 0 on success, 2 if story not found

    Errors:
      - story_not_found (SystemExit): No story with the given ID exists in state
          exit_code: 2
      - state_file_missing (SystemExit): State file does not exist
          exit_code: 4
      - state_file_corrupt (SystemExit): State file contains unparseable lines
          exit_code: 4

    Side effects: Reads JSONL state file, Prints to stdout/stderr
    Idempotent: yes
    """
    ...

def cli_cmd_replay(
    file_path: str,            # length(1..4096)
    config_path: str,
    suppress_sinks: bool,
) -> int:
    """
    Handler for `chronicler replay <file>`. Constructs engine in replay mode with clock=event_time and optionally suppressed sinks. Reads the JSONL file line by line, parses each as an Event, submits to the engine. Uses event timestamps as the clock source for deterministic replay. Returns a ReplayResult summary.

    Preconditions:
      - file_path points to a readable JSONL file
      - config_path points to a valid YAML config

    Postconditions:
      - All parseable events from file have been processed through the engine
      - ReplayResult JSON printed to stdout
      - Engine has been stopped
      - Returns 0 on success

    Errors:
      - file_not_found (SystemExit): JSONL replay file does not exist
          exit_code: 3
      - config_error (SystemExit): Config file missing or invalid
          exit_code: 1
      - parse_errors (None): Individual lines fail to parse as Events (logged and skipped, counted in events_skipped)

    Side effects: Reads JSONL replay file, Runs engine in replay mode, Persists state
    Idempotent: yes
    """
    ...

def mcp_server_create(
    engine: any,
) -> any:
    """
    Create and configure the MCP server instance. Registers all four tools (chronicler_status, chronicler_stories_list, chronicler_stories_show, chronicler_events_replay) with the mcp SDK. Binds tool handlers to engine methods. Returns the configured server ready to run over stdio transport.

    Preconditions:
      - engine is a valid ChroniclerEngine instance
      - Engine phase is RUNNING or STARTING

    Postconditions:
      - MCP server is configured with all four tools registered
      - Server is ready to run but not yet started

    Errors:
      - mcp_sdk_unavailable (ImportError): The mcp Python package is not installed
      - tool_registration_error (RuntimeError): Tool registration fails due to schema conflict

    Side effects: none
    Idempotent: yes
    """
    ...

def mcp_server_run(
    server: any,
) -> None:
    """
    Async coroutine that runs the MCP server over stdio transport. Blocks until the engine shuts down or the stdio connection is closed. Designed to be run as a task within the engine's TaskGroup.

    Preconditions:
      - server is a configured MCP server instance
      - Called within an async context

    Postconditions:
      - MCP server has stopped
      - Stdio transport is closed

    Errors:
      - transport_error (None): Stdio transport encounters an I/O error (logged, task exits)
      - cancelled (CancelledError): Task is cancelled during engine shutdown

    Side effects: Reads/writes stdio, Delegates to engine methods
    Idempotent: no
    """
    ...

def mcp_tool_chronicler_status() -> McpToolResult:
    """
    MCP tool handler for chronicler_status. Delegates to engine.status() and returns the result as a McpToolResult with EngineStatus serialized in the data field.

    Preconditions:
      - Engine is accessible and not in ERROR phase

    Postconditions:
      - Returns McpToolResult with success=true and EngineStatus data
      - No state mutation

    Errors:
      - engine_unavailable (None): Engine reference is invalid or engine is in ERROR phase

    Side effects: none
    Idempotent: yes
    """
    ...

def mcp_tool_chronicler_stories_list(
    filter: StoryFilter = ALL,
) -> McpToolResult:
    """
    MCP tool handler for chronicler_stories_list. Accepts an optional filter parameter. Delegates to story_manager.list_stories() and returns results as McpToolResult with StoryListResult serialized in the data field.

    Preconditions:
      - Engine is accessible and in RUNNING phase

    Postconditions:
      - Returns McpToolResult with success=true and StoryListResult data

    Errors:
      - engine_unavailable (None): Engine reference is invalid

    Side effects: none
    Idempotent: yes
    """
    ...

def mcp_tool_chronicler_stories_show(
    story_id: str,             # length(1..256)
) -> McpToolResult:
    """
    MCP tool handler for chronicler_stories_show. Accepts a story_id parameter. Delegates to story_manager.get_story() and returns the full Story as McpToolResult with Story serialized in the data field.

    Preconditions:
      - Engine is accessible and in RUNNING phase

    Postconditions:
      - Returns McpToolResult with success=true and Story data if found
      - Returns McpToolResult with success=false and error_message if not found

    Errors:
      - story_not_found (None): No story with the given ID exists
      - engine_unavailable (None): Engine reference is invalid

    Side effects: none
    Idempotent: yes
    """
    ...

def mcp_tool_chronicler_events_replay(
    file_path: str,            # length(1..4096)
    suppress_sinks: bool = false,
) -> McpToolResult:
    """
    MCP tool handler for chronicler_events_replay. Accepts file_path and optional suppress_sinks parameters. Constructs a replay pipeline and processes the file through the running engine. Returns ReplayResult as McpToolResult. Note: replay through MCP runs within the live engine, using event timestamps for clock but processing through the existing correlation/story pipeline.

    Preconditions:
      - Engine is accessible and in RUNNING phase
      - file_path points to a readable JSONL file

    Postconditions:
      - Returns McpToolResult with success=true and ReplayResult data on success
      - Returns McpToolResult with success=false and error_message on failure
      - All parseable events from the file have been submitted to the engine

    Errors:
      - file_not_found (None): JSONL file does not exist at file_path
      - file_read_error (None): Cannot read the JSONL file
      - engine_unavailable (None): Engine is not in RUNNING phase

    Side effects: Reads JSONL file, Submits events to engine
    Idempotent: no
    """
    ...

# ── REQUIRED EXPORTS ──────────────────────────────────
# Your implementation module MUST export ALL of these names
# with EXACTLY these spellings. Tests import them by name.
# __all__ = ['EnginePhase', 'SourceHealth', 'SinkHealth', 'SourceStatus', 'SinkStatus', 'EngineStatus', 'OptionalDatetimeISO', 'EngineConfig', 'StoryFilter', 'StorySummary', 'StoryListResult', 'ReplayResult', 'ReplayFileSpec', 'CliExitCode', 'McpToolName', 'McpToolResult', 'OptionalString', 'ValidationError', 'cli_main', 'SystemExit', 'cli_cmd_start', 'cli_cmd_status', 'cli_cmd_stories_list', 'cli_cmd_stories_show', 'cli_cmd_replay', 'mcp_server_create', 'ImportError', 'mcp_server_run', 'CancelledError', 'mcp_tool_chronicler_status', 'mcp_tool_chronicler_stories_list', 'mcp_tool_chronicler_stories_show', 'mcp_tool_chronicler_events_replay']

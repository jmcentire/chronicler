# === Story Sinks (sinks) v1 ===
#  Dependencies: schemas, config
# Four SinkProtocol implementations for emitting closed stories: (1) disk.py — writes closed stories as append-only JSONL with async file I/O and read-back deserialization, (2) stigmergy.py — placeholder that emits stories as Signals to Stigmergy, (3) apprentice.py — placeholder that emits stories as training sequences, (4) kindex.py — filtered emission of noteworthy stories as knowledge nodes based on configurable thresholds (story_type, min event count, duration). All sinks are fire-and-forget with error propagation to the orchestrator's safe_emit wrapper; a failing sink never blocks others or crashes the engine. Covers AC7 (disk persistence), AC8 (sink fan-out), AC17 (kindex filtering).

# Module invariants:
#   - All sink implementations satisfy the @runtime_checkable SinkProtocol with async methods: emit(story: Story) -> None, start() -> None, close() -> None.
#   - A failing sink's emit() call never blocks, delays, or prevents other sinks from receiving the same story.
#   - Sinks propagate exceptions from emit() to the caller; they do NOT internally swallow errors. Error handling is centralized in the orchestrator's safe_emit wrapper.
#   - DiskSink writes are serialized via asyncio.Lock — concurrent emit() calls produce well-formed, non-interleaved JSONL lines.
#   - DiskSink file is opened in append mode; existing data is never overwritten or truncated.
#   - read_stories never raises for corrupted/unparseable lines; it logs and skips them, yielding only valid Story objects.
#   - KindexSink only emits stories for which is_noteworthy(story, config) returns true.
#   - is_noteworthy is a pure function with no side effects — all filtering logic is testable without I/O.
#   - Placeholder sinks (stigmergy, apprentice, kindex emit action) log at DEBUG level only and perform no network I/O.
#   - All config models (DiskSinkConfig, KindexSinkConfig) are frozen Pydantic v2 models (immutable after construction).
#   - start() must be called before emit(); emit() after close() raises RuntimeError for stateful sinks (DiskSink).
#   - safe_emit never raises — it captures all exceptions into SafeEmitOutcome.error_message.

class SinkName(Enum):
    """Canonical names for the four built-in sink implementations."""
    disk = "disk"
    stigmergy = "stigmergy"
    apprentice = "apprentice"
    kindex = "kindex"

class DiskSinkConfig:
    """Frozen Pydantic configuration model for the disk JSONL sink."""
    path: FilePath                           # required, Filesystem path to the JSONL output file for closed stories.
    flush_on_write: bool = true              # optional, Whether to flush the file handle after each write. Defaults to true for durability.

FilePath = primitive  # A non-empty filesystem path string.

class KindexSinkConfig:
    """Frozen Pydantic configuration model for the kindex knowledge-capture sink. Defines thresholds that determine whether a story is 'noteworthy' enough to emit as a knowledge node (per AS12)."""
    story_types: OptionalStringSet = None    # optional, If set, only stories whose story_type is in this set are considered noteworthy. None means all story types qualify.
    min_event_count: PositiveInt = 1         # optional, Minimum number of events a story must contain to be noteworthy.
    min_duration_seconds: NonNegativeFloat = 0.0 # optional, Minimum duration in seconds between opened_at and closed_at for a story to be noteworthy.

OptionalStringSet = Any | None

PositiveInt = primitive  # An integer >= 1.

NonNegativeFloat = primitive  # A float >= 0.0.

class SinkEmitResult(Enum):
    """Outcome of a single sink emit attempt within the safe_emit orchestrator wrapper."""
    emitted = "emitted"
    filtered = "filtered"
    error = "error"

class SafeEmitOutcome:
    """Result record from the safe_emit orchestrator wrapper for one sink invocation."""
    sink_name: str                           # required, Canonical name of the sink that was invoked.
    story_id: str                            # required, ID of the story that was emitted (or attempted).
    result: SinkEmitResult                   # required, Whether the emit succeeded, was filtered, or errored.
    error_message: str = None                # optional, Error detail if result is 'error', empty string otherwise.

class Story:
    """Reference to schemas.Story — the closed story object that sinks receive. Defined in the schemas component; referenced here for type completeness."""
    story_id: str                            # required, Unique identifier for the story.
    story_type: str                          # required, Classification/type label for the story.
    events: list[any]                        # required, Ordered list of correlated Event objects.
    opened_at: str                           # required, ISO-8601 timestamp when the story was opened.
    closed_at: str                           # required, ISO-8601 timestamp when the story was closed.
    metadata: dict = {}                      # optional, Arbitrary metadata attached to the story.

def DiskSink.start() -> None:
    """
    Opens the JSONL output file handle via aiofiles for the lifetime of the sink. Must be called before any emit() calls. Acquires the internal asyncio.Lock.

    Preconditions:
      - DiskSink was constructed with a valid DiskSinkConfig.
      - start() has not already been called (no double-start).

    Postconditions:
      - An aiofiles file handle is open in append mode at the configured path.
      - The internal asyncio.Lock is initialized.
      - The sink is ready to accept emit() calls.

    Errors:
      - file_open_error (OSError): The configured path cannot be opened for writing (permissions, missing directory, disk full).
      - already_started (RuntimeError): start() is called when the file handle is already open.

    Side effects: none
    Idempotent: no
    """
    ...

def DiskSink.emit(
    story: Story,
) -> None:
    """
    Serializes a closed Story to JSON and appends it as a single line to the JSONL file. Write is protected by an asyncio.Lock to ensure atomicity of the line write. Errors propagate to the caller (the orchestrator's safe_emit wrapper).

    Preconditions:
      - start() has been called and the file handle is open.
      - story is a valid, closed Story object.

    Postconditions:
      - The story's JSON representation followed by a newline has been appended to the file.
      - If flush_on_write is true, the file buffer has been flushed to disk.

    Errors:
      - not_started (RuntimeError): emit() is called before start().
      - serialization_error (ValueError): The story cannot be serialized to JSON (should not happen with valid Pydantic models).
      - write_error (OSError): The filesystem write fails (disk full, handle closed unexpectedly, I/O error).

    Side effects: none
    Idempotent: no
    """
    ...

def DiskSink.close() -> None:
    """
    Flushes and closes the JSONL file handle. After close(), no further emit() calls are valid.

    Preconditions:
      - start() has been called.

    Postconditions:
      - The file handle is flushed and closed.
      - Any subsequent emit() call will raise RuntimeError.

    Errors:
      - not_started (RuntimeError): close() is called before start().
      - close_error (OSError): The file handle close operation fails.

    Side effects: none
    Idempotent: yes
    """
    ...

def read_stories(
    path: FilePath,
) -> AsyncIterator[Story]:
    """
    Standalone async generator that reads a JSONL file and yields deserialized Story objects. Corrupted lines are logged at WARNING and skipped — never raises for bad data. This is not part of SinkProtocol; it is a utility function in disk.py for read-back.

    Preconditions:
      - The file at path exists and is readable.

    Postconditions:
      - Every valid JSON line in the file is yielded as a Story object.
      - Corrupted or unparseable lines are skipped with a warning log.
      - The file handle is closed after iteration completes or on error.

    Errors:
      - file_not_found (FileNotFoundError): The specified path does not exist.
      - permission_denied (PermissionError): The process lacks read permissions for the file.

    Side effects: none
    Idempotent: yes
    """
    ...

def StigmergySink.start() -> None:
    """
    No-op initialization for the Stigmergy placeholder sink. Logs at DEBUG that the sink is starting.

    Postconditions:
      - The sink is marked as started.

    Side effects: none
    Idempotent: yes
    """
    ...

def StigmergySink.emit(
    story: Story,
) -> None:
    """
    Placeholder: logs at DEBUG level the story_id and story_type that would be emitted as a Signal to Stigmergy. In a real implementation, this would make an HTTP POST to the Stigmergy service.

    Preconditions:
      - start() has been called.

    Postconditions:
      - A DEBUG log line has been emitted describing the story that would be sent.

    Side effects: none
    Idempotent: yes
    """
    ...

def StigmergySink.close() -> None:
    """
    No-op teardown for the Stigmergy placeholder sink. Logs at DEBUG that the sink is closing.

    Side effects: none
    Idempotent: yes
    """
    ...

def ApprenticeSink.start() -> None:
    """
    No-op initialization for the Apprentice placeholder sink. Logs at DEBUG that the sink is starting.

    Postconditions:
      - The sink is marked as started.

    Side effects: none
    Idempotent: yes
    """
    ...

def ApprenticeSink.emit(
    story: Story,
) -> None:
    """
    Placeholder: logs at DEBUG level the story_id and event count that would be emitted as a training sequence to Apprentice. In a real implementation, this would make an HTTP POST to the Apprentice service.

    Preconditions:
      - start() has been called.

    Postconditions:
      - A DEBUG log line has been emitted describing the story that would be sent.

    Side effects: none
    Idempotent: yes
    """
    ...

def ApprenticeSink.close() -> None:
    """
    No-op teardown for the Apprentice placeholder sink. Logs at DEBUG that the sink is closing.

    Side effects: none
    Idempotent: yes
    """
    ...

def KindexSink.start() -> None:
    """
    No-op initialization for the Kindex placeholder sink. Logs at DEBUG that the sink is starting with the active filter configuration.

    Preconditions:
      - KindexSink was constructed with a valid KindexSinkConfig.

    Postconditions:
      - The sink is marked as started.
      - The active KindexSinkConfig has been logged at DEBUG level.

    Side effects: none
    Idempotent: yes
    """
    ...

def KindexSink.emit(
    story: Story,
) -> None:
    """
    Evaluates whether the story is noteworthy using is_noteworthy(), and if so, logs at DEBUG what would be emitted as a knowledge node. Stories that do not pass the filter are silently skipped (no error). In a real implementation, noteworthy stories would be posted to the Kindex service.

    Preconditions:
      - start() has been called.

    Postconditions:
      - If is_noteworthy(story, config) returned true, a DEBUG log line describes the knowledge node that would be emitted.
      - If not noteworthy, the story is silently dropped with no side effects beyond an optional TRACE/DEBUG log.

    Side effects: none
    Idempotent: yes
    """
    ...

def KindexSink.close() -> None:
    """
    No-op teardown for the Kindex placeholder sink. Logs at DEBUG that the sink is closing.

    Side effects: none
    Idempotent: yes
    """
    ...

def is_noteworthy(
    story: Story,
    config: KindexSinkConfig,
) -> bool:
    """
    Pure predicate function that evaluates whether a story meets the KindexSinkConfig thresholds for emission as a knowledge node. Extracted as a standalone function for direct unit testability without needing the full KindexSink.

    Preconditions:
      - story has a valid closed_at timestamp (it is a closed story).
      - config is a valid KindexSinkConfig.

    Postconditions:
      - Returns true if and only if all of the following hold: (1) config.story_types is None OR story.story_type is in config.story_types, (2) len(story.events) >= config.min_event_count, (3) (story.closed_at - story.opened_at) in seconds >= config.min_duration_seconds.

    Errors:
      - invalid_timestamps (ValueError): story.opened_at or story.closed_at cannot be parsed as ISO-8601 timestamps.

    Side effects: none
    Idempotent: yes
    """
    ...

def safe_emit(
    sink_name: str,
    sink_emit: any,
    story: Story,
) -> SafeEmitOutcome:
    """
    Orchestrator wrapper (defined in engine.py, contracted here as part of the sink fan-out protocol) that calls sink.emit(story) inside a try/except Exception block. On exception, logs a structured error with sink name and story ID and returns an error outcome. Used by asyncio.gather for concurrent fan-out to all registered sinks.

    Preconditions:
      - The sink has been started via start().
      - story is a valid closed Story.

    Postconditions:
      - Returns SafeEmitOutcome with result='emitted' if emit succeeded.
      - Returns SafeEmitOutcome with result='error' and error_message populated if emit raised an exception.
      - Never raises an exception itself — all errors are captured in the outcome.

    Side effects: none
    Idempotent: no
    """
    ...

# ── REQUIRED EXPORTS ──────────────────────────────────
# Your implementation module MUST export ALL of these names
# with EXACTLY these spellings. Tests import them by name.
# __all__ = ['SinkName', 'DiskSinkConfig', 'KindexSinkConfig', 'OptionalStringSet', 'SinkEmitResult', 'SafeEmitOutcome', 'Story', 'read_stories', 'is_noteworthy', 'safe_emit']

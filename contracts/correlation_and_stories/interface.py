# === Correlation Engine & Story Manager (correlation_and_stories) v1 ===
# Two tightly coupled modules plus a persistence layer: (1) correlation.py — matches incoming events to correlation rules by extracting group_by keys, O(1) lookup via dict keyed by (rule_name, key_tuple), creates new stories when no match found, implements chain_by (on story close, register chain_by keys so subsequent events link to new story with parent_story_id). (2) story_manager.py — manages open stories in bounded memory with max_open_stories and LRU eviction (OrderedDict), periodic async timeout sweep, closes stories on timeout/terminal event/eviction, emits closed stories to registered sink callbacks. Evicted stories emitted with status='evicted'. Event ordering by timestamp with arrival-time tiebreaker and monotonic sequence. (3) state.py — JSONL append-only persistence for story state records and replay-based recovery. Covers AC2, AC3, AC4, AC5, AC8, AC14.

# Module invariants:
#   - INV-01: The number of open stories in StoryManager never exceeds max_open_stories
#   - INV-02: The correlation dict key (rule_name, key_tuple) maps to at most one open story at any time
#   - INV-03: Every event in a closed Story is sorted by (event_time, received_at, sequence) ascending
#   - INV-04: The monotonic sequence counter is strictly increasing across all processed events
#   - INV-05: Evicted stories are always emitted to sinks with status='evicted' before removal
#   - INV-06: Chain registry entries are never used after their expires_at time
#   - INV-07: A story created via chain_by always has a non-empty parent_story_id
#   - INV-08: Stories cannot be modified after close_story returns — the returned Story is frozen (Pydantic frozen=True)
#   - INV-09: State file records are strictly append-only — no in-place modifications or truncation during normal operation
#   - INV-10: Sink callback errors never prevent story closure or crash the engine
#   - INV-11: The OrderedDict in StoryManager maintains LRU order — least recently used story is always at position 0
#   - INV-12: recover_state produces the same set of open stories regardless of how many times it is called on the same file (idempotent)
#   - INV-13: Events missing required group_by fields are logged and dropped — they never create or modify stories

EventTime = primitive  # UTC datetime string in ISO 8601 format representing when the event occurred at the source.

StoryId = primitive  # Unique identifier for a story. UUID4 string by default, injectable factory for testing.

EventId = primitive  # Unique identifier for an event. UUID4 string.

RuleName = primitive  # Name identifier for a correlation rule. Must be non-empty, lowercase alphanumeric with underscores/hyphens.

SequenceNumber = primitive  # Monotonically increasing integer assigned by the engine to each event for deterministic ordering.

MaxOpenStories = primitive  # Upper bound on simultaneously open stories in the StoryManager.

TimeoutSeconds = primitive  # Timeout duration in seconds for story inactivity or chain TTL.

SweepIntervalSeconds = primitive  # Interval in seconds between periodic timeout sweep passes.

class MatchPredicate:
    """A single field-matching predicate within a correlation rule. Matches event payload field to an exact value or regex pattern."""
    field: str                               # required, Dot-delimited path into event payload (e.g. 'source_type' or 'payload.severity')
    value: str                               # required, Exact value or regex pattern to match against
    is_regex: bool                           # required, If true, value is interpreted as a regex pattern

class TerminalCondition:
    """Condition that, when matched by an event, triggers story closure with status 'completed'."""
    field: str                               # required, Dot-delimited path into event payload
    value: str                               # required, Exact value that triggers terminal closure
    is_regex: bool                           # required, If true, value is a regex pattern

class CorrelationRule:
    """Frozen Pydantic model defining how events are grouped into stories. Specifies match predicates, group_by key extraction, timeout, optional terminal conditions, and optional chain_by linking."""
    name: RuleName                           # required, Unique rule identifier
    match: list                              # required, List of MatchPredicate — all must match for the rule to apply to an event
    group_by: list                           # required, length(1..), List of event payload field paths used to construct the composite key for story lookup
    timeout: TimeoutSeconds                  # required, Inactivity timeout after which the story is closed with status 'timed_out'
    terminal_conditions: list = []           # optional, Optional list of conditions that trigger immediate story closure
    chain_by: list = []                      # optional, Optional list of field paths for chain-by linking. On story close, these fields from the closing event register chain keys so subsequent events create a new linked story.
    chain_ttl: TimeoutSeconds = 300.0        # optional, TTL for chain registry entries. After expiry, chain keys are discarded.

class Event:
    """Immutable event envelope. Frozen Pydantic model carrying source metadata and an opaque payload dict. Events are the input to the correlation engine."""
    event_id: EventId                        # required, Globally unique event identifier (UUID4)
    event_time: EventTime                    # required, Source-provided UTC timestamp of when the event occurred
    received_at: EventTime                   # required, UTC timestamp of when the engine received the event
    sequence: SequenceNumber                 # required, Monotonically increasing integer assigned by the engine for deterministic ordering
    source_type: str                         # required, Identifier of the source that produced this event (e.g. 'webhook', 'otlp', 'sentinel')
    payload: dict                            # required, Opaque event data as a JSON-serializable dictionary

class StoryStatus(Enum):
    """Terminal status of a closed story."""
    completed = "completed"
    timed_out = "timed_out"
    evicted = "evicted"

class Story:
    """Frozen Pydantic model representing a completed story. Produced by _OpenStory.to_story() at close time. This is the public output type emitted to sinks."""
    story_id: StoryId                        # required, Unique identifier for this story
    rule_name: RuleName                      # required, Name of the correlation rule that created this story
    group_key: dict                          # required, The composite key dict (field_name -> value) used for correlation lookup
    status: StoryStatus                      # required, Terminal status indicating how the story was closed
    events: list                             # required, Ordered list of events in this story, sorted by (event_time, received_at, sequence)
    parent_story_id: str = None              # optional, Story ID of the parent story if this story was created via chain_by linking. Empty string if no parent.
    created_at: EventTime                    # required, UTC timestamp of when the story was first created
    closed_at: EventTime                     # required, UTC timestamp of when the story was closed
    metadata: dict = {}                      # optional, Optional metadata dict for extensibility
    event_count: int                         # required, Number of events in the story (denormalized for convenience)

class CompositeKey:
    """The composite lookup key used in the correlation dict. Composed of rule_name and a sorted tuple of (field, value) pairs extracted from event payload via group_by."""
    rule_name: RuleName                      # required, Name of the correlation rule
    key_tuple: list                          # required, Sorted list of [field_name, value] pairs extracted from event payload via group_by fields

class ChainEntry:
    """Entry in the chain-by registry. Records the parent story ID and expiration time so subsequent events matching chain keys create linked stories."""
    parent_story_id: StoryId                 # required, ID of the story that closed and registered this chain entry
    expires_at: float                        # required, Monotonic clock time at which this chain entry expires and should be discarded

class RecordType(Enum):
    """Discriminator for JSONL state records."""
    story_opened = "story_opened"
    event_appended = "event_appended"
    story_closed = "story_closed"

class StoryOpenedRecord:
    """JSONL state record emitted when a new story is created. Frozen Pydantic model."""
    record_type: RecordType                  # required, Always 'story_opened'
    story_id: StoryId                        # required, ID of the newly created story
    rule_name: RuleName                      # required, Correlation rule that created this story
    group_key: dict                          # required, Composite key dict for this story
    parent_story_id: str                     # required, Parent story ID if chain-linked, empty string otherwise
    created_at: EventTime                    # required, UTC timestamp of story creation
    timeout: float                           # required, Timeout in seconds from the correlation rule

class EventAppendedRecord:
    """JSONL state record emitted when an event is appended to an open story."""
    record_type: RecordType                  # required, Always 'event_appended'
    story_id: StoryId                        # required, ID of the story the event was appended to
    event: Event                             # required, The full event envelope that was appended

class StoryClosedRecord:
    """JSONL state record emitted when a story is closed (completed, timed_out, or evicted)."""
    record_type: RecordType                  # required, Always 'story_closed'
    story_id: StoryId                        # required, ID of the closed story
    status: StoryStatus                      # required, Terminal status of the story
    closed_at: EventTime                     # required, UTC timestamp of when the story was closed

StateRecord = StoryOpenedRecord | EventAppendedRecord | StoryClosedRecord

SinkCallback = primitive  # An async callable that accepts a closed Story and returns None. Signature: async (Story) -> None. Registered with StoryManager to receive closed stories.

ClockCallable = primitive  # A callable returning monotonic time as float. Signature: () -> float. Defaults to time.monotonic. Injectable for testability.

IdFactory = primitive  # A callable returning a new unique ID string. Signature: () -> str. Defaults to lambda: str(uuid4()). Injectable for deterministic test IDs.

FilePath = primitive  # Filesystem path for JSONL state file.

class RecoveryResult:
    """Result of replaying JSONL state file to recover open stories."""
    open_stories: list                       # required, List of reconstructed open story dicts (to be loaded into StoryManager)
    records_read: int                        # required, Total number of valid records successfully read
    records_skipped: int                     # required, Number of corrupt or malformed records skipped
    chain_entries: list                      # required, Recovered chain registry entries still within TTL

class CorrelationEngineConfig:
    """Configuration for the CorrelationEngine, typically loaded from YAML."""
    rules: list                              # required, length(1..), List of correlation rules to evaluate against incoming events
    max_open_stories: MaxOpenStories         # required, Upper bound on simultaneously open stories
    sweep_interval: SweepIntervalSeconds     # required, Interval between timeout sweep passes
    state_file: FilePath                     # required, Path to JSONL state file for persistence
    clock: ClockCallable = None              # optional, Injectable monotonic clock for testability
    id_factory: IdFactory = None             # optional, Injectable ID generator for testability

class EngineStats:
    """Runtime statistics for monitoring and observability."""
    open_story_count: int                    # required, Current number of open stories
    total_events_processed: int              # required, Total events processed since startup
    total_stories_created: int               # required, Total stories created since startup
    total_stories_closed: int                # required, Total stories closed (completed + timed_out + evicted)
    total_stories_evicted: int               # required, Total stories evicted due to max_open_stories
    total_chain_links: int                   # required, Total stories created via chain_by linking
    current_sequence: int                    # required, Current value of the monotonic sequence counter
    active_chain_entries: int                # required, Current number of unexpired chain registry entries

def CorrelationEngine.__init__(
    config: CorrelationEngineConfig,
    story_manager: StoryManager,
    clock: ClockCallable = None,
    id_factory: IdFactory = None,
) -> None:
    """
    Initialize the correlation engine with configuration, story manager reference, injectable clock, and injectable ID factory. Validates that all rule names are unique. Does NOT start the sweep task — that is done via start().

    Preconditions:
      - All rule names in config.rules are unique
      - story_manager is initialized but not yet started

    Postconditions:
      - Internal correlation dict is empty
      - Chain registry is empty
      - Monotonic sequence counter is 0
      - Rules are indexed for O(1) lookup by name

    Errors:
      - duplicate_rule_name (ValueError): Two or more rules share the same name
          detail: Duplicate rule names found: {names}
      - empty_rules (ValueError): config.rules is empty
          detail: At least one correlation rule is required

    Side effects: none
    Idempotent: no
    """
    ...

def CorrelationEngine.process_event(
    event: Event,
) -> None:
    """
    Match an incoming event against all correlation rules. For each matching rule, extract the composite key via group_by fields, perform O(1) dict lookup for an existing open story. If found, append the event and check terminal conditions. If not found, check chain registry for a chain link and create a new story (with parent_story_id if chain-linked). Assigns sequence number and received_at timestamp. Wraps entire body in try/except — logs and drops events that cause errors, never crashes.

    Preconditions:
      - Engine has been initialized with valid rules
      - Event has a valid event_id and payload

    Postconditions:
      - Sequence counter incremented by 1
      - Event appended to matching open story OR new story created OR event dropped with log
      - If terminal condition matched, story closed via StoryManager
      - If story closed with chain_by, chain entry registered
      - State records written for any story creation or event append

    Errors:
      - no_matching_rule (None): Event does not match any correlation rule's match predicates
          detail: Event dropped — no matching rule. Logged at DEBUG level.
      - missing_group_by_field (None): Event payload missing a field specified in rule.group_by
          detail: Event dropped — missing group_by field '{field}'. Logged at WARNING level.
      - correlation_error (None): Any unexpected exception during processing
          detail: Exception caught, logged at ERROR level, event dropped. Engine continues.

    Side effects: Writes state records to JSONL file, May trigger sink callbacks if story closes, Mutates internal correlation dict and chain registry
    Idempotent: no
    """
    ...

def CorrelationEngine.register_chain_entry(
    story: Story,
    rule: CorrelationRule,
) -> None:
    """
    Register a chain-by entry in the chain registry when a story with chain_by fields closes. Extracts chain key from the last event in the story and stores it with the parent story ID and expiration time.

    Preconditions:
      - rule.chain_by is non-empty
      - story.events is non-empty
      - story.status is 'completed' or 'timed_out' (not 'evicted')

    Postconditions:
      - Chain registry contains new entry keyed by (rule_name, chain_key_tuple)
      - Entry expires_at is set to clock() + rule.chain_ttl

    Errors:
      - missing_chain_field (None): Last event in story missing a chain_by field
          detail: Chain entry not registered — missing field '{field}' in last event. Logged at WARNING.
      - empty_events (None): Story has no events (should not happen)
          detail: Chain entry not registered — story has no events. Logged at ERROR.

    Side effects: none
    Idempotent: no
    """
    ...

def CorrelationEngine.purge_expired_chains() -> int:
    """
    Remove all expired entries from the chain registry. Called periodically by the sweep task or on demand.

    Postconditions:
      - All chain entries with expires_at < clock() are removed
      - Return value is the count of purged entries

    Side effects: none
    Idempotent: yes
    """
    ...

def CorrelationEngine.get_stats() -> EngineStats:
    """
    Return current engine statistics for monitoring and observability. Pure read, no side effects.

    Postconditions:
      - Returned stats reflect current engine state at time of call

    Side effects: none
    Idempotent: yes
    """
    ...

def StoryManager.__init__(
    max_open_stories: MaxOpenStories,
    sweep_interval: SweepIntervalSeconds,
    clock: ClockCallable = None,
) -> None:
    """
    Initialize the story manager with max_open_stories limit, sweep interval, injectable clock, and empty OrderedDict for open stories. Does NOT start the sweep task.

    Preconditions:
      - max_open_stories >= 1
      - sweep_interval >= 0.1

    Postconditions:
      - Internal OrderedDict is empty
      - Sink callback list is empty
      - Sweep task is not running

    Side effects: none
    Idempotent: no
    """
    ...

def StoryManager.register_sink(
    callback: SinkCallback,
) -> None:
    """
    Register an async callback to be invoked with each closed Story. Callbacks are invoked concurrently via asyncio.gather with return_exceptions=True.

    Preconditions:
      - callback is a valid async callable

    Postconditions:
      - callback is appended to the internal sink callback list

    Side effects: none
    Idempotent: no
    """
    ...

def StoryManager.register_story(
    story_id: StoryId,
    rule_name: RuleName,
    group_key: dict,
    timeout: TimeoutSeconds,
    parent_story_id: str = None,
    created_at: EventTime,
) -> None:
    """
    Register a new open story. Adds to OrderedDict (most recently used position). If adding would exceed max_open_stories, evicts the least recently used story first (emitting it to sinks with status='evicted').

    Preconditions:
      - story_id is not already present in the open story dict

    Postconditions:
      - Story is present in the OrderedDict at the most-recently-used position
      - If a story was evicted, it has been emitted to all sinks with status='evicted'
      - len(open_stories) <= max_open_stories

    Errors:
      - duplicate_story_id (ValueError): story_id already exists in open stories
          detail: Story {story_id} already registered
      - eviction_sink_error (None): A sink callback raises during eviction emission
          detail: Sink error during eviction logged at ERROR level, processing continues

    Side effects: none
    Idempotent: no
    """
    ...

def StoryManager.append_event(
    story_id: StoryId,
    event: Event,
) -> None:
    """
    Append an event to an existing open story and move it to most-recently-used position in the OrderedDict.

    Preconditions:
      - story_id exists in the open story dict

    Postconditions:
      - Event is appended to the story's event list
      - Story is at most-recently-used position in OrderedDict
      - Story's last_activity_time is updated to clock()

    Errors:
      - story_not_found (KeyError): story_id not in open stories
          detail: Story {story_id} not found in open stories

    Side effects: none
    Idempotent: no
    """
    ...

def StoryManager.touch_story(
    story_id: StoryId,
) -> None:
    """
    Move a story to the most-recently-used position in the OrderedDict without appending an event. Updates last_activity_time.

    Preconditions:
      - story_id exists in the open story dict

    Postconditions:
      - Story is at the most-recently-used position
      - Story's last_activity_time is updated to clock()

    Errors:
      - story_not_found (KeyError): story_id not in open stories
          detail: Story {story_id} not found in open stories

    Side effects: none
    Idempotent: yes
    """
    ...

def StoryManager.close_story(
    story_id: StoryId,
    status: StoryStatus,
) -> Story:
    """
    Close an open story with the given status. Removes from OrderedDict, converts internal _OpenStory to frozen Story via to_story(), emits to all registered sink callbacks via asyncio.gather with return_exceptions=True. Sink errors are logged but do not propagate.

    Preconditions:
      - story_id exists in the open story dict

    Postconditions:
      - Story is removed from the OrderedDict
      - Returned Story is a frozen Pydantic model with events sorted by (event_time, received_at, sequence)
      - All registered sink callbacks have been invoked (errors logged, not propagated)
      - Story.status matches the provided status
      - Story.closed_at is set to current UTC time

    Errors:
      - story_not_found (KeyError): story_id not in open stories
          detail: Story {story_id} not found in open stories
      - sink_callback_error (None): One or more sink callbacks raise an exception
          detail: Sink error logged at ERROR level. Story still closed and returned.

    Side effects: none
    Idempotent: no
    """
    ...

def StoryManager.start_sweep_task() -> None:
    """
    Start the periodic async background task that sweeps for timed-out stories. Uses injectable clock for testability. The task runs in a loop with asyncio.sleep(sweep_interval), checking each open story's last_activity_time against its timeout.

    Preconditions:
      - Sweep task is not already running

    Postconditions:
      - An asyncio.Task is created and stored internally
      - The task will periodically close stories whose inactivity exceeds their timeout

    Errors:
      - already_running (RuntimeError): Sweep task is already active
          detail: Sweep task is already running

    Side effects: none
    Idempotent: no
    """
    ...

def StoryManager.stop_sweep_task() -> None:
    """
    Cancel the periodic sweep task and await its completion. Safe to call if not running (no-op).

    Postconditions:
      - Sweep task is cancelled and awaited
      - Internal task reference is cleared

    Side effects: none
    Idempotent: yes
    """
    ...

def StoryManager.sweep_once() -> int:
    """
    Perform a single timeout sweep pass. Iterates open stories, closes any whose (clock() - last_activity_time) exceeds their timeout with status='timed_out'. Returns the number of stories closed. Exposed publicly for testing.

    Postconditions:
      - All stories exceeding their timeout have been closed with status='timed_out'
      - Return value is the count of stories closed in this sweep

    Errors:
      - close_error (None): Error during close_story for a timed-out story
          detail: Error logged at ERROR level, sweep continues to next story

    Side effects: none
    Idempotent: no
    """
    ...

def StoryManager.get_open_story_count() -> int:
    """
    Return the current number of open stories. Pure read.

    Postconditions:
      - Return value equals len(open_stories)

    Side effects: none
    Idempotent: yes
    """
    ...

def StoryManager.load_recovered_stories(
    recovery_result: RecoveryResult,
) -> int:
    """
    Load recovered stories from state recovery into the OrderedDict. Used during startup after recover_state. Stories are inserted in creation order. If count exceeds max_open_stories, oldest are evicted.

    Preconditions:
      - StoryManager OrderedDict is empty (fresh initialization or after shutdown)
      - Sweep task is not running

    Postconditions:
      - All non-evicted recovered stories are present in OrderedDict
      - Return value is the number of stories loaded
      - len(open_stories) <= max_open_stories

    Errors:
      - invalid_recovery_data (None): A recovered story dict is malformed or missing required fields
          detail: Malformed story skipped with WARNING log, recovery continues

    Side effects: none
    Idempotent: no
    """
    ...

def append_record(
    path: FilePath,
    record: StateRecord,
) -> None:
    """
    Append a single state record to the JSONL state file. Uses aiofiles for async I/O. Each record is serialized via Pydantic model_dump(mode='json') and written as a single line followed by newline. Flushes after write. I/O errors are caught and logged — never crashes the engine.

    Preconditions:
      - path is writable or the parent directory exists and is writable

    Postconditions:
      - Record is appended as a single JSON line to the file
      - File is flushed after write
      - On I/O error: error is logged, no exception propagates

    Errors:
      - io_error (None): File write or flush fails (disk full, permissions, etc.)
          detail: I/O error logged at ERROR level with record context. Engine continues.
      - serialization_error (None): Record fails to serialize to JSON
          detail: Serialization error logged at ERROR level. Record dropped.

    Side effects: none
    Idempotent: no
    """
    ...

def recover_state(
    path: FilePath,
    clock: ClockCallable = None,
) -> RecoveryResult:
    """
    Read the JSONL state file and replay all records to reconstruct the set of open stories at the time of last shutdown. Skips corrupt/malformed lines with warning logs. Returns a RecoveryResult with open stories (those with StoryOpened but no corresponding StoryClosed) and recovered chain entries.

    Postconditions:
      - Returned open_stories contains only stories with StoryOpened record but no corresponding StoryClosed record
      - Each open story has its full event list from replayed EventAppended records
      - records_skipped counts all malformed lines
      - records_read + records_skipped equals total lines in file
      - If file does not exist, returns empty RecoveryResult with all counts 0

    Errors:
      - file_not_found (None): State file does not exist
          detail: Returns empty RecoveryResult. Logged at INFO level.
      - corrupt_line (None): A line in the JSONL file is not valid JSON or fails deserialization
          detail: Line skipped with WARNING log including line number. Recovery continues.
      - io_error (OSError): File read fails due to permissions or other I/O error
          detail: Unrecoverable I/O error during state recovery

    Side effects: none
    Idempotent: yes
    """
    ...

# ── REQUIRED EXPORTS ──────────────────────────────────
# Your implementation module MUST export ALL of these names
# with EXACTLY these spellings. Tests import them by name.
# __all__ = ['MatchPredicate', 'TerminalCondition', 'CorrelationRule', 'Event', 'StoryStatus', 'Story', 'CompositeKey', 'ChainEntry', 'RecordType', 'StoryOpenedRecord', 'EventAppendedRecord', 'StoryClosedRecord', 'StateRecord', 'RecoveryResult', 'CorrelationEngineConfig', 'EngineStats', 'append_record', 'recover_state']

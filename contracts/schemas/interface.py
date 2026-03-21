# === Core Data Models (schemas) v1 ===
# Pydantic v2 frozen models for Event (content-addressable SHA-256 ID from canonical JSON of identity fields), Story (open→closed lifecycle with ordered events, close reasons, chain_by support), CorrelationRule (group_by/chain_by/terminal_event/timeout), and all configuration models (source configs, sink configs, memory limits, kindex noteworthiness filter). Also defines SourceProtocol and SinkProtocol as runtime_checkable Protocol classes. This is the shared data model used by every other Chronicler component. Covers AC1 (Event model), AC2 (Story model), AC16 (config models, protocols).

# Module invariants:
#   - All Event instances are frozen (immutable) — no attribute can be reassigned after construction
#   - All Story instances are frozen (immutable) — mutation only via model_copy(update={...}) producing a new instance
#   - All CorrelationRule instances are frozen (immutable)
#   - Event.id is always the SHA-256 hex digest of canonical JSON of identity fields (event_kind, entity_kind, entity_id, actor, timestamp, context, correlation_keys)
#   - Canonical JSON uses json.dumps(sort_keys=True, separators=(',',':'), ensure_ascii=True, default=str) with timestamps as ISO 8601 'Z' suffix
#   - Two Events with identical identity fields always have the same id (content-addressable)
#   - Event.source is excluded from the content hash — it is provenance metadata only
#   - An open Story has status='open', end_ts=None, duration=None, close_reason=None
#   - A closed Story has status='closed', end_ts set (timezone-aware), duration set (>=0), close_reason set to 'terminal'|'timeout'|'evicted'
#   - Story.event_count always equals len(Story.events)
#   - Story.events is a tuple of SHA-256 hex event ID strings (ordered by append time)
#   - Story.duration (when set) equals (end_ts - start_ts).total_seconds()
#   - CorrelationRule.event_kinds is a frozenset with at least one member
#   - CorrelationRule.group_by is a non-empty tuple
#   - All timestamps in Event and Story are timezone-aware (AwareDatetime) — naive datetimes are rejected at validation
#   - SourceProtocol is a runtime_checkable typing.Protocol with async start(), async stop(), and subscribe(callback) methods
#   - SinkProtocol is a runtime_checkable typing.Protocol with async start(), async stop(), and async emit(item: Event|Story) methods
#   - ChroniclerConfig requires at least one source, one sink, and one correlation rule
#   - CorrelationRule names are unique within a ChroniclerConfig
#   - All config models use Pydantic v2 with frozen=True

EventKind = primitive  # A dot-separated event kind identifier (e.g. 'ci.build.completed', 'incident.opened'). Must be non-empty, lowercase alphanumeric with dots and underscores.

EntityKind = primitive  # The kind/type of the entity this event pertains to (e.g. 'repository', 'pipeline', 'incident'). Lowercase alphanumeric with dots and underscores.

EntityId = primitive  # A non-empty identifier for the entity this event pertains to. Free-form but non-empty, max 1024 chars.

ActorId = primitive  # Identifier for the actor that caused the event (user, service, system). Non-empty, max 512 chars.

Sha256Hex = primitive  # A lowercase hex-encoded SHA-256 hash (64 characters).

StoryId = primitive  # UUID4 string identifier for a Story.

AwareDatetime = primitive  # A timezone-aware ISO 8601 datetime string. Naive datetimes are rejected at validation time. Internally stored as Python datetime with tzinfo set.

ContextValue = str | int | float | bool | None

ContextDict = primitive  # A dict[str, ContextValue] — the flexible context bag on an Event. Keys are non-empty strings. Values are constrained to str|int|float|bool|None for deterministic canonical JSON hashing.

CorrelationKeys = primitive  # A dict[str, str] of correlation key-value pairs used for grouping events into stories. All keys and values must be non-empty strings.

class StoryStatus(Enum):
    """Lifecycle status of a Story."""
    open = "open"
    closed = "closed"

class CloseReason(Enum):
    """Reason why a Story was closed."""
    terminal = "terminal"
    timeout = "timeout"
    evicted = "evicted"

class Event:
    """Immutable event envelope (frozen=True). The id field is a content-addressable SHA-256 hex digest computed via model_validator(mode='before') from canonical JSON of the identity fields: event_kind, entity_kind, entity_id, actor, timestamp, context, correlation_keys. Canonical JSON uses json.dumps(sort_keys=True, separators=(',',':'), ensure_ascii=True, default=str) on a fixed-key dict of those fields, with timestamps serialized as ISO 8601 with 'Z' suffix. The 'source' field is excluded from the hash (provenance metadata only)."""
    id: Sha256Hex                            # required, Content-addressable SHA-256 hex digest of canonical JSON of identity fields. Computed automatically by model_validator(mode='before'); must not be set manually unless replaying.
    event_kind: EventKind                    # required, Dot-separated kind of event (e.g. 'ci.build.completed').
    entity_kind: EntityKind                  # required, Kind of entity this event pertains to (e.g. 'repository').
    entity_id: EntityId                      # required, Identifier for the specific entity instance.
    actor: ActorId                           # required, Who or what caused this event.
    timestamp: AwareDatetime                 # required, When the event occurred. Must be timezone-aware.
    context: ContextDict                     # required, Flexible key-value context data. Values constrained to str|int|float|bool|None for deterministic hashing.
    correlation_keys: CorrelationKeys        # required, Key-value pairs used by CorrelationRules to group events into stories.
    source: str = None                       # optional, Provenance label indicating which source produced this event. Excluded from content hash.

class Story:
    """Immutable story model (frozen=True). Represents a correlated sequence of events with an open→closed lifecycle. Mutation is performed via model_copy(update={...}) to produce new Story instances. A model_validator(mode='after') enforces that closed stories have end_ts and close_reason set, and open stories have neither. Events are stored as a tuple of event ID strings (not full Event objects) for frozen compatibility and copy efficiency."""
    story_id: StoryId                        # required, UUID4 identifier for this story.
    story_type: str                          # required, length(1..255), Type label for this story, derived from the CorrelationRule.story_type.
    correlation_rule: str                    # required, length(1..255), Name of the CorrelationRule that created this story.
    group_key: CorrelationKeys               # required, The group_by key-value pairs that define this story's identity within its rule.
    events: EventIdTuple                     # required, Ordered tuple of event IDs (SHA-256 hex) belonging to this story.
    status: StoryStatus                      # required, Lifecycle status: 'open' or 'closed'.
    start_ts: AwareDatetime                  # required, Timestamp of the first event added to this story.
    end_ts: AwareDatetime = None             # optional, Timestamp when the story was closed. Required when status is 'closed', must be absent/None when 'open'.
    duration: float = None                   # optional, range(0.0..), Duration in seconds from start_ts to end_ts. Set when closed.
    event_count: int                         # required, range(0..), Number of events in this story. Must equal len(events).
    close_reason: CloseReason = None         # optional, Reason for closing: 'terminal' (terminal event matched), 'timeout' (rule timeout elapsed), 'evicted' (memory pressure). Required when closed, must be absent/None when open.
    parent_story_id: StoryId = None          # optional, If this story was created via chain_by from a closed story, the parent story's ID.

EventIdTuple = list[Sha256Hex]
# An ordered tuple of event IDs (SHA-256 hex strings). Stored as tuple for frozen model hashability.

TerminalEventPattern = primitive  # A dict[str, str] representing a simple field=value pattern match on an event. All specified fields must match for the event to be considered terminal. Keys are dot-path field references (e.g. 'event_kind', 'context.status').

GroupByKeys = list[str]
# Ordered tuple of field paths used for grouping events into stories (e.g. ('correlation_keys.repo', 'correlation_keys.branch')). Order matters for deterministic group key construction.

ChainByKeys = list[str]
# Ordered tuple of field paths linking a closed story to a new story via chain_by semantics.

class CorrelationRule:
    """Immutable rule definition (frozen=True) for correlating events into stories. Defines which event kinds to match, how to group them, optional chaining from closed stories, terminal event detection, and timeout."""
    name: str                                # required, regex(^[a-zA-Z][a-zA-Z0-9_.-]{0,254}$), Unique name for this correlation rule.
    event_kinds: EventKindSet                # required, Set of event kinds this rule applies to. An event must have an event_kind in this set to be considered by this rule.
    group_by: GroupByKeys                    # required, Ordered tuple of field paths used to compute the group key. Events with the same group key values are correlated into the same story.
    chain_by: ChainByKeys = None             # optional, Optional ordered tuple of field paths for chain_by semantics. When a story is closed by terminal event, a new story is opened if a subsequent event matches these fields from the closed story.
    terminal_event: TerminalEventPattern = None # optional, Optional simple field=value pattern. When an event matches all specified fields, the story is closed with close_reason='terminal'.
    timeout: float = None                    # optional, range(0.001..), Optional timeout in seconds. If no new event arrives within this duration after the last event, the story is closed with close_reason='timeout'.
    story_type: str                          # required, length(1..255), The story_type label assigned to stories created by this rule.

EventKindSet = primitive  # A frozenset of EventKind strings. Used for unordered set membership checks in CorrelationRule.event_kinds.

class SourceType(Enum):
    """Discriminator for source configuration types."""
    webhook = "webhook"
    otlp = "otlp"
    file = "file"
    sentinel = "sentinel"

class WebhookSourceConfig:
    """Configuration for the HTTP POST webhook source."""
    type: str                                # required, Discriminator, always 'webhook'.
    host: str = 0.0.0.0                      # optional, Bind address for the webhook HTTP server.
    port: int                                # required, range(1..65535), Port to listen on.
    path: str = /events                      # optional, regex(^/[a-zA-Z0-9/_-]*$), URL path to accept POSTs on.
    source_label: str = webhook              # optional, Value to set in Event.source for events from this source.

class OtlpSourceConfig:
    """Configuration for the OTLP gRPC collector source."""
    type: str                                # required, Discriminator, always 'otlp'.
    host: str = 0.0.0.0                      # optional, Bind address for the OTLP gRPC server.
    port: int                                # required, range(1..65535), Port to listen on.
    source_label: str = otlp                 # optional, Value to set in Event.source for events from this source.

class FileSourceConfig:
    """Configuration for the JSONL file tailer source."""
    type: str                                # required, Discriminator, always 'file'.
    path: str                                # required, length(1..4096), Path to the JSONL file to tail.
    poll_interval_seconds: float = 1.0       # optional, range(0.1..3600), How often to poll for new lines in seconds.
    source_label: str = file                 # optional, Value to set in Event.source for events from this source.

class SentinelSourceConfig:
    """Configuration for the Sentinel incident receiver source."""
    type: str                                # required, Discriminator, always 'sentinel'.
    host: str = 0.0.0.0                      # optional, Bind address.
    port: int                                # required, range(1..65535), Port to listen on.
    source_label: str = sentinel             # optional, Value to set in Event.source for events from this source.

SourceConfig = WebhookSourceConfig | OtlpSourceConfig | FileSourceConfig | SentinelSourceConfig

class SinkType(Enum):
    """Discriminator for sink configuration types."""
    stigmergy = "stigmergy"
    apprentice = "apprentice"
    disk = "disk"
    kindex = "kindex"

class StigmergySinkConfig:
    """Configuration for emitting events/stories to Stigmergy."""
    type: str                                # required, Discriminator, always 'stigmergy'.
    url: str                                 # required, regex(^https?://), Stigmergy endpoint URL.
    timeout_seconds: float = 10.0            # optional, range(0.5..300), HTTP request timeout in seconds.

class ApprenticeSinkConfig:
    """Configuration for emitting events/stories to Apprentice."""
    type: str                                # required, Discriminator, always 'apprentice'.
    url: str                                 # required, regex(^https?://), Apprentice endpoint URL.
    timeout_seconds: float = 10.0            # optional, range(0.5..300), HTTP request timeout in seconds.

class DiskSinkConfig:
    """Configuration for the JSONL disk persistence sink."""
    type: str                                # required, Discriminator, always 'disk'.
    events_path: str                         # required, length(1..4096), Path for the events JSONL file.
    stories_path: str                        # required, length(1..4096), Path for the stories JSONL file.

class NoteworthinessFilter:
    """Configuration for filtering which stories are considered noteworthy for Kindex knowledge capture."""
    min_event_count: int = 1                 # optional, range(1..), Minimum number of events in a story for it to be noteworthy.
    min_duration_seconds: float = 0.0        # optional, range(0.0..), Minimum story duration in seconds for noteworthiness.
    story_types: list = None                 # optional, If non-empty, only stories with these story_types are noteworthy. Empty means all types qualify.
    exclude_close_reasons: list = None       # optional, Close reasons to exclude from noteworthiness (e.g. ['evicted'] to skip evicted stories).

class KindexSinkConfig:
    """Configuration for the Kindex knowledge capture sink."""
    type: str                                # required, Discriminator, always 'kindex'.
    url: str                                 # required, regex(^https?://), Kindex endpoint URL.
    timeout_seconds: float = 10.0            # optional, range(0.5..300), HTTP request timeout in seconds.
    noteworthiness: NoteworthinessFilter = None # optional, Filter controlling which stories are forwarded to Kindex.

SinkConfig = StigmergySinkConfig | ApprenticeSinkConfig | DiskSinkConfig | KindexSinkConfig

class MemoryLimitsConfig:
    """Configuration for memory pressure management in the story manager."""
    max_open_stories: int                    # required, range(1..1000000), Maximum number of concurrently open stories. When exceeded, oldest stories are evicted with close_reason='evicted'.
    max_events_per_story: int                # required, range(1..100000), Maximum number of events in a single story. When exceeded, the story is closed with close_reason='evicted'.

class ChroniclerConfig:
    """Top-level Chronicler configuration loaded from YAML. Validated at startup; invalid config is a fatal error."""
    sources: list                            # required, custom(len(value) >= 1), List of source configurations (at least one required).
    sinks: list                              # required, custom(len(value) >= 1), List of sink configurations (at least one required).
    correlation_rules: list                  # required, custom(len(value) >= 1), List of CorrelationRule definitions (at least one required).
    memory_limits: MemoryLimitsConfig        # required, Memory pressure management configuration.
    state_path: str = ./chronicler_state     # optional, length(1..4096), Directory path for JSONL state persistence files.

EventOrStory = Event | Story

def compute_event_id(
    event_kind: str,
    entity_kind: str,
    entity_id: str,
    actor: str,
    timestamp: AwareDatetime,
    context: ContextDict,
    correlation_keys: CorrelationKeys,
) -> Sha256Hex:
    """
    Computes the content-addressable SHA-256 hex ID for an Event from its identity fields. Constructs a fixed-key dict of {event_kind, entity_kind, entity_id, actor, timestamp, context, correlation_keys}, serializes to canonical JSON (json.dumps(sort_keys=True, separators=(',',':'), ensure_ascii=True, default=str)) with timestamps as ISO 8601 'Z' suffix, then returns the lowercase hex SHA-256 digest. This is a pure function with no side effects.

    Preconditions:
      - timestamp must be timezone-aware (has tzinfo)
      - All context values must be str|int|float|bool|None
      - All correlation_keys values must be non-empty strings

    Postconditions:
      - Returned string is exactly 64 lowercase hex characters
      - Same inputs always produce the same output (deterministic)
      - Different inputs with any single field changed produce a different output (collision-resistant)

    Errors:
      - naive_timestamp (ValueError): timestamp has no tzinfo (naive datetime)
          message: Event timestamp must be timezone-aware
      - invalid_context_value_type (TypeError): A value in the context dict is not str|int|float|bool|None
          message: Context values must be str, int, float, bool, or None

    Side effects: none
    Idempotent: yes
    """
    ...

def create_event(
    event_kind: EventKind,
    entity_kind: EntityKind,
    entity_id: EntityId,
    actor: ActorId,
    timestamp: AwareDatetime,
    context: ContextDict,
    correlation_keys: CorrelationKeys,
    source: str = None,
) -> Event:
    """
    Creates a new Event instance. The id field is automatically computed via compute_event_id from the identity fields using the model_validator(mode='before'). This is the primary factory for Event creation. Returns a frozen Pydantic model instance.

    Preconditions:
      - event_kind matches ^[a-z][a-z0-9_.]{0,254}$
      - entity_kind matches ^[a-z][a-z0-9_.]{0,254}$
      - entity_id is non-empty, max 1024 chars
      - actor is non-empty, max 512 chars
      - timestamp is timezone-aware
      - All context values are str|int|float|bool|None
      - All correlation_keys keys and values are non-empty strings

    Postconditions:
      - Returned Event is frozen (immutable)
      - Event.id is the SHA-256 of canonical JSON of identity fields
      - Creating the same event twice produces Events with identical id fields
      - Event.source is excluded from the hash computation

    Errors:
      - invalid_event_kind (ValidationError): event_kind does not match the EventKind pattern
          field: event_kind
      - invalid_entity_kind (ValidationError): entity_kind does not match the EntityKind pattern
          field: entity_kind
      - empty_entity_id (ValidationError): entity_id is empty
          field: entity_id
      - empty_actor (ValidationError): actor is empty
          field: actor
      - naive_timestamp (ValidationError): timestamp has no tzinfo
          field: timestamp
      - invalid_context_value (ValidationError): A context value is not str|int|float|bool|None
          field: context
      - invalid_correlation_key (ValidationError): A correlation key or value is empty or not a string
          field: correlation_keys

    Side effects: none
    Idempotent: yes
    """
    ...

def create_story(
    story_type: str,
    correlation_rule: str,
    group_key: CorrelationKeys,
    first_event_id: Sha256Hex,
    start_ts: AwareDatetime,
    parent_story_id: StoryId = None,
) -> Story:
    """
    Creates a new open Story instance with a generated UUID4 story_id, the given story_type and correlation_rule name, an initial group_key, and the first event ID. Status is set to 'open', end_ts/duration/close_reason are None.

    Preconditions:
      - story_type is non-empty
      - correlation_rule is non-empty
      - first_event_id is a valid SHA-256 hex string
      - start_ts is timezone-aware

    Postconditions:
      - Returned Story is frozen (immutable)
      - story.status == 'open'
      - story.events == (first_event_id,)
      - story.event_count == 1
      - story.end_ts is None
      - story.duration is None
      - story.close_reason is None
      - story.story_id is a valid UUID4

    Errors:
      - empty_story_type (ValidationError): story_type is empty
          field: story_type
      - empty_correlation_rule (ValidationError): correlation_rule is empty
          field: correlation_rule
      - invalid_first_event_id (ValidationError): first_event_id is not a valid SHA-256 hex string
          field: first_event_id

    Side effects: none
    Idempotent: no
    """
    ...

def append_event_to_story(
    story: Story,
    event_id: Sha256Hex,
) -> Story:
    """
    Produces a new Story with an additional event appended. Uses model_copy(update={...}) on the source story to add the event ID to events, increment event_count. The story must be open.

    Preconditions:
      - story.status == 'open'
      - event_id is a valid SHA-256 hex string
      - event_id is not already in story.events (no duplicates)

    Postconditions:
      - Returned Story is frozen (immutable)
      - Returned story.events == story.events + (event_id,)
      - Returned story.event_count == story.event_count + 1
      - Returned story.status == 'open'
      - Returned story.story_id == story.story_id (same identity)
      - Original story is unchanged (immutable)

    Errors:
      - story_not_open (ValueError): story.status != 'open'
          message: Cannot append event to a closed story
      - duplicate_event_id (ValueError): event_id already in story.events
          message: Event ID already exists in story

    Side effects: none
    Idempotent: no
    """
    ...

def close_story(
    story: Story,
    close_reason: CloseReason,
    end_ts: AwareDatetime,
) -> Story:
    """
    Produces a new Story with status='closed', the given close_reason, end_ts set, and duration computed as (end_ts - start_ts).total_seconds(). Uses model_copy(update={...}) on the source story.

    Preconditions:
      - story.status == 'open'
      - end_ts >= story.start_ts
      - end_ts is timezone-aware

    Postconditions:
      - Returned Story is frozen (immutable)
      - Returned story.status == 'closed'
      - Returned story.close_reason == close_reason
      - Returned story.end_ts == end_ts
      - Returned story.duration == (end_ts - start_ts).total_seconds()
      - Returned story.duration >= 0.0
      - Returned story.story_id == story.story_id
      - Returned story.events == story.events (unchanged)
      - Original story is unchanged (immutable)

    Errors:
      - story_already_closed (ValueError): story.status == 'closed'
          message: Cannot close an already closed story
      - end_before_start (ValueError): end_ts < story.start_ts
          message: end_ts must not be before start_ts

    Side effects: none
    Idempotent: no
    """
    ...

def validate_story_consistency(
    story: Story,
) -> bool:
    """
    Validates the internal consistency of a Story model. Checks that: open stories have no end_ts/duration/close_reason; closed stories have all three set; event_count matches len(events); duration matches (end_ts - start_ts).total_seconds() for closed stories. This is used by the model_validator(mode='after') and can also be called explicitly for testing.

    Postconditions:
      - Returns True if story is internally consistent
      - Raises ValidationError if any consistency check fails

    Errors:
      - open_story_has_end_ts (ValidationError): story.status == 'open' and story.end_ts is not None
          message: Open story must not have end_ts
      - open_story_has_close_reason (ValidationError): story.status == 'open' and story.close_reason is not None
          message: Open story must not have close_reason
      - open_story_has_duration (ValidationError): story.status == 'open' and story.duration is not None
          message: Open story must not have duration
      - closed_story_missing_end_ts (ValidationError): story.status == 'closed' and story.end_ts is None
          message: Closed story must have end_ts
      - closed_story_missing_close_reason (ValidationError): story.status == 'closed' and story.close_reason is None
          message: Closed story must have close_reason
      - closed_story_missing_duration (ValidationError): story.status == 'closed' and story.duration is None
          message: Closed story must have duration
      - event_count_mismatch (ValidationError): story.event_count != len(story.events)
          message: event_count must equal len(events)

    Side effects: none
    Idempotent: yes
    """
    ...

def matches_terminal_pattern(
    event: Event,
    pattern: TerminalEventPattern,
) -> bool:
    """
    Checks whether an Event matches a TerminalEventPattern from a CorrelationRule. For each key-value pair in the pattern, the event must have a matching value at the specified field path. Supports 'event_kind', 'entity_kind', 'entity_id', 'actor' as direct field paths, and 'context.<key>' or 'correlation_keys.<key>' for nested access.

    Preconditions:
      - event is a valid Event instance
      - pattern keys are valid field paths

    Postconditions:
      - Returns True if all pattern entries match the event's field values
      - Returns False if any pattern entry does not match or field does not exist
      - Empty pattern matches all events (vacuously true)

    Errors:
      - invalid_field_path (ValueError): A pattern key is not a recognized field path (not event_kind/entity_kind/entity_id/actor/context.*/correlation_keys.*)
          message: Unrecognized terminal event pattern field path

    Side effects: none
    Idempotent: yes
    """
    ...

def extract_group_key(
    event: Event,
    group_by: GroupByKeys,
) -> CorrelationKeys:
    """
    Extracts the group key values from an Event according to a CorrelationRule's group_by field paths. Returns a CorrelationKeys dict mapping each group_by path to the corresponding value from the event. Field paths reference 'correlation_keys.<key>', 'context.<key>', or top-level event fields.

    Preconditions:
      - event is a valid Event instance
      - group_by contains at least one field path
      - All field paths in group_by resolve to string values on the event

    Postconditions:
      - Returned dict has exactly len(group_by) entries
      - Each key in the returned dict is the field path from group_by
      - Each value is the string representation of the event's field at that path

    Errors:
      - missing_field (KeyError): A group_by field path does not resolve to a value on the event
          message: Event is missing required group_by field
      - non_string_value (TypeError): A resolved field value is not a string and cannot be deterministically converted
          message: Group key field must resolve to a string value

    Side effects: none
    Idempotent: yes
    """
    ...

def load_chronicler_config(
    config_path: str,
) -> ChroniclerConfig:
    """
    Loads and validates a ChroniclerConfig from a YAML file path. Uses Pydantic v2 validation on the parsed YAML. Fails fast with a clear error if the config is invalid (fatal startup error per SOP). Validates all source/sink configs via discriminated unions, all CorrelationRules, and memory limits.

    Preconditions:
      - config_path is a readable file path
      - File contains valid YAML

    Postconditions:
      - Returned ChroniclerConfig is fully validated
      - All source configs are valid instances of their discriminated types
      - All sink configs are valid instances of their discriminated types
      - All CorrelationRules are valid
      - memory_limits has valid max_open_stories and max_events_per_story
      - At least one source, one sink, and one correlation rule are configured

    Errors:
      - file_not_found (FileNotFoundError): config_path does not exist or is not readable
          message: Configuration file not found
      - invalid_yaml (ValueError): File content is not valid YAML
          message: Configuration file contains invalid YAML
      - validation_error (ValidationError): Parsed YAML does not conform to ChroniclerConfig schema
          message: Configuration validation failed
      - duplicate_rule_names (ValidationError): Two or more CorrelationRules have the same name
          message: CorrelationRule names must be unique
      - empty_sources (ValidationError): No sources configured
          message: At least one source must be configured
      - empty_sinks (ValidationError): No sinks configured
          message: At least one sink must be configured
      - empty_rules (ValidationError): No correlation rules configured
          message: At least one correlation rule must be configured

    Side effects: none
    Idempotent: yes
    """
    ...

def event_to_dict(
    event: Event,
) -> dict:
    """
    Serializes an Event to a plain dict suitable for JSON serialization. Uses Pydantic's model_dump(mode='json'). Timestamps are serialized as ISO 8601 strings. The id field is included.

    Preconditions:
      - event is a valid Event instance

    Postconditions:
      - Returned dict contains all Event fields
      - Returned dict is JSON-serializable
      - Timestamp fields are ISO 8601 strings
      - Round-tripping through event_from_dict(event_to_dict(e)) produces an Event with the same id

    Side effects: none
    Idempotent: yes
    """
    ...

def event_from_dict(
    data: dict,
) -> Event:
    """
    Deserializes an Event from a plain dict (e.g. parsed from JSON). Validates all fields. If 'id' is present in the dict, it is verified against the computed hash; if it mismatches, a ValidationError is raised. If 'id' is absent, it is computed.

    Preconditions:
      - data contains at minimum: event_kind, entity_kind, entity_id, actor, timestamp, context, correlation_keys

    Postconditions:
      - Returned Event is frozen and valid
      - Event.id matches the content hash of identity fields
      - If input dict had an 'id' field, it matches the computed id

    Errors:
      - missing_required_field (ValidationError): A required field (event_kind, entity_kind, entity_id, actor, timestamp, context, correlation_keys) is missing
          message: Missing required event field
      - id_mismatch (ValidationError): The provided 'id' field does not match the computed SHA-256 hash
          message: Event ID does not match computed content hash
      - invalid_field_value (ValidationError): A field value fails validation (e.g. naive timestamp, invalid event_kind pattern)
          message: Event field validation failed

    Side effects: none
    Idempotent: yes
    """
    ...

def story_to_dict(
    story: Story,
) -> dict:
    """
    Serializes a Story to a plain dict suitable for JSON serialization. Uses Pydantic's model_dump(mode='json'). Events tuple is serialized as a JSON array of ID strings.

    Preconditions:
      - story is a valid Story instance

    Postconditions:
      - Returned dict contains all Story fields
      - Returned dict is JSON-serializable
      - events field is a list of SHA-256 hex strings
      - Timestamp fields are ISO 8601 strings or null

    Side effects: none
    Idempotent: yes
    """
    ...

def story_from_dict(
    data: dict,
) -> Story:
    """
    Deserializes a Story from a plain dict (e.g. parsed from JSON). Validates all fields including the lifecycle consistency checks (model_validator(mode='after')).

    Preconditions:
      - data contains all required Story fields

    Postconditions:
      - Returned Story is frozen and valid
      - Lifecycle consistency checks pass (open↔closed invariants)
      - event_count matches len(events)

    Errors:
      - missing_required_field (ValidationError): A required field is missing from the dict
          message: Missing required story field
      - lifecycle_inconsistency (ValidationError): Story lifecycle invariants are violated (e.g. open story with end_ts)
          message: Story lifecycle consistency check failed
      - event_count_mismatch (ValidationError): event_count does not match len(events)
          message: event_count must equal len(events)

    Side effects: none
    Idempotent: yes
    """
    ...

# ── REQUIRED EXPORTS ──────────────────────────────────
# Your implementation module MUST export ALL of these names
# with EXACTLY these spellings. Tests import them by name.
# __all__ = ['ContextValue', 'StoryStatus', 'CloseReason', 'Event', 'Story', 'EventIdTuple', 'GroupByKeys', 'ChainByKeys', 'CorrelationRule', 'SourceType', 'WebhookSourceConfig', 'OtlpSourceConfig', 'FileSourceConfig', 'SentinelSourceConfig', 'SourceConfig', 'SinkType', 'StigmergySinkConfig', 'ApprenticeSinkConfig', 'DiskSinkConfig', 'NoteworthinessFilter', 'KindexSinkConfig', 'SinkConfig', 'MemoryLimitsConfig', 'ChroniclerConfig', 'EventOrStory', 'compute_event_id', 'create_event', 'ValidationError', 'create_story', 'append_event_to_story', 'close_story', 'validate_story_consistency', 'matches_terminal_pattern', 'extract_group_key', 'load_chronicler_config', 'event_to_dict', 'event_from_dict', 'story_to_dict', 'story_from_dict']

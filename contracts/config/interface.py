# === Configuration Loading (config) v1 ===
# Load and validate chronicler.yaml using Pydantic v2. Parses correlation rules, source configs, sink configs, memory limits (max_open_stories), timeout settings, and kindex noteworthiness filters. Fails fast with clear error messages on invalid config. Produces frozen typed config objects. Pure function: explicit Path in, typed ChroniclerConfig out. Covers AC9.

# Module invariants:
#   - All config model classes use Pydantic v2 with frozen=True (immutable after construction).
#   - YAML is parsed exclusively with yaml.safe_load(); yaml.load() is never used.
#   - ConfigError is raised for all failure modes — no other exception type escapes load_config.
#   - The config module performs no network I/O, no state mutation, and no writes to disk.
#   - A kindex sink in the sinks list requires a non-null kindex section at the top level (cross-field invariant).
#   - All correlation rule names within a single config must be unique.
#   - All regex patterns in CorrelationRuleConfig match conditions (where is_regex=true) must be valid Python re-compatible regular expressions.
#   - Default values for max_open_stories (10000) and story_timeout_seconds (300) are applied when omitted from YAML.
#   - Source and sink config types are discriminated unions keyed on the 'type' field.
#   - The load_config function is deterministic and idempotent: same file content always yields the same ChroniclerConfig or the same ConfigError.

FilePath = primitive  # A validated filesystem path string pointing to an existing readable file.

class SourceType(Enum):
    """Discriminator enum for source configuration types."""
    webhook = "webhook"
    otlp = "otlp"
    file = "file"
    sentinel = "sentinel"

class SinkType(Enum):
    """Discriminator enum for sink configuration types."""
    stigmergy = "stigmergy"
    apprentice = "apprentice"
    disk = "disk"
    kindex = "kindex"

class WebhookSourceConfig:
    """Configuration for the HTTP POST webhook event source. Frozen Pydantic model."""
    type: SourceType                         # required, Discriminator literal, always 'webhook'.
    bind_address: str = 0.0.0.0              # optional, length(min=1,max=253), IP address or hostname to bind the webhook HTTP server.
    port: int = 8080                         # optional, range(min=1,max=65535), TCP port for the webhook HTTP server.
    path: str = /events                      # optional, regex(^/.*), URL path to listen on for incoming webhooks.

class OtlpSourceConfig:
    """Configuration for the OTLP gRPC collector source. Frozen Pydantic model."""
    type: SourceType                         # required, Discriminator literal, always 'otlp'.
    bind_address: str = 0.0.0.0              # optional, length(min=1,max=253), IP address or hostname to bind the OTLP gRPC server.
    port: int = 4317                         # optional, range(min=1,max=65535), TCP port for the OTLP gRPC server.

class FileSourceConfig:
    """Configuration for the JSONL file tailer source. Frozen Pydantic model."""
    type: SourceType                         # required, Discriminator literal, always 'file'.
    path: str                                # required, length(min=1), Filesystem path to the JSONL file to tail.
    poll_interval_seconds: float = 1.0       # optional, range(min=0.1,max=3600.0), Interval in seconds between file polls for new lines.

class SentinelSourceConfig:
    """Configuration for the Sentinel incident receiver source. Frozen Pydantic model."""
    type: SourceType                         # required, Discriminator literal, always 'sentinel'.
    bind_address: str = 0.0.0.0              # optional, length(min=1,max=253), IP address or hostname to bind the Sentinel receiver.
    port: int = 8081                         # optional, range(min=1,max=65535), TCP port for the Sentinel incident receiver.

SourceConfig = WebhookSourceConfig | OtlpSourceConfig | FileSourceConfig | SentinelSourceConfig

class StigmergySinkConfig:
    """Configuration for the Stigmergy emit sink. Frozen Pydantic model."""
    type: SinkType                           # required, Discriminator literal, always 'stigmergy'.
    url: str                                 # required, regex(^https?://.+), Base URL of the Stigmergy API endpoint.

class ApprenticeSinkConfig:
    """Configuration for the Apprentice emit sink. Frozen Pydantic model."""
    type: SinkType                           # required, Discriminator literal, always 'apprentice'.
    url: str                                 # required, regex(^https?://.+), Base URL of the Apprentice API endpoint.

class DiskSinkConfig:
    """Configuration for the JSONL disk persistence sink. Frozen Pydantic model."""
    type: SinkType                           # required, Discriminator literal, always 'disk'.
    output_dir: str = ./stories              # optional, length(min=1), Directory path where JSONL story files are written.

class KindexSinkConfig:
    """Configuration for the Kindex knowledge capture sink. Frozen Pydantic model."""
    type: SinkType                           # required, Discriminator literal, always 'kindex'.
    url: str                                 # required, regex(^https?://.+), Base URL of the Kindex API endpoint.

SinkConfig = StigmergySinkConfig | ApprenticeSinkConfig | DiskSinkConfig | KindexSinkConfig

class MatchCondition:
    """A single field-level match condition for a correlation rule. Specifies a field name and a pattern (exact string or regex) to match against incoming event values."""
    field: str                               # required, length(min=1,max=256), Dot-notated path to the event field to match (e.g. 'event_type', 'metadata.service').
    pattern: str                             # required, length(min=1), Exact string value or regex pattern to match against the field value.
    is_regex: bool = false                   # optional, If true, 'pattern' is interpreted as a regex. If false, exact match.

class CorrelationRuleConfig:
    """Definition of a correlation rule that groups matching events into stories. Frozen Pydantic model."""
    name: str                                # required, length(min=1,max=128), regex(^[a-zA-Z0-9_.-]+$), Unique human-readable name for this correlation rule.
    match_conditions: list                   # required, length(min=1), List of conditions that an event must satisfy to be captured by this rule. All conditions must match (AND logic).
    group_by: list                           # required, length(min=1), List of event field paths whose values form the story grouping key. Events with the same group_by values are correlated into the same story.
    window_seconds: int = 300                # optional, range(min=1,max=86400), Maximum time window in seconds within which events can be correlated into the same story under this rule.

NoteworthinessThreshold = primitive  # A float value between 0.0 and 1.0 representing the minimum noteworthiness score for kindex capture.

class KindexConfig:
    """Configuration for Kindex noteworthiness filtering. Controls which completed stories are forwarded to the Kindex knowledge capture sink. Frozen Pydantic model."""
    noteworthiness_threshold: float = 0.5    # optional, range(min=0.0,max=1.0), Minimum noteworthiness score (0.0–1.0) a completed story must meet to be captured by Kindex.
    event_type_filters: list = []            # optional, If non-empty, only stories containing at least one event of these types are considered for Kindex capture. Empty list means no event type filtering.
    tag_patterns: list = []                  # optional, If non-empty, only stories with at least one tag matching these patterns (glob or regex) are considered for Kindex capture. Empty list means no tag filtering.

class ChroniclerConfig:
    """Top-level Chronicler configuration. Root frozen Pydantic model parsed from chronicler.yaml. Contains all sources, sinks, correlation rules, operational limits, and optional kindex settings."""
    sources: list                            # required, length(min=1), List of event source configurations. At least one source must be defined.
    sinks: list                              # required, length(min=1), List of sink configurations for story output. At least one sink must be defined.
    rules: list                              # required, length(min=1), List of correlation rule definitions that govern how events are grouped into stories.
    max_open_stories: int = 10000            # optional, range(min=1,max=1000000), Maximum number of stories that may be open (in-progress) simultaneously. Oldest stories are evicted when this limit is exceeded.
    story_timeout_seconds: int = 300         # optional, range(min=1,max=604800), Number of seconds of inactivity after which an open story is considered timed out and finalized.
    kindex: KindexConfig = null              # optional, Optional Kindex noteworthiness filter configuration. Required if a kindex sink is configured; ignored otherwise.

class ConfigError:
    """Custom exception wrapping YAML parse errors or Pydantic validation errors with file path context for clear fail-fast error messages at startup."""
    file_path: str                           # required, The filesystem path of the config file that caused the error.
    message: str                             # required, Human-readable error message describing what went wrong.
    line_number: int = -1                    # optional, Line number in the YAML file where the error occurred, if available. -1 if not applicable.
    validation_errors: list = []             # optional, List of individual validation error messages from Pydantic, if the error was a validation failure. Empty list for YAML parse errors.

def load_config(
    path: FilePath,
) -> ChroniclerConfig:
    """
    Load and validate a Chronicler configuration from a YAML file. Reads the file at the given path, parses it with yaml.safe_load(), validates the resulting dict against the ChroniclerConfig Pydantic model, runs cross-field validators (e.g., kindex sink requires kindex config section), and returns a frozen typed config object. Fails fast with ConfigError on any failure: file not found, YAML syntax error, or Pydantic validation error.

    Preconditions:
      - path must point to an existing, readable file on disk.
      - The file must contain valid YAML syntax.
      - The YAML content must conform to the ChroniclerConfig schema.

    Postconditions:
      - The returned ChroniclerConfig is fully validated and frozen (immutable).
      - All field-level constraints (ranges, lengths, patterns) have been verified.
      - Cross-field consistency checks have passed (e.g., kindex sink implies kindex config section is present).
      - All correlation rule names within the config are unique.
      - No source or sink port conflicts exist within the same bind_address.
      - All regex patterns in match_conditions with is_regex=true are valid compiled regexes.

    Errors:
      - file_not_found (ConfigError): The specified config file path does not exist on disk.
          message: Configuration file not found: {path}
      - file_not_readable (ConfigError): The config file exists but cannot be read due to permission errors.
          message: Configuration file not readable: {path}
      - yaml_parse_error (ConfigError): The file contains invalid YAML syntax that yaml.safe_load() cannot parse.
          message: YAML parse error in {path} at line {line_number}: {detail}
      - empty_config (ConfigError): The YAML file is empty or parses to None/null.
          message: Configuration file is empty: {path}
      - validation_error (ConfigError): The parsed YAML structure does not conform to ChroniclerConfig schema: missing required fields, wrong types, constraint violations.
          message: Configuration validation failed for {path}: {error_count} error(s)
      - kindex_sink_without_kindex_config (ConfigError): A kindex sink is configured in the sinks list but the top-level kindex config section is missing or null.
          message: Kindex sink requires a 'kindex' configuration section in {path}
      - duplicate_rule_names (ConfigError): Two or more correlation rules share the same name.
          message: Duplicate correlation rule name '{name}' in {path}
      - invalid_regex_pattern (ConfigError): A match condition with is_regex=true contains a pattern that is not a valid regular expression.
          message: Invalid regex pattern in rule '{rule_name}', condition field '{field}': {detail}

    Side effects: none
    Idempotent: yes
    """
    ...

def validate_config_dict(
    data: dict,
    source_label: str = in-memory,
) -> ChroniclerConfig:
    """
    Validate a pre-parsed configuration dictionary against the ChroniclerConfig Pydantic model without reading from disk. Useful for testing and programmatic config construction. Pure function with no I/O side effects.

    Preconditions:
      - data must be a non-null dictionary.

    Postconditions:
      - The returned ChroniclerConfig is fully validated and frozen (immutable).
      - All field-level and cross-field validations have passed.
      - All correlation rule names are unique.
      - All regex patterns in match_conditions with is_regex=true are valid compiled regexes.

    Errors:
      - validation_error (ConfigError): The dictionary does not conform to ChroniclerConfig schema.
          message: Configuration validation failed for {source_label}: {error_count} error(s)
      - kindex_sink_without_kindex_config (ConfigError): A kindex sink is configured but the kindex config section is missing.
          message: Kindex sink requires a 'kindex' configuration section in {source_label}
      - duplicate_rule_names (ConfigError): Two or more correlation rules share the same name.
          message: Duplicate correlation rule name '{name}' in {source_label}
      - invalid_regex_pattern (ConfigError): A match condition with is_regex=true has an invalid regex pattern.
          message: Invalid regex pattern in rule '{rule_name}', condition field '{field}': {detail}

    Side effects: none
    Idempotent: yes
    """
    ...

# ── REQUIRED EXPORTS ──────────────────────────────────
# Your implementation module MUST export ALL of these names
# with EXACTLY these spellings. Tests import them by name.
# __all__ = ['SourceType', 'SinkType', 'WebhookSourceConfig', 'OtlpSourceConfig', 'FileSourceConfig', 'SentinelSourceConfig', 'SourceConfig', 'StigmergySinkConfig', 'ApprenticeSinkConfig', 'DiskSinkConfig', 'KindexSinkConfig', 'SinkConfig', 'MatchCondition', 'CorrelationRuleConfig', 'KindexConfig', 'ChroniclerConfig', 'ConfigError', 'load_config', 'validate_config_dict']

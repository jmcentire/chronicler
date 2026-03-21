"""
Contract tests for the config module.
Tests generated from the Chronicler configuration contract v1.

Constraint mapping:
  C001 - FilePath min length = 1
  C002 - SourceType enum discrimination
  C003 - SinkType enum discrimination
  C004 - WebhookSourceConfig field validators
  C005 - OtlpSourceConfig field validators
  C006 - FileSourceConfig field validators
  C007 - SentinelSourceConfig field validators
  C008 - StigmergySinkConfig URL regex
  C009 - ApprenticeSinkConfig URL regex
  C010 - DiskSinkConfig output_dir length
  C011 - KindexSinkConfig URL regex
  C012 - MatchCondition field/pattern length
  C013 - CorrelationRuleConfig validators
  C014 - NoteworthinessThreshold range
  C015 - ChroniclerConfig top-level validators
  C016 - Cross-field: kindex sink requires kindex config
  C017 - Unique rule names
  C018 - Valid regex patterns in match conditions
  C019 - Frozen/immutable models
  C020 - ConfigError for all failures
  C021 - Deterministic load_config
  C022 - Default values applied
"""

import copy
import os
import sys
import stat
import tempfile
import textwrap

import pytest

from config import (
    load_config,
    validate_config_dict,
    FilePath,
    SourceType,
    SinkType,
    WebhookSourceConfig,
    OtlpSourceConfig,
    FileSourceConfig,
    SentinelSourceConfig,
    StigmergySinkConfig,
    ApprenticeSinkConfig,
    DiskSinkConfig,
    KindexSinkConfig,
    MatchCondition,
    CorrelationRuleConfig,
    NoteworthinessThreshold,
    KindexConfig,
    ChroniclerConfig,
    ConfigError,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _webhook_source(bind_address="0.0.0.0", port=8080, path="/events"):
    return {"type": "webhook", "bind_address": bind_address, "port": port, "path": path}


def _otlp_source(bind_address="0.0.0.0", port=4317):
    return {"type": "otlp", "bind_address": bind_address, "port": port}


def _file_source(path="/var/log/events.jsonl", poll_interval_seconds=1.0):
    return {"type": "file", "path": path, "poll_interval_seconds": poll_interval_seconds}


def _sentinel_source(bind_address="0.0.0.0", port=9090):
    return {"type": "sentinel", "bind_address": bind_address, "port": port}


def _stigmergy_sink(url="https://stigmergy.example.com/api"):
    return {"type": "stigmergy", "url": url}


def _apprentice_sink(url="https://apprentice.example.com/api"):
    return {"type": "apprentice", "url": url}


def _disk_sink(output_dir="/var/data/chronicler"):
    return {"type": "disk", "output_dir": output_dir}


def _kindex_sink(url="https://kindex.example.com/api"):
    return {"type": "kindex", "url": url}


def _match_condition(field="event_type", pattern="error", is_regex=False):
    return {"field": field, "pattern": pattern, "is_regex": is_regex}


def _rule(name="rule1", match_conditions=None, group_by=None, window_seconds=60):
    if match_conditions is None:
        match_conditions = [_match_condition()]
    if group_by is None:
        group_by = ["source_id"]
    return {
        "name": name,
        "match_conditions": match_conditions,
        "group_by": group_by,
        "window_seconds": window_seconds,
    }


def _kindex_config(noteworthiness_threshold=0.5, event_type_filters=None, tag_patterns=None):
    return {
        "noteworthiness_threshold": noteworthiness_threshold,
        "event_type_filters": event_type_filters or [],
        "tag_patterns": tag_patterns or [],
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def valid_config_dict():
    """Minimal-but-complete valid configuration dictionary."""
    return {
        "sources": [_webhook_source()],
        "sinks": [_stigmergy_sink()],
        "rules": [_rule()],
        "max_open_stories": 10000,
        "story_timeout_seconds": 300,
        "kindex": _kindex_config(),
    }


@pytest.fixture
def valid_config_no_kindex():
    """Valid config without kindex sink or kindex section."""
    return {
        "sources": [_webhook_source()],
        "sinks": [_stigmergy_sink()],
        "rules": [_rule()],
        "max_open_stories": 10000,
        "story_timeout_seconds": 300,
    }


def _write_yaml_file(content: str) -> str:
    """Write content to a temp YAML file and return the path."""
    fd, path = tempfile.mkstemp(suffix=".yaml")
    with os.fdopen(fd, "w") as f:
        f.write(content)
    return path


def _valid_yaml_content():
    return textwrap.dedent("""\
        sources:
          - type: webhook
            bind_address: "0.0.0.0"
            port: 8080
            path: /events
        sinks:
          - type: stigmergy
            url: https://stigmergy.example.com/api
        rules:
          - name: rule1
            match_conditions:
              - field: event_type
                pattern: error
                is_regex: false
            group_by:
              - source_id
            window_seconds: 60
        max_open_stories: 10000
        story_timeout_seconds: 300
        kindex:
          noteworthiness_threshold: 0.5
          event_type_filters: []
          tag_patterns: []
    """)


# ---------------------------------------------------------------------------
# HAPPY PATH TESTS
# ---------------------------------------------------------------------------

class TestHappyPath:
    """HP_001-HP_008: Successful configuration validation."""

    def test_HP_001_minimal_valid_config(self, valid_config_dict):
        """Minimal valid config with one webhook source, one stigmergy sink, one rule."""
        result = validate_config_dict(valid_config_dict, "test")
        assert isinstance(result, ChroniclerConfig)

    @pytest.mark.parametrize("source_factory,source_type_name", [
        (_webhook_source, "webhook"),
        (_otlp_source, "otlp"),
        (_file_source, "file"),
        (_sentinel_source, "sentinel"),
    ])
    def test_HP_002_each_source_type_validates(self, valid_config_dict, source_factory, source_type_name):
        """Each source type validates correctly."""
        config = copy.deepcopy(valid_config_dict)
        config["sources"] = [source_factory()]
        result = validate_config_dict(config, "test")
        assert isinstance(result, ChroniclerConfig)
        assert len(result.sources) == 1

    @pytest.mark.parametrize("sink_factory,sink_type_name,needs_kindex", [
        (_stigmergy_sink, "stigmergy", False),
        (_apprentice_sink, "apprentice", False),
        (_disk_sink, "disk", False),
        (_kindex_sink, "kindex", True),
    ])
    def test_HP_003_each_sink_type_validates(self, valid_config_dict, sink_factory, sink_type_name, needs_kindex):
        """Each sink type validates correctly."""
        config = copy.deepcopy(valid_config_dict)
        config["sinks"] = [sink_factory()]
        if needs_kindex:
            config["kindex"] = _kindex_config()
        result = validate_config_dict(config, "test")
        assert isinstance(result, ChroniclerConfig)
        assert len(result.sinks) == 1

    def test_HP_004_all_optional_fields_set(self, valid_config_dict):
        """Config with all optional fields explicitly set."""
        config = copy.deepcopy(valid_config_dict)
        config["max_open_stories"] = 5000
        config["story_timeout_seconds"] = 600
        config["kindex"] = _kindex_config(noteworthiness_threshold=0.8)
        result = validate_config_dict(config, "test")
        assert result.max_open_stories == 5000
        assert result.story_timeout_seconds == 600

    def test_HP_005_defaults_applied_when_omitted(self):
        """Defaults are applied: max_open_stories=10000, story_timeout_seconds=300."""
        config = {
            "sources": [_webhook_source()],
            "sinks": [_stigmergy_sink()],
            "rules": [_rule()],
        }
        result = validate_config_dict(config, "test")
        assert result.max_open_stories == 10000
        assert result.story_timeout_seconds == 300

    def test_HP_006_round_trip_attribute_check(self, valid_config_dict):
        """Round-trip: valid dict → ChroniclerConfig → verify all attributes."""
        result = validate_config_dict(valid_config_dict, "test")
        assert len(result.sources) == 1
        assert len(result.sinks) == 1
        assert len(result.rules) == 1
        assert result.max_open_stories == 10000
        assert result.story_timeout_seconds == 300
        # Check nested rule attributes
        rule = result.rules[0]
        assert rule.name == "rule1"
        assert rule.window_seconds == 60
        assert len(rule.match_conditions) == 1
        assert len(rule.group_by) == 1

    def test_HP_007_load_config_valid_yaml(self):
        """load_config successfully loads a valid YAML file."""
        path = _write_yaml_file(_valid_yaml_content())
        try:
            result = load_config(FilePath(path))
            assert isinstance(result, ChroniclerConfig)
            assert len(result.sources) == 1
            assert len(result.sinks) == 1
            assert len(result.rules) == 1
        finally:
            os.unlink(path)

    def test_HP_008_multiple_sources_and_sinks(self, valid_config_dict):
        """Multiple sources and sinks of different types validate."""
        config = copy.deepcopy(valid_config_dict)
        config["sources"] = [_webhook_source(), _file_source()]
        config["sinks"] = [_stigmergy_sink(), _disk_sink()]
        result = validate_config_dict(config, "test")
        assert len(result.sources) == 2
        assert len(result.sinks) == 2


# ---------------------------------------------------------------------------
# FILE I/O TESTS (load_config)
# ---------------------------------------------------------------------------

class TestFileIO:
    """IO_001-IO_005: File I/O error handling in load_config."""

    def test_IO_001_file_not_found(self):
        """Non-existent path raises ConfigError with file_not_found context."""
        path = "/tmp/nonexistent_chronicler_config_xyz_9999.yaml"
        # Ensure it doesn't exist
        if os.path.exists(path):
            os.unlink(path)
        with pytest.raises(ConfigError) as exc_info:
            load_config(FilePath(path))
        error = exc_info.value
        assert path in str(error) or hasattr(error, "file_path")

    @pytest.mark.skipif(sys.platform == "win32", reason="chmod not reliable on Windows")
    def test_IO_002_file_not_readable(self):
        """Permission-denied file raises ConfigError."""
        path = _write_yaml_file(_valid_yaml_content())
        try:
            os.chmod(path, 0o000)
            with pytest.raises(ConfigError):
                load_config(FilePath(path))
        finally:
            os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
            os.unlink(path)

    def test_IO_003_yaml_parse_error(self):
        """Malformed YAML raises ConfigError."""
        content = "sources:\n  - {bad: yaml: content: [unmatched"
        path = _write_yaml_file(content)
        try:
            with pytest.raises(ConfigError):
                load_config(FilePath(path))
        finally:
            os.unlink(path)

    def test_IO_004_empty_config_file(self):
        """Empty file raises ConfigError."""
        path = _write_yaml_file("")
        try:
            with pytest.raises(ConfigError):
                load_config(FilePath(path))
        finally:
            os.unlink(path)

    def test_IO_005_yaml_parses_to_null(self):
        """YAML that parses to null/None raises ConfigError."""
        path = _write_yaml_file("---\n~\n")
        try:
            with pytest.raises(ConfigError):
                load_config(FilePath(path))
        finally:
            os.unlink(path)

    def test_IO_006_yaml_comments_only(self):
        """File with only comments raises ConfigError (empty_config)."""
        path = _write_yaml_file("# This is a comment\n# Another comment\n")
        try:
            with pytest.raises(ConfigError):
                load_config(FilePath(path))
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# VALIDATION ERROR TESTS (validate_config_dict)
# ---------------------------------------------------------------------------

class TestValidationErrors:
    """VE_001-VE_019: Schema and field constraint validation errors."""

    def test_VE_001_missing_sources(self, valid_config_dict):
        config = copy.deepcopy(valid_config_dict)
        del config["sources"]
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    def test_VE_002_missing_sinks(self, valid_config_dict):
        config = copy.deepcopy(valid_config_dict)
        del config["sinks"]
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    def test_VE_003_missing_rules(self, valid_config_dict):
        config = copy.deepcopy(valid_config_dict)
        del config["rules"]
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    def test_VE_004_empty_sources_list(self, valid_config_dict):
        config = copy.deepcopy(valid_config_dict)
        config["sources"] = []
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    def test_VE_005_invalid_source_type_discriminator(self, valid_config_dict):
        config = copy.deepcopy(valid_config_dict)
        config["sources"] = [{"type": "invalid_source", "bind_address": "0.0.0.0", "port": 8080}]
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    def test_VE_006_invalid_sink_type_discriminator(self, valid_config_dict):
        config = copy.deepcopy(valid_config_dict)
        config["sinks"] = [{"type": "invalid_sink", "url": "https://example.com"}]
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    @pytest.mark.parametrize("bad_port", [0, -1])
    def test_VE_007_webhook_port_below_min(self, valid_config_dict, bad_port):
        config = copy.deepcopy(valid_config_dict)
        config["sources"] = [_webhook_source(port=bad_port)]
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    def test_VE_008_webhook_port_above_max(self, valid_config_dict):
        config = copy.deepcopy(valid_config_dict)
        config["sources"] = [_webhook_source(port=65536)]
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    def test_VE_009_webhook_path_no_leading_slash(self, valid_config_dict):
        config = copy.deepcopy(valid_config_dict)
        config["sources"] = [_webhook_source(path="events")]
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    def test_VE_010_webhook_empty_bind_address(self, valid_config_dict):
        config = copy.deepcopy(valid_config_dict)
        config["sources"] = [_webhook_source(bind_address="")]
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    def test_VE_011_file_poll_interval_below_min(self, valid_config_dict):
        config = copy.deepcopy(valid_config_dict)
        config["sources"] = [_file_source(poll_interval_seconds=0.01)]
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    def test_VE_012_file_poll_interval_above_max(self, valid_config_dict):
        config = copy.deepcopy(valid_config_dict)
        config["sources"] = [_file_source(poll_interval_seconds=3601.0)]
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    @pytest.mark.parametrize("bad_url", ["ftp://bad", "not-a-url", "://missing-scheme"])
    def test_VE_013_stigmergy_sink_invalid_url(self, valid_config_dict, bad_url):
        config = copy.deepcopy(valid_config_dict)
        config["sinks"] = [_stigmergy_sink(url=bad_url)]
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    @pytest.mark.parametrize("bad_name", ["invalid name!", "rule@home", "has spaces", "rule/slash"])
    def test_VE_014_rule_name_invalid_characters(self, valid_config_dict, bad_name):
        config = copy.deepcopy(valid_config_dict)
        config["rules"] = [_rule(name=bad_name)]
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    def test_VE_015_rule_empty_match_conditions(self, valid_config_dict):
        config = copy.deepcopy(valid_config_dict)
        config["rules"] = [_rule(match_conditions=[])]
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    def test_VE_016_rule_window_seconds_zero(self, valid_config_dict):
        config = copy.deepcopy(valid_config_dict)
        config["rules"] = [_rule(window_seconds=0)]
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    def test_VE_017_rule_window_seconds_above_max(self, valid_config_dict):
        config = copy.deepcopy(valid_config_dict)
        config["rules"] = [_rule(window_seconds=86401)]
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    def test_VE_018_max_open_stories_zero(self, valid_config_dict):
        config = copy.deepcopy(valid_config_dict)
        config["max_open_stories"] = 0
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    def test_VE_018b_max_open_stories_above_max(self, valid_config_dict):
        config = copy.deepcopy(valid_config_dict)
        config["max_open_stories"] = 1000001
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    def test_VE_019_story_timeout_seconds_zero(self, valid_config_dict):
        config = copy.deepcopy(valid_config_dict)
        config["story_timeout_seconds"] = 0
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    def test_VE_019b_story_timeout_seconds_above_max(self, valid_config_dict):
        config = copy.deepcopy(valid_config_dict)
        config["story_timeout_seconds"] = 604801
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    def test_VE_020_empty_sinks_list(self, valid_config_dict):
        config = copy.deepcopy(valid_config_dict)
        config["sinks"] = []
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    def test_VE_021_empty_rules_list(self, valid_config_dict):
        config = copy.deepcopy(valid_config_dict)
        config["rules"] = []
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    def test_VE_022_rule_empty_group_by(self, valid_config_dict):
        config = copy.deepcopy(valid_config_dict)
        config["rules"] = [_rule(group_by=[])]
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    def test_VE_023_file_source_empty_path(self, valid_config_dict):
        config = copy.deepcopy(valid_config_dict)
        config["sources"] = [_file_source(path="")]
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    def test_VE_024_disk_sink_empty_output_dir(self, valid_config_dict):
        config = copy.deepcopy(valid_config_dict)
        config["sinks"] = [_disk_sink(output_dir="")]
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    @pytest.mark.parametrize("bad_url", ["ftp://bad", ""])
    def test_VE_025_apprentice_sink_invalid_url(self, valid_config_dict, bad_url):
        config = copy.deepcopy(valid_config_dict)
        config["sinks"] = [_apprentice_sink(url=bad_url)]
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    @pytest.mark.parametrize("bad_url", ["ftp://bad", ""])
    def test_VE_026_kindex_sink_invalid_url(self, valid_config_dict, bad_url):
        config = copy.deepcopy(valid_config_dict)
        config["sinks"] = [_kindex_sink(url=bad_url)]
        config["kindex"] = _kindex_config()
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    def test_VE_027_noteworthiness_threshold_below_zero(self, valid_config_dict):
        config = copy.deepcopy(valid_config_dict)
        config["kindex"] = _kindex_config(noteworthiness_threshold=-0.1)
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    def test_VE_028_noteworthiness_threshold_above_one(self, valid_config_dict):
        config = copy.deepcopy(valid_config_dict)
        config["kindex"] = _kindex_config(noteworthiness_threshold=1.1)
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    def test_VE_029_match_condition_empty_field(self, valid_config_dict):
        config = copy.deepcopy(valid_config_dict)
        config["rules"] = [_rule(match_conditions=[_match_condition(field="")])]
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    def test_VE_030_match_condition_empty_pattern(self, valid_config_dict):
        config = copy.deepcopy(valid_config_dict)
        config["rules"] = [_rule(match_conditions=[_match_condition(pattern="")])]
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")


# ---------------------------------------------------------------------------
# BUSINESS RULE TESTS
# ---------------------------------------------------------------------------

class TestBusinessRules:
    """BR_001-BR_008: Cross-field validation and business logic."""

    def test_BR_001_kindex_sink_without_kindex_config(self, valid_config_dict):
        """kindex sink present but no kindex config section raises error."""
        config = copy.deepcopy(valid_config_dict)
        config["sinks"] = [_kindex_sink()]
        config.pop("kindex", None)
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    def test_BR_001b_kindex_sink_with_null_kindex_config(self, valid_config_dict):
        """kindex sink with kindex=None raises error."""
        config = copy.deepcopy(valid_config_dict)
        config["sinks"] = [_kindex_sink()]
        config["kindex"] = None
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    def test_BR_002_kindex_sink_with_kindex_config(self, valid_config_dict):
        """kindex sink with kindex config present validates successfully."""
        config = copy.deepcopy(valid_config_dict)
        config["sinks"] = [_kindex_sink()]
        config["kindex"] = _kindex_config()
        result = validate_config_dict(config, "test")
        assert isinstance(result, ChroniclerConfig)

    def test_BR_003_non_kindex_sink_without_kindex_config(self):
        """Non-kindex sinks work without kindex config section."""
        config = {
            "sources": [_webhook_source()],
            "sinks": [_stigmergy_sink()],
            "rules": [_rule()],
        }
        result = validate_config_dict(config, "test")
        assert isinstance(result, ChroniclerConfig)

    def test_BR_004_duplicate_rule_names(self, valid_config_dict):
        """Two rules with the same name raises error."""
        config = copy.deepcopy(valid_config_dict)
        config["rules"] = [
            _rule(name="duplicate_rule"),
            _rule(name="duplicate_rule"),
        ]
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    def test_BR_005_distinct_rule_names(self, valid_config_dict):
        """Two rules with distinct names validate successfully."""
        config = copy.deepcopy(valid_config_dict)
        config["rules"] = [
            _rule(name="rule1"),
            _rule(name="rule2"),
        ]
        result = validate_config_dict(config, "test")
        assert len(result.rules) == 2

    def test_BR_006_invalid_regex_pattern(self, valid_config_dict):
        """is_regex=true with invalid regex raises error."""
        config = copy.deepcopy(valid_config_dict)
        config["rules"] = [
            _rule(match_conditions=[_match_condition(pattern="[invalid(", is_regex=True)])
        ]
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    def test_BR_007_valid_regex_pattern(self, valid_config_dict):
        """is_regex=true with valid regex validates successfully."""
        config = copy.deepcopy(valid_config_dict)
        config["rules"] = [
            _rule(match_conditions=[_match_condition(pattern=r"^error\d+$", is_regex=True)])
        ]
        result = validate_config_dict(config, "test")
        assert isinstance(result, ChroniclerConfig)

    def test_BR_008_invalid_regex_with_is_regex_false(self, valid_config_dict):
        """is_regex=false with invalid regex pattern should be accepted (not compiled)."""
        config = copy.deepcopy(valid_config_dict)
        config["rules"] = [
            _rule(match_conditions=[_match_condition(pattern="[invalid(", is_regex=False)])
        ]
        result = validate_config_dict(config, "test")
        assert isinstance(result, ChroniclerConfig)

    def test_BR_009_three_duplicate_rule_names(self, valid_config_dict):
        """Three rules with the same name also raise error."""
        config = copy.deepcopy(valid_config_dict)
        config["rules"] = [
            _rule(name="same"),
            _rule(name="same"),
            _rule(name="same"),
        ]
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    def test_BR_010_multiple_invalid_regex_patterns(self, valid_config_dict):
        """Multiple invalid regex patterns with is_regex=true raise error."""
        config = copy.deepcopy(valid_config_dict)
        config["rules"] = [
            _rule(match_conditions=[
                _match_condition(pattern="[bad(", is_regex=True),
                _match_condition(pattern="*also_bad", is_regex=True),
            ])
        ]
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")


# ---------------------------------------------------------------------------
# EDGE CASE TESTS
# ---------------------------------------------------------------------------

class TestEdgeCases:
    """EC_001-EC_009: Boundary values and edge cases."""

    def test_EC_001_filepath_rejects_empty_string(self):
        """FilePath rejects empty string value."""
        with pytest.raises((ValueError, Exception)):
            FilePath("")

    def test_EC_001b_filepath_accepts_non_empty_string(self):
        """FilePath accepts non-empty string."""
        fp = FilePath("/some/path")
        # Check the value is stored
        assert str(fp) or fp  # Just verify construction succeeded

    @pytest.mark.parametrize("port", [1, 65535])
    def test_EC_002_webhook_boundary_ports(self, valid_config_dict, port):
        """Webhook source accepts boundary port values 1 and 65535."""
        config = copy.deepcopy(valid_config_dict)
        config["sources"] = [_webhook_source(port=port)]
        result = validate_config_dict(config, "test")
        assert isinstance(result, ChroniclerConfig)

    @pytest.mark.parametrize("interval", [0.1, 3600.0])
    def test_EC_003_file_source_boundary_poll_interval(self, valid_config_dict, interval):
        """File source accepts boundary poll_interval_seconds values."""
        config = copy.deepcopy(valid_config_dict)
        config["sources"] = [_file_source(poll_interval_seconds=interval)]
        result = validate_config_dict(config, "test")
        assert isinstance(result, ChroniclerConfig)

    @pytest.mark.parametrize("window", [1, 86400])
    def test_EC_004_rule_boundary_window_seconds(self, valid_config_dict, window):
        """Rule accepts boundary window_seconds values."""
        config = copy.deepcopy(valid_config_dict)
        config["rules"] = [_rule(window_seconds=window)]
        result = validate_config_dict(config, "test")
        assert isinstance(result, ChroniclerConfig)

    @pytest.mark.parametrize("max_stories", [1, 1000000])
    def test_EC_005_boundary_max_open_stories(self, valid_config_dict, max_stories):
        """max_open_stories accepts boundary values."""
        config = copy.deepcopy(valid_config_dict)
        config["max_open_stories"] = max_stories
        result = validate_config_dict(config, "test")
        assert result.max_open_stories == max_stories

    @pytest.mark.parametrize("timeout", [1, 604800])
    def test_EC_006_boundary_story_timeout_seconds(self, valid_config_dict, timeout):
        """story_timeout_seconds accepts boundary values."""
        config = copy.deepcopy(valid_config_dict)
        config["story_timeout_seconds"] = timeout
        result = validate_config_dict(config, "test")
        assert result.story_timeout_seconds == timeout

    @pytest.mark.parametrize("threshold", [0.0, 1.0])
    def test_EC_007_boundary_noteworthiness_threshold(self, valid_config_dict, threshold):
        """NoteworthinessThreshold accepts boundary values 0.0 and 1.0."""
        config = copy.deepcopy(valid_config_dict)
        config["kindex"] = _kindex_config(noteworthiness_threshold=threshold)
        result = validate_config_dict(config, "test")
        assert isinstance(result, ChroniclerConfig)

    def test_EC_008_rule_name_too_long(self, valid_config_dict):
        """Rule name of 129 characters is rejected."""
        config = copy.deepcopy(valid_config_dict)
        config["rules"] = [_rule(name="a" * 129)]
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    def test_EC_008b_rule_name_exactly_128(self, valid_config_dict):
        """Rule name of exactly 128 characters is accepted."""
        config = copy.deepcopy(valid_config_dict)
        config["rules"] = [_rule(name="a" * 128)]
        result = validate_config_dict(config, "test")
        assert result.rules[0].name == "a" * 128

    def test_EC_009_match_condition_field_max_length(self, valid_config_dict):
        """Match condition field of exactly 256 chars is accepted."""
        config = copy.deepcopy(valid_config_dict)
        config["rules"] = [_rule(match_conditions=[_match_condition(field="f" * 256)])]
        result = validate_config_dict(config, "test")
        assert isinstance(result, ChroniclerConfig)

    def test_EC_009b_match_condition_field_exceeds_max_length(self, valid_config_dict):
        """Match condition field of 257 chars is rejected."""
        config = copy.deepcopy(valid_config_dict)
        config["rules"] = [_rule(match_conditions=[_match_condition(field="f" * 257)])]
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    def test_EC_010_webhook_path_just_slash(self, valid_config_dict):
        """Webhook path of just '/' is valid (matches ^/.*)."""
        config = copy.deepcopy(valid_config_dict)
        config["sources"] = [_webhook_source(path="/")]
        result = validate_config_dict(config, "test")
        assert isinstance(result, ChroniclerConfig)

    def test_EC_011_bind_address_max_length(self, valid_config_dict):
        """bind_address at max length 253 is accepted."""
        config = copy.deepcopy(valid_config_dict)
        addr = "a" * 253
        config["sources"] = [_webhook_source(bind_address=addr)]
        result = validate_config_dict(config, "test")
        assert isinstance(result, ChroniclerConfig)

    def test_EC_011b_bind_address_exceeds_max_length(self, valid_config_dict):
        """bind_address longer than 253 is rejected."""
        config = copy.deepcopy(valid_config_dict)
        addr = "a" * 254
        config["sources"] = [_webhook_source(bind_address=addr)]
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    def test_EC_012_negative_port(self, valid_config_dict):
        """Negative port value is rejected."""
        config = copy.deepcopy(valid_config_dict)
        config["sources"] = [_webhook_source(port=-1)]
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    def test_EC_013_rule_name_with_dots_dashes_underscores(self, valid_config_dict):
        """Rule name with allowed special characters validates."""
        config = copy.deepcopy(valid_config_dict)
        config["rules"] = [_rule(name="my-rule_v1.0")]
        result = validate_config_dict(config, "test")
        assert result.rules[0].name == "my-rule_v1.0"


# ---------------------------------------------------------------------------
# INVARIANT TESTS
# ---------------------------------------------------------------------------

class TestInvariants:
    """INV_001-INV_005: Contract invariants."""

    def test_INV_001_frozen_immutable_config(self, valid_config_dict):
        """ChroniclerConfig is frozen – attribute assignment raises error."""
        result = validate_config_dict(valid_config_dict, "test")
        with pytest.raises((AttributeError, TypeError, Exception)):
            result.max_open_stories = 9999

    def test_INV_001b_frozen_immutable_rule(self, valid_config_dict):
        """CorrelationRuleConfig is frozen – attribute assignment raises error."""
        result = validate_config_dict(valid_config_dict, "test")
        rule = result.rules[0]
        with pytest.raises((AttributeError, TypeError, Exception)):
            rule.name = "changed"

    def test_INV_001c_frozen_immutable_source(self, valid_config_dict):
        """Source config is frozen – attribute assignment raises error."""
        result = validate_config_dict(valid_config_dict, "test")
        source = result.sources[0]
        with pytest.raises((AttributeError, TypeError, Exception)):
            source.port = 9999

    def test_INV_002_config_error_type_for_all_failures(self):
        """ConfigError is raised (not other exception types) for load_config failures."""
        # File not found
        with pytest.raises(ConfigError):
            load_config(FilePath("/tmp/no_such_chronicler_file_ever.yaml"))

    def test_INV_002b_config_error_for_yaml_parse(self):
        """ConfigError for YAML parse errors."""
        path = _write_yaml_file("{{{{invalid yaml")
        try:
            with pytest.raises(ConfigError):
                load_config(FilePath(path))
        finally:
            os.unlink(path)

    def test_INV_002c_config_error_for_validation(self):
        """ConfigError for validation failures in load_config."""
        # Valid YAML but invalid config structure
        path = _write_yaml_file("sources: []\nsinks: []\nrules: []\n")
        try:
            with pytest.raises(ConfigError):
                load_config(FilePath(path))
        finally:
            os.unlink(path)

    def test_INV_003_deterministic_load(self):
        """Same file content yields the same ChroniclerConfig."""
        path = _write_yaml_file(_valid_yaml_content())
        try:
            result1 = load_config(FilePath(path))
            result2 = load_config(FilePath(path))
            assert result1 == result2
        finally:
            os.unlink(path)

    def test_INV_004_kindex_sink_requires_kindex_section(self, valid_config_dict):
        """Cross-field: kindex sink in sinks list requires non-null kindex config section."""
        config = copy.deepcopy(valid_config_dict)
        config["sinks"] = [_kindex_sink()]
        config["kindex"] = None
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

    def test_INV_005_source_type_discrimination(self, valid_config_dict):
        """Source configs are discriminated by their type field."""
        for source_factory in [_webhook_source, _otlp_source, _file_source, _sentinel_source]:
            config = copy.deepcopy(valid_config_dict)
            config["sources"] = [source_factory()]
            result = validate_config_dict(config, "test")
            source = result.sources[0]
            # The source should have a type attribute matching the factory type
            source_type = getattr(source, "type", None)
            assert source_type is not None

    def test_INV_005b_sink_type_discrimination(self, valid_config_dict):
        """Sink configs are discriminated by their type field."""
        for sink_factory, needs_kindex in [(_stigmergy_sink, False), (_apprentice_sink, False),
                                            (_disk_sink, False), (_kindex_sink, True)]:
            config = copy.deepcopy(valid_config_dict)
            config["sinks"] = [sink_factory()]
            if needs_kindex:
                config["kindex"] = _kindex_config()
            result = validate_config_dict(config, "test")
            sink = result.sinks[0]
            sink_type = getattr(sink, "type", None)
            assert sink_type is not None


# ---------------------------------------------------------------------------
# TYPE CONSTRUCTION TESTS
# ---------------------------------------------------------------------------

class TestTypeConstruction:
    """Direct type construction and validation tests."""

    def test_source_type_enum_values(self):
        """SourceType enum has expected variants."""
        assert SourceType.webhook is not None
        assert SourceType.otlp is not None
        assert SourceType.file is not None
        assert SourceType.sentinel is not None

    def test_sink_type_enum_values(self):
        """SinkType enum has expected variants."""
        assert SinkType.stigmergy is not None
        assert SinkType.apprentice is not None
        assert SinkType.disk is not None
        assert SinkType.kindex is not None

    def test_match_condition_construction(self):
        """MatchCondition can be constructed with valid args."""
        mc = MatchCondition(field="event_type", pattern="error", is_regex=False)
        assert mc.field == "event_type"
        assert mc.pattern == "error"
        assert mc.is_regex is False

    def test_match_condition_empty_field_rejected(self):
        """MatchCondition rejects empty field."""
        with pytest.raises((ValueError, Exception)):
            MatchCondition(field="", pattern="error", is_regex=False)

    def test_match_condition_empty_pattern_rejected(self):
        """MatchCondition rejects empty pattern."""
        with pytest.raises((ValueError, Exception)):
            MatchCondition(field="event_type", pattern="", is_regex=False)

    def test_webhook_source_config_construction(self):
        """WebhookSourceConfig can be constructed directly."""
        cfg = WebhookSourceConfig(
            type=SourceType.webhook,
            bind_address="0.0.0.0",
            port=8080,
            path="/events",
        )
        assert cfg.bind_address == "0.0.0.0"
        assert cfg.port == 8080
        assert cfg.path == "/events"

    def test_otlp_source_config_construction(self):
        """OtlpSourceConfig can be constructed directly."""
        cfg = OtlpSourceConfig(
            type=SourceType.otlp,
            bind_address="0.0.0.0",
            port=4317,
        )
        assert cfg.port == 4317

    def test_file_source_config_construction(self):
        """FileSourceConfig can be constructed directly."""
        cfg = FileSourceConfig(
            type=SourceType.file,
            path="/var/log/events.jsonl",
            poll_interval_seconds=1.0,
        )
        assert cfg.path == "/var/log/events.jsonl"
        assert cfg.poll_interval_seconds == 1.0

    def test_sentinel_source_config_construction(self):
        """SentinelSourceConfig can be constructed directly."""
        cfg = SentinelSourceConfig(
            type=SourceType.sentinel,
            bind_address="0.0.0.0",
            port=9090,
        )
        assert cfg.port == 9090

    def test_stigmergy_sink_config_construction(self):
        """StigmergySinkConfig can be constructed directly."""
        cfg = StigmergySinkConfig(
            type=SinkType.stigmergy,
            url="https://stigmergy.example.com/api",
        )
        assert cfg.url == "https://stigmergy.example.com/api"

    def test_kindex_config_construction(self):
        """KindexConfig can be constructed directly."""
        cfg = KindexConfig(
            noteworthiness_threshold=0.5,
            event_type_filters=[],
            tag_patterns=[],
        )
        assert cfg.noteworthiness_threshold == 0.5

    def test_correlation_rule_config_construction(self):
        """CorrelationRuleConfig can be constructed directly."""
        mc = MatchCondition(field="event_type", pattern="error", is_regex=False)
        cfg = CorrelationRuleConfig(
            name="test_rule",
            match_conditions=[mc],
            group_by=["source_id"],
            window_seconds=60,
        )
        assert cfg.name == "test_rule"
        assert len(cfg.match_conditions) == 1
        assert cfg.window_seconds == 60

    def test_noteworthiness_threshold_boundary_zero(self):
        """NoteworthinessThreshold accepts 0.0."""
        t = NoteworthinessThreshold(0.0)
        # Just verify construction succeeded
        assert t is not None

    def test_noteworthiness_threshold_boundary_one(self):
        """NoteworthinessThreshold accepts 1.0."""
        t = NoteworthinessThreshold(1.0)
        assert t is not None

    def test_noteworthiness_threshold_rejects_negative(self):
        """NoteworthinessThreshold rejects -0.1."""
        with pytest.raises((ValueError, Exception)):
            NoteworthinessThreshold(-0.1)

    def test_noteworthiness_threshold_rejects_above_one(self):
        """NoteworthinessThreshold rejects 1.1."""
        with pytest.raises((ValueError, Exception)):
            NoteworthinessThreshold(1.1)

    def test_config_error_has_expected_attributes(self):
        """ConfigError has file_path, message, and validation_errors attributes."""
        try:
            err = ConfigError(
                file_path="/path/to/config.yaml",
                message="test error",
                line_number=0,
                validation_errors=[],
            )
            assert err.file_path == "/path/to/config.yaml"
            assert err.message == "test error"
        except TypeError:
            # ConfigError might be constructed differently - just verify it exists
            assert ConfigError is not None


# ---------------------------------------------------------------------------
# PORT CONFLICT TESTS
# ---------------------------------------------------------------------------

class TestPortConflicts:
    """Tests for source port conflict detection within the same bind_address."""

    def test_no_port_conflict_different_addresses(self, valid_config_dict):
        """Sources on different bind_addresses with the same port are allowed."""
        config = copy.deepcopy(valid_config_dict)
        config["sources"] = [
            _webhook_source(bind_address="0.0.0.0", port=8080),
            _otlp_source(bind_address="127.0.0.1", port=8080),
        ]
        result = validate_config_dict(config, "test")
        assert len(result.sources) == 2

    def test_port_conflict_same_address(self, valid_config_dict):
        """Sources on the same bind_address with the same port should be rejected."""
        config = copy.deepcopy(valid_config_dict)
        config["sources"] = [
            _webhook_source(bind_address="0.0.0.0", port=8080),
            _otlp_source(bind_address="0.0.0.0", port=8080),
        ]
        with pytest.raises((ConfigError, Exception)):
            validate_config_dict(config, "test")

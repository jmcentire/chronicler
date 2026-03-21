"""
Hidden adversarial acceptance tests for Configuration Loading component.

These tests target gaps in the visible test suite to catch implementations
that hardcode returns or take shortcuts based on visible test inputs.
"""
import os
import stat
import tempfile
import textwrap

import pytest
import yaml

from chronicler.config import (
    ConfigError,
    ChroniclerConfig,
    load_config,
    validate_config_dict,
)


# ── Helpers ──────────────────────────────────────────────────────────────

def _minimal_config(**overrides):
    """Return a minimal valid config dict, with optional overrides merged in."""
    base = {
        "sources": [
            {"type": "webhook", "bind_address": "0.0.0.0", "port": 8080, "path": "/events"}
        ],
        "sinks": [
            {"type": "stigmergy", "url": "http://stigmergy.local:9000"}
        ],
        "rules": [
            {
                "name": "default-rule",
                "match_conditions": [{"field": "event_type", "pattern": "alert", "is_regex": False}],
                "group_by": ["source_id"],
                "window_seconds": 60,
            }
        ],
    }
    base.update(overrides)
    return base


def _write_yaml_tempfile(content: str) -> str:
    """Write content to a temp YAML file, return its path."""
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False)
    f.write(content)
    f.close()
    return f.name


# ── Tests ────────────────────────────────────────────────────────────────

class TestGoodhartDuplicateRuleNames:
    def test_goodhart_three_duplicate_rule_names(self):
        """Three or more correlation rules with the same name should trigger duplicate_rule_names error, not just exactly two duplicates."""
        data = _minimal_config(rules=[
            {"name": "rule-alpha", "match_conditions": [{"field": "f", "pattern": "p", "is_regex": False}], "group_by": ["g"], "window_seconds": 10},
            {"name": "rule-alpha", "match_conditions": [{"field": "f", "pattern": "p", "is_regex": False}], "group_by": ["g"], "window_seconds": 20},
            {"name": "rule-alpha", "match_conditions": [{"field": "f", "pattern": "p", "is_regex": False}], "group_by": ["g"], "window_seconds": 30},
        ])
        with pytest.raises(ConfigError):
            validate_config_dict(data, "test")


class TestGoodhartPortConflicts:
    def test_goodhart_same_address_same_port_two_webhooks(self):
        """Port conflict detection: two sources on the same bind_address and port should be rejected."""
        data = _minimal_config(sources=[
            {"type": "webhook", "bind_address": "0.0.0.0", "port": 8080, "path": "/a"},
            {"type": "webhook", "bind_address": "0.0.0.0", "port": 8080, "path": "/b"},
        ])
        with pytest.raises(ConfigError):
            validate_config_dict(data, "test")

    def test_goodhart_cross_type_port_conflict(self):
        """Port conflict: webhook and sentinel sources on same bind_address and same port should be detected."""
        data = _minimal_config(sources=[
            {"type": "webhook", "bind_address": "0.0.0.0", "port": 8080, "path": "/hook"},
            {"type": "sentinel", "bind_address": "0.0.0.0", "port": 8080},
        ])
        with pytest.raises(ConfigError):
            validate_config_dict(data, "test")

    def test_goodhart_different_address_same_port_no_conflict(self):
        """Same port on different bind addresses should NOT conflict."""
        data = _minimal_config(sources=[
            {"type": "webhook", "bind_address": "127.0.0.1", "port": 8080, "path": "/a"},
            {"type": "webhook", "bind_address": "192.168.1.1", "port": 8080, "path": "/b"},
        ])
        result = validate_config_dict(data, "test")
        assert len(result.sources) == 2


class TestGoodhartNoteworthinessThresholdBoundaries:
    def test_goodhart_threshold_slightly_above_one(self):
        """NoteworthinessThreshold must reject values slightly above 1.0."""
        data = _minimal_config(
            sinks=[{"type": "kindex", "url": "http://kindex.local"}],
            kindex={"noteworthiness_threshold": 1.01, "event_type_filters": [], "tag_patterns": []},
        )
        with pytest.raises(ConfigError):
            validate_config_dict(data, "test")

    def test_goodhart_threshold_slightly_below_zero(self):
        """NoteworthinessThreshold must reject negative values."""
        data = _minimal_config(
            sinks=[{"type": "kindex", "url": "http://kindex.local"}],
            kindex={"noteworthiness_threshold": -0.01, "event_type_filters": [], "tag_patterns": []},
        )
        with pytest.raises(ConfigError):
            validate_config_dict(data, "test")


class TestGoodhartBindAddressLength:
    def test_goodhart_bind_address_max_253_accepted(self):
        """bind_address at exactly 253 characters should be accepted."""
        addr = "a" * 253
        data = _minimal_config(sources=[
            {"type": "webhook", "bind_address": addr, "port": 8080, "path": "/events"}
        ])
        result = validate_config_dict(data, "test")
        assert len(result.sources[0].bind_address) == 253

    def test_goodhart_bind_address_254_rejected(self):
        """bind_address at 254 characters (above max 253) should be rejected."""
        addr = "a" * 254
        data = _minimal_config(sources=[
            {"type": "webhook", "bind_address": addr, "port": 8080, "path": "/events"}
        ])
        with pytest.raises(ConfigError):
            validate_config_dict(data, "test")


class TestGoodhartRuleNameEdge:
    def test_goodhart_name_exactly_128_accepted(self):
        """Rule name at exactly 128 characters should be accepted."""
        name = "a" * 128
        data = _minimal_config(rules=[
            {"name": name, "match_conditions": [{"field": "f", "pattern": "p", "is_regex": False}], "group_by": ["g"], "window_seconds": 60}
        ])
        result = validate_config_dict(data, "test")
        assert result.rules[0].name == name

    def test_goodhart_name_single_char(self):
        """Rule name with only a single character should be accepted."""
        data = _minimal_config(rules=[
            {"name": "a", "match_conditions": [{"field": "f", "pattern": "p", "is_regex": False}], "group_by": ["g"], "window_seconds": 60}
        ])
        result = validate_config_dict(data, "test")
        assert result.rules[0].name == "a"

    def test_goodhart_name_with_dots_hyphens_underscores(self):
        """Rule name with dots, hyphens, and underscores should be accepted."""
        data = _minimal_config(rules=[
            {"name": "my-rule.v2_test", "match_conditions": [{"field": "f", "pattern": "p", "is_regex": False}], "group_by": ["g"], "window_seconds": 60}
        ])
        result = validate_config_dict(data, "test")
        assert result.rules[0].name == "my-rule.v2_test"

    def test_goodhart_name_with_spaces_rejected(self):
        """Rule name containing spaces should be rejected by the name regex."""
        data = _minimal_config(rules=[
            {"name": "my rule", "match_conditions": [{"field": "f", "pattern": "p", "is_regex": False}], "group_by": ["g"], "window_seconds": 60}
        ])
        with pytest.raises(ConfigError):
            validate_config_dict(data, "test")


class TestGoodhartMatchConditionValidation:
    def test_goodhart_empty_pattern_rejected(self):
        """MatchCondition pattern must not accept empty string."""
        data = _minimal_config(rules=[
            {"name": "r1", "match_conditions": [{"field": "f", "pattern": "", "is_regex": False}], "group_by": ["g"], "window_seconds": 60}
        ])
        with pytest.raises(ConfigError):
            validate_config_dict(data, "test")

    def test_goodhart_empty_group_by_rejected(self):
        """CorrelationRuleConfig group_by must have at least one element."""
        data = _minimal_config(rules=[
            {"name": "r1", "match_conditions": [{"field": "f", "pattern": "p", "is_regex": False}], "group_by": [], "window_seconds": 60}
        ])
        with pytest.raises(ConfigError):
            validate_config_dict(data, "test")

    def test_goodhart_single_char_field_accepted(self):
        """MatchCondition field at exactly 1 character should be accepted."""
        data = _minimal_config(rules=[
            {"name": "r1", "match_conditions": [{"field": "x", "pattern": "p", "is_regex": False}], "group_by": ["g"], "window_seconds": 60}
        ])
        result = validate_config_dict(data, "test")
        assert result.rules[0].match_conditions[0].field == "x"

    def test_goodhart_second_match_condition_invalid_regex(self):
        """Invalid regex in a non-first match condition should still be caught."""
        data = _minimal_config(rules=[
            {
                "name": "r1",
                "match_conditions": [
                    {"field": "f1", "pattern": "ok", "is_regex": False},
                    {"field": "f2", "pattern": "*invalid", "is_regex": True},
                ],
                "group_by": ["g"],
                "window_seconds": 60,
            }
        ])
        with pytest.raises(ConfigError):
            validate_config_dict(data, "test")

    def test_goodhart_complex_valid_regex_accepted(self):
        """Complex but valid regex with lookahead should be accepted."""
        data = _minimal_config(rules=[
            {
                "name": "r1",
                "match_conditions": [{"field": "msg", "pattern": "(?=.*error)(?=.*critical).*", "is_regex": True}],
                "group_by": ["g"],
                "window_seconds": 60,
            }
        ])
        result = validate_config_dict(data, "test")
        assert result.rules[0].match_conditions[0].is_regex is True


class TestGoodhartSinkValidation:
    def test_goodhart_disk_sink_empty_output_dir_rejected(self):
        """DiskSinkConfig should reject empty output_dir string."""
        data = _minimal_config(sinks=[{"type": "disk", "output_dir": ""}])
        with pytest.raises(ConfigError):
            validate_config_dict(data, "test")

    def test_goodhart_apprentice_ftp_url_rejected(self):
        """ApprenticeSinkConfig should reject non-http(s) URLs like ftp://."""
        data = _minimal_config(sinks=[{"type": "apprentice", "url": "ftp://example.com"}])
        with pytest.raises(ConfigError):
            validate_config_dict(data, "test")

    def test_goodhart_kindex_invalid_url_rejected(self):
        """KindexSinkConfig should reject URLs not matching ^https?://.+."""
        data = _minimal_config(
            sinks=[{"type": "kindex", "url": "not-a-url"}],
            kindex={"noteworthiness_threshold": 0.5, "event_type_filters": [], "tag_patterns": []},
        )
        with pytest.raises(ConfigError):
            validate_config_dict(data, "test")

    def test_goodhart_stigmergy_http_and_https_accepted(self):
        """Both http and https URLs should be accepted for stigmergy sink."""
        data = _minimal_config(sinks=[
            {"type": "stigmergy", "url": "http://stig.local:9000"},
            {"type": "stigmergy", "url": "https://stig.secure:9443"},
        ])
        result = validate_config_dict(data, "test")
        assert len(result.sinks) == 2


class TestGoodhartSourceTypeValidation:
    def test_goodhart_otlp_port_zero_rejected(self):
        """OtlpSourceConfig should reject port=0."""
        data = _minimal_config(sources=[
            {"type": "otlp", "bind_address": "0.0.0.0", "port": 0}
        ])
        with pytest.raises(ConfigError):
            validate_config_dict(data, "test")

    def test_goodhart_sentinel_port_zero_rejected(self):
        """SentinelSourceConfig should reject port=0."""
        data = _minimal_config(sources=[
            {"type": "sentinel", "bind_address": "0.0.0.0", "port": 0}
        ])
        with pytest.raises(ConfigError):
            validate_config_dict(data, "test")

    def test_goodhart_sentinel_empty_bind_address_rejected(self):
        """SentinelSourceConfig with empty bind_address should be rejected."""
        data = _minimal_config(sources=[
            {"type": "sentinel", "bind_address": "", "port": 8080}
        ])
        with pytest.raises(ConfigError):
            validate_config_dict(data, "test")

    def test_goodhart_otlp_empty_bind_address_rejected(self):
        """OtlpSourceConfig with empty bind_address should be rejected."""
        data = _minimal_config(sources=[
            {"type": "otlp", "bind_address": "", "port": 4317}
        ])
        with pytest.raises(ConfigError):
            validate_config_dict(data, "test")

    def test_goodhart_file_source_empty_path_rejected(self):
        """FileSourceConfig should reject empty path string."""
        data = _minimal_config(sources=[
            {"type": "file", "path": "", "poll_interval_seconds": 1.0}
        ])
        with pytest.raises(ConfigError):
            validate_config_dict(data, "test")

    def test_goodhart_otlp_boundary_ports(self):
        """OtlpSourceConfig should accept boundary port values 1 and 65535."""
        for port in [1, 65535]:
            data = _minimal_config(sources=[
                {"type": "otlp", "bind_address": "0.0.0.0", "port": port}
            ])
            result = validate_config_dict(data, "test")
            assert result.sources[0].port == port

    def test_goodhart_sentinel_boundary_port(self):
        """SentinelSourceConfig should accept port=1."""
        data = _minimal_config(sources=[
            {"type": "sentinel", "bind_address": "localhost", "port": 1}
        ])
        result = validate_config_dict(data, "test")
        assert result.sources[0].port == 1


class TestGoodhartFileSourcePollInterval:
    def test_goodhart_poll_interval_just_below_min_rejected(self):
        """FileSourceConfig poll_interval_seconds=0.09 should be rejected."""
        data = _minimal_config(sources=[
            {"type": "file", "path": "/var/log/events.jsonl", "poll_interval_seconds": 0.09}
        ])
        with pytest.raises(ConfigError):
            validate_config_dict(data, "test")


class TestGoodhartOnlyFileAndSentinelSources:
    def test_goodhart_file_and_sentinel_only(self):
        """Config with only file and sentinel sources should validate."""
        data = _minimal_config(sources=[
            {"type": "file", "path": "/var/log/events.jsonl", "poll_interval_seconds": 5.0},
            {"type": "sentinel", "bind_address": "0.0.0.0", "port": 9090},
        ])
        result = validate_config_dict(data, "test")
        assert len(result.sources) == 2


class TestGoodhartKindexConfigPresence:
    def test_goodhart_explicit_null_kindex_no_kindex_sink(self):
        """Config with kindex explicitly null and no kindex sink should validate."""
        data = _minimal_config(kindex=None)
        result = validate_config_dict(data, "test")
        assert result.kindex is None


class TestGoodhartWebhookPath:
    def test_goodhart_root_path_accepted(self):
        """WebhookSourceConfig path with just '/' should be accepted."""
        data = _minimal_config(sources=[
            {"type": "webhook", "bind_address": "0.0.0.0", "port": 8080, "path": "/"}
        ])
        result = validate_config_dict(data, "test")
        assert result.sources[0].path == "/"

    def test_goodhart_deep_nested_path_accepted(self):
        """WebhookSourceConfig path with deep nesting should be accepted."""
        data = _minimal_config(sources=[
            {"type": "webhook", "bind_address": "0.0.0.0", "port": 8080, "path": "/api/v2/events/webhook"}
        ])
        result = validate_config_dict(data, "test")
        assert result.sources[0].path == "/api/v2/events/webhook"


class TestGoodhartUncommonButValidValues:
    def test_goodhart_non_default_max_open_stories_and_timeout(self):
        """Returned config preserves exact values for non-typical field values."""
        data = _minimal_config(max_open_stories=42, story_timeout_seconds=999)
        result = validate_config_dict(data, "test")
        assert result.max_open_stories == 42
        assert result.story_timeout_seconds == 999

    def test_goodhart_kindex_with_filters_and_tags(self):
        """Kindex config with non-empty event_type_filters and tag_patterns should be accessible."""
        data = _minimal_config(
            sinks=[{"type": "kindex", "url": "http://kindex.local"}],
            kindex={
                "noteworthiness_threshold": 0.5,
                "event_type_filters": ["alert", "incident"],
                "tag_patterns": ["security.*"],
            },
        )
        result = validate_config_dict(data, "test")
        assert result.kindex.noteworthiness_threshold == 0.5
        assert len(result.kindex.event_type_filters) == 2
        assert len(result.kindex.tag_patterns) == 1
        assert "alert" in result.kindex.event_type_filters
        assert "security.*" in result.kindex.tag_patterns


class TestGoodhartManyRules:
    def test_goodhart_ten_unique_rules(self):
        """Config with 10 rules with unique names should validate."""
        rules = []
        for i in range(10):
            rules.append({
                "name": f"rule-{i}",
                "match_conditions": [{"field": "f", "pattern": "p", "is_regex": False}],
                "group_by": ["g"],
                "window_seconds": 60,
            })
        data = _minimal_config(rules=rules)
        result = validate_config_dict(data, "test")
        assert len(result.rules) == 10
        names = [r.name for r in result.rules]
        assert len(set(names)) == 10


class TestGoodhartFrozenNested:
    def test_goodhart_nested_source_immutable(self):
        """Nested config objects (sources) should also be frozen/immutable."""
        data = _minimal_config()
        result = validate_config_dict(data, "test")
        with pytest.raises((TypeError, AttributeError, Exception)):
            result.sources[0].port = 9999

    def test_goodhart_nested_rule_immutable(self):
        """Nested config objects (rules) should also be frozen/immutable."""
        data = _minimal_config()
        result = validate_config_dict(data, "test")
        with pytest.raises((TypeError, AttributeError, Exception)):
            result.rules[0].name = "changed"

    def test_goodhart_nested_sink_immutable(self):
        """Nested config objects (sinks) should also be frozen/immutable."""
        data = _minimal_config()
        result = validate_config_dict(data, "test")
        with pytest.raises((TypeError, AttributeError, Exception)):
            result.sinks[0].url = "http://changed.local"


class TestGoodhartYAMLNonDictRoot:
    def test_goodhart_yaml_scalar_raises_config_error(self):
        """YAML that parses to a scalar should raise ConfigError, not crash."""
        path = _write_yaml_tempfile("hello\n")
        try:
            with pytest.raises(ConfigError):
                load_config(path)
        finally:
            os.unlink(path)

    def test_goodhart_yaml_list_raises_config_error(self):
        """YAML that parses to a list should raise ConfigError, not crash."""
        path = _write_yaml_tempfile("- item1\n- item2\n")
        try:
            with pytest.raises(ConfigError):
                load_config(path)
        finally:
            os.unlink(path)

    def test_goodhart_yaml_integer_raises_config_error(self):
        """YAML that parses to an integer should raise ConfigError."""
        path = _write_yaml_tempfile("42\n")
        try:
            with pytest.raises(ConfigError):
                load_config(path)
        finally:
            os.unlink(path)


class TestGoodhartNonDictInput:
    def test_goodhart_list_input_to_validate(self):
        """Passing a list instead of dict to validate_config_dict should raise an error."""
        with pytest.raises((ConfigError, TypeError, ValueError, Exception)):
            validate_config_dict([{"sources": []}], "test")


class TestGoodhartMultipleInvalidRegexes:
    def test_goodhart_multiple_rules_with_invalid_regex(self):
        """Multiple invalid regex patterns across different rules should all be caught."""
        data = _minimal_config(rules=[
            {
                "name": "rule-a",
                "match_conditions": [{"field": "f", "pattern": "[unclosed", "is_regex": True}],
                "group_by": ["g"],
                "window_seconds": 60,
            },
            {
                "name": "rule-b",
                "match_conditions": [{"field": "f", "pattern": "(?P<bad", "is_regex": True}],
                "group_by": ["g"],
                "window_seconds": 60,
            },
        ])
        with pytest.raises(ConfigError):
            validate_config_dict(data, "test")


class TestGoodhartLoadConfigViaFile:
    def test_goodhart_valid_config_via_yaml_file(self):
        """load_config should produce the same result as validate_config_dict for the same data."""
        data = _minimal_config(max_open_stories=777, story_timeout_seconds=123)
        yaml_content = yaml.dump(data)
        path = _write_yaml_tempfile(yaml_content)
        try:
            result = load_config(path)
            assert result.max_open_stories == 777
            assert result.story_timeout_seconds == 123
            assert len(result.sources) == 1
            assert len(result.sinks) == 1
            assert len(result.rules) == 1
        finally:
            os.unlink(path)

    def test_goodhart_load_config_validation_error_in_file(self):
        """load_config should raise ConfigError for a YAML file with invalid schema."""
        yaml_content = yaml.dump({"sources": [], "sinks": [], "rules": []})
        path = _write_yaml_tempfile(yaml_content)
        try:
            with pytest.raises(ConfigError):
                load_config(path)
        finally:
            os.unlink(path)

    def test_goodhart_load_config_kindex_cross_field_in_file(self):
        """load_config should enforce kindex cross-field check from YAML file."""
        data = _minimal_config(sinks=[{"type": "kindex", "url": "http://kindex.local"}])
        # No kindex config section
        yaml_content = yaml.dump(data)
        path = _write_yaml_tempfile(yaml_content)
        try:
            with pytest.raises(ConfigError):
                load_config(path)
        finally:
            os.unlink(path)

    def test_goodhart_load_config_duplicate_rules_in_file(self):
        """load_config should enforce unique rule names from YAML file."""
        data = _minimal_config(rules=[
            {"name": "dup", "match_conditions": [{"field": "f", "pattern": "p", "is_regex": False}], "group_by": ["g"], "window_seconds": 60},
            {"name": "dup", "match_conditions": [{"field": "f", "pattern": "p", "is_regex": False}], "group_by": ["g"], "window_seconds": 30},
        ])
        yaml_content = yaml.dump(data)
        path = _write_yaml_tempfile(yaml_content)
        try:
            with pytest.raises(ConfigError):
                load_config(path)
        finally:
            os.unlink(path)

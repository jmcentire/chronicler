"""Configuration loading and validation for Chronicler.

Loads chronicler.yaml using Pydantic v2. Parses correlation rules, source configs,
sink configs, memory limits, timeout settings, and kindex noteworthiness filters.
"""

from __future__ import annotations

import re
from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator


# ── Enums ───────────────────────────────────────────────────────────────────

class SourceType(str, Enum):
    webhook = "webhook"
    otlp = "otlp"
    file = "file"
    sentinel = "sentinel"


class SinkType(str, Enum):
    stigmergy = "stigmergy"
    apprentice = "apprentice"
    disk = "disk"
    kindex = "kindex"


# ── Primitive Types ─────────────────────────────────────────────────────────

class FilePath(str):
    """Validated filesystem path string. Must be non-empty."""
    def __new__(cls, value: str) -> FilePath:
        if not value:
            raise ValueError("FilePath must be non-empty")
        return str.__new__(cls, value)


class NoteworthinessThreshold(float):
    """Float between 0.0 and 1.0."""
    def __new__(cls, value: float) -> NoteworthinessThreshold:
        if value < 0.0 or value > 1.0:
            raise ValueError(f"NoteworthinessThreshold must be between 0.0 and 1.0, got {value}")
        return float.__new__(cls, value)


# ── Config Error ────────────────────────────────────────────────────────────

class ConfigError(Exception):
    """Custom exception for configuration errors."""
    def __init__(
        self,
        file_path: str = "",
        message: str = "",
        line_number: int = -1,
        validation_errors: list | None = None,
    ):
        self.file_path = file_path
        self.message = message
        self.line_number = line_number
        self.validation_errors = validation_errors or []
        super().__init__(f"{message} ({file_path})")


# ── Source Configs ──────────────────────────────────────────────────────────

class WebhookSourceConfig(BaseModel):
    model_config = {"frozen": True}
    type: SourceType = SourceType.webhook
    bind_address: str = Field(default="0.0.0.0", min_length=1, max_length=253)
    port: int = Field(default=8080, ge=1, le=65535)
    path: str = Field(default="/events", pattern=r'^/.*')

    @field_validator('type', mode='before')
    @classmethod
    def _coerce_type(cls, v: Any) -> SourceType:
        if isinstance(v, str):
            return SourceType(v)
        return v


class OtlpSourceConfig(BaseModel):
    model_config = {"frozen": True}
    type: SourceType = SourceType.otlp
    bind_address: str = Field(default="0.0.0.0", min_length=1, max_length=253)
    port: int = Field(default=4317, ge=1, le=65535)

    @field_validator('type', mode='before')
    @classmethod
    def _coerce_type(cls, v: Any) -> SourceType:
        if isinstance(v, str):
            return SourceType(v)
        return v


class FileSourceConfig(BaseModel):
    model_config = {"frozen": True}
    type: SourceType = SourceType.file
    path: str = Field(min_length=1)
    poll_interval_seconds: float = Field(default=1.0, ge=0.1, le=3600.0)

    @field_validator('type', mode='before')
    @classmethod
    def _coerce_type(cls, v: Any) -> SourceType:
        if isinstance(v, str):
            return SourceType(v)
        return v


class SentinelSourceConfig(BaseModel):
    model_config = {"frozen": True}
    type: SourceType = SourceType.sentinel
    bind_address: str = Field(default="0.0.0.0", min_length=1, max_length=253)
    port: int = Field(default=8081, ge=1, le=65535)

    @field_validator('type', mode='before')
    @classmethod
    def _coerce_type(cls, v: Any) -> SourceType:
        if isinstance(v, str):
            return SourceType(v)
        return v


SourceConfig = WebhookSourceConfig | OtlpSourceConfig | FileSourceConfig | SentinelSourceConfig

_SOURCE_TYPE_MAP = {
    "webhook": WebhookSourceConfig,
    "otlp": OtlpSourceConfig,
    "file": FileSourceConfig,
    "sentinel": SentinelSourceConfig,
}


# ── Sink Configs ────────────────────────────────────────────────────────────

class StigmergySinkConfig(BaseModel):
    model_config = {"frozen": True}
    type: SinkType = SinkType.stigmergy
    url: str = Field(pattern=r'^https?://.+')

    @field_validator('type', mode='before')
    @classmethod
    def _coerce_type(cls, v: Any) -> SinkType:
        if isinstance(v, str):
            return SinkType(v)
        return v


class ApprenticeSinkConfig(BaseModel):
    model_config = {"frozen": True}
    type: SinkType = SinkType.apprentice
    url: str = Field(pattern=r'^https?://.+')

    @field_validator('type', mode='before')
    @classmethod
    def _coerce_type(cls, v: Any) -> SinkType:
        if isinstance(v, str):
            return SinkType(v)
        return v


class DiskSinkConfig(BaseModel):
    model_config = {"frozen": True}
    type: SinkType = SinkType.disk
    output_dir: str = Field(default="./stories", min_length=1)

    @field_validator('type', mode='before')
    @classmethod
    def _coerce_type(cls, v: Any) -> SinkType:
        if isinstance(v, str):
            return SinkType(v)
        return v


class KindexSinkConfig(BaseModel):
    model_config = {"frozen": True}
    type: SinkType = SinkType.kindex
    url: str = Field(pattern=r'^https?://.+')

    @field_validator('type', mode='before')
    @classmethod
    def _coerce_type(cls, v: Any) -> SinkType:
        if isinstance(v, str):
            return SinkType(v)
        return v


SinkConfig = StigmergySinkConfig | ApprenticeSinkConfig | DiskSinkConfig | KindexSinkConfig

_SINK_TYPE_MAP = {
    "stigmergy": StigmergySinkConfig,
    "apprentice": ApprenticeSinkConfig,
    "disk": DiskSinkConfig,
    "kindex": KindexSinkConfig,
}


# ── Correlation Rule Config ────────────────────────────────────────────────

class MatchCondition(BaseModel):
    model_config = {"frozen": True}
    field: str = Field(min_length=1, max_length=256)
    pattern: str = Field(min_length=1)
    is_regex: bool = False


class CorrelationRuleConfig(BaseModel):
    model_config = {"frozen": True}
    name: str = Field(min_length=1, max_length=128, pattern=r'^[a-zA-Z0-9_.\-]+$')
    match_conditions: list[MatchCondition] = Field(min_length=1)
    group_by: list[str] = Field(min_length=1)
    window_seconds: int = Field(default=300, ge=1, le=86400)


# ── Kindex Config ───────────────────────────────────────────────────────────

class KindexConfig(BaseModel):
    model_config = {"frozen": True}
    noteworthiness_threshold: float = Field(default=0.5, ge=0.0, le=1.0)
    event_type_filters: list[str] = Field(default_factory=list)
    tag_patterns: list[str] = Field(default_factory=list)


# ── Top-level Config ────────────────────────────────────────────────────────

class ChroniclerConfig(BaseModel):
    model_config = {"frozen": True}
    sources: list[SourceConfig] = Field(min_length=1)
    sinks: list[SinkConfig] = Field(min_length=1)
    rules: list[CorrelationRuleConfig] = Field(min_length=1)
    max_open_stories: int = Field(default=10000, ge=1, le=1000000)
    story_timeout_seconds: int = Field(default=300, ge=1, le=604800)
    kindex: KindexConfig | None = None

    @model_validator(mode='before')
    @classmethod
    def _parse_discriminated_unions(cls, values: dict) -> dict:
        """Parse source and sink dicts into typed config objects."""
        if not isinstance(values, dict):
            return values

        # Parse sources
        raw_sources = values.get('sources', [])
        if isinstance(raw_sources, list):
            parsed_sources = []
            for src in raw_sources:
                if isinstance(src, dict):
                    src_type = src.get('type', '')
                    if src_type not in _SOURCE_TYPE_MAP:
                        raise ValueError(f"Unknown source type: {src_type}")
                    parsed_sources.append(_SOURCE_TYPE_MAP[src_type].model_validate(src))
                else:
                    parsed_sources.append(src)
            values['sources'] = parsed_sources

        # Parse sinks
        raw_sinks = values.get('sinks', [])
        if isinstance(raw_sinks, list):
            parsed_sinks = []
            for snk in raw_sinks:
                if isinstance(snk, dict):
                    snk_type = snk.get('type', '')
                    if snk_type not in _SINK_TYPE_MAP:
                        raise ValueError(f"Unknown sink type: {snk_type}")
                    parsed_sinks.append(_SINK_TYPE_MAP[snk_type].model_validate(snk))
                else:
                    parsed_sinks.append(snk)
            values['sinks'] = parsed_sinks

        return values

    @model_validator(mode='after')
    def _cross_field_validation(self) -> ChroniclerConfig:
        # Check duplicate rule names
        names = [r.name for r in self.rules]
        seen = set()
        for name in names:
            if name in seen:
                raise ValueError(f"Duplicate correlation rule name '{name}'")
            seen.add(name)

        # Check kindex sink requires kindex config
        has_kindex_sink = any(
            (isinstance(s, KindexSinkConfig) or
             (isinstance(s, dict) and s.get('type') == 'kindex'))
            for s in self.sinks
        )
        if has_kindex_sink and self.kindex is None:
            raise ValueError("Kindex sink requires a 'kindex' configuration section")

        # Check regex patterns validity
        for rule in self.rules:
            for mc in rule.match_conditions:
                if mc.is_regex:
                    try:
                        re.compile(mc.pattern)
                    except re.error as e:
                        raise ValueError(
                            f"Invalid regex pattern in rule '{rule.name}', "
                            f"condition field '{mc.field}': {e}"
                        )

        # Check port conflicts
        bind_ports: dict[tuple[str, int], str] = {}
        for src in self.sources:
            if hasattr(src, 'bind_address') and hasattr(src, 'port'):
                key = (src.bind_address, src.port)
                if key in bind_ports:
                    raise ValueError(
                        f"Port conflict: {src.type} and {bind_ports[key]} "
                        f"both bind to {src.bind_address}:{src.port}"
                    )
                bind_ports[key] = str(src.type)

        return self


# ── Public Functions ────────────────────────────────────────────────────────

def load_config(path: FilePath | str) -> ChroniclerConfig:
    """Load and validate a Chronicler configuration from a YAML file."""
    import yaml

    file_path = str(path)
    p = Path(file_path)

    if not p.exists():
        raise ConfigError(
            file_path=file_path,
            message=f"Configuration file not found: {file_path}",
        )

    try:
        content = p.read_text()
    except PermissionError:
        raise ConfigError(
            file_path=file_path,
            message=f"Configuration file not readable: {file_path}",
        )

    try:
        data = yaml.safe_load(content)
    except yaml.YAMLError as e:
        line = getattr(e, 'problem_mark', None)
        line_number = line.line + 1 if line else -1
        raise ConfigError(
            file_path=file_path,
            message=f"YAML parse error in {file_path} at line {line_number}: {e}",
            line_number=line_number,
        )

    if data is None:
        raise ConfigError(
            file_path=file_path,
            message=f"Configuration file is empty: {file_path}",
        )

    if not isinstance(data, dict):
        raise ConfigError(
            file_path=file_path,
            message=f"Configuration file is empty: {file_path}",
        )

    try:
        return ChroniclerConfig.model_validate(data)
    except Exception as e:
        raise ConfigError(
            file_path=file_path,
            message=f"Configuration validation failed for {file_path}: {e}",
            validation_errors=[str(e)],
        )


def validate_config_dict(
    data: dict,
    source_label: str = "in-memory",
) -> ChroniclerConfig:
    """Validate a pre-parsed configuration dictionary."""
    try:
        return ChroniclerConfig.model_validate(data)
    except Exception as e:
        raise ConfigError(
            file_path=source_label,
            message=f"Configuration validation failed for {source_label}: {e}",
            validation_errors=[str(e)],
        )


# ── Exports ─────────────────────────────────────────────────────────────────

__all__ = [
    'SourceType', 'SinkType', 'WebhookSourceConfig', 'OtlpSourceConfig',
    'FileSourceConfig', 'SentinelSourceConfig', 'SourceConfig',
    'StigmergySinkConfig', 'ApprenticeSinkConfig', 'DiskSinkConfig',
    'KindexSinkConfig', 'SinkConfig', 'MatchCondition',
    'CorrelationRuleConfig', 'KindexConfig', 'ChroniclerConfig',
    'ConfigError', 'load_config', 'validate_config_dict',
    'FilePath', 'NoteworthinessThreshold',
]

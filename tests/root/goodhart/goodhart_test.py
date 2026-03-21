"""
Adversarial hidden acceptance tests for the Root component.
These tests detect implementations that pass visible tests via shortcuts
(hardcoded returns, missing validation, etc.) rather than truly satisfying the contract.
"""
import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from pydantic import ValidationError

from chronicler.root import (
    StartupResult, ShutdownResult, SystemHealth, HealthStatus, LifecyclePhase,
    StartupStep, ShutdownStep, TypeCategory, TypeOwnershipEntry,
    IntegrationSeam, IntegrationSeamSpec, ErrorBoundaryZone,
    ErrorBoundaryPolicy, ErrorHandlingStrategy, PublicApiExport,
    SerializationConvention, PackageVersion,
    get_type_ownership_map, get_integration_seams, get_error_boundary_policies,
    get_public_api_exports, validate_component_wiring, run_cli,
)


# ============================================================
# Struct validation boundary tests
# ============================================================

class TestGoodhartStartupResultBoundaries:
    """StartupResult range validation beyond visible test values."""

    def test_goodhart_startup_result_boundary_zero_duration(self):
        """StartupResult must accept duration_seconds=0.0 as the lower boundary of the valid range."""
        result = StartupResult(
            success=True, completed_steps=[], failed_step="",
            error_message="", duration_seconds=0.0
        )
        assert result.duration_seconds == 0.0

    def test_goodhart_startup_result_boundary_max_duration(self):
        """StartupResult must accept duration_seconds=3600.0 as the upper boundary of the valid range."""
        result = StartupResult(
            success=True, completed_steps=[], failed_step="",
            error_message="", duration_seconds=3600.0
        )
        assert result.duration_seconds == 3600.0

    def test_goodhart_startup_result_frozen(self):
        """StartupResult instances should be immutable (frozen=True) per the contract invariant."""
        result = StartupResult(
            success=True, completed_steps=[], failed_step="",
            error_message="", duration_seconds=1.0
        )
        with pytest.raises(Exception):  # ValidationError or AttributeError
            result.success = False


class TestGoodhartShutdownResultBoundaries:
    """ShutdownResult range validation beyond visible test values."""

    def test_goodhart_shutdown_result_boundary_max_events(self):
        """ShutdownResult must accept events_drained at its upper boundary of 1000000000."""
        result = ShutdownResult(
            success=True, completed_steps=[], errors=[],
            events_drained=1000000000, stories_flushed=0,
            duration_seconds=1.0
        )
        assert result.events_drained == 1000000000

    def test_goodhart_shutdown_result_boundary_max_stories(self):
        """ShutdownResult must accept stories_flushed at its upper boundary of 1000000."""
        result = ShutdownResult(
            success=True, completed_steps=[], errors=[],
            events_drained=0, stories_flushed=1000000,
            duration_seconds=1.0
        )
        assert result.stories_flushed == 1000000

    def test_goodhart_shutdown_result_exceeds_max_events(self):
        """ShutdownResult must reject events_drained exceeding its upper bound of 1000000000."""
        with pytest.raises(ValidationError):
            ShutdownResult(
                success=True, completed_steps=[], errors=[],
                events_drained=1000000001, stories_flushed=0,
                duration_seconds=1.0
            )

    def test_goodhart_shutdown_result_negative_duration(self):
        """ShutdownResult must reject negative duration_seconds values."""
        with pytest.raises(ValidationError):
            ShutdownResult(
                success=True, completed_steps=[], errors=[],
                events_drained=0, stories_flushed=0,
                duration_seconds=-0.1
            )

    def test_goodhart_shutdown_result_negative_stories(self):
        """ShutdownResult must reject negative stories_flushed values."""
        with pytest.raises(ValidationError):
            ShutdownResult(
                success=True, completed_steps=[], errors=[],
                events_drained=0, stories_flushed=-1,
                duration_seconds=1.0
            )

    def test_goodhart_shutdown_result_frozen(self):
        """ShutdownResult instances should be immutable (frozen=True) per the contract invariant."""
        result = ShutdownResult(
            success=True, completed_steps=[], errors=[],
            events_drained=0, stories_flushed=0,
            duration_seconds=1.0
        )
        with pytest.raises(Exception):
            result.success = False


class TestGoodhartSystemHealthBoundaries:
    """SystemHealth range validation for fields not covered by visible tests."""

    def test_goodhart_system_health_rejects_negative_sources_healthy(self):
        """SystemHealth must reject negative values for sources_healthy."""
        with pytest.raises(ValidationError):
            SystemHealth(
                status=HealthStatus.HEALTHY, phase=LifecyclePhase.RUNNING,
                sources_healthy=-1, sources_total=2,
                sinks_healthy=1, sinks_total=1,
                open_stories=0, queue_depth=0,
                uptime_seconds=100.0, detail=""
            )

    def test_goodhart_system_health_rejects_negative_queue_depth(self):
        """SystemHealth must reject negative queue_depth."""
        with pytest.raises(ValidationError):
            SystemHealth(
                status=HealthStatus.HEALTHY, phase=LifecyclePhase.RUNNING,
                sources_healthy=1, sources_total=2,
                sinks_healthy=1, sinks_total=1,
                open_stories=0, queue_depth=-1,
                uptime_seconds=100.0, detail=""
            )

    def test_goodhart_system_health_rejects_exceeds_sinks_total(self):
        """SystemHealth must reject sinks_total exceeding its upper bound of 100."""
        with pytest.raises(ValidationError):
            SystemHealth(
                status=HealthStatus.HEALTHY, phase=LifecyclePhase.RUNNING,
                sources_healthy=1, sources_total=1,
                sinks_healthy=1, sinks_total=101,
                open_stories=0, queue_depth=0,
                uptime_seconds=100.0, detail=""
            )

    def test_goodhart_system_health_rejects_exceeds_open_stories(self):
        """SystemHealth must reject open_stories exceeding its upper bound of 1000000."""
        with pytest.raises(ValidationError):
            SystemHealth(
                status=HealthStatus.HEALTHY, phase=LifecyclePhase.RUNNING,
                sources_healthy=1, sources_total=1,
                sinks_healthy=1, sinks_total=1,
                open_stories=1000001, queue_depth=0,
                uptime_seconds=100.0, detail=""
            )

    def test_goodhart_system_health_rejects_exceeds_queue_depth(self):
        """SystemHealth must reject queue_depth exceeding its upper bound of 100000."""
        with pytest.raises(ValidationError):
            SystemHealth(
                status=HealthStatus.HEALTHY, phase=LifecyclePhase.RUNNING,
                sources_healthy=1, sources_total=1,
                sinks_healthy=1, sinks_total=1,
                open_stories=0, queue_depth=100001,
                uptime_seconds=100.0, detail=""
            )

    def test_goodhart_system_health_rejects_negative_uptime(self):
        """SystemHealth must reject negative uptime_seconds."""
        with pytest.raises(ValidationError):
            SystemHealth(
                status=HealthStatus.HEALTHY, phase=LifecyclePhase.RUNNING,
                sources_healthy=1, sources_total=1,
                sinks_healthy=1, sinks_total=1,
                open_stories=0, queue_depth=0,
                uptime_seconds=-0.001, detail=""
            )

    def test_goodhart_system_health_rejects_exceeds_uptime(self):
        """SystemHealth must reject uptime_seconds exceeding 31536000.0 (1 year)."""
        with pytest.raises(ValidationError):
            SystemHealth(
                status=HealthStatus.HEALTHY, phase=LifecyclePhase.RUNNING,
                sources_healthy=1, sources_total=1,
                sinks_healthy=1, sinks_total=1,
                open_stories=0, queue_depth=0,
                uptime_seconds=31536001.0, detail=""
            )

    def test_goodhart_system_health_max_boundary_open_stories(self):
        """SystemHealth must accept open_stories at its upper boundary of 1000000."""
        h = SystemHealth(
            status=HealthStatus.HEALTHY, phase=LifecyclePhase.RUNNING,
            sources_healthy=1, sources_total=1,
            sinks_healthy=1, sinks_total=1,
            open_stories=1000000, queue_depth=0,
            uptime_seconds=100.0, detail=""
        )
        assert h.open_stories == 1000000

    def test_goodhart_system_health_max_boundary_queue_depth(self):
        """SystemHealth must accept queue_depth at its upper boundary of 100000."""
        h = SystemHealth(
            status=HealthStatus.HEALTHY, phase=LifecyclePhase.RUNNING,
            sources_healthy=1, sources_total=1,
            sinks_healthy=1, sinks_total=1,
            open_stories=0, queue_depth=100000,
            uptime_seconds=100.0, detail=""
        )
        assert h.queue_depth == 100000

    def test_goodhart_system_health_frozen(self):
        """SystemHealth instances should be immutable (frozen=True)."""
        h = SystemHealth(
            status=HealthStatus.HEALTHY, phase=LifecyclePhase.RUNNING,
            sources_healthy=1, sources_total=1,
            sinks_healthy=1, sinks_total=1,
            open_stories=0, queue_depth=0,
            uptime_seconds=100.0, detail=""
        )
        with pytest.raises(Exception):
            h.status = HealthStatus.DEGRADED


class TestGoodhartTypeOwnershipEntryBoundaries:
    """TypeOwnershipEntry boundary tests not covered by visible tests."""

    def test_goodhart_type_ownership_entry_max_name_length(self):
        """TypeOwnershipEntry must accept type_name at its maximum allowed length of 256 characters."""
        entry = TypeOwnershipEntry(
            type_name="A" * 256,
            owner_module="chronicler.schemas",
            category=TypeCategory.DOMAIN_MODEL
        )
        assert len(entry.type_name) == 256

    def test_goodhart_type_ownership_entry_exceeds_name_length(self):
        """TypeOwnershipEntry must reject type_name exceeding the maximum length of 256 characters."""
        with pytest.raises(ValidationError):
            TypeOwnershipEntry(
                type_name="A" * 257,
                owner_module="chronicler.schemas",
                category=TypeCategory.DOMAIN_MODEL
            )

    def test_goodhart_type_ownership_entry_chronicler_dot_required(self):
        """TypeOwnershipEntry must reject owner_module='chronicler' without a dot suffix."""
        with pytest.raises(ValidationError):
            TypeOwnershipEntry(
                type_name="Event",
                owner_module="chronicler",
                category=TypeCategory.DOMAIN_MODEL
            )


class TestGoodhartPublicApiExportBoundaries:
    """PublicApiExport boundary tests."""

    def test_goodhart_public_api_export_max_name_length(self):
        """PublicApiExport must accept a name at its maximum allowed length of 128 characters."""
        export = PublicApiExport(
            name="A" * 128,
            source_module="chronicler.schemas",
            kind="class"
        )
        assert len(export.name) == 128

    def test_goodhart_public_api_export_exceeds_name_length(self):
        """PublicApiExport must reject a name exceeding 128 characters."""
        with pytest.raises(ValidationError):
            PublicApiExport(
                name="A" * 129,
                source_module="chronicler.schemas",
                kind="class"
            )


class TestGoodhartErrorBoundaryPolicyValidation:
    """ErrorBoundaryPolicy log_level validation beyond visible tests."""

    def test_goodhart_error_boundary_policy_all_log_levels(self):
        """ErrorBoundaryPolicy must accept all five valid log level strings."""
        for level in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            policy = ErrorBoundaryPolicy(
                zone=ErrorBoundaryZone.SOURCE,
                strategy=ErrorHandlingStrategy.CATCH_LOG_CONTINUE,
                log_level=level,
                restartable=True,
                fatal=False
            )
            assert policy.log_level == level

    def test_goodhart_error_boundary_policy_rejects_lowercase_log_level(self):
        """ErrorBoundaryPolicy must reject lowercase log level strings like 'error'."""
        with pytest.raises(ValidationError):
            ErrorBoundaryPolicy(
                zone=ErrorBoundaryZone.SOURCE,
                strategy=ErrorHandlingStrategy.CATCH_LOG_CONTINUE,
                log_level="error",
                restartable=True,
                fatal=False
            )

    def test_goodhart_error_boundary_policy_rejects_mixed_case_log_level(self):
        """ErrorBoundaryPolicy must reject mixed-case log level strings like 'Warning'."""
        with pytest.raises(ValidationError):
            ErrorBoundaryPolicy(
                zone=ErrorBoundaryZone.SOURCE,
                strategy=ErrorHandlingStrategy.CATCH_LOG_CONTINUE,
                log_level="Warning",
                restartable=True,
                fatal=False
            )

    def test_goodhart_error_boundary_policy_frozen(self):
        """ErrorBoundaryPolicy instances should be immutable (frozen=True)."""
        policy = ErrorBoundaryPolicy(
            zone=ErrorBoundaryZone.SOURCE,
            strategy=ErrorHandlingStrategy.CATCH_LOG_CONTINUE,
            log_level="WARNING",
            restartable=True,
            fatal=False
        )
        with pytest.raises(Exception):
            policy.fatal = True


class TestGoodhartSerializationConventionValidation:
    """SerializationConvention validation beyond visible tests."""

    def test_goodhart_serialization_convention_rejects_xml_format(self):
        """SerializationConvention must reject format='xml'."""
        with pytest.raises(ValidationError):
            SerializationConvention(
                format="xml",
                datetime_format="iso8601_utc",
                id_format="sha256_hex",
                null_handling="omit",
                discriminator_field="record_type"
            )

    def test_goodhart_serialization_convention_rejects_invalid_datetime_format(self):
        """SerializationConvention must reject datetime_format values other than 'iso8601_utc'."""
        with pytest.raises(ValidationError):
            SerializationConvention(
                format="json",
                datetime_format="unix_epoch",
                id_format="sha256_hex",
                null_handling="omit",
                discriminator_field="record_type"
            )

    def test_goodhart_serialization_convention_rejects_null_handling_none(self):
        """SerializationConvention must reject null_handling='none'."""
        with pytest.raises(ValidationError):
            SerializationConvention(
                format="json",
                datetime_format="iso8601_utc",
                id_format="sha256_hex",
                null_handling="none",
                discriminator_field="record_type"
            )


class TestGoodhartPackageVersionValidation:
    """PackageVersion validation beyond visible tests."""

    def test_goodhart_package_version_rejects_two_segment(self):
        """PackageVersion must reject version strings with only two segments like '1.0'."""
        with pytest.raises(ValidationError):
            PackageVersion(version="1.0", python_requires=">=3.11")

    def test_goodhart_package_version_rejects_leading_v(self):
        """PackageVersion must reject version strings with a leading 'v' prefix."""
        with pytest.raises(ValidationError):
            PackageVersion(version="v1.0.0", python_requires=">=3.11")

    def test_goodhart_package_version_accepts_build_metadata(self):
        """PackageVersion must accept version strings with build metadata."""
        pv = PackageVersion(version="1.0.0+build.123", python_requires=">=3.11")
        assert pv.version == "1.0.0+build.123"


# ============================================================
# Enum count and exhaustiveness tests
# ============================================================

class TestGoodhartEnumCounts:
    """Verify enum member counts to detect extra or missing members."""

    def test_goodhart_enum_lifecycle_phase_count(self):
        """LifecyclePhase enum must have exactly 6 members."""
        assert len(LifecyclePhase) == 6

    def test_goodhart_enum_startup_step_count(self):
        """StartupStep enum must have exactly 12 members."""
        assert len(StartupStep) == 12

    def test_goodhart_enum_shutdown_step_count(self):
        """ShutdownStep enum must have exactly 7 members."""
        assert len(ShutdownStep) == 7


# ============================================================
# Pure function introspection tests — structure and consistency
# ============================================================

class TestGoodhartTypeOwnershipMap:
    """Detect hardcoded or incomplete type ownership map implementations."""

    def test_goodhart_type_ownership_no_duplicates(self):
        """The type ownership map must contain no duplicate type names."""
        entries = get_type_ownership_map()
        names = [e.type_name for e in entries]
        assert len(names) == len(set(names)), f"Duplicate type names found: {[n for n in names if names.count(n) > 1]}"

    def test_goodhart_type_ownership_contains_core_domain_types(self):
        """The type ownership map must include entries for Event, Story, and CorrelationRule."""
        entries = get_type_ownership_map()
        names = {e.type_name for e in entries}
        for core_type in ["Event", "Story", "CorrelationRule"]:
            assert core_type in names, f"Missing core type: {core_type}"

    def test_goodhart_type_ownership_entries_are_typed(self):
        """Each entry must be a proper TypeOwnershipEntry instance."""
        entries = get_type_ownership_map()
        for entry in entries:
            assert isinstance(entry, TypeOwnershipEntry), f"Entry is not TypeOwnershipEntry: {type(entry)}"
            assert hasattr(entry, 'type_name')
            assert hasattr(entry, 'owner_module')
            assert hasattr(entry, 'category')

    def test_goodhart_type_ownership_categories_valid(self):
        """Every entry must use a valid TypeCategory enum value."""
        entries = get_type_ownership_map()
        for entry in entries:
            assert isinstance(entry.category, TypeCategory), f"Invalid category for {entry.type_name}: {entry.category}"

    def test_goodhart_get_type_ownership_map_idempotent(self):
        """get_type_ownership_map is a pure function — two calls must return identical results."""
        result1 = get_type_ownership_map()
        result2 = get_type_ownership_map()
        assert result1 == result2


class TestGoodhartIntegrationSeams:
    """Detect hardcoded or structurally invalid integration seam implementations."""

    def test_goodhart_integration_seams_unique(self):
        """Each integration seam must appear exactly once."""
        seams = get_integration_seams()
        seam_ids = [s.seam for s in seams]
        assert len(seam_ids) == len(set(seam_ids)), "Duplicate seam identifiers found"

    def test_goodhart_integration_seams_are_typed(self):
        """Each entry must be a proper IntegrationSeamSpec instance."""
        seams = get_integration_seams()
        for seam in seams:
            assert isinstance(seam, IntegrationSeamSpec), f"Entry is not IntegrationSeamSpec: {type(seam)}"
            assert hasattr(seam, 'seam')
            assert hasattr(seam, 'producer')
            assert hasattr(seam, 'consumer')
            assert hasattr(seam, 'transport')
            assert hasattr(seam, 'data_type')
            assert hasattr(seam, 'back_pressure')
            assert hasattr(seam, 'error_boundary')

    def test_goodhart_integration_seams_error_boundaries_valid(self):
        """Each integration seam must reference a valid ErrorBoundaryZone."""
        seams = get_integration_seams()
        for seam in seams:
            assert isinstance(seam.error_boundary, ErrorBoundaryZone), \
                f"Invalid error_boundary for {seam.seam}: {seam.error_boundary}"

    def test_goodhart_integration_seams_source_to_engine_boundary(self):
        """SOURCE_TO_ENGINE seam must specify SOURCE as its error boundary zone."""
        seams = get_integration_seams()
        source_seam = [s for s in seams if s.seam == IntegrationSeam.SOURCE_TO_ENGINE]
        assert len(source_seam) == 1
        assert source_seam[0].error_boundary == ErrorBoundaryZone.SOURCE

    def test_goodhart_integration_seams_engine_to_correlation_boundary(self):
        """ENGINE_TO_CORRELATION seam must specify CORRELATION as its error boundary zone."""
        seams = get_integration_seams()
        corr_seam = [s for s in seams if s.seam == IntegrationSeam.ENGINE_TO_CORRELATION]
        assert len(corr_seam) == 1
        assert corr_seam[0].error_boundary == ErrorBoundaryZone.CORRELATION

    def test_goodhart_integration_seams_story_manager_to_sinks_boundary(self):
        """STORY_MANAGER_TO_SINKS seam must specify SINK as its error boundary zone."""
        seams = get_integration_seams()
        sink_seam = [s for s in seams if s.seam == IntegrationSeam.STORY_MANAGER_TO_SINKS]
        assert len(sink_seam) == 1
        assert sink_seam[0].error_boundary == ErrorBoundaryZone.SINK

    def test_goodhart_get_integration_seams_idempotent(self):
        """get_integration_seams is a pure function — two calls must return identical results."""
        result1 = get_integration_seams()
        result2 = get_integration_seams()
        assert result1 == result2


class TestGoodhartErrorBoundaryPolicies:
    """Detect hardcoded or structurally invalid error boundary policy implementations."""

    def test_goodhart_error_policies_are_typed(self):
        """Each policy must be a proper ErrorBoundaryPolicy instance."""
        policies = get_error_boundary_policies()
        for p in policies:
            assert isinstance(p, ErrorBoundaryPolicy), f"Entry is not ErrorBoundaryPolicy: {type(p)}"

    def test_goodhart_error_policies_no_duplicate_zones(self):
        """Each error boundary zone must have exactly one policy."""
        policies = get_error_boundary_policies()
        zones = [p.zone for p in policies]
        assert len(zones) == len(set(zones)), "Duplicate zone entries found"

    def test_goodhart_error_policies_log_levels_valid(self):
        """All error boundary policies must use valid Python log level strings."""
        import re
        policies = get_error_boundary_policies()
        pattern = re.compile(r'^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$')
        for p in policies:
            assert pattern.match(p.log_level), f"Invalid log_level for {p.zone}: {p.log_level}"

    def test_goodhart_error_policies_strategies_valid(self):
        """All error boundary policies must reference valid ErrorHandlingStrategy enum values."""
        policies = get_error_boundary_policies()
        for p in policies:
            assert isinstance(p.strategy, ErrorHandlingStrategy), \
                f"Invalid strategy for {p.zone}: {p.strategy}"

    def test_goodhart_get_error_boundary_policies_idempotent(self):
        """get_error_boundary_policies is a pure function — two calls must return identical results."""
        result1 = get_error_boundary_policies()
        result2 = get_error_boundary_policies()
        assert result1 == result2


class TestGoodhartPublicApiExports:
    """Detect hardcoded or structurally invalid public API export implementations."""

    def test_goodhart_public_api_exports_are_typed(self):
        """Each export must be a proper PublicApiExport instance."""
        exports = get_public_api_exports()
        for e in exports:
            assert isinstance(e, PublicApiExport), f"Entry is not PublicApiExport: {type(e)}"
            assert hasattr(e, 'name')
            assert hasattr(e, 'source_module')
            assert hasattr(e, 'kind')

    def test_goodhart_public_api_exports_no_duplicate_names(self):
        """No symbol name should appear twice in the public API."""
        exports = get_public_api_exports()
        names = [e.name for e in exports]
        assert len(names) == len(set(names)), f"Duplicate API exports: {[n for n in names if names.count(n) > 1]}"

    def test_goodhart_public_api_exports_valid_kinds(self):
        """Every export must declare a valid kind."""
        exports = get_public_api_exports()
        valid_kinds = {"class", "function", "enum", "type_alias"}
        for e in exports:
            assert e.kind in valid_kinds, f"Invalid kind for {e.name}: {e.kind}"

    def test_goodhart_public_api_cli_main_is_function(self):
        """cli_main must be declared as a function."""
        exports = get_public_api_exports()
        cli_main_entries = [e for e in exports if e.name == "cli_main"]
        assert len(cli_main_entries) == 1, "cli_main not found in public API exports"
        assert cli_main_entries[0].kind == "function"

    def test_goodhart_public_api_event_kind(self):
        """Event must be declared as a class."""
        exports = get_public_api_exports()
        event_entries = [e for e in exports if e.name == "Event"]
        assert len(event_entries) == 1, "Event not found in public API exports"
        assert event_entries[0].kind == "class"


class TestGoodhartWiringValidation:
    """Detect validate_component_wiring that always returns True or doesn't actually validate."""

    def test_goodhart_wiring_validates_against_actual_introspection_data(self):
        """validate_component_wiring must return True with the real introspection data."""
        types = get_type_ownership_map()
        seams = get_integration_seams()
        policies = get_error_boundary_policies()
        result = validate_component_wiring(types, seams, policies)
        assert result is True

    def test_goodhart_wiring_rejects_empty_policies(self):
        """validate_component_wiring must reject when no error policies are provided but seams reference error zones."""
        types = get_type_ownership_map()
        seams = get_integration_seams()
        with pytest.raises(ValueError):
            validate_component_wiring(types, seams, [])

    def test_goodhart_wiring_rejects_partial_policies(self):
        """validate_component_wiring must reject when only some error zones are covered."""
        types = get_type_ownership_map()
        seams = get_integration_seams()
        policies = get_error_boundary_policies()
        # Remove one policy that is used by a seam
        partial_policies = [p for p in policies if p.zone != ErrorBoundaryZone.SOURCE]
        with pytest.raises(ValueError):
            validate_component_wiring(types, seams, partial_policies)

    def test_goodhart_wiring_rejects_empty_type_map_if_seams_exist(self):
        """validate_component_wiring should raise ValueError when type ownership map is empty but seams reference types."""
        seams = get_integration_seams()
        policies = get_error_boundary_policies()
        # This should fail because seams reference data types that should be in the ownership map
        with pytest.raises(ValueError):
            validate_component_wiring([], seams, policies)


class TestGoodhartCliReturnType:
    """Verify run_cli always returns an integer."""

    def test_goodhart_cli_returns_integer(self):
        """run_cli must always return an integer exit code."""
        # Test with an empty argv to trigger help or error
        result = run_cli(["--help"])
        assert isinstance(result, int), f"run_cli returned {type(result)}, expected int"

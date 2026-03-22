# CLAUDE.md — Chronicler

Event collection and story assembly for the FOSS governance stack. Collects events from webhooks, OTLP spans, log files, and Sentinel incidents, correlates them into stories at configurable granularities, and emits to Stigmergy (pattern discovery) and Apprentice (learning).

## Quick Reference

```bash
pip install -e .
pytest tests/                    # 1272 tests
chronicler start chronicler.yaml # Start the engine
chronicler status                # Check engine status
chronicler stories --type journey # List journey stories
```

## Architecture

```
Sources → Correlation Engine → Sinks
          (bounded memory,
           LRU eviction,
           timeout sweep)
```

**Story types**: request (trace_id, 30s), service (entity_id+component_id, 5m), journey (session_id, 30m)

**Sources**: webhook (HTTP POST), OTLP (gRPC), file (JSONL tail), Sentinel (incident lifecycle)

**Sinks**: Stigmergy, Apprentice, disk (JSONL), Kindex

## Conventions

- Python 3.12+, Pydantic v2, frozen=True, async throughout
- Protocol classes for sources and sinks (not ABC)
- JSONL persistence in .chronicler/
- Fire-and-forget for sinks
- Content-addressable event IDs (SHA-256)
- All imports: `from chronicler.X import Y` (no bare module aliases)

## Stack Integration

- **Baton** → OTLP spans forwarded to Chronicler
- **Sentinel** → incident lifecycle events emitted to Chronicler
- **Chronicler** → Stigmergy (stories as signals)
- **Chronicler** → Apprentice (stories as training sequences)
- **Chronicler** → Kindex (noteworthy stories as knowledge nodes)

## Known Issues

- 1 skipped test: `TestMcpToolChroniclerStatus::test_engine_unavailable` — Pact test-gen bug (contradictory async/sync expectations for same function)
- MCP tool functions are stubs returning static data
- Source transports (webhook HTTP server, OTLP gRPC server) are basic implementations

## Release

No automated PyPI publishing yet. Manual: bump version in pyproject.toml, tag, push, create GitHub release.

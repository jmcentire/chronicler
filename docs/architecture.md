# Chronicler Architecture

## Overview

Chronicler is a correlation engine that assembles raw events into stories. It has three layers:

```
Ingestion → Correlation → Emission
```

**Ingestion** accepts events from pluggable sources (webhook, OTLP, file, Sentinel). Each source normalizes data into the common `Event` envelope.

**Correlation** matches events to open stories using configurable rules. The engine maintains a bounded set of open stories in memory, with LRU eviction and periodic timeout sweep.

**Emission** delivers closed stories to pluggable sinks (Stigmergy, Apprentice, disk, Kindex). All emission is fire-and-forget.

## Event Model

Every event has a content-addressable ID computed as SHA-256 of its identity fields:

```
SHA-256(canonical_json({
    actor, context, correlation_keys,
    entity_id, entity_kind, event_kind, timestamp
}))
```

This guarantees automatic idempotency: the same event submitted twice produces the same ID and is deduplicated.

Events carry `correlation_keys` — a map of key names to values that the correlation engine uses to match events to stories.

## Story Types

Stories are defined by correlation rules in `chronicler.yaml`:

| Type | Group By | Timeout | Use Case |
|------|----------|---------|----------|
| Request | `trace_id` | 30s | Single request + spans |
| Service | `entity_id, component_id` | 5m | Entity lifecycle at one service |
| Journey | `session_id` (chain: `entity_id`) | 30m | Full user path across services |

**Multi-story participation**: A single event can join multiple stories simultaneously. An event with `trace_id`, `entity_id`, and `session_id` creates or joins three stories at once.

**Chain-by**: When a journey story closes, its `entity_id` values are carried forward. If a new event arrives with a matching `entity_id` and `session_id`, the story resumes.

## Correlation Engine

The engine maintains two data structures:

1. **Open stories** — `OrderedDict` keyed by `(rule_name, key_tuple)`. O(1) lookup. LRU ordering via move-to-end on access.
2. **Timeout sweep** — async task that periodically checks all open stories against their rule's timeout. Closed stories are emitted to sinks.

Memory is bounded by `max_open_stories`. When exceeded, the least-recently-used story is evicted. Evicted stories are emitted to sinks (not dropped).

## Source Protocol

```python
class SourceProtocol(Protocol):
    def subscribe(self, callback: Callable) -> None: ...
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
```

Sources call `emit(subscribers, event)` which handles both sync and async callbacks.

## Sink Protocol

```python
class SinkProtocol(Protocol):
    async def start(self) -> None: ...
    async def emit(self, story: Story) -> None: ...
    async def close(self) -> None: ...
    async def stop(self) -> None: ...
```

Sinks are fire-and-forget. Failures are logged but never block story processing.

## Configuration

All configuration via `chronicler.yaml`. Validated at startup with Pydantic v2. Invalid config is a fatal error (fail fast).

Key config sections:
- `correlation_rules` — list of rules with `name`, `group_by`, `timeout_seconds`, optional `chain_by` and `terminal_events`
- `sources` — list of source configs (discriminated union on `type`)
- `sinks` — list of sink configs (discriminated union on `type`)
- `memory_limits` — `max_open_stories`, `queue_max_size`

## File Layout

```
src/chronicler/
    schemas.py              Event, Story, CorrelationRule, config models
    config.py               YAML loading, validation, ConfigError
    correlation.py          CorrelationEngine, StoryManager, state persistence
    engine_cli_mcp.py       ChroniclerEngine, CLI, MCP tools
    root.py                 Lifecycle, health, wiring validation, factory
    sources/
        base.py             SourceProtocol, emit()
        webhook.py          HTTP POST receiver
        otlp.py             OTLP gRPC collector
        file.py             JSONL file tailer
        sentinel.py         Sentinel incident receiver
    sinks/
        base.py             SinkProtocol
        disk.py             JSONL persistence
        stigmergy.py        Stigmergy signal emission
        apprentice.py       Apprentice training sequence emission
        kindex.py           Knowledge graph capture
```

## Constraints

See `constraints.yaml` for the full list. Key constraints:

- **C001**: Single event may only contribute to one active story per story type
- **C002**: Stories must be evicted or emitted before memory bounds exceeded
- **C004**: Event ingestion must not block on story assembly or output
- **C005**: Eviction policies must not violate story timeout guarantees
- **C008**: Disk output must use JSONL format consistent with stack patterns

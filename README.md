# Chronicler

**Event collection and story assembly for the FOSS governance stack.**

Chronicler sits between raw event sources and pattern consumers. It collects events from webhooks, OTLP spans, log files, and Sentinel incidents, correlates them into "stories" at configurable granularities, and emits completed stories to Stigmergy (for pattern discovery) and Apprentice (for learning).

A story is a correlated group of events that represents a meaningful narrative:

- **Request story** -- a single request and its downstream spans (group by `trace_id`, ~30s timeout)
- **Service story** -- a sequence of requests to one component for one entity (group by `entity_id` + `component_id`, ~5m timeout)
- **Journey story** -- a causal chain across components following a user's path (group by `session_id`, chain by `entity_id`, ~30m timeout)

Chronicler is not an event bus. It is not a message broker. It is a correlation engine with bounded memory and configurable story assembly rules.

## Why

The FOSS governance stack has two feedback loops:

1. **Engineering loop** (Sentinel → Pact): production errors tighten contracts, eliminating bug classes automatically.
2. **Product loop** (Chronicler → Stigmergy → Apprentice): production usage patterns surface insights and drive adaptation.

Chronicler closes the product loop.

## Quick Start

```bash
pip install chronicler
```

Create a `chronicler.yaml`:

```yaml
correlation_rules:
  - name: request_story
    group_by: [trace_id]
    timeout_seconds: 30

  - name: service_story
    group_by: [entity_id, component_id]
    timeout_seconds: 300

  - name: journey_story
    group_by: [session_id]
    chain_by: [entity_id]
    timeout_seconds: 1800

sources:
  - type: webhook
    port: 8080
    path: /events

  - type: otlp
    port: 4317

sinks:
  - type: disk
    output_dir: .chronicler/stories

  - type: stigmergy
    endpoint: http://localhost:8400

  - type: apprentice
    endpoint: http://localhost:8401
```

```bash
chronicler start chronicler.yaml
chronicler status
chronicler stories --type journey
```

## Architecture

```
Sources                    Chronicler                      Sinks
────────                   ──────────                      ─────
Webhook  ─┐               ┌─────────────┐           ┌──→ Stigmergy
OTLP     ─┤──→ Events ──→ │ Correlation │──→ Stories ├──→ Apprentice
File     ─┤               │   Engine    │            ├──→ Disk (JSONL)
Sentinel ─┘               └─────────────┘            └──→ Kindex
```

### Event Model

Events arrive with a content-addressable ID (SHA-256 of identity fields), ensuring automatic idempotency:

```python
Event(
    event_id="7a88e802...",          # Content-addressable SHA-256
    event_kind="booking.created",    # Dotted type
    entity_kind="booking",
    entity_id="booking_abc123",
    actor="user@example.com",
    timestamp="2026-03-21T10:30:00Z",
    source="webhook",
    context={"status": "confirmed"},
    correlation_keys={"trace_id": "abc", "session_id": "xyz"},
)
```

### Correlation Engine

Events are matched to stories via configurable rules. One event can participate in multiple stories simultaneously (a request event with `trace_id`, `entity_id`, and `session_id` joins three stories at once).

Correlation lookup is O(1) via dict keyed by `(rule_name, key_tuple)`.

Stories close when:
- Their configured timeout expires (periodic sweep)
- A terminal event arrives
- Memory pressure triggers LRU eviction (evicted stories are emitted, not dropped)

### Sources

| Source | Transport | Purpose |
|--------|-----------|---------|
| **Webhook** | HTTP POST | Application events |
| **OTLP** | gRPC | Baton span forwarding |
| **File** | JSONL tail | Structured log ingestion |
| **Sentinel** | HTTP POST | Incident lifecycle events |

All sources implement `SourceProtocol`: `start()`, `stop()`, `subscribe(callback)`.

### Sinks

| Sink | Transport | Purpose |
|------|-----------|---------|
| **Stigmergy** | HTTP POST | Pattern discovery via ART mesh |
| **Apprentice** | HTTP POST | Journey-level learning |
| **Disk** | JSONL append | Persistence and replay |
| **Kindex** | MCP tools | Knowledge graph capture |

All sinks implement `SinkProtocol`: `start()`, `stop()`, `emit(story)`, `close()`. Emission is fire-and-forget -- unreachable sinks don't block story processing.

## Stack Integration

Chronicler is part of a larger governance stack:

| Tool | Relationship |
|------|-------------|
| **Baton** | Forwards OTLP spans to Chronicler for story assembly |
| **Sentinel** | Emits incident lifecycle events to Chronicler |
| **Stigmergy** | Consumes stories for organizational pattern discovery |
| **Apprentice** | Consumes stories for journey-level learning |
| **Kindex** | Records noteworthy stories as knowledge graph nodes |
| **Constrain** | Produced the specification artifacts that guided Chronicler's design |
| **Pact** | Generated 97 function contracts and 1017 tests for Chronicler |

## Constraints

- Python 3.12+, Pydantic v2
- Async throughout (asyncio)
- Bounded memory with configurable `max_open_stories` and LRU eviction
- JSONL persistence consistent with Baton/Sentinel patterns
- All data classified as PUBLIC tier (no PII collection)

## Testing

```bash
git clone https://github.com/jmcentire/chronicler.git
cd chronicler
pip install -e ".[dev]"
pytest tests/
```

1272 tests covering all 7 components (schemas, config, correlation engine, sources, sinks, engine/CLI/MCP, root orchestration).

## License

MIT

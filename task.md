# Task: Build Chronicler

Build Chronicler — a general-purpose event collector and story assembler for the FOSS governance stack.

## What to build

1. **Event model**: Immutable event envelope with content-addressable ID, dotted event_kind, entity_kind/id, actor, timestamp, arbitrary context payload, and correlation_keys map.

2. **Story model**: Story lifecycle (open → closed). Accumulates ordered events. Closes on timeout or terminal event. Tracks story_type (request/service/journey), correlation rule that created it, start/end timestamps, duration, event count.

3. **Correlation engine**: Match incoming events to correlation rules by checking which group_by keys the event carries. O(1) lookup via dict keyed by (rule_name, key_tuple). Create new story if no match. Support chain_by for following entities across stories.

4. **Story manager**: Manage open stories in bounded memory (max_open_stories with LRU eviction). Periodic timeout sweep. Emit closed stories to registered sinks.

5. **Source protocol + 4 sources**: Protocol (start/stop/subscribe). Webhook (HTTP POST), OTLP (gRPC span collector), File (JSONL tail), Sentinel (incident lifecycle events).

6. **Sink protocol + 4 sinks**: Protocol (emit/start/stop). Stigmergy (stories as Signals), Apprentice (stories as training sequences), Disk (JSONL), Kindex (noteworthy stories as knowledge nodes).

7. **Config**: Load chronicler.yaml with correlation rules, source configs, sink configs, memory limits.

8. **Engine**: Async orchestrator that starts sources, runs correlation, manages stories, emits to sinks.

9. **CLI**: chronicler start, status, stories (list/show), replay.

10. **MCP server**: Query open/closed stories, check status, replay.

## Constraints

See constraints.yaml for 13 hard constraints (C001–C013).

## Integration points

- Baton → OTLP source
- Sentinel → Sentinel source
- Chronicler → Stigmergy (SourceAdapter protocol)
- Chronicler → Apprentice (training sequences)
- Chronicler → Kindex (knowledge nodes via MCP tools)

## Tech stack

Python 3.12+, Pydantic v2, async (asyncio), JSONL persistence, no external broker.

# Design: chronicler

*Version 1 — Auto-maintained by pact*

## Decomposition

- [C] **Root** (`root`)
  # Task: Build Chronicler

Build Chronicler — a general-purpose event collector and story assembler for the FOSS governance stack.

## What to build

1. **Event model**: Immutable event envelope with c
  - [C] **Configuration Loading** (`config`)
    Load and validate chronicler.yaml using Pydantic v2. Parses correlation rules, source configs, sink configs, memory limits (max_open_stories), timeout settings, and kindex noteworthiness filters. Fails fast with clear error messages on invalid config. Produces typed config objects. Includes chronicler.yaml.example. Covers AC9.
  - [C] **Correlation Engine & Story Manager** (`correlation_and_stories`)
    Two tightly coupled modules: (1) correlation.py — matches incoming events to correlation rules by extracting group_by keys, O(1) lookup via dict keyed by (rule_name, key_tuple), creates new stories when no match found, implements chain_by (on story close, register chain_by keys so subsequent events link to new story with parent_story_id). (2) story_manager.py — manages open stories in bounded memory with max_open_stories and LRU eviction (OrderedDict), periodic async timeout sweep, closes stories on timeout/terminal event/eviction, emits closed stories to registered sink callbacks. Evicted stories emitted with status='evicted'. Event ordering by timestamp with arrival-time tiebreaker. (3) state.py — JSONL persistence for story state (append-only writes, read-back deserialization). Covers AC2, AC3, AC4, AC5, AC8, AC14.
  - [C] **Engine, CLI & MCP Server** (`engine_cli_mcp`)
    Three related orchestration modules: (1) engine.py — top-level async orchestrator that loads config, instantiates and starts all configured sources and sinks, wires source events through correlation engine into story manager, manages full lifecycle (start all → run → stop all). Runs until explicitly stopped. No external broker. (2) cli.py — CLI entry points: `chronicler start` (launches engine), `chronicler status` (running state), `chronicler stories list/show <id>` (open/closed stories), `chronicler replay <file>` (replays JSONL events through engine). (3) mcp_server.py — MCP server using `mcp` Python SDK over stdio transport (per AS5), exposes tools to query open/closed stories, check status, and replay. Covers AC10, AC11, AC13, AC14, AC15.
  - [C] **Core Data Models** (`schemas`)
    Pydantic v2 models for Event (frozen, content-addressable SHA-256 ID from canonical JSON of event_kind/entity_kind/entity_id/actor/timestamp/context/correlation_keys), Story (lifecycle open→closed, ordered events, story_type, correlation_rule, start/end timestamps, duration, event_count, close_reason including 'evicted'/'timeout'/'terminal', optional parent_story_id for chain_by), CorrelationRule (group_by keys, chain_by keys, terminal_event pattern, timeout), and all config models (source configs, sink configs, memory limits, kindex noteworthiness filter). This is the shared data model used by every other component. Also defines the SourceProtocol (start/stop/subscribe) and SinkProtocol (emit/start/stop) as Protocol classes. Covers AC1, AC2, AC16 (schemas.py + sources/base.py + sinks/base.py).
  - [C] **Story Sinks** (`sinks`)
    Four SinkProtocol implementations: (1) disk.py — writes closed stories as JSONL (append-only), supports read-back deserialization. (2) stigmergy.py — placeholder SourceAdapter protocol, emits stories as Signals to Stigmergy. (3) apprentice.py — placeholder, emits stories as training sequences. (4) kindex.py — placeholder, emits noteworthy stories (filtered by config: story_type, min event count, duration threshold per AS12) as knowledge nodes. All sinks are fire-and-forget with error logging; a failing sink never blocks others or crashes the engine. Covers AC7, AC8, AC17.
  - [C] **Event Sources** (`sources`)
    Four SourceProtocol implementations: (1) webhook.py — aiohttp HTTP POST receiver, accepts JSON events, validates and converts to Event model. (2) otlp.py — OTLP/HTTP JSON receiver (NOT gRPC per AS3), accepts spans via aiohttp and converts to Events. (3) file.py — JSONL file tailer using asyncio polling (periodic stat/read per AS10), reads new lines and emits Events. (4) sentinel.py — specialized webhook handler for Sentinel incident lifecycle events with sentinel-specific event_kind prefix (per AS9, reuses webhook HTTP transport). Each source implements start/stop/subscribe, catches all exceptions internally, logs and continues. Covers AC6, AC13.

## Engineering Decisions

### 
**Decision:** 
**Rationale:** The correlation engine and story manager are tightly coupled — correlation creates/finds stories, story manager closes them and feeds chain_by keys back to correlation. Splitting them would create a circular dependency. JSONL state persistence (state.py) is also included here since it's exclusively used by the story manager for story read/write.

### 
**Decision:** 
**Rationale:** The engine is the orchestrator, and CLI/MCP are thin entry points that invoke the engine. CLI and MCP have no logic of their own beyond parsing commands and delegating to the engine's query/control interface. Building them together ensures a consistent control API.

### 
**Decision:** 
**Rationale:** SourceProtocol and SinkProtocol are Protocol classes (structural typing). They belong with the data models since every source/sink/engine depends on them. Placing them in schemas avoids circular imports and makes schemas the single shared foundation.

### 
**Decision:** 
**Rationale:** Per AS9, Sentinel uses the same HTTP POST transport. The sentinel.py module will import/reuse webhook machinery but add sentinel-specific event_kind parsing and lifecycle mapping. Both live in the sources component.

### 
**Decision:** 
**Rationale:** Per AS1, constraints.yaml was not provided. Each component will derive relevant constraints from the spec text and acceptance criteria, defining them explicitly in test files and flagging for review. AC12 is distributed across all components.

### 
**Decision:** 
**Rationale:** Config loading involves YAML parsing, file I/O, and validation logic distinct from pure data model definitions. It depends on schemas for model types but adds its own loading/validation logic. Separating it keeps schemas pure and testable without filesystem.

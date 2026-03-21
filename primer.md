# Chronicler: Event Collection and Story Assembly

## What This Is

Chronicler is a general-purpose event collector and story assembler for the FOSS governance stack (Pact, Baton, Arbiter, Sentinel, Ledger, Constrain, Cartographer, Stigmergy, Apprentice, Kindex).

It sits between raw event sources and pattern consumers. It is NOT an event bus, NOT a message broker, NOT a routing engine. It collects events, correlates them into "stories" at configurable granularities, and emits completed stories to downstream consumers.

## Why It Exists

The governance stack has two feedback loops:

1. **Engineering loop** (closed): Pact → Baton → Sentinel → Pact. Production errors tighten contracts.
2. **Product loop** (open): Events happen in production → ??? → insights → system improvements.

Chronicler closes the product loop by:
- Collecting application-level events (user actions, business events, system events)
- Assembling them into stories (narratives at different granularities)
- Emitting stories to Stigmergy (for pattern discovery) and Apprentice (for learning)

## What a "Story" Is

A story is a correlated group of events that represents a meaningful narrative:

- **Request story**: A single request and its downstream spans/effects (group by trace_id, timeout ~30s)
- **Service story**: A sequence of requests to one component for one entity (group by entity_id + component_id, timeout ~5m)
- **Journey story**: A causal chain across components following a user's path (group by session_id, timeout ~30m, chain by entity_id)

Stories close when their timeout expires or an explicit terminal event arrives.

## Event Sources (Inputs)

Chronicler accepts events from:
1. **HTTP webhooks** — applications POST structured events
2. **OTLP spans** — Baton forwards spans (Chronicler acts as an OTLP collector)
3. **File tail** — watch log files for structured events
4. **Sentinel incidents** — closed incidents emitted as event sequences
5. **Application SDKs** — lightweight client libraries that emit events

## Story Consumers (Outputs)

Completed stories are emitted to:
1. **Stigmergy** — via its SourceAdapter protocol (connect/subscribe/backfill). Stories become Signals with sequential structure in metadata.
2. **Apprentice** — as training sequences (ordered step lists instead of atomic Q&A pairs). Journey-level evaluation data.
3. **Persistence** — stories saved to disk for replay and analysis.

## Architecture Constraints

- **Python 3.12+**, consistent with the rest of the FOSS stack
- **Pydantic v2** for all data models
- **Async throughout** — event ingestion is inherently concurrent
- **No external message broker** — in-process correlation. Projects that already have Kafka/Redis can push to Chronicler via webhook.
- **Pluggable sources and sinks** — Protocol-based adapters for both input and output
- **Configurable correlation** — YAML rules defining grouping keys, timeouts, and chaining
- **Bounded memory** — open stories held in memory with configurable limits and eviction
- **JSONL persistence** — completed stories to disk, consistent with Baton/Sentinel patterns
- **MCP server** — for Claude Code integration (query stories, check status)
- **CLI** — for manual operation and debugging

## What Chronicler Does NOT Do

- Route events to subsystems (that's the Event Service / event bus)
- Replace Baton's signal capture (Baton captures at proxy layer; Chronicler correlates)
- Replace Sentinel's error attribution (Sentinel watches for errors; Chronicler assembles narratives)
- Provide exploratory querying (that's your OTLP backend — Honeycomb, Jaeger, etc.)
- Store events long-term (that's your audit log / data warehouse)

## Integration Points

- **Baton** → Chronicler: OTLP span forwarding (Chronicler as additional OTLP sink)
- **Sentinel** → Chronicler: Closed incidents emitted as event sequences
- **Chronicler** → Stigmergy: Stories as Signals via SourceAdapter protocol
- **Chronicler** → Apprentice: Stories as training sequences
- **Chronicler** → Disk: JSONL persistence for replay

## Relationship to Wander Event Service

The Wander Event Service is a domain-specific event router (Kafka, Effect-TS, five subsystems). Chronicler is the FOSS general-purpose equivalent that adds story assembly. In a Wander deployment, the Event Service handles event routing; Chronicler sits downstream consuming from the Event Service's analytics topic to build stories. In non-Wander deployments, Chronicler accepts events directly.

## Principal Investigator

Jeremy McEntire — ~30 years software engineering, author of the governance stack (Pact, Baton, Arbiter, Sentinel, Ledger, Constrain, Cartographer, Stigmergy, Apprentice, Kindex).

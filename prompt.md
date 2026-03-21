# Chronicler: Event Collection and Story Assembly System

## System Context

Chronicler is a general-purpose event collector and story assembler within the FOSS governance stack. It bridges the gap between raw event sources and pattern discovery systems by collecting heterogeneous events and correlating them into coherent "stories" at configurable granularities. The system serves as a critical feedback loop component, connecting production events to insights that drive governance decisions.

Primary users include:
- FOSS governance stack components (Pact, Baton, Arbiter, Sentinel, Ledger, Constrain, Cartographer, Stigmergy, Apprentice, Kindex)
- Applications posting structured events via SDKs
- Stigmergy for pattern discovery workloads
- Apprentice for learning sequence generation

Chronicler explicitly IS NOT an event bus, message broker, or routing engine. It's a correlation engine with bounded memory and configurable story assembly rules.

## Consequence Map

**Critical (System Failure)**
- Memory leaks from untimed story eviction leading to service degradation
- Data loss from conflicting event authority resolution
- Cross-component data contamination from unclear authority boundaries

**High (Data Integrity)**
- Partial story emission causing downstream pattern corruption
- Event correlation conflicts creating duplicate or fragmented stories
- Story timeout mishandling leading to incomplete training data

**Medium (Performance)**
- Memory pressure from unbounded story accumulation
- Blocking I/O degrading concurrent event ingestion
- Inefficient eviction policies causing story completeness violations

## Failure Archaeology

**What's Gone Wrong:**
- Story timeout handling remains architecturally unclear, creating risk of memory leaks or premature data loss
- Single events matching multiple active story correlation keys creates unresolved conflicts
- Authority resolution for conflicting event data lacks clear precedence rules
- Memory eviction policies may violate story completeness guarantees

**What Was Tried:**
- Bounded memory design with configurable limits identified as requirement
- Three story types specified (Request, Service, Journey) with distinct timeouts
- Async patterns selected for concurrent event handling
- JSONL persistence chosen for stack consistency

**What Was Learned:**
- Clear correlation rules and timeout policies are critical for memory safety
- Data authority model must be established before story assembly begins
- Eviction policies need careful balance between memory bounds and data completeness

## Dependency Landscape

**What This Touches:**
- Baton: forwards OTLP spans, receives story telemetry
- Sentinel: receives incident events, potential story correlation data
- Stigmergy: consumes completed stories via SourceAdapter protocol
- Apprentice: consumes story sequences for training
- File systems: JSONL persistence and file tail monitoring

**What Touches This:**
- Applications: post events via HTTP webhooks and SDKs
- FOSS stack components: generate events during normal operations
- External systems: potential file-based event sources

## Boundary Conditions

**Scope:**
- Event ingestion from 5 defined sources
- Story correlation for 3 story types with specified timeouts
- Output to 3 consumer types
- Bounded memory operation with configurable limits

**Non-Goals:**
- Event bus or message broker functionality
- Long-term event storage
- Exploratory event querying
- External message broker requirements
- Replacement of existing Baton/Sentinel functionality

**Constraints:**
- Python 3.12+ runtime requirement
- Pydantic v2 data validation
- Async patterns throughout
- JSONL format consistency
- Pluggable adapter architecture

## Success Shape

A good solution exhibits:
- **Async Excellence**: Non-blocking event ingestion with concurrent story assembly
- **Memory Discipline**: Bounded operation with predictable eviction behavior
- **Protocol Clarity**: Clean adapter interfaces for sources and sinks
- **Correlation Precision**: Unambiguous story assembly rules with conflict resolution
- **Authority Respect**: Clear data ownership model preventing cross-component contamination
- **Stack Harmony**: Architectural consistency with existing FOSS components

## Done When

- [ ] Request stories (trace_id correlation, 30s timeout) successfully assembled and emitted
- [ ] Service stories (entity_id + component_id correlation, 5m timeout) successfully assembled and emitted  
- [ ] Journey stories (session_id correlation, 30m timeout) successfully assembled and emitted
- [ ] HTTP webhook event ingestion functional with schema validation
- [ ] OTLP span forwarding from Baton integrated and tested
- [ ] File tail monitoring operational for structured log ingestion
- [ ] Sentinel incident event integration complete
- [ ] Application SDK event posting verified
- [ ] Stigmergy story consumption via SourceAdapter protocol validated
- [ ] Apprentice training sequence generation operational
- [ ] JSONL disk persistence working with rotation policies
- [ ] Memory bounds enforced with configured eviction policies
- [ ] Story timeout handling prevents memory leaks
- [ ] Event correlation conflicts resolved via defined authority model
- [ ] Cross-component data authority boundaries enforced

## Trust and Authority Model

Data is classified into PUBLIC tier (events, stories, correlation metadata) with no PII, financial, authentication, or compliance data expected in this event collection system. The Chronicler Core component owns all story assembly and correlation logic domains. Event Ingress components have read-only access to PUBLIC data for forwarding to core processing. Output Adapters have read-only access for story emission to downstream consumers.

Human gates are not required for this PUBLIC-tier system. Canary soaking uses 1-hour base duration with 1000 target requests for any configuration changes. Trust floor is set to 0.10 for basic operational confidence, with authority override floor at 0.40 for story correlation conflict resolution.

## Component Topology

The system consists of five core components: Chronicler Core handles story correlation and timeout management as the central service; Event Ingress provides HTTP webhook endpoints and file monitoring as the ingress service; OTLP Bridge forwards spans from Baton integration; Stigmergy Adapter emits completed stories via SourceAdapter protocol; and Apprentice Adapter formats story sequences for training data.

Data flows include: events from external sources to Event Ingress to Chronicler Core; spans from Baton through OTLP Bridge to Core; completed stories from Core to both output adapters; and persistence data from Core to disk storage. All connections use HTTP or file-based protocols for stack consistency.
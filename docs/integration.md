# Stack Integration

Chronicler connects to the FOSS governance stack at five points.

## Baton → Chronicler (OTLP spans)

Baton forwards OTLP spans to Chronicler's OTLP source. Configure Baton's `baton.yaml`:

```yaml
observability:
  otlp_enabled: true
  otlp_endpoints:
    - name: collector
      endpoint: localhost:4317
      protocol: grpc
    - name: chronicler
      endpoint: localhost:4318
      protocol: grpc
```

Chronicler's OTLP source converts spans to events with `trace_id` and `span_id` as correlation keys.

## Sentinel → Chronicler (incident events)

Sentinel emits incident lifecycle events to Chronicler's webhook source. Configure Sentinel's `sentinel.yaml`:

```yaml
chronicler:
  enabled: true
  endpoint: http://localhost:8080/events
```

Events follow the sequence: `incident.detected` → `incident.triaging` → `incident.remediating` → `incident.resolved` or `incident.escalated`.

## Chronicler → Stigmergy (stories as signals)

Completed stories are emitted to Stigmergy via its `SourceAdapter` protocol. Stigmergy's ART mesh routes stories to workers by familiarity, including sequence-aware familiarity scoring for multi-step narratives.

Configure in `chronicler.yaml`:

```yaml
sinks:
  - type: stigmergy
    endpoint: http://localhost:8400
```

## Chronicler → Apprentice (training sequences)

Completed stories are converted to ordered step sequences for journey-level learning. Apprentice's `StoryCollector` persists them and the `JourneyEvaluator` scores goal completion, step efficiency, backtracking, and consistency.

```yaml
sinks:
  - type: apprentice
    endpoint: http://localhost:8401
```

## Chronicler → Kindex (knowledge capture)

Noteworthy stories (anomalous, high-impact, or pattern-forming) are recorded as knowledge graph nodes via Kindex MCP tools.

```yaml
sinks:
  - type: kindex
    noteworthiness_threshold: 0.7
```

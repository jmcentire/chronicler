# Standard Operating Procedures: Chronicler

## Code Standards

- Python 3.12+ with type hints throughout
- Pydantic v2 for all data models (frozen=True for immutable types)
- Protocol classes for source and sink interfaces (not ABC)
- Async/await for all I/O (asyncio, not threading)
- No external dependencies beyond: pydantic, aiohttp (webhook/OTLP), aiofiles
- JSONL for all persistence (append-only where possible)

## Testing

- pytest with pytest-asyncio for async tests
- One test file per module
- Every constraint (C001–C013) must have at least one dedicated test
- No mocking of core logic — mock only external I/O (network, filesystem)
- No GPU or API keys required for any test

## Project Structure

```
chronicler/
  pyproject.toml
  chronicler.yaml.example
  src/chronicler/
    __init__.py
    schemas.py          # Event, Story, CorrelationRule models
    correlation.py      # Correlation engine
    story_manager.py    # Open story lifecycle, timeout, eviction
    engine.py           # Top-level async orchestrator
    config.py           # YAML config loading and validation
    cli.py              # CLI entry points
    mcp_server.py       # MCP server
    state.py            # JSONL persistence
    sources/
      __init__.py
      base.py           # SourceProtocol
      webhook.py        # HTTP POST receiver
      otlp.py           # OTLP gRPC collector
      file.py           # JSONL file tailer
      sentinel.py       # Sentinel incident receiver
    sinks/
      __init__.py
      base.py           # SinkProtocol
      stigmergy.py      # Emit to Stigmergy
      apprentice.py     # Emit to Apprentice
      disk.py           # JSONL persistence
      kindex.py         # Kindex knowledge capture
  tests/
    ...
```

## Naming Conventions

- Models: PascalCase (Event, Story, CorrelationRule)
- Functions: snake_case
- Constants: UPPER_SNAKE_CASE
- Files: snake_case matching module name

## Error Handling

- Sources: catch all exceptions, log, continue. Never crash the engine.
- Sinks: fire-and-forget with error logging. Unreachable sinks don't block.
- Correlation: invalid events logged and dropped, never crash.
- Config: fail fast at startup. Invalid config is a fatal error.

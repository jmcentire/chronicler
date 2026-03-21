"""Root conftest.py — makes chronicler submodules importable by short names.

Pact-generated contract tests import from bare module names like:
  from schemas import EventKind, ...
  from config import load_config, ...
  from correlation_and_stories import CorrelationEngine, ...

Goodhart tests import via ``from src.X import *`` paths.

This conftest registers all the required aliases so those imports work.
"""

import sys
import types
import importlib


def _register_alias(alias: str, real_module: str) -> None:
    """Register a top-level module alias for a chronicler submodule."""
    if alias in sys.modules:
        return  # Don't clobber existing modules
    try:
        mod = importlib.import_module(real_module)
        sys.modules[alias] = mod
    except ImportError:
        pass


def _register_aggregate_alias(alias: str, real_modules: list[str]) -> None:
    """Register an alias that aggregates exports from multiple submodules."""
    if alias in sys.modules:
        return
    aggregate = types.ModuleType(alias)
    aggregate.__package__ = alias
    for mod_name in real_modules:
        try:
            mod = importlib.import_module(mod_name)
            for attr in dir(mod):
                if not attr.startswith("_"):
                    setattr(aggregate, attr, getattr(mod, attr))
        except ImportError:
            pass
    sys.modules[alias] = aggregate


# Register aliases before any tests import — bare module names
_register_alias("schemas", "chronicler.schemas")
_register_alias("config", "chronicler.config")
_register_alias("correlation_and_stories", "chronicler.correlation")
_register_alias("engine_cli_mcp", "chronicler.engine_cli_mcp")
_register_alias("root", "chronicler.root")

# Goodhart tests use ``from src.X import *`` — create a virtual ``src`` package
# and register its sub-aliases.
if "src" not in sys.modules:
    src_pkg = types.ModuleType("src")
    src_pkg.__path__ = []
    src_pkg.__package__ = "src"
    sys.modules["src"] = src_pkg

_register_alias("src.config", "chronicler.config")
_register_alias("src.schemas", "chronicler.schemas")
_register_alias("src.root", "chronicler.root")
_register_alias("src.engine_cli_mcp", "chronicler.engine_cli_mcp")
_register_alias("src.correlation_and_stories", "chronicler.correlation")

# Sources and sinks aggregate from multiple submodules
_register_aggregate_alias("src.sources", [
    "chronicler.sources.base",
    "chronicler.sources.webhook",
    "chronicler.sources.otlp",
    "chronicler.sources.file",
    "chronicler.sources.sentinel",
])
_register_aggregate_alias("src.sinks", [
    "chronicler.sinks.base",
    "chronicler.sinks.types",
    "chronicler.sinks.engine",
    "chronicler.sinks.disk",
    "chronicler.sinks.stigmergy",
    "chronicler.sinks.apprentice",
    "chronicler.sinks.kindex",
])

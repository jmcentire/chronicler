"""Deployable HTTP receiver for Reeve-style Chronicler events."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiofiles
from aiohttp import web
from pydantic import BaseModel, Field, ValidationError, field_validator

from chronicler import __version__


EVENTS_PATH_KEY = web.AppKey("events_path", str)
API_KEY = web.AppKey("api_key", str)


class ReeveChroniclerEvent(BaseModel):
    """Open event envelope emitted by Reeve's Chronicler client."""

    source: str = Field(min_length=1, max_length=128)
    kind: str = Field(min_length=1, max_length=255)
    timestamp: str = Field(min_length=1, max_length=64)
    trace_id: str | None = Field(default=None, max_length=256)
    entity_id: str | None = Field(default=None, max_length=256)
    session_id: str | None = Field(default=None, max_length=256)
    tenant_id: str | None = Field(default=None, max_length=256)
    component_id: str | None = Field(default=None, max_length=256)
    payload: dict[str, Any] = Field(default_factory=dict)

    @field_validator("kind")
    @classmethod
    def _validate_kind(cls, value: str) -> str:
        if not all(part and part.replace("_", "").replace("-", "").isalnum() for part in value.split(".")):
            raise ValueError("kind must be a dotted identifier")
        return value


def event_id_for(event: ReeveChroniclerEvent) -> str:
    encoded = json.dumps(
        event.model_dump(),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


async def health(_: web.Request) -> web.Response:
    return web.json_response({"status": "ok", "version": __version__})


def _authorized(request: web.Request) -> bool:
    expected = request.app[API_KEY]
    if not expected:
        return True
    return request.headers.get("authorization") == f"Bearer {expected}"


async def receive_event(request: web.Request) -> web.Response:
    if not _authorized(request):
        return web.json_response({"error": "unauthorized"}, status=401)

    if request.content_type != "application/json":
        return web.json_response({"error": "content-type must be application/json"}, status=415)

    try:
        body = await request.json()
    except json.JSONDecodeError:
        return web.json_response({"error": "invalid JSON"}, status=400)

    if not isinstance(body, dict):
        return web.json_response({"error": "body must be a JSON object"}, status=400)

    try:
        event = ReeveChroniclerEvent.model_validate(body)
    except ValidationError as err:
        return web.json_response({"error": "invalid event", "details": err.errors()}, status=400)

    received_at = datetime.now(UTC).isoformat()
    event_id = event_id_for(event)
    record = {
        "event_id": event_id,
        "received_at": received_at,
        "event": event.model_dump(exclude_none=True),
    }

    path = Path(request.app[EVENTS_PATH_KEY])
    path.parent.mkdir(parents=True, exist_ok=True)
    async with aiofiles.open(path, "a", encoding="utf-8") as handle:
        await handle.write(json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=True))
        await handle.write("\n")

    return web.json_response({"status": "accepted", "event_id": event_id}, status=202)


def create_app(*, events_path: str | None = None, api_key: str | None = None) -> web.Application:
    app = web.Application(client_max_size=1_048_576)
    app[EVENTS_PATH_KEY] = events_path or os.environ.get(
        "CHRONICLER_EVENTS_PATH",
        "/var/lib/chronicler/events.jsonl",
    )
    app[API_KEY] = api_key if api_key is not None else os.environ.get("CHRONICLER_API_KEY", "")
    app.router.add_get("/health", health)
    app.router.add_post("/events", receive_event)
    return app


async def _run() -> None:
    host = os.environ.get("CHRONICLER_HTTP_HOST", "0.0.0.0")
    port = int(os.environ.get("CHRONICLER_HTTP_PORT", "8080"))
    runner = web.AppRunner(create_app())
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    while True:
        await asyncio.sleep(3600)


def main() -> None:
    asyncio.run(_run())

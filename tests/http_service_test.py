import json

from aiohttp.test_utils import TestClient, TestServer

from chronicler.http_service import create_app


async def test_health_reports_version():
    async with TestClient(TestServer(create_app(api_key=""))) as client:
        response = await client.get("/health")

        assert response.status == 200
        body = await response.json()
        assert body["status"] == "ok"
        assert body["version"] == "0.3.0"


async def test_receive_reeve_event_writes_jsonl(tmp_path):
    events_path = tmp_path / "events.jsonl"
    event = {
        "source": "reeve",
        "kind": "reeve.action.shipped",
        "timestamp": "2026-05-06T18:00:00Z",
        "trace_id": "trace-1",
        "tenant_id": "tenant-1",
        "payload": {"action_id": "act_1"},
    }

    async with TestClient(TestServer(create_app(events_path=str(events_path), api_key="secret"))) as client:
        response = await client.post(
            "/events",
            json=event,
            headers={"authorization": "Bearer secret"},
        )

        assert response.status == 202
        body = await response.json()
        assert body["status"] == "accepted"

    record = json.loads(events_path.read_text(encoding="utf-8").strip())
    assert record["event_id"] == body["event_id"]
    assert record["event"] == event


async def test_receive_reeve_event_requires_bearer_when_configured(tmp_path):
    async with TestClient(
        TestServer(create_app(events_path=str(tmp_path / "events.jsonl"), api_key="secret"))
    ) as client:
        response = await client.post(
            "/events",
            json={
                "source": "reeve",
                "kind": "reeve.action.shipped",
                "timestamp": "2026-05-06T18:00:00Z",
                "payload": {},
            },
        )

        assert response.status == 401


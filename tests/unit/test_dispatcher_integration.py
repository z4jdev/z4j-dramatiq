"""End-to-end dispatcher integration: real Dramatiq engine + bare dispatcher."""

from __future__ import annotations

import json
from pathlib import Path

import dramatiq
import pytest

from z4j_bare.buffer import BufferStore
from z4j_bare.dispatcher import CommandDispatcher
from z4j_core.transport.frames import CommandFrame, CommandPayload

from z4j_dramatiq.engine import DramatiqEngineAdapter

from tests.unit.conftest import FakeBroker


@pytest.fixture
def buf(tmp_path: Path) -> BufferStore:
    store = BufferStore(path=tmp_path / "buf.sqlite")
    yield store
    store.close()


@pytest.mark.asyncio
async def test_schedule_fire_end_to_end_through_dispatcher(
    broker: FakeBroker, buf: BufferStore,
) -> None:
    saved = dramatiq.broker.global_broker
    dramatiq.broker.global_broker = broker
    try:
        engine = DramatiqEngineAdapter(broker=broker)
        dispatcher = CommandDispatcher(
            engines={"dramatiq": engine},
            schedulers={},
            buffer=buf,
        )

        frame = CommandFrame(
            id="cmd_e2e_dramatiq_01",
            payload=CommandPayload(
                action="schedule.fire",
                target={},
                parameters={
                    "schedule_id": "s1",
                    "schedule_name": "nightly-emails",
                    "task_name": "myapp.tasks.send_email",
                    "engine": "dramatiq",
                    "queue": "default",
                    "args": ["alice@example.com"],
                    "kwargs": {"template": "welcome"},
                    "fire_id": "f1",
                },
            ),
            hmac="deadbeef" * 8,
        )

        await dispatcher.handle(frame)
    finally:
        dramatiq.broker.global_broker = saved

    actor = broker.actors["myapp.tasks.send_email"]
    assert len(actor.sent) == 1
    assert actor.sent[0]["args"] == ("alice@example.com",)
    assert actor.sent[0]["kwargs"] == {"template": "welcome"}

    results = [e for e in buf.drain(10) if e.kind == "command_result"]
    parsed = json.loads(results[0].payload.decode("utf-8"))
    assert parsed["payload"]["status"] == "success"
    assert parsed["payload"]["result"]["engine"] == "dramatiq"

"""Tests for ``DramatiqEngineAdapter.submit_task``.

The bare-agent dispatcher's v1.1.0 ``schedule.fire`` path routes
brain-side scheduler ticks to ``engine.submit_task(...)``. These tests
pin the contract for the Dramatiq engine: when the actor is registered
with the global broker, ``submit_task`` MUST call
``actor.send_with_options(args, kwargs, queue_name)``. When it isn't
registered, the adapter falls back to constructing a raw
``dramatiq.Message`` and ``broker.enqueue(msg)`` so cross-process actor
routing still works.
"""

from __future__ import annotations

from typing import Any

import dramatiq
import pytest

from z4j_dramatiq.engine import DramatiqEngineAdapter

from tests.unit.conftest import FakeActor, FakeBroker


@pytest.fixture
def adapter_with_global_broker(broker: FakeBroker):
    """Register ``broker`` as dramatiq's global broker so the
    adapter's ``dramatiq.get_broker()`` returns it. Restored at
    teardown so other tests don't see the override.
    """
    # dramatiq.set_broker enforces isinstance(broker, Broker); use the
    # private slot to swap in a duck-typed FakeBroker without
    # subclassing the real one (which would drag in dramatiq's full
    # broker init machinery the unit fixtures intentionally skip).
    saved = dramatiq.broker.global_broker
    dramatiq.broker.global_broker = broker
    try:
        adapter = DramatiqEngineAdapter(broker=broker)
        yield adapter, broker
    finally:
        dramatiq.broker.global_broker = saved


@pytest.mark.asyncio
class TestSubmitTask:
    async def test_capability_advertised(self, broker: FakeBroker) -> None:
        adapter = DramatiqEngineAdapter(broker=broker)
        assert "submit_task" in adapter.capabilities()

    async def test_registered_actor_uses_send_with_options(
        self, adapter_with_global_broker,
    ) -> None:
        """If the actor is registered with the global broker, the
        adapter prefers ``actor.send_with_options`` so the routing
        rules of the actor (queue_name) are respected.
        """
        adapter, broker = adapter_with_global_broker
        # Conftest's broker fixture pre-registers myapp.tasks.send_email.
        actor = broker.actors["myapp.tasks.send_email"]
        assert actor.sent == []

        result = await adapter.submit_task(
            "myapp.tasks.send_email",
            args=("alice@example.com",),
            kwargs={"template": "welcome"},
        )

        assert result.status == "success"
        assert result.result["engine"] == "dramatiq"
        assert len(actor.sent) == 1
        assert actor.sent[0]["args"] == ("alice@example.com",)
        assert actor.sent[0]["kwargs"] == {"template": "welcome"}

    async def test_queue_kwarg_overrides_actor_default(
        self, adapter_with_global_broker,
    ) -> None:
        adapter, broker = adapter_with_global_broker
        actor = broker.actors["myapp.tasks.send_email"]
        result = await adapter.submit_task(
            "myapp.tasks.send_email",
            args=(),
            kwargs={},
            queue="high-priority",
        )
        assert result.status == "success"
        assert actor.sent[-1]["queue_name"] == "high-priority"

    async def test_unregistered_actor_uses_message_fallback(
        self, broker: FakeBroker,
    ) -> None:
        """If the actor isn't in the global broker (cross-process
        case), the adapter constructs a raw dramatiq.Message and
        calls ``broker.enqueue(msg)``. We swap in a broker that
        records enqueue calls and verify the fallback path.
        """
        enqueued: list[Any] = []

        class RecordingBroker(FakeBroker):
            def enqueue(self_inner, msg) -> None:  # noqa: N805
                enqueued.append(msg)

        rec_broker = RecordingBroker()
        # Don't register any actor -> get_actor will raise ->
        # adapter falls back to Message + broker.enqueue.
        saved = dramatiq.broker.global_broker
        dramatiq.broker.global_broker = rec_broker
        try:
            adapter = DramatiqEngineAdapter(broker=rec_broker)
            result = await adapter.submit_task(
                "remote.tasks.crunch_numbers",
                args=(1, 2, 3),
                kwargs={"factor": 10},
                queue="batch",
            )
        finally:
            dramatiq.broker.global_broker = saved

        assert result.status == "success"
        assert len(enqueued) == 1
        msg = enqueued[0]
        assert msg.actor_name == "remote.tasks.crunch_numbers"
        assert msg.queue_name == "batch"
        assert msg.args == (1, 2, 3)
        assert msg.kwargs == {"factor": 10}

    async def test_broker_exception_returns_failed_result(
        self, broker: FakeBroker,
    ) -> None:
        class ExplodingBroker(FakeBroker):
            def enqueue(self_inner, msg) -> None:  # noqa: ARG002, N805
                raise RuntimeError("rabbitmq down")

        exp_broker = ExplodingBroker()
        saved = dramatiq.broker.global_broker
        dramatiq.broker.global_broker = exp_broker
        try:
            adapter = DramatiqEngineAdapter(broker=exp_broker)
            result = await adapter.submit_task("not.registered.task")
        finally:
            dramatiq.broker.global_broker = saved

        assert result.status == "failed"
        assert "rabbitmq down" in (result.error or "")

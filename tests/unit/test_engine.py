"""Smoke + protocol-conformance tests for :class:`DramatiqEngineAdapter`."""

from __future__ import annotations

import asyncio
import inspect

import pytest

from z4j_core.protocols import QueueEngineAdapter

from z4j_dramatiq.capabilities import (
    ABORTABLE_CAPABILITIES,
    DEFAULT_CAPABILITIES,
)
from z4j_dramatiq.engine import DramatiqEngineAdapter


class TestProtocolConformance:
    def test_satisfies_queue_engine_adapter(self, broker):
        adapter = DramatiqEngineAdapter(broker=broker)
        assert isinstance(adapter, QueueEngineAdapter)

    def test_engine_name_is_dramatiq(self, broker):
        adapter = DramatiqEngineAdapter(broker=broker)
        assert adapter.name == "dramatiq"


class TestCapabilityPromotion:
    def test_default_capabilities_without_abortable(self, broker):
        adapter = DramatiqEngineAdapter(broker=broker)
        assert adapter.capabilities() == set(DEFAULT_CAPABILITIES)
        assert "cancel_task" not in adapter.capabilities()

    def test_capabilities_promoted_with_abortable(self, broker_with_abortable):
        adapter = DramatiqEngineAdapter(broker=broker_with_abortable)
        assert adapter.capabilities() == set(ABORTABLE_CAPABILITIES)
        assert "cancel_task" in adapter.capabilities()


class TestUnsupportedActionsHonest:
    @pytest.mark.asyncio
    async def test_bulk_retry_now_succeeds(self, broker):
        """Promoted to shipped feature in v2026.5."""
        adapter = DramatiqEngineAdapter(broker=broker)
        result = await adapter.bulk_retry({}, max=10)
        assert result.status == "success"
        assert "retried" in (result.result or {})

    @pytest.mark.asyncio
    async def test_dlq_requires_actor_name(self, broker):
        """Per-task DLQ without actor_name context fails loudly
        (the brain is expected to pass actor_name from its snapshot)."""
        adapter = DramatiqEngineAdapter(broker=broker)
        result = await adapter.requeue_dead_letter("any-id")
        assert result.status == "failed"
        assert "actor_name" in result.error

    @pytest.mark.asyncio
    async def test_restart_worker_returns_failed_with_explanation(self, broker):
        adapter = DramatiqEngineAdapter(broker=broker)
        result = await adapter.restart_worker("worker-1")
        assert result.status == "failed"
        assert "remote-control" in result.error.lower()

    @pytest.mark.asyncio
    async def test_rate_limit_returns_failed_with_explanation(self, broker):
        adapter = DramatiqEngineAdapter(broker=broker)
        result = await adapter.rate_limit("myapp.tasks.work", "10/m")
        assert result.status == "failed"
        assert "throttler" in result.error.lower() or "v1.1" in result.error


class TestAsyncMethodShape:
    @pytest.mark.parametrize(
        "method_name",
        [
            "discover_tasks", "list_queues", "list_workers", "get_task",
            "retry_task", "cancel_task", "purge_queue",
            "bulk_retry", "requeue_dead_letter", "rate_limit",
            "restart_worker",
        ],
    )
    def test_method_is_coroutine(self, broker, method_name):
        adapter = DramatiqEngineAdapter(broker=broker)
        assert inspect.iscoroutinefunction(getattr(adapter, method_name))

    @pytest.mark.parametrize(
        "method_name", ["subscribe_events", "subscribe_registry_changes"],
    )
    def test_subscribe_methods_are_async_generators(self, broker, method_name):
        adapter = DramatiqEngineAdapter(broker=broker)
        assert inspect.isasyncgenfunction(getattr(adapter, method_name))


class TestSubscribeEventsDelivery:
    @pytest.mark.asyncio
    async def test_subscribe_events_yields_enqueued_event(self, broker):
        from datetime import UTC, datetime
        from uuid import uuid4

        from z4j_core.models import Event, EventKind

        adapter = DramatiqEngineAdapter(broker=broker)
        placeholder = uuid4()
        ev = Event(
            id=uuid4(),
            project_id=placeholder,
            agent_id=placeholder,
            engine="dramatiq",
            task_id="t-1",
            kind=EventKind.TASK_SUCCEEDED,
            occurred_at=datetime.now(UTC),
            data={},
        )
        adapter._enqueue_event(ev)

        async def _first():
            async for e in adapter.subscribe_events():
                return e
            raise AssertionError("no event yielded")

        got = await asyncio.wait_for(_first(), timeout=1.0)
        assert got.task_id == "t-1"


class TestDiscoverWalksBrokerActors:
    @pytest.mark.asyncio
    async def test_discover_returns_one_definition_per_actor(self, broker):
        adapter = DramatiqEngineAdapter(broker=broker)
        defs = await adapter.discover_tasks()
        assert len(defs) == 1
        assert defs[0].name == "myapp.tasks.send_email"
        assert defs[0].engine == "dramatiq"


class TestHealthDictShape:
    def test_health_includes_broker_type_and_abortable_marker(self, broker):
        adapter = DramatiqEngineAdapter(broker=broker)
        health = adapter.get_health()
        assert "broker_type" in health
        assert "broker_connected" in health
        assert "abortable_installed" in health
        assert health["abortable_installed"] is False

    def test_health_marks_abortable_when_present(self, broker_with_abortable):
        adapter = DramatiqEngineAdapter(broker=broker_with_abortable)
        health = adapter.get_health()
        assert health["abortable_installed"] is True

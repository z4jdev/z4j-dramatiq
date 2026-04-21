"""Dramatiq adapter tests for ``bulk_retry`` + ``requeue_dead_letter``."""

from __future__ import annotations

import pytest

from z4j_dramatiq.actions.bulk_retry import bulk_retry_action
from z4j_dramatiq.actions.dlq import requeue_dead_letter_action


class TestBulkRetry:
    @pytest.mark.asyncio
    async def test_retries_every_id_with_actor_mapping(self, broker):
        result = await bulk_retry_action(
            broker,
            filter={
                "task_ids": ["msg-1", "msg-2"],
                "actors": {
                    "msg-1": "myapp.tasks.send_email",
                    "msg-2": "myapp.tasks.send_email",
                },
            },
            max=10,
        )
        assert result.status == "success"
        assert result.result["retried"] == 2
        actor = broker.get_actor("myapp.tasks.send_email")
        assert len(actor.sent) == 2

    @pytest.mark.asyncio
    async def test_missing_actor_mapping_skipped(self, broker):
        result = await bulk_retry_action(
            broker,
            filter={"task_ids": ["msg-1"], "actors": {}},
            max=10,
        )
        assert result.status == "success"
        assert result.result["retried"] == 0
        assert result.result["skipped"] == 1

    @pytest.mark.asyncio
    async def test_forwards_overrides(self, broker):
        result = await bulk_retry_action(
            broker,
            filter={
                "task_ids": ["msg-1"],
                "actors": {"msg-1": "myapp.tasks.send_email"},
                "args": {"msg-1": ["payload"]},
                "kwargs": {"msg-1": {"to": "x@example.com"}},
                "queues": {"msg-1": "urgent"},
            },
            max=10,
        )
        assert result.status == "success"
        actor = broker.get_actor("myapp.tasks.send_email")
        last = actor.sent[-1]
        assert last["args"] == ("payload",)
        assert last["kwargs"] == {"to": "x@example.com"}
        assert last["queue_name"] == "urgent"

    @pytest.mark.asyncio
    async def test_capped_flag_set_when_exceeds_absolute_max(self, broker):
        ids = [f"m-{i}" for i in range(20_000)]
        result = await bulk_retry_action(
            broker,
            filter={"task_ids": ids},
            max=20_000,
        )
        assert result.result["capped"] is True


class TestDlqRequeue:
    @pytest.mark.asyncio
    async def test_fallback_when_no_native_dlq(self, broker):
        # The FakeBroker has no ``get_dead_letter`` method - the
        # action falls back to generic retry.
        from z4j_dramatiq.actions.dlq import requeue_dead_letter_action
        result = await requeue_dead_letter_action(
            broker,
            task_id="msg-1",
            actor_name="myapp.tasks.send_email",
            override_args=("hello",),
        )
        assert result.status == "success"
        assert result.result["source"] == "dlq_fallback"

    @pytest.mark.asyncio
    async def test_missing_actor_name_fails_loudly(self, broker):
        result = await requeue_dead_letter_action(
            broker, task_id="msg-1",
        )
        assert result.status == "failed"
        assert "actor_name" in result.error


class TestCapabilityPromotion:
    def test_bulk_retry_and_dlq_in_default(self):
        from z4j_dramatiq.capabilities import DEFAULT_CAPABILITIES
        assert "bulk_retry" in DEFAULT_CAPABILITIES
        assert "requeue_dead_letter" in DEFAULT_CAPABILITIES

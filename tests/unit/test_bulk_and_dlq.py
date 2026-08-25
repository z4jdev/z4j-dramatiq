"""Dramatiq tests for intentionally unavailable DLQ-derived actions."""

from __future__ import annotations

import pytest
from z4j_dramatiq.actions.bulk_retry import bulk_retry_action
from z4j_dramatiq.actions.dlq import requeue_dead_letter_action


@pytest.mark.asyncio
async def test_bulk_retry_fails_closed_without_invoking_client_actor(broker):
    actor = broker.get_actor("myapp.tasks.send_email")
    result = await bulk_retry_action(
        broker,
        filter={
            "task_ids": ["msg-1"],
            "actors": {"msg-1": "myapp.tasks.send_email"},
            "args": {"msg-1": ["attacker-input"]},
        },
    )
    assert result.status == "failed"
    assert "no recoverable dead-letter API" in (result.error or "")
    assert actor.sent == []


@pytest.mark.asyncio
async def test_requeue_dead_letter_fails_even_for_fictional_interface(broker):
    """Do not revive the non-existent API through structural duck typing."""

    class DeadLetter:
        def __init__(self):
            self.called = False

        def resurrect(self, _task_id):
            self.called = True

    middleware = DeadLetter()
    broker.middleware.append(middleware)
    broker.get_dead_letter = lambda _task_id: ["fake"]

    result = await requeue_dead_letter_action(broker, task_id="msg-1")
    assert result.status == "failed"
    assert "stock Dramatiq" in (result.error or "")
    assert middleware.called is False


def test_dlq_derived_capabilities_are_absent():
    from z4j_dramatiq.capabilities import DEFAULT_CAPABILITIES

    assert "bulk_retry" not in DEFAULT_CAPABILITIES
    assert "requeue_dead_letter" not in DEFAULT_CAPABILITIES

"""Action-helper unit tests for the Dramatiq adapter."""

from __future__ import annotations

import pytest
from z4j_dramatiq.actions.cancel import cancel_task_action
from z4j_dramatiq.actions.purge import purge_queue_action
from z4j_dramatiq.actions.retry import retry_task_action


class TestRetry:
    @pytest.mark.asyncio
    async def test_retry_with_actor_name_resends(self, broker):
        result = await retry_task_action(
            broker,
            task_id="msg-1",
            actor_name="myapp.tasks.send_email",
            override_args=("hello",),
            override_kwargs={"to": "x@example.com"},
        )
        assert result.status == "success"
        actor = broker.get_actor("myapp.tasks.send_email")
        assert actor.sent[-1]["args"] == ("hello",)
        assert actor.sent[-1]["kwargs"] == {"to": "x@example.com"}

    @pytest.mark.asyncio
    async def test_retry_without_overrides_fails_closed_when_no_dlq(self, broker):
        # 1.7.1: no overrides -> requeue-by-reference via the DLQ. The fake
        # broker has no dead-letter queue, so it must fail CLOSED rather
        # than re-send the actor with an empty payload.
        result = await retry_task_action(broker, task_id="msg-1")
        assert result.status == "failed"
        assert "dead-letter queue" in result.error
        assert "retry with different inputs" in result.error

    @pytest.mark.asyncio
    async def test_retry_unknown_actor_fails_on_override_path(self, broker):
        # With operator overrides the actor is resolved for the re-send;
        # an unknown actor fails with "not registered".
        result = await retry_task_action(
            broker,
            task_id="msg-1",
            actor_name="ghost.tasks.never",
            override_args=("x",),
            override_kwargs={},  # RH2: both halves required to reach the actor path
        )
        assert result.status == "failed"
        assert "not registered" in result.error

    @pytest.mark.asyncio
    async def test_retry_to_different_queue_uses_send_with_options(self, broker):
        result = await retry_task_action(
            broker,
            task_id="msg-1",
            actor_name="myapp.tasks.send_email",
            queue_name="urgent",
            override_args=("urgent-payload",),
            override_kwargs={},  # RH2: both override halves required
        )
        assert result.status == "success"
        actor = broker.get_actor("myapp.tasks.send_email")
        # send_with_options recorded queue_name override
        assert actor.sent[-1].get("queue_name") == "urgent"


class TestCancelGatedByAbortable:
    @pytest.mark.asyncio
    async def test_cancel_without_abortable_fails(self, broker):
        result = await cancel_task_action(broker, task_id="msg-1")
        assert result.status == "failed"
        assert "Abortable" in result.error

    @pytest.mark.asyncio
    async def test_cancel_with_abortable_calls_dramatiq_abort(
        self,
        broker_with_abortable,
        monkeypatch,
    ):
        # B15: with the Abortable middleware present AND dramatiq-abort
        # installed, cancel must actually call abort(message_id) -- the
        # pre-fix code imported the nonexistent dramatiq.middleware.Abortable
        # and always failed even though the engine advertised the capability.
        import sys
        import types

        calls: list[str] = []
        fake = types.ModuleType("dramatiq_abort")
        fake.abort = calls.append  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "dramatiq_abort", fake)

        result = await cancel_task_action(broker_with_abortable, task_id="msg-9")
        assert result.status == "success"
        assert calls == ["msg-9"]

    @pytest.mark.asyncio
    async def test_cancel_with_abortable_but_package_missing_fails_clearly(
        self,
        broker_with_abortable,
        monkeypatch,
    ):
        # Abortable middleware present but dramatiq-abort NOT installed:
        # a clear, actionable error (not a silent success or opaque crash).
        import sys

        monkeypatch.setitem(sys.modules, "dramatiq_abort", None)  # force ImportError
        result = await cancel_task_action(broker_with_abortable, task_id="msg-9")
        assert result.status == "failed"
        assert "dramatiq-abort" in result.error


class TestPurge:
    @pytest.mark.asyncio
    async def test_purge_with_force_skips_token(self, broker):
        result = await purge_queue_action(
            broker,
            queue_name="default",
            force=True,
        )
        assert result.status == "success"
        assert "default" in broker.purged

    @pytest.mark.asyncio
    async def test_purge_without_token_refused(self, broker):
        result = await purge_queue_action(broker, queue_name="default")
        assert result.status == "failed"
        assert "confirm_token" in result.error

    @pytest.mark.asyncio
    async def test_purge_with_correct_token_succeeds(self, broker, monkeypatch):
        from z4j_core.purge_token import legacy_purge_confirm_token

        # No Z4J_HMAC_SECRET here -> only the legacy unkeyed token is
        # available, which is OFF by default now; opt into the grace window.
        monkeypatch.setenv("Z4J_ACCEPT_LEGACY_PURGE_TOKEN", "1")
        token = legacy_purge_confirm_token(
            queue_name="default",
            queue_depth=broker.queue_counts["default"],
        )
        result = await purge_queue_action(
            broker,
            queue_name="default",
            confirm_token=token,
        )
        assert result.status == "success"

    @pytest.mark.asyncio
    async def test_purge_above_threshold_refused_without_force(
        self,
        broker,
        monkeypatch,
    ):
        monkeypatch.setenv("Z4J_PURGE_THRESHOLD", "2")
        broker.queue_counts["hot"] = 50
        from z4j_core.purge_token import legacy_purge_confirm_token

        token = legacy_purge_confirm_token(
            queue_name="hot",
            queue_depth=broker.queue_counts["hot"],
        )
        result = await purge_queue_action(
            broker,
            queue_name="hot",
            confirm_token=token,
        )
        assert result.status == "failed"
        assert "Z4J_PURGE_THRESHOLD" in result.error

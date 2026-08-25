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
        assert "no recoverable dead-letter API" in result.error
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
    async def test_retry_to_different_queue_fails_without_sending(self, broker):
        actor = broker.get_actor("myapp.tasks.send_email")
        result = await retry_task_action(
            broker,
            task_id="msg-1",
            actor_name="myapp.tasks.send_email",
            queue_name="urgent",
            override_args=("urgent-payload",),
            override_kwargs={},  # RH2: both override halves required
        )
        assert result.status == "failed"
        assert "cannot override" in (result.error or "")
        assert actor.sent == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize(("option", "value"), [("eta", 123.0), ("priority", 9)])
    async def test_unsupported_retry_option_does_not_send(
        self,
        broker,
        option,
        value,
    ):
        actor = broker.get_actor("myapp.tasks.send_email")
        result = await retry_task_action(
            broker,
            task_id="msg-1",
            actor_name="myapp.tasks.send_email",
            override_args=(),
            override_kwargs={},
            **{option: value},
        )
        assert result.status == "failed"
        assert option in (result.error or "")
        assert actor.sent == []


class TestCancelPendingOnly:
    @pytest.mark.asyncio
    async def test_cancel_without_abortable_fails(self, broker):
        result = await cancel_task_action(broker, task_id="msg-1")
        assert result.status == "failed"
        assert "no built-in cancel" in result.error

    @pytest.mark.asyncio
    async def test_cancel_with_abortable_requests_pending_only_cancel(
        self,
        broker_with_abortable,
        monkeypatch,
    ):
        from z4j_dramatiq.actions import cancel as cancel_module

        abortable = broker_with_abortable.middleware[-1]

        class AbortMode:
            CANCEL = object()
            ABORT = object()

        calls: list[tuple[str, object, object]] = []

        def abort(task_id, *, middleware, mode):
            calls.append((task_id, middleware, mode))

        monkeypatch.setattr(
            cancel_module,
            "_load_abort_api",
            lambda: (type(abortable), abort, AbortMode),
        )

        result = await cancel_task_action(broker_with_abortable, task_id="msg-9")
        assert result.status == "success"
        assert result.result == {
            "task_id": "msg-9",
            "cancelled": True,
            "pending_only": True,
        }
        assert calls == [("msg-9", abortable, AbortMode.CANCEL)]
        assert all(mode is not AbortMode.ABORT for _, _, mode in calls)

    @pytest.mark.asyncio
    async def test_cancel_with_abortable_but_package_missing_fails_clearly(
        self,
        broker_with_abortable,
        monkeypatch,
    ):
        # Abortable middleware present but dramatiq-abort NOT installed:
        # a clear, actionable error (not a silent success or opaque crash).
        from z4j_dramatiq.actions import cancel as cancel_module

        def missing_abort_api():
            raise ImportError("dramatiq-abort missing")

        monkeypatch.setattr(cancel_module, "_load_abort_api", missing_abort_api)
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

    @pytest.mark.asyncio
    async def test_purge_guard_counts_ready_delayed_and_dead(
        self,
        broker,
        monkeypatch,
    ):
        monkeypatch.setenv("Z4J_PURGE_THRESHOLD", "10000")
        broker.get_queue_message_counts = lambda _queue: (0, 10001, 0)
        result = await purge_queue_action(broker, queue_name="default")
        assert result.status == "failed"
        assert "depth 10001" in (result.error or "")
        assert broker.purged == []

    @pytest.mark.asyncio
    async def test_redis_depth_counts_ready_and_delay_queue(self, broker, monkeypatch):
        pytest.importorskip("redis", reason="requires z4j-dramatiq[redis]")
        from dramatiq.brokers.redis import dq_name
        from z4j_core.purge_token import legacy_purge_confirm_token

        broker.get_queue_message_counts = None
        counts = {"default": 2, dq_name("default"): 3}
        broker.do_qsize = lambda queue: counts[queue]
        monkeypatch.setenv("Z4J_ACCEPT_LEGACY_PURGE_TOKEN", "1")
        token = legacy_purge_confirm_token(queue_name="default", queue_depth=5)
        result = await purge_queue_action(
            broker,
            queue_name="default",
            confirm_token=token,
        )
        assert result.status == "success"
        assert result.result["purged"] == 5

"""Adversarial test pass for z4j-dramatiq.

Same threat-model lens as the RQ + Celery sweeps. Each test name
doubles as the threat statement.
"""

from __future__ import annotations

from typing import Any

import pytest

from z4j_core.models import EventKind
from z4j_core.redaction.engine import RedactionEngine

from z4j_dramatiq.events.mapper import build_event
from z4j_dramatiq.events.middleware import Z4JMiddleware


# ---------------------------------------------------------------------------
# T1 - mapper never forwards args/kwargs (no-pickle / no-leak rule)
# ---------------------------------------------------------------------------


class TestMapperDoesNotForwardArgsOrKwargs:
    def test_secret_in_kwargs_does_not_leak(self, message):
        message.kwargs = {
            "stripe_secret_key": "sk_live_DO_NOT_LEAK",
            "password": "hunter2",
        }
        ev = build_event(
            kind=EventKind.TASK_STARTED,
            message=message,
            redaction=RedactionEngine(),
        )
        flat = repr(ev.data)
        assert "sk_live_DO_NOT_LEAK" not in flat
        assert "hunter2" not in flat

    def test_secret_in_positional_args_does_not_leak(self, message):
        message.args = ("user-1", "Bearer eyJsensitive.payload.sig")
        ev = build_event(
            kind=EventKind.TASK_STARTED,
            message=message,
            redaction=RedactionEngine(),
        )
        flat = repr(ev.data)
        assert "Bearer eyJsensitive" not in flat


# ---------------------------------------------------------------------------
# T2 - exception payload bounded
# ---------------------------------------------------------------------------


class TestExceptionPayloadBounded:
    def test_extreme_traceback_truncated(self, message):
        ev = build_event(
            kind=EventKind.TASK_FAILED,
            message=message,
            redaction=RedactionEngine(),
            exception=RuntimeError("X" * 1_000_000),
        )
        assert len(ev.data["exception"].encode("utf-8")) <= 5_000


# ---------------------------------------------------------------------------
# T3 - middleware never raises into Dramatiq
# ---------------------------------------------------------------------------


class TestMiddlewareIsBoundary:
    def test_emit_swallows_internal_exception(
        self, message, broker, monkeypatch,
    ):
        from z4j_dramatiq.events import middleware as mw_mod

        def _boom(**_kw: Any) -> None:
            raise RuntimeError("synthetic mapper bug")

        monkeypatch.setattr(mw_mod, "build_event", _boom)
        sink_called: list[Any] = []
        mw = Z4JMiddleware(
            sink=lambda ev: sink_called.append(ev),
            redaction=RedactionEngine(),
        )
        # All four hook entry points must NOT raise.
        mw.after_enqueue(broker, message, None)
        mw.before_process_message(broker, message)
        mw.after_process_message(broker, message, exception=None)
        mw.after_process_message(broker, message, exception=RuntimeError())
        # Sink got nothing - every emit failed cleanly.
        assert sink_called == []


# ---------------------------------------------------------------------------
# T4 - capabilities() is honest about Abortable presence
# ---------------------------------------------------------------------------


class TestCapabilitiesHonestAboutAbortable:
    def test_no_cancel_without_abortable(self, broker):
        from z4j_dramatiq.engine import DramatiqEngineAdapter
        adapter = DramatiqEngineAdapter(broker=broker)
        assert "cancel_task" not in adapter.capabilities()

    def test_cancel_appears_with_abortable(self, broker_with_abortable):
        from z4j_dramatiq.engine import DramatiqEngineAdapter
        adapter = DramatiqEngineAdapter(broker=broker_with_abortable)
        assert "cancel_task" in adapter.capabilities()

    @pytest.mark.asyncio
    async def test_cancel_action_refuses_without_abortable(self, broker):
        """Even if a brain ignores capabilities and dispatches cancel,
        the action must fail loudly rather than silently no-op."""
        from z4j_dramatiq.actions.cancel import cancel_task_action
        result = await cancel_task_action(broker, task_id="any-id")
        assert result.status == "failed"
        assert "Abortable" in result.error


# ---------------------------------------------------------------------------
# T5 - purge confirm-token can't be bypassed
# ---------------------------------------------------------------------------


class TestPurgeTokenIsAuthoritative:
    @pytest.mark.asyncio
    async def test_empty_string_token_rejected(self, broker):
        from z4j_dramatiq.actions.purge import purge_queue_action
        result = await purge_queue_action(
            broker, queue_name="default", confirm_token="",
        )
        assert result.status == "failed"

    @pytest.mark.asyncio
    async def test_garbage_token_rejected(self, broker):
        from z4j_dramatiq.actions.purge import purge_queue_action
        result = await purge_queue_action(
            broker, queue_name="default", confirm_token="0" * 64,
        )
        assert result.status == "failed"


# ---------------------------------------------------------------------------
# T6 - connect/disconnect lifecycle is churn-safe
# ---------------------------------------------------------------------------


class TestEngineLifecycleChurn:
    def test_repeated_connect_disconnect_is_safe(self, broker):
        from z4j_dramatiq.engine import DramatiqEngineAdapter
        adapter = DramatiqEngineAdapter(broker=broker)
        for _ in range(5):
            adapter.connect_signals()
            adapter.disconnect_signals()
        assert adapter._middleware is None
        # Middleware list on the broker should be empty after teardown.
        assert all(
            "Z4J" not in type(mw).__name__
            for mw in (broker.middleware or [])
        )


# ---------------------------------------------------------------------------
# T7 - capabilities() doesn't claim methods that aren't implemented
# ---------------------------------------------------------------------------


class TestCapabilitiesDoNotLie:
    def test_every_advertised_capability_has_method(self, broker_with_abortable):
        from z4j_dramatiq.engine import DramatiqEngineAdapter
        adapter = DramatiqEngineAdapter(broker=broker_with_abortable)
        method_for = {
            "submit_task": "submit_task",
            "retry_task": "retry_task",
            "cancel_task": "cancel_task",
            "purge_queue": "purge_queue",
            "bulk_retry": "bulk_retry",
            "requeue_dead_letter": "requeue_dead_letter",
        }
        for cap in adapter.capabilities():
            method = method_for.get(cap)
            assert method, f"capability {cap!r} has no documented method binding"
            assert callable(getattr(adapter, method))


# ---------------------------------------------------------------------------
# T8 - R7 H-2 regression: retry path NEVER reads broker-stored message body
# ---------------------------------------------------------------------------


class TestRetryDoesNotReadBrokerStoredMessageBody:
    """R7 audit finding H-2 (pickle-in-retry) regression guard.

    Dramatiq's default MessageEncoder is JSON, not pickle, but an
    operator may swap in any encoder (including pickle). The retry
    surface must NEVER read the broker-stored Message body - it
    must rely solely on brain-supplied actor_name + override
    args/kwargs - regardless of which encoder is in play. This
    test asserts the structural invariant by spying on every
    likely message-fetch entry point on the broker.
    """

    @pytest.mark.asyncio
    async def test_retry_does_not_call_any_broker_message_fetch(
        self, broker, monkeypatch,
    ):
        from z4j_dramatiq.actions.retry import retry_task_action

        fetch_calls: list[str] = []

        # Add spies on every broker method that could plausibly
        # return a stored Message. If retry_task_action calls any
        # of them, the test fails - that would mean the retry path
        # is one bug away from triggering pickle/JSON deserialization
        # of attacker-controlled payload.
        for suspect in (
            "get_message",
            "fetch_message",
            "peek_message",
            "consume",
            "get_dead_letter",
        ):
            def _make_spy(name: str) -> Any:
                def _spy(*_a: Any, **_kw: Any) -> Any:
                    fetch_calls.append(name)
                    raise AssertionError(
                        f"retry called broker.{name}() - "
                        "H-2 regression",
                    )
                return _spy
            monkeypatch.setattr(broker, suspect, _make_spy(suspect),
                                raising=False)

        result = await retry_task_action(
            broker,
            task_id="msg-1",
            actor_name="myapp.tasks.send_email",
            override_args=("safe",),
            override_kwargs={"to": "ops@example.com"},
        )

        assert result.status == "success"
        assert fetch_calls == [], (
            f"retry must not touch broker-stored bodies; "
            f"called: {fetch_calls}"
        )

    @pytest.mark.asyncio
    async def test_retry_refuses_without_actor_name(self, broker):
        """Without a brain-supplied actor_name we cannot reconstruct
        the target. We must fail loudly rather than scan the broker
        for the message and infer the actor from the stored body.
        """
        from z4j_dramatiq.actions.retry import retry_task_action
        result = await retry_task_action(broker, task_id="msg-1")
        assert result.status == "failed"
        assert "actor_name" in result.error

    @pytest.mark.asyncio
    async def test_dlq_requeue_refuses_without_actor_name(self, broker):
        """Same invariant for the DLQ resurrection path."""
        from z4j_dramatiq.actions.dlq import requeue_dead_letter_action
        result = await requeue_dead_letter_action(broker, task_id="msg-1")
        assert result.status == "failed"
        assert "actor_name" in result.error

    @pytest.mark.asyncio
    async def test_bulk_retry_skips_ids_without_actor_mapping(self, broker):
        """The bulk path also refuses to guess - missing actor in the
        brain-supplied mapping yields a ``skipped`` count, not a
        broker-side lookup.
        """
        from z4j_dramatiq.actions.bulk_retry import bulk_retry_action
        result = await bulk_retry_action(
            broker,
            filter={"task_ids": ["msg-1", "msg-2"], "actors": {}},
            max=10,
        )
        assert result.status == "success"
        assert result.result["retried"] == 0
        assert result.result["skipped"] == 2

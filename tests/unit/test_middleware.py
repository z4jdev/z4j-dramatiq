"""Tests for :class:`Z4JMiddleware` event capture + boundary safety."""

from __future__ import annotations

from typing import Any

import pytest

from z4j_core.models import Event, EventKind
from z4j_core.redaction.engine import RedactionEngine

from z4j_dramatiq.events.middleware import Z4JMiddleware


class _Sink:
    def __init__(self) -> None:
        self.events: list[Event] = []

    def __call__(self, event: Event) -> None:
        self.events.append(event)


@pytest.fixture
def sink() -> _Sink:
    return _Sink()


@pytest.fixture
def middleware(sink: _Sink) -> Z4JMiddleware:
    return Z4JMiddleware(sink=sink, redaction=RedactionEngine())


class TestEventEmission:
    def test_after_enqueue_emits_received(self, middleware, sink, message, broker):
        middleware.after_enqueue(broker, message, None)
        assert len(sink.events) == 1
        assert sink.events[0].kind == EventKind.TASK_RECEIVED

    def test_before_process_emits_started(self, middleware, sink, message, broker):
        middleware.before_process_message(broker, message)
        assert sink.events[-1].kind == EventKind.TASK_STARTED

    def test_after_process_emits_succeeded_when_no_exc(
        self, middleware, sink, message, broker,
    ):
        middleware.after_process_message(broker, message, exception=None)
        assert sink.events[-1].kind == EventKind.TASK_SUCCEEDED

    def test_after_process_emits_failed_when_exc(
        self, middleware, sink, message, broker,
    ):
        middleware.after_process_message(
            broker, message, exception=RuntimeError("kaboom"),
        )
        ev = sink.events[-1]
        assert ev.kind == EventKind.TASK_FAILED
        assert "kaboom" in ev.data["exception"]


class TestBoundarySafety:
    """A bug in z4j must NEVER raise into Dramatiq's middleware chain."""

    def test_emit_swallows_internal_exception(
        self, sink, message, broker, monkeypatch,
    ):
        # Force build_event to raise - simulates an internal bug.
        from z4j_dramatiq.events import middleware as mw_mod

        def _boom(**_kw: Any) -> None:
            raise RuntimeError("synthetic mapper bug")

        monkeypatch.setattr(mw_mod, "build_event", _boom)
        mw = Z4JMiddleware(sink=sink, redaction=RedactionEngine())
        # All four hook entry points must swallow.
        mw.after_enqueue(broker, message, None)
        mw.before_process_message(broker, message)
        mw.after_process_message(broker, message, exception=None)
        mw.after_process_message(broker, message, exception=RuntimeError())
        # Sink received nothing because every emit failed cleanly.
        assert sink.events == []


class TestArgsKwargsNeverLeak:
    """SECURITY: raw args/kwargs must NEVER appear in emitted events."""

    def test_kwargs_secret_does_not_leak(
        self, middleware, sink, broker,
    ):
        from tests.unit.conftest import FakeMessage  # local fake
        msg = FakeMessage(
            message_id="msg-secret",
            kwargs={"api_key": "sk_live_NEVER_LEAK"},
        )
        middleware.before_process_message(broker, msg)
        flat = repr(sink.events[-1].data)
        assert "sk_live_NEVER_LEAK" not in flat

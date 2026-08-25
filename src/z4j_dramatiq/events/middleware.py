"""The Z4JMiddleware that captures Dramatiq lifecycle events.

Dramatiq's middleware chain is its blessed observability hook -
the same surface ``CurrentMessage``, ``Retries``, and ``Abortable``
use. Adding our middleware to the user's broker is the *correct*
integration path; no monkey-patching needed.

Lifecycle hooks we use:

- ``after_enqueue`` → ``task.received``
- ``before_process_message`` → ``task.started``
- ``after_process_message`` → ``task.succeeded`` if ``exception is None``;
  otherwise ``task.retried`` when Retries re-enqueued it, or ``task.failed``
  when the attempt exhausted its retries

Hooks we deliberately do NOT use:

- ``before_enqueue`` - fires before message_id is finalized; we'd
  miss ``task_id`` correlation.
- ``before_worker_boot`` / ``after_worker_boot`` - worker lifecycle,
  surfaced via the heartbeat instead.

Safety properties:

- Every mapped emit runs through an ``Exception`` boundary. A build or sink
  failure is logged and dropped; process-lifecycle exceptions still propagate.
- The middleware does not deserialize args/kwargs. Mapper drops
  them by design.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from z4j_core.models import Event, EventKind
from z4j_core.redaction.engine import RedactionEngine

from z4j_dramatiq.events.mapper import build_event

logger = logging.getLogger("z4j.adapter.dramatiq.middleware")


# Subclass dramatiq.Middleware lazily - many test environments
# do not have the ``dramatiq`` package importable. The engine
# adapter only constructs Z4JMiddleware when it is wiring into a
# real broker; tests can construct a duck-typed instance directly
# (see TestZ4JMiddleware in tests/unit/test_middleware.py).
def _resolve_base() -> type:
    try:
        from dramatiq.middleware import Middleware  # type: ignore[import-not-found]
    except ImportError:
        # Fallback so the class is importable even without dramatiq -
        # callers that try to register the middleware on a real broker
        # will get a clear ImportError when they construct it.
        class _StubBase:
            pass

        return _StubBase
    return Middleware


_Base = _resolve_base()


class Z4JMiddleware(_Base):  # type: ignore[misc, valid-type]
    """Attempt mapped z4j lifecycle events from Dramatiq middleware hooks.

    Event construction or sink failures are logged and dropped. The engine's
    downstream event queue is also bounded, so this is observational capture,
    not a guarantee that every message reaches the brain.

    Args:
        sink: Callable invoked with each :class:`Event` produced.
              The engine adapter wires this to its internal asyncio
              queue via ``call_soon_threadsafe``.
        redaction: Shared :class:`RedactionEngine`. The agent
                   runtime's own engine is passed in production.
    """

    def __init__(
        self,
        *,
        sink: Callable[[Event], None],
        redaction: RedactionEngine,
    ) -> None:
        self._sink = sink
        self._redaction = redaction
        # The Dramatiq base class historically does not have an
        # __init__ but recent versions accept arbitrary kwargs; do
        # not call super().__init__ - we have nothing to forward.

    # ------------------------------------------------------------------
    # Dramatiq middleware hooks
    # ------------------------------------------------------------------

    def after_enqueue(
        self,
        broker: Any,
        message: Any,
        delay: int | None,
    ) -> None:
        """Message has been written to the broker - emit ``task.received``."""
        self._safe_emit(
            EventKind.TASK_RECEIVED,
            message,
            broker=broker,
            use_message_timestamp=True,
        )

    def before_process_message(self, broker: Any, message: Any) -> None:
        """Worker has fetched the message and is about to run it."""
        self._safe_emit(EventKind.TASK_STARTED, message, broker=broker)

    def after_process_message(
        self,
        broker: Any,
        message: Any,
        *,
        result: Any = None,
        exception: BaseException | None = None,
    ) -> None:
        """Worker has finished - success or failure based on ``exception``."""
        if exception is None:
            kind = EventKind.TASK_SUCCEEDED
        elif not bool(getattr(message, "failed", False)) and "requeue_timestamp" in (
            getattr(message, "options", None) or {}
        ):
            # Retries has already run because Z4J is inserted before it and
            # Dramatiq invokes after-hooks in reverse. The requeue timestamp is
            # Retries' proof that replacement work was enqueued.
            kind = EventKind.TASK_RETRIED
        else:
            kind = EventKind.TASK_FAILED
        self._safe_emit(kind, message, broker=broker, exception=exception)

    # ------------------------------------------------------------------
    # Internal: build + sink without ever raising into Dramatiq
    # ------------------------------------------------------------------

    def _safe_emit(
        self,
        kind: EventKind,
        message: Any,
        *,
        broker: Any,
        exception: BaseException | None = None,
        use_message_timestamp: bool = False,
    ) -> None:
        sink = self._sink
        redaction = self._redaction
        try:
            event = build_event(
                kind=kind,
                message=message,
                redaction=redaction,
                actor=_resolve_actor(broker, message),
                exception=exception,
                use_message_timestamp=use_message_timestamp,
            )
            sink(event)
        except Exception:
            logger.exception(
                "z4j dramatiq: middleware emit raised - dropping event "
                "(this is a bug in z4j, NOT in your task code)",
            )


def _resolve_actor(broker: Any, message: Any) -> Any | None:
    """Best-effort lookup of the actor for ``message``.

    Dramatiq stores every registered actor on the broker. If the
    user passes us a Message we can resolve back to the actor (and therefore
    to ``@z4j_meta`` decorations) on the broker that invoked the hook. Failures
    are silent - we just drop the meta lookup.
    """
    try:
        actor_name = getattr(message, "actor_name", None)
        if actor_name is None:
            return None
        return broker.get_actor(actor_name)
    except Exception:
        return None


__all__ = ["Z4JMiddleware"]
